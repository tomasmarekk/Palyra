//! Managed Codex app-server bridge for the AgentHarnessV2 runtime.
//!
//! The bridge translates bounded Palyra runtime frames to Codex JSON-RPC while
//! keeping approvals, dynamic tools, cancellation, and cleanup under host authority.

use std::{
    collections::{BTreeMap, VecDeque},
    env, fs,
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use palyra_common::redaction::redact_diagnostic_text;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    agent_harness::{AgentHarnessCapabilities, AgentHarnessDescriptor},
    agent_harness_v2::AgentHarnessSteerOutcomeV2,
    managed_runtime::{ManagedRuntimeDescriptor, RuntimeTransportError},
};

pub const CODEX_MANAGED_RUNTIME_PROTOCOL_VERSION: &str = "palyra.codex-app-server-bridge.v1";
pub const CODEX_MANAGED_RUNTIME_ID: &str = "codex_app_server";

const INTERNAL_BRIDGE_ARGUMENT: &str = "--internal-codex-app-server-bridge";
const CODEX_EXECUTABLE_ENV: &str = "PALYRA_INTERNAL_CODEX_EXECUTABLE";
const CODEX_ARGS_ENV: &str = "PALYRA_INTERNAL_CODEX_ARGS_JSON";
const CODEX_ENV_ENV: &str = "PALYRA_INTERNAL_CODEX_ENV_JSON";
const CODEX_VERSION_POLICY_ENV: &str = "PALYRA_INTERNAL_CODEX_VERSION_POLICY_JSON";
const RUNTIME_PROTOCOL_ENV: &str = "PALYRA_RUNTIME_PROTOCOL_VERSION";
const RUNTIME_CAPABILITY_ENV: &str = "PALYRA_RUNTIME_CAPABILITY_DIGEST";
const RUNTIME_NONCE_ENV: &str = "PALYRA_RUNTIME_NONCE";
const RUNTIME_GENERATION_ENV: &str = "PALYRA_RUNTIME_GENERATION";
const RUNTIME_RESUME_ENV: &str = "PALYRA_RUNTIME_RESUME_METADATA_JSON";
const MAX_BRIDGE_FRAME_BYTES: usize = 1024 * 1024;
const MAX_BRIDGE_TEXT_BYTES: usize = 1024 * 1024;
const MAX_CODEX_STDERR_BYTES: usize = 16 * 1024;
const MAX_CODEX_ARGS: usize = 64;
const MAX_CODEX_ENV: usize = 64;
const MAX_CODEX_EVENTS: u64 = 4_096;
const CODEX_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const BRIDGE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Version window accepted for a Codex app-server binary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexAppServerVersionPolicy {
    pub required_major: u64,
    pub minimum_minor: u64,
    pub maximum_minor_exclusive: u64,
}

impl Default for CodexAppServerVersionPolicy {
    fn default() -> Self {
        Self { required_major: 0, minimum_minor: 120, maximum_minor_exclusive: 1_000 }
    }
}

impl CodexAppServerVersionPolicy {
    fn accepts(self, major: u64, minor: u64) -> bool {
        major == self.required_major
            && minor >= self.minimum_minor
            && minor < self.maximum_minor_exclusive
    }

    fn validate(self) -> Result<(), RuntimeTransportError> {
        if self.minimum_minor >= self.maximum_minor_exclusive {
            return Err(RuntimeTransportError::InvalidDescriptor);
        }
        Ok(())
    }
}

/// Trusted launch plan for the managed Codex app-server bridge.
#[derive(Debug, Clone)]
pub struct ManagedCodexAppServerConfig {
    pub bridge_executable: PathBuf,
    pub codex_executable: PathBuf,
    pub codex_args: Vec<String>,
    pub codex_env: BTreeMap<String, String>,
    pub cwd: PathBuf,
    pub version_policy: CodexAppServerVersionPolicy,
}

impl ManagedCodexAppServerConfig {
    /// Resolves and validates the default local Codex app-server launch plan.
    ///
    /// # Errors
    /// Returns [`RuntimeTransportError::InvalidDescriptor`] when either executable,
    /// the working directory, arguments, environment, or version policy is unsafe.
    pub fn resolve_default(cwd: &Path) -> Result<Self, RuntimeTransportError> {
        let bridge_executable =
            env::current_exe().map_err(|_| RuntimeTransportError::InvalidDescriptor)?;
        let codex_executable = resolve_executable("codex")?;
        let mut codex_env = inherited_codex_environment();
        for key in ["CODEX_HOME", "CODEX_SQLITE_HOME", "RUST_LOG"] {
            if let Ok(value) = env::var(key) {
                codex_env.insert(key.to_owned(), value);
            }
        }
        let config = Self {
            bridge_executable,
            codex_executable,
            codex_args: vec!["app-server".to_owned(), "--stdio".to_owned()],
            codex_env,
            cwd: cwd.to_path_buf(),
            version_policy: CodexAppServerVersionPolicy::default(),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), RuntimeTransportError> {
        self.version_policy.validate()?;
        if !self.bridge_executable.is_absolute()
            || !self.bridge_executable.is_file()
            || !self.codex_executable.is_absolute()
            || !self.codex_executable.is_file()
            || !self.cwd.is_absolute()
            || !self.cwd.is_dir()
            || self.codex_args.len() > MAX_CODEX_ARGS
            || self.codex_args.iter().any(|argument| argument.len() > 4_096)
            || self.codex_env.len() > MAX_CODEX_ENV
            || self.codex_env.iter().any(|(key, value)| {
                key.trim().is_empty() || key.len() > 256 || value.len() > 32_768
            })
        {
            return Err(RuntimeTransportError::InvalidDescriptor);
        }
        Ok(())
    }
}

/// Durable, payload-free Codex runtime binding state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexRuntimeBindingV1 {
    pub schema_version: u32,
    pub session_id: String,
    pub generation: u64,
    pub thread_id: Option<String>,
    pub server_version: String,
    pub tool_catalog_epoch: u64,
    pub last_acknowledged_sequence: u64,
}

/// Host-owned dynamic-tool request forwarded by the Codex bridge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexToolBridgeEnvelope {
    pub request_id: Value,
    pub call_id: String,
    pub tool_name: String,
    pub arguments: Value,
    pub tool_catalog_epoch: u64,
    pub generation: u64,
}

/// Deterministic thread reconstruction decision after a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexResumeDecision {
    NativeThreadResume,
    ProjectSanitizedTranscript,
}

/// Result of steering an active Codex turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexSteerOutcome {
    Accepted { generation: u64 },
    Rejected { reason_code: String },
}

impl From<AgentHarnessSteerOutcomeV2> for CodexSteerOutcome {
    fn from(outcome: AgentHarnessSteerOutcomeV2) -> Self {
        match outcome {
            AgentHarnessSteerOutcomeV2::Accepted { generation } => Self::Accepted { generation },
            AgentHarnessSteerOutcomeV2::Rejected { reason_code } => Self::Rejected { reason_code },
        }
    }
}

/// Builds the reusable managed-runtime descriptor for Codex app-server.
///
/// # Errors
/// Returns [`RuntimeTransportError::InvalidDescriptor`] for an unsafe launch plan.
pub fn codex_managed_runtime_descriptor(
    config: &ManagedCodexAppServerConfig,
) -> Result<ManagedRuntimeDescriptor, RuntimeTransportError> {
    config.validate()?;
    let mut bridge_env = BTreeMap::new();
    bridge_env.insert(
        CODEX_EXECUTABLE_ENV.to_owned(),
        config.codex_executable.to_string_lossy().into_owned(),
    );
    bridge_env.insert(
        CODEX_ARGS_ENV.to_owned(),
        serde_json::to_string(&config.codex_args)
            .map_err(|_| RuntimeTransportError::InvalidDescriptor)?,
    );
    bridge_env.insert(
        CODEX_ENV_ENV.to_owned(),
        serde_json::to_string(&config.codex_env)
            .map_err(|_| RuntimeTransportError::InvalidDescriptor)?,
    );
    bridge_env.insert(
        CODEX_VERSION_POLICY_ENV.to_owned(),
        serde_json::to_string(&config.version_policy)
            .map_err(|_| RuntimeTransportError::InvalidDescriptor)?,
    );
    let capability_digest = codex_capability_digest();
    let descriptor = ManagedRuntimeDescriptor {
        runtime_id: CODEX_MANAGED_RUNTIME_ID.to_owned(),
        protocol_version: CODEX_MANAGED_RUNTIME_PROTOCOL_VERSION.to_owned(),
        capability_digest,
        executable: config.bridge_executable.clone(),
        args: vec![INTERNAL_BRIDGE_ARGUMENT.to_owned()],
        cwd: config.cwd.clone(),
        env: bridge_env,
        handshake_timeout: Duration::from_secs(45),
        command_timeout: Duration::from_secs(30),
        lease_duration: Duration::from_secs(6 * 60 * 60),
    };
    descriptor.validate()?;
    Ok(descriptor)
}

/// Descriptor advertised by the native managed Codex adapter.
#[must_use]
pub fn codex_agent_harness_descriptor() -> AgentHarnessDescriptor {
    AgentHarnessDescriptor::with_capabilities(
        CODEX_MANAGED_RUNTIME_ID,
        "Codex app-server harness",
        false,
        AgentHarnessCapabilities {
            steering: true,
            resume: true,
            compaction: true,
            dynamic_tools: true,
            approvals: true,
            computer_use: false,
            transcript_mirror: true,
        },
    )
}

/// Dispatches the hidden bridge mode before normal daemon initialization.
///
/// Normal daemon invocations return immediately. A matching hidden invocation
/// owns the process lifetime and never returns.
#[doc(hidden)]
pub fn dispatch_internal_codex_app_server_bridge() {
    if env::args().nth(1).as_deref() != Some(INTERNAL_BRIDGE_ARGUMENT) {
        return;
    }
    let exit_code = match run_codex_bridge() {
        Ok(()) => 0,
        Err(error) => {
            let safe = redact_diagnostic_text(error.to_string().as_str());
            eprintln!("codex app-server bridge failed: {safe}");
            70
        }
    };
    std::process::exit(exit_code);
}

#[derive(Debug)]
struct BridgeError {
    safe_message: String,
}

impl BridgeError {
    fn new(message: impl Into<String>) -> Self {
        Self { safe_message: message.into() }
    }
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.safe_message.as_str())
    }
}

impl std::error::Error for BridgeError {}

struct CodexChild {
    child: Child,
    stdin: ChildStdin,
    messages: Receiver<Result<Value, BridgeError>>,
    pending_messages: VecDeque<Value>,
}

impl CodexChild {
    fn send(&mut self, value: &Value) -> Result<(), BridgeError> {
        let encoded = serde_json::to_vec(value)
            .map_err(|_| BridgeError::new("failed to encode Codex JSON-RPC frame"))?;
        if encoded.len() > MAX_BRIDGE_FRAME_BYTES {
            return Err(BridgeError::new("Codex JSON-RPC frame exceeded the bridge limit"));
        }
        self.stdin
            .write_all(encoded.as_slice())
            .and_then(|()| self.stdin.write_all(b"\n"))
            .and_then(|()| self.stdin.flush())
            .map_err(|_| BridgeError::new("failed to write Codex JSON-RPC frame"))
    }

    fn receive_channel_timeout(&self, timeout: Duration) -> Result<Value, BridgeError> {
        match self.messages.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                Err(BridgeError::new("Codex app-server response timed out"))
            }
            Err(RecvTimeoutError::Disconnected) => {
                Err(BridgeError::new("Codex app-server response stream closed"))
            }
        }
    }

    fn poll_message(&mut self, timeout: Duration) -> Result<Option<Value>, BridgeError> {
        if let Some(message) = self.pending_messages.pop_front() {
            return Ok(Some(message));
        }
        match self.messages.recv_timeout(timeout) {
            Ok(result) => result.map(Some),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => {
                Err(BridgeError::new("Codex app-server response stream closed"))
            }
        }
    }

    fn request(
        &mut self,
        request_id: u64,
        method: &str,
        params: Value,
    ) -> Result<Value, BridgeError> {
        self.send(&json!({"id": request_id, "method": method, "params": params}))?;
        let mut pending_messages = VecDeque::new();
        loop {
            let message = self.receive_channel_timeout(CODEX_RPC_TIMEOUT)?;
            if message.get("id").and_then(Value::as_u64) != Some(request_id) {
                pending_messages.push_back(message);
                continue;
            }
            self.pending_messages.append(&mut pending_messages);
            if let Some(error) = message.get("error") {
                let code = error.get("code").and_then(Value::as_i64).unwrap_or_default();
                return Err(BridgeError::new(format!(
                    "Codex JSON-RPC request failed with code {code}"
                )));
            }
            return message
                .get("result")
                .cloned()
                .ok_or_else(|| BridgeError::new("Codex JSON-RPC response omitted its result"));
        }
    }
}

impl Drop for CodexChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ParentCommand {
    #[serde(rename = "type")]
    frame_type: String,
    #[serde(default)]
    command_id: String,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    method: String,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    deadline_unix_ms: i64,
}

struct ActiveCodexAttempt {
    command_id: String,
    generation: u64,
    thread_id: String,
    turn_id: String,
    final_message: String,
    tool_catalog_epoch: u64,
}

struct CodexBridge {
    generation: u64,
    sequence: u64,
    next_rpc_id: u64,
    server_version: String,
    resume_metadata: Option<Value>,
    codex: CodexChild,
    parent_commands: Receiver<Result<ParentCommand, BridgeError>>,
    active: Option<ActiveCodexAttempt>,
}

impl CodexBridge {
    fn run(&mut self) -> Result<(), BridgeError> {
        loop {
            match self.parent_commands.try_recv() {
                Ok(command) => {
                    if self.handle_parent_command(command?)? {
                        return Ok(());
                    }
                    continue;
                }
                Err(TryRecvError::Disconnected) => return Ok(()),
                Err(TryRecvError::Empty) => {}
            }
            match self.codex.poll_message(BRIDGE_POLL_INTERVAL) {
                Ok(Some(message)) => self.handle_codex_message(message)?,
                Ok(None) => {}
                Err(_) => {
                    self.terminalize_active(
                        "failed",
                        json!({
                            "reason_code": "codex_app_server.process_exited",
                            "safe_message": "Codex app-server exited before the active turn completed.",
                        }),
                    )?;
                    return Err(BridgeError::new("Codex app-server response stream closed"));
                }
            }
        }
    }

    fn handle_parent_command(&mut self, command: ParentCommand) -> Result<bool, BridgeError> {
        if command.generation != self.generation {
            return Err(BridgeError::new("Palyra parent sent a stale runtime generation"));
        }
        if command.frame_type != "close" && command.deadline_unix_ms <= now_unix_ms() {
            return Err(BridgeError::new("Palyra parent command deadline expired"));
        }
        match command.frame_type.as_str() {
            "close" => return Ok(true),
            "cancel" => {
                self.cancel_active(command.command_id.as_str(), command.generation)?;
            }
            "command" if command.method == "run_attempt" => {
                self.start_attempt(command)?;
            }
            "command" if command.method == "host_response" => {
                self.forward_host_response(&command.payload)?;
            }
            "command" if command.method == "steer" => {
                self.steer_active(&command)?;
            }
            "command" => {
                self.write_terminal(
                    command.command_id.as_str(),
                    command.generation,
                    "failed",
                    json!({
                        "reason_code": "codex_bridge.unsupported_command",
                        "safe_message": "The Codex bridge rejected an unsupported command.",
                    }),
                )?;
            }
            _ => return Err(BridgeError::new("Palyra parent sent an invalid bridge frame")),
        }
        Ok(false)
    }

    fn start_attempt(&mut self, command: ParentCommand) -> Result<(), BridgeError> {
        if command.generation != self.generation {
            return Err(BridgeError::new("Palyra parent sent a stale runtime generation"));
        }
        if self.active.is_some() {
            self.write_terminal(
                command.command_id.as_str(),
                command.generation,
                "failed",
                json!({
                    "reason_code": "codex_bridge.turn_already_active",
                    "safe_message": "Only one Codex turn may be active per managed bridge.",
                }),
            )?;
            return Ok(());
        }
        self.write_accepted(command.command_id.as_str(), command.generation)?;
        let model = required_payload_string(&command.payload, "model_id")?;
        let workspace_root =
            command.payload.get("workspace_root").and_then(Value::as_str).map(str::to_owned);
        let tool_catalog_epoch = command
            .payload
            .get("tool_catalog_epoch")
            .and_then(Value::as_u64)
            .ok_or_else(|| BridgeError::new("Codex attempt omitted tool catalog epoch"))?;
        let dynamic_tools =
            normalize_dynamic_tools(command.payload.get("tool_surface").unwrap_or(&Value::Null));
        let resume_thread_id = self
            .resume_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("codex_thread_id"))
            .and_then(Value::as_str)
            .filter(|value| valid_identifier(value))
            .map(str::to_owned);
        let (thread_id, resume_decision) = if let Some(thread_id) = resume_thread_id {
            let request_id = self.issue_rpc_id()?;
            let result =
                self.codex.request(request_id, "thread/resume", json!({"threadId": thread_id}))?;
            (
                required_pointer_string(&result, "/thread/id")?,
                CodexResumeDecision::NativeThreadResume,
            )
        } else {
            let request_id = self.issue_rpc_id()?;
            let result = self.codex.request(
                request_id,
                "thread/start",
                json!({
                    "model": model,
                    "cwd": workspace_root,
                    "approvalPolicy": "never",
                    "sandbox": "read-only",
                    "dynamicTools": dynamic_tools,
                    "ephemeral": true,
                }),
            )?;
            (
                required_pointer_string(&result, "/thread/id")?,
                CodexResumeDecision::ProjectSanitizedTranscript,
            )
        };
        self.write_event(
            command.command_id.as_str(),
            command.generation,
            "progress",
            json!({
                "completed_units": 1,
                "total_units": 2,
                "label": match resume_decision {
                    CodexResumeDecision::NativeThreadResume => "codex_thread_resumed",
                    CodexResumeDecision::ProjectSanitizedTranscript => {
                        "codex_thread_recreated_from_sanitized_transcript"
                    }
                },
                "server_version": self.server_version,
                "thread_id_sha256": sha256_hex(thread_id.as_bytes()),
            }),
        )?;
        let prompt = projected_prompt(&command.payload)?;
        let request_id = self.issue_rpc_id()?;
        let result = self.codex.request(
            request_id,
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt}],
                "model": model,
                "cwd": workspace_root,
                "approvalPolicy": "never",
                "sandboxPolicy": {"type": "readOnly", "networkAccess": false},
            }),
        )?;
        let turn_id = required_pointer_string(&result, "/turn/id")?;
        self.active = Some(ActiveCodexAttempt {
            command_id: command.command_id,
            generation: command.generation,
            thread_id,
            turn_id,
            final_message: String::new(),
            tool_catalog_epoch,
        });
        Ok(())
    }

    fn cancel_active(&mut self, command_id: &str, generation: u64) -> Result<(), BridgeError> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        if generation != active.generation || command_id != active.command_id {
            self.active = Some(active);
            return Err(BridgeError::new("Palyra parent sent a stale Codex cancellation"));
        }
        let request_id = self.issue_rpc_id()?;
        self.codex.send(&json!({
            "id": request_id,
            "method": "turn/interrupt",
            "params": {"threadId": active.thread_id, "turnId": active.turn_id},
        }))?;
        self.write_terminal(
            active.command_id.as_str(),
            active.generation,
            "cancelled",
            json!({"reason_code": "codex_app_server.turn_interrupted"}),
        )
    }

    fn steer_active(&mut self, command: &ParentCommand) -> Result<(), BridgeError> {
        let Some(active) = self.active.as_ref() else {
            return self.write_terminal(
                command.command_id.as_str(),
                command.generation,
                "failed",
                json!({
                    "reason_code": "codex_app_server.no_active_turn",
                    "safe_message": "Codex steering requires an active turn.",
                }),
            );
        };
        if active.generation != command.generation {
            return Err(BridgeError::new("Palyra parent sent stale Codex steering"));
        }
        let thread_id = active.thread_id.clone();
        let turn_id = active.turn_id.clone();
        let input = required_payload_string(&command.payload, "input")?;
        let request_id = self.issue_rpc_id()?;
        self.codex.send(&json!({
            "id": request_id,
            "method": "turn/steer",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "input": [{"type": "text", "text": input}],
            },
        }))
    }

    fn forward_host_response(&mut self, payload: &Value) -> Result<(), BridgeError> {
        let request_id = payload
            .get("request_id")
            .cloned()
            .ok_or_else(|| BridgeError::new("host response omitted Codex request id"))?;
        let result = payload
            .get("result")
            .cloned()
            .ok_or_else(|| BridgeError::new("host response omitted result"))?;
        self.codex.send(&json!({"id": request_id, "result": result}))?;
        let request_kind = payload.get("request_kind").and_then(Value::as_str).unwrap_or_default();
        let Some(active) = self.active.as_ref() else {
            return Ok(());
        };
        let command_id = active.command_id.clone();
        let generation = active.generation;
        match request_kind {
            "dynamic_tool" => self.write_event(
                command_id.as_str(),
                generation,
                "tool_outcome",
                json!({
                    "call_id": payload.get("call_id").cloned().unwrap_or(Value::Null),
                    "outcome": if result.get("success").and_then(Value::as_bool).unwrap_or(false) {
                        "completed"
                    } else {
                        "failed"
                    },
                }),
            ),
            "approval" => self.write_event(
                command_id.as_str(),
                generation,
                "approval_resolved",
                json!({
                    "approval_id": payload
                        .get("approval_id")
                        .cloned()
                        .unwrap_or(Value::Null),
                    "outcome": if result.get("decision").and_then(Value::as_str) == Some("accept") {
                        "approved"
                    } else {
                        "denied"
                    },
                }),
            ),
            _ => Ok(()),
        }
    }

    fn handle_codex_message(&mut self, message: Value) -> Result<(), BridgeError> {
        if message.get("id").is_some() && message.get("method").is_some() {
            return self.handle_codex_request(&message);
        }
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return Ok(());
        };
        match method {
            "item/agentMessage/delta" => {
                let text = message
                    .pointer("/params/delta")
                    .and_then(Value::as_str)
                    .ok_or_else(|| BridgeError::new("Codex text delta was malformed"))?
                    .to_owned();
                if let Some(active) = self.active.as_mut() {
                    if active.final_message.len().saturating_add(text.len())
                        <= MAX_BRIDGE_TEXT_BYTES
                    {
                        active.final_message.push_str(text.as_str());
                    }
                    let command_id = active.command_id.clone();
                    let generation = active.generation;
                    self.write_event(
                        command_id.as_str(),
                        generation,
                        "text_delta",
                        json!({"text": text}),
                    )?;
                }
            }
            "turn/completed" => {
                let status = message
                    .pointer("/params/turn/status")
                    .and_then(Value::as_str)
                    .unwrap_or("failed");
                let payload = match status {
                    "completed" => json!({}),
                    "interrupted" => {
                        json!({"reason_code": "codex_app_server.turn_interrupted"})
                    }
                    _ => json!({
                        "reason_code": "codex_app_server.turn_failed",
                        "safe_message": "Codex app-server reported a failed turn.",
                    }),
                };
                let outcome = match status {
                    "completed" => "completed",
                    "interrupted" => "cancelled",
                    _ => "failed",
                };
                self.terminalize_active(outcome, payload)?;
            }
            "thread/compacted" => {
                if let Some(active) = self.active.as_ref() {
                    let command_id = active.command_id.clone();
                    let generation = active.generation;
                    self.write_event(
                        command_id.as_str(),
                        generation,
                        "progress",
                        json!({
                            "completed_units": 1,
                            "total_units": 1,
                            "label": "codex_context_compacted",
                        }),
                    )?;
                }
            }
            _ => {
                if let Some(active) = self.active.as_ref() {
                    let command_id = active.command_id.clone();
                    let generation = active.generation;
                    self.write_event(
                        command_id.as_str(),
                        generation,
                        "progress",
                        json!({
                            "completed_units": 0,
                            "total_units": 1,
                            "label": "codex_unknown_event_ignored",
                            "method_sha256": sha256_hex(method.as_bytes()),
                        }),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn handle_codex_request(&mut self, message: &Value) -> Result<(), BridgeError> {
        let request_id = message
            .get("id")
            .cloned()
            .ok_or_else(|| BridgeError::new("Codex request omitted id"))?;
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| BridgeError::new("Codex request omitted method"))?;
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        let Some(active) = self.active.as_ref() else {
            return self.codex.send(&json!({
                "id": request_id,
                "error": {"code": -32000, "message": "No active Palyra turn"},
            }));
        };
        let command_id = active.command_id.clone();
        let generation = active.generation;
        let tool_catalog_epoch = active.tool_catalog_epoch;
        match method {
            "item/tool/call" => {
                let call_id = required_payload_string(&params, "callId")?;
                let tool_name = required_payload_string(&params, "tool")?;
                self.write_event(
                    command_id.as_str(),
                    generation,
                    "tool_proposed",
                    json!({
                        "request_id": request_id,
                        "request_kind": "dynamic_tool",
                        "call_id": call_id,
                        "tool_name": tool_name,
                        "arguments": params.get("arguments").cloned().unwrap_or(Value::Null),
                        "tool_catalog_epoch": tool_catalog_epoch,
                    }),
                )
            }
            "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval" => {
                let call_id = params
                    .get("itemId")
                    .or_else(|| params.get("turnId"))
                    .and_then(Value::as_str)
                    .unwrap_or("codex-approval")
                    .to_owned();
                let approval_id = params
                    .get("approvalId")
                    .and_then(Value::as_str)
                    .unwrap_or(call_id.as_str())
                    .to_owned();
                self.write_event(
                    command_id.as_str(),
                    generation,
                    "approval_required",
                    json!({
                        "request_id": request_id,
                        "request_kind": "approval",
                        "approval_method": method,
                        "call_id": call_id,
                        "approval_id": approval_id,
                        "request": params,
                    }),
                )
            }
            "item/tool/requestUserInput" => {
                let question_id = params
                    .get("itemId")
                    .or_else(|| params.get("turnId"))
                    .and_then(Value::as_str)
                    .unwrap_or("codex-side-question")
                    .to_owned();
                self.write_event(
                    command_id.as_str(),
                    generation,
                    "side_question",
                    json!({
                        "request_id": request_id,
                        "request_kind": "side_question",
                        "question_id": question_id,
                        "request": params,
                    }),
                )
            }
            _ => self.codex.send(&json!({
                "id": request_id,
                "error": {"code": -32601, "message": "Unsupported host-owned request"},
            })),
        }
    }

    fn terminalize_active(&mut self, outcome: &str, mut payload: Value) -> Result<(), BridgeError> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        if outcome == "completed" && !active.final_message.is_empty() {
            payload["final_message"] = Value::String(active.final_message);
        }
        payload["codex_thread_id"] = Value::String(active.thread_id);
        payload["server_version"] = Value::String(self.server_version.clone());
        self.write_terminal(active.command_id.as_str(), active.generation, outcome, payload)
    }

    fn issue_rpc_id(&mut self) -> Result<u64, BridgeError> {
        self.next_rpc_id = self
            .next_rpc_id
            .checked_add(1)
            .ok_or_else(|| BridgeError::new("Codex JSON-RPC request id exhausted"))?;
        Ok(self.next_rpc_id)
    }

    fn next_sequence(&mut self) -> Result<u64, BridgeError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| BridgeError::new("Codex bridge sequence exhausted"))?;
        if self.sequence > MAX_CODEX_EVENTS {
            return Err(BridgeError::new("Codex bridge event budget exhausted"));
        }
        Ok(self.sequence)
    }

    fn write_accepted(&mut self, command_id: &str, generation: u64) -> Result<(), BridgeError> {
        let sequence = self.next_sequence()?;
        write_parent_frame(&json!({
            "type": "accepted",
            "command_id": command_id,
            "generation": generation,
            "sequence": sequence,
        }))
    }

    fn write_event(
        &mut self,
        command_id: &str,
        generation: u64,
        method: &str,
        payload: Value,
    ) -> Result<(), BridgeError> {
        let sequence = self.next_sequence()?;
        write_parent_frame(&json!({
            "type": "event",
            "command_id": command_id,
            "generation": generation,
            "sequence": sequence,
            "method": method,
            "payload": payload,
        }))
    }

    fn write_terminal(
        &mut self,
        command_id: &str,
        generation: u64,
        outcome: &str,
        payload: Value,
    ) -> Result<(), BridgeError> {
        let sequence = self.next_sequence()?;
        write_parent_frame(&json!({
            "type": "terminal",
            "command_id": command_id,
            "generation": generation,
            "sequence": sequence,
            "outcome": outcome,
            "payload": payload,
        }))
    }
}

fn run_codex_bridge() -> Result<(), BridgeError> {
    let protocol_version = required_env(RUNTIME_PROTOCOL_ENV)?;
    let capability_digest = required_env(RUNTIME_CAPABILITY_ENV)?;
    let nonce = required_env(RUNTIME_NONCE_ENV)?;
    let generation = required_env(RUNTIME_GENERATION_ENV)?
        .parse::<u64>()
        .map_err(|_| BridgeError::new("runtime generation was invalid"))?;
    if protocol_version != CODEX_MANAGED_RUNTIME_PROTOCOL_VERSION
        || capability_digest != codex_capability_digest()
        || generation == 0
    {
        return Err(BridgeError::new("managed Codex bridge handshake metadata mismatched"));
    }
    let config = read_bridge_config()?;
    let mut codex = spawn_codex(&config)?;
    let initialize = codex.request(
        1,
        "initialize",
        json!({
            "clientInfo": {
                "name": "palyra",
                "title": "Palyra",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "capabilities": {"experimentalApi": true},
        }),
    )?;
    let user_agent = required_payload_string(&initialize, "userAgent")?;
    let (major, minor, patch) = parse_codex_version(user_agent.as_str())
        .ok_or_else(|| BridgeError::new("Codex app-server user agent omitted a trusted version"))?;
    if !config.version_policy.accepts(major, minor) {
        return Err(BridgeError::new("Codex app-server version is outside the trusted window"));
    }
    codex.send(&json!({"method": "initialized", "params": {}}))?;
    write_parent_frame(&json!({
        "type": "hello",
        "protocol_version": protocol_version,
        "capability_digest": capability_digest,
        "nonce": nonce,
        "generation": generation,
    }))?;
    let parent_commands = spawn_parent_reader()?;
    let resume_metadata =
        env::var(RUNTIME_RESUME_ENV).ok().and_then(|value| serde_json::from_str(&value).ok());
    CodexBridge {
        generation,
        sequence: 0,
        next_rpc_id: 1,
        server_version: format!("{major}.{minor}.{patch}"),
        resume_metadata,
        codex,
        parent_commands,
        active: None,
    }
    .run()
}

struct BridgeConfig {
    codex_executable: PathBuf,
    codex_args: Vec<String>,
    codex_env: BTreeMap<String, String>,
    version_policy: CodexAppServerVersionPolicy,
}

fn read_bridge_config() -> Result<BridgeConfig, BridgeError> {
    let codex_executable = PathBuf::from(required_env(CODEX_EXECUTABLE_ENV)?);
    let codex_executable = canonical_executable(codex_executable.as_path())
        .map_err(|_| BridgeError::new("Codex executable was not a trusted absolute file"))?;
    let codex_args = serde_json::from_str::<Vec<String>>(required_env(CODEX_ARGS_ENV)?.as_str())
        .map_err(|_| BridgeError::new("Codex argument plan was malformed"))?;
    let codex_env =
        serde_json::from_str::<BTreeMap<String, String>>(required_env(CODEX_ENV_ENV)?.as_str())
            .map_err(|_| BridgeError::new("Codex environment plan was malformed"))?;
    let version_policy = serde_json::from_str::<CodexAppServerVersionPolicy>(
        required_env(CODEX_VERSION_POLICY_ENV)?.as_str(),
    )
    .map_err(|_| BridgeError::new("Codex version policy was malformed"))?;
    if codex_args.len() > MAX_CODEX_ARGS
        || codex_args.iter().any(|argument| argument.len() > 4_096)
        || codex_env.len() > MAX_CODEX_ENV
        || !version_policy.accepts(version_policy.required_major, version_policy.minimum_minor)
    {
        return Err(BridgeError::new("Codex bridge launch plan violated bounded policy"));
    }
    Ok(BridgeConfig { codex_executable, codex_args, codex_env, version_policy })
}

fn spawn_codex(config: &BridgeConfig) -> Result<CodexChild, BridgeError> {
    let mut child = Command::new(config.codex_executable.as_path())
        .args(config.codex_args.iter())
        .env_clear()
        .envs(config.codex_env.iter())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| BridgeError::new("failed to spawn trusted Codex app-server executable"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| BridgeError::new("Codex app-server stdin was unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| BridgeError::new("Codex app-server stdout was unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| BridgeError::new("Codex app-server stderr was unavailable"))?;
    let (sender, messages) = mpsc::sync_channel(256);
    spawn_codex_stdout_reader(stdout, sender)?;
    spawn_codex_stderr_reader(stderr)?;
    Ok(CodexChild { child, stdin, messages, pending_messages: VecDeque::new() })
}

fn spawn_codex_stdout_reader(
    stdout: impl Read + Send + 'static,
    sender: SyncSender<Result<Value, BridgeError>>,
) -> Result<(), BridgeError> {
    thread::Builder::new()
        .name("palyra-codex-app-server-stdout".to_owned())
        .spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                match read_bounded_line(&mut reader, MAX_BRIDGE_FRAME_BYTES) {
                    Ok(Some(line)) => {
                        let message = serde_json::from_slice::<Value>(&line)
                            .map_err(|_| BridgeError::new("Codex emitted malformed JSON-RPC"));
                        if sender.send(message).is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(error) => {
                        let _ = sender.send(Err(error));
                        return;
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(|_| BridgeError::new("failed to start Codex stdout reader"))
}

fn spawn_codex_stderr_reader(stderr: impl Read + Send + 'static) -> Result<(), BridgeError> {
    thread::Builder::new()
        .name("palyra-codex-app-server-stderr".to_owned())
        .spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut retained = 0_usize;
            loop {
                let Ok(Some(line)) = read_bounded_line(&mut reader, MAX_BRIDGE_FRAME_BYTES) else {
                    return;
                };
                if retained >= MAX_CODEX_STDERR_BYTES {
                    continue;
                }
                let line = String::from_utf8_lossy(&line);
                let safe = redact_diagnostic_text(line.as_ref());
                let remaining = MAX_CODEX_STDERR_BYTES.saturating_sub(retained);
                let safe = truncate_utf8(safe.as_str(), remaining);
                retained = retained.saturating_add(safe.len());
                let _ = writeln!(io::stderr(), "{safe}");
            }
        })
        .map(|_| ())
        .map_err(|_| BridgeError::new("failed to start Codex stderr reader"))
}

fn spawn_parent_reader() -> Result<Receiver<Result<ParentCommand, BridgeError>>, BridgeError> {
    let (sender, receiver) = mpsc::sync_channel(72);
    thread::Builder::new()
        .name("palyra-codex-bridge-parent".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            let mut reader = stdin.lock();
            loop {
                match read_bounded_line(&mut reader, MAX_BRIDGE_FRAME_BYTES) {
                    Ok(Some(line)) => {
                        let command = serde_json::from_slice::<ParentCommand>(&line)
                            .map_err(|_| BridgeError::new("Palyra parent frame was malformed"));
                        if sender.send(command).is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(error) => {
                        let _ = sender.send(Err(error));
                        return;
                    }
                }
            }
        })
        .map(|_| receiver)
        .map_err(|_| BridgeError::new("failed to start Palyra parent reader"))
}

fn normalize_dynamic_tools(tool_surface: &Value) -> Vec<Value> {
    let candidates =
        tool_surface.as_array().or_else(|| tool_surface.get("tools").and_then(Value::as_array));
    candidates
        .into_iter()
        .flatten()
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?;
            if !valid_identifier(name) {
                return None;
            }
            let description = tool
                .get("description")
                .and_then(Value::as_str)
                .map(|value| truncate_utf8(value, 4_096))
                .unwrap_or("Palyra host-owned tool");
            let input_schema = tool
                .get("inputSchema")
                .or_else(|| tool.get("input_schema"))
                .cloned()
                .unwrap_or_else(|| json!({"type": "object"}));
            Some(json!({
                "type": "function",
                "name": name,
                "description": description,
                "inputSchema": input_schema,
                "deferLoading": false,
            }))
        })
        .take(256)
        .collect()
}

fn projected_prompt(payload: &Value) -> Result<String, BridgeError> {
    let transcript = payload
        .get("sanitized_transcript")
        .ok_or_else(|| BridgeError::new("Codex attempt omitted sanitized transcript"))?;
    let encoded = serde_json::to_string(transcript)
        .map_err(|_| BridgeError::new("sanitized transcript could not be projected"))?;
    let prompt = format!(
        "Continue this Palyra-hosted run. Treat the following as a host-sanitized transcript \
         projection; all tools and approvals remain host-owned.\n\n{encoded}"
    );
    if prompt.len() > MAX_BRIDGE_TEXT_BYTES {
        return Err(BridgeError::new("sanitized transcript exceeded the Codex prompt limit"));
    }
    Ok(prompt)
}

fn write_parent_frame(frame: &Value) -> Result<(), BridgeError> {
    let encoded = serde_json::to_vec(frame)
        .map_err(|_| BridgeError::new("failed to encode Palyra bridge frame"))?;
    if encoded.len() > MAX_BRIDGE_FRAME_BYTES {
        return Err(BridgeError::new("Palyra bridge frame exceeded its byte limit"));
    }
    io::stdout()
        .write_all(encoded.as_slice())
        .and_then(|()| io::stdout().write_all(b"\n"))
        .and_then(|()| io::stdout().flush())
        .map_err(|_| BridgeError::new("failed to write Palyra bridge frame"))
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, BridgeError> {
    let mut line = Vec::new();
    let read = reader
        .take(u64::try_from(max_bytes.saturating_add(1)).unwrap_or(u64::MAX))
        .read_until(b'\n', &mut line)
        .map_err(|_| BridgeError::new("bridge frame read failed"))?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > max_bytes || !line.ends_with(b"\n") {
        return Err(BridgeError::new("bridge frame violated bounded JSONL framing"));
    }
    Ok(Some(line))
}

fn required_env(key: &str) -> Result<String, BridgeError> {
    env::var(key).map_err(|_| BridgeError::new(format!("missing required bridge setting {key}")))
}

fn required_payload_string(payload: &Value, key: &str) -> Result<String, BridgeError> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= MAX_BRIDGE_TEXT_BYTES)
        .map(str::to_owned)
        .ok_or_else(|| BridgeError::new(format!("bridge payload omitted valid {key}")))
}

fn required_pointer_string(payload: &Value, pointer: &str) -> Result<String, BridgeError> {
    payload
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| valid_identifier(value))
        .map(str::to_owned)
        .ok_or_else(|| BridgeError::new("Codex response omitted a bounded identifier"))
}

fn parse_codex_version(user_agent: &str) -> Option<(u64, u64, u64)> {
    let marker = user_agent.find(|character: char| character.is_ascii_digit())?;
    let version = user_agent.get(marker..)?.split_whitespace().next()?;
    let version =
        version.trim_matches(|character: char| !character.is_ascii_digit() && character != '.');
    let mut components = version.split('.');
    Some((
        components.next()?.parse().ok()?,
        components.next()?.parse().ok()?,
        components.next()?.parse().ok()?,
    ))
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 256
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._:-/".contains(character))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    &value[..end]
}

fn codex_capability_digest() -> String {
    sha256_hex(b"codex:v1|text|reasoning|usage|dynamic_tools|approvals|steer|resume|compaction")
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn inherited_codex_environment() -> BTreeMap<String, String> {
    let mut inherited = BTreeMap::new();
    for key in [
        "SYSTEMROOT",
        "WINDIR",
        "USERPROFILE",
        "HOME",
        "LOCALAPPDATA",
        "APPDATA",
        "TEMP",
        "TMP",
        "PATH",
        "PATHEXT",
    ] {
        if let Ok(value) = env::var(key) {
            inherited.insert(key.to_owned(), value);
        }
    }
    inherited
}

fn resolve_executable(name: &str) -> Result<PathBuf, RuntimeTransportError> {
    let candidate = PathBuf::from(name);
    if candidate.is_absolute() {
        return canonical_executable(candidate.as_path())
            .map_err(|_| RuntimeTransportError::InvalidDescriptor);
    }
    let path = env::var_os("PATH").ok_or(RuntimeTransportError::InvalidDescriptor)?;
    #[cfg(windows)]
    let extensions = env::var_os("PATHEXT")
        .map(|value| {
            value
                .to_string_lossy()
                .split(';')
                .filter(|extension| !extension.trim().is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![".EXE".to_owned(), ".CMD".to_owned(), ".BAT".to_owned()]);
    #[cfg(not(windows))]
    let extensions = vec![String::new()];
    for root in env::split_paths(&path) {
        for extension in &extensions {
            let candidate = if extension.is_empty()
                || name.to_ascii_lowercase().ends_with(extension.to_ascii_lowercase().as_str())
            {
                root.join(name)
            } else {
                root.join(format!("{name}{extension}"))
            };
            if let Ok(resolved) = canonical_executable(candidate.as_path()) {
                return Ok(resolved);
            }
        }
    }
    Err(RuntimeTransportError::InvalidDescriptor)
}

fn canonical_executable(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() || !path.is_file() {
        return Err(io::Error::other("executable must be an absolute file"));
    }
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_file() {
        return Err(io::Error::other("canonical executable is not a file"));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_current_codex_user_agent_shapes() {
        assert_eq!(
            parse_codex_version(
                "Codex Desktop/0.145.0 (Windows 10.0.26100; x86_64) unknown (palyra; 0.1.0)"
            ),
            Some((0, 145, 0))
        );
        assert_eq!(parse_codex_version("codex-cli/0.146.1"), Some((0, 146, 1)));
        assert_eq!(parse_codex_version("codex-without-version"), None);
    }

    #[test]
    fn tool_projection_accepts_only_bounded_function_descriptors() {
        let tools = normalize_dynamic_tools(&json!({
            "tools": [
                {
                    "name": "palyra.fs.read",
                    "description": "Read a host-approved file.",
                    "input_schema": {"type": "object"}
                },
                {"name": "unsafe name", "description": "ignored"}
            ]
        }));
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["name"], "palyra.fs.read");
    }

    #[test]
    fn harness_descriptor_advertises_full_managed_capabilities() {
        let descriptor = codex_agent_harness_descriptor();
        assert_eq!(
            descriptor.contract_version,
            super::super::agent_harness::AGENT_HARNESS_CONTRACT_VERSION_V2
        );
        assert!(descriptor.capabilities.steering);
        assert!(descriptor.capabilities.resume);
        assert!(descriptor.capabilities.dynamic_tools);
        assert!(descriptor.capabilities.approvals);
        assert!(!descriptor.capabilities.computer_use);
    }
}
