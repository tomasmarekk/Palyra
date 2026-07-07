//! Codex app-server harness adapter contracts.
//!
//! The adapter is intentionally only a harness boundary. Palyra still owns
//! provider routing, credentials, approvals, workspace writes, sandbox policy,
//! tool execution, and journal projection.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use palyra_common::{
    redaction::{is_sensitive_key, redact_diagnostic_text},
    runtime_contracts::AgentHarnessCallbackKind,
};
use serde::{Deserialize, Serialize};

use crate::application::agent_harness::{
    AgentHarness, AgentHarnessDescriptor, AgentHarnessRunOutcome, AgentHarnessSupportDecision,
    AgentHarnessSupportRequest, PreparedAgentAttempt,
};

pub const CODEX_APP_SERVER_HARNESS_ID: &str = "codex_app_server";
const CODEX_APP_SERVER_PROTOCOL_VERSION: &str = "codex-app-server.v1";
const DEFAULT_STDERR_TAIL_BYTES: usize = 8 * 1024;

/// Process launch policy for a Codex app-server stdio JSON-RPC child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexAppServerProcessConfig {
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub expected_protocol_version: String,
    pub stderr_tail_bytes: usize,
}

impl Default for CodexAppServerProcessConfig {
    fn default() -> Self {
        Self {
            command: "codex".to_owned(),
            args: vec!["app-server".to_owned(), "--stdio".to_owned()],
            env: BTreeMap::new(),
            expected_protocol_version: CODEX_APP_SERVER_PROTOCOL_VERSION.to_owned(),
            stderr_tail_bytes: DEFAULT_STDERR_TAIL_BYTES,
        }
    }
}

/// Redacted process plan handed to the host process supervisor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexAppServerProcessPlan {
    pub command_label: String,
    pub args: Vec<String>,
    pub env_keys: Vec<String>,
    pub expected_protocol_version: String,
    pub transport: String,
    pub kill_on_drop: bool,
    pub stderr_tail_bytes: usize,
}

/// Builds the process-supervisor plan without exposing secret env values.
#[must_use]
pub fn codex_app_server_process_plan(
    config: &CodexAppServerProcessConfig,
) -> CodexAppServerProcessPlan {
    CodexAppServerProcessPlan {
        command_label: command_label(config.command.as_str()),
        args: config.args.clone(),
        env_keys: sanitized_env_keys(&config.env),
        expected_protocol_version: config.expected_protocol_version.clone(),
        transport: "stdio_json_rpc".to_owned(),
        kill_on_drop: true,
        stderr_tail_bytes: config.stderr_tail_bytes,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexAppServerInitializeResponse {
    pub protocol_version: String,
    pub server_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexThreadStartRequest {
    pub run_id: String,
    pub session_id: String,
    pub trace_context: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexThreadStartResponse {
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexTurnStartRequest {
    pub thread_id: String,
    pub run_id: String,
    pub model_id: String,
    pub workspace_root_present: bool,
    pub tool_surface_present: bool,
    pub sandbox: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexTurnStartResponse {
    pub turn_id: String,
    pub final_message: Option<String>,
}

/// Redacted diagnostics emitted by the adapter boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodexAppServerDiagnostic {
    pub status: String,
    pub reason_code: String,
    pub safe_message: String,
    pub command_label: String,
    pub stderr_tail_redacted: Option<String>,
    pub json_rpc_error_code: Option<i64>,
    pub env_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexAppServerError {
    pub reason_code: String,
    pub safe_message: String,
    pub stderr_tail: Option<String>,
    pub json_rpc_error_code: Option<i64>,
}

impl CodexAppServerError {
    pub fn process_crashed(stderr_tail: impl Into<String>) -> Self {
        Self {
            reason_code: "codex_app_server.process_crashed".to_owned(),
            safe_message: "Codex app-server process exited before the request completed."
                .to_owned(),
            stderr_tail: Some(stderr_tail.into()),
            json_rpc_error_code: None,
        }
    }

    pub fn json_rpc(code: i64, message: impl Into<String>) -> Self {
        Self {
            reason_code: "codex_app_server.json_rpc_error".to_owned(),
            safe_message: message.into(),
            stderr_tail: None,
            json_rpc_error_code: Some(code),
        }
    }
}

/// Narrow JSON-RPC transport used by the harness adapter.
pub trait CodexAppServerRpc: Send + Sync {
    fn initialize(
        &self,
        expected_protocol_version: &str,
    ) -> Result<CodexAppServerInitializeResponse, CodexAppServerError>;

    fn start_thread(
        &self,
        request: CodexThreadStartRequest,
    ) -> Result<CodexThreadStartResponse, CodexAppServerError>;

    fn start_turn(
        &self,
        request: CodexTurnStartRequest,
    ) -> Result<CodexTurnStartResponse, CodexAppServerError>;

    fn cancel_turn(&self, turn_id: &str) -> Result<(), CodexAppServerError>;
}

/// Native Codex app-server adapter registered as an agent harness.
pub struct CodexAppServerHarnessAdapter<Rpc> {
    descriptor: AgentHarnessDescriptor,
    config: CodexAppServerProcessConfig,
    rpc: Arc<Rpc>,
    diagnostics: Mutex<Vec<CodexAppServerDiagnostic>>,
}

impl<Rpc> CodexAppServerHarnessAdapter<Rpc>
where
    Rpc: CodexAppServerRpc,
{
    #[must_use]
    pub fn new(config: CodexAppServerProcessConfig, rpc: Arc<Rpc>) -> Self {
        Self {
            descriptor: AgentHarnessDescriptor::new(
                CODEX_APP_SERVER_HARNESS_ID,
                "Codex app-server harness",
                false,
            ),
            config,
            rpc,
            diagnostics: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn process_plan(&self) -> CodexAppServerProcessPlan {
        codex_app_server_process_plan(&self.config)
    }

    #[must_use]
    pub fn diagnostics(&self) -> Vec<CodexAppServerDiagnostic> {
        self.diagnostics.lock().map_or_else(|_| Vec::new(), |guard| guard.clone())
    }

    fn run_codex_attempt(
        &self,
        attempt: PreparedAgentAttempt<'_>,
    ) -> Result<CodexTurnStartResponse, CodexAppServerError> {
        if attempt.cancellation.is_cancelled() {
            return Ok(CodexTurnStartResponse {
                turn_id: "cancelled-before-start".to_owned(),
                final_message: None,
            });
        }

        let initialize = self.rpc.initialize(self.config.expected_protocol_version.as_str())?;
        if initialize.protocol_version != self.config.expected_protocol_version {
            return Err(CodexAppServerError {
                reason_code: "codex_app_server.version_mismatch".to_owned(),
                safe_message: format!(
                    "Codex app-server protocol version mismatch: expected {}, got {}.",
                    self.config.expected_protocol_version, initialize.protocol_version
                ),
                stderr_tail: None,
                json_rpc_error_code: None,
            });
        }

        let thread = self.rpc.start_thread(CodexThreadStartRequest {
            run_id: redact_diagnostic_text(attempt.run_id),
            session_id: redact_diagnostic_text(attempt.session_id),
            trace_context: redact_diagnostic_text(attempt.trace_context),
        })?;
        let turn = self.rpc.start_turn(CodexTurnStartRequest {
            thread_id: thread.thread_id,
            run_id: redact_diagnostic_text(attempt.run_id),
            model_id: attempt.model_id.to_owned(),
            workspace_root_present: attempt.workspace_root.is_some(),
            tool_surface_present: !attempt.tool_surface.is_null(),
            sandbox: attempt.sandbox.to_owned(),
        })?;
        if attempt.cancellation.is_cancelled() {
            let _ = self.rpc.cancel_turn(turn.turn_id.as_str());
        }
        Ok(turn)
    }

    fn record_diagnostic(&self, status: &str, error: &CodexAppServerError) {
        let diagnostic = CodexAppServerDiagnostic {
            status: status.to_owned(),
            reason_code: error.reason_code.clone(),
            safe_message: redact_diagnostic_text(error.safe_message.as_str()),
            command_label: command_label(self.config.command.as_str()),
            stderr_tail_redacted: error
                .stderr_tail
                .as_deref()
                .map(|tail| bounded_stderr_tail(tail, self.config.stderr_tail_bytes)),
            json_rpc_error_code: error.json_rpc_error_code,
            env_keys: sanitized_env_keys(&self.config.env),
        };
        if let Ok(mut diagnostics) = self.diagnostics.lock() {
            diagnostics.push(diagnostic);
        }
    }
}

impl<Rpc> AgentHarness for CodexAppServerHarnessAdapter<Rpc>
where
    Rpc: CodexAppServerRpc + 'static,
{
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    fn supports(&self, request: &AgentHarnessSupportRequest<'_>) -> AgentHarnessSupportDecision {
        if request.explicit_harness_id.is_some_and(|id| id != self.descriptor.id.as_str()) {
            return AgentHarnessSupportDecision::declined("codex_app_server.explicit_id_mismatch");
        }
        AgentHarnessSupportDecision::supported("codex_app_server.host_owned_adapter")
    }

    fn run_attempt(&self, attempt: PreparedAgentAttempt<'_>) -> AgentHarnessRunOutcome {
        if attempt.cancellation.is_cancelled() {
            return AgentHarnessRunOutcome {
                status: "cancelled".to_owned(),
                emitted_callbacks: vec![AgentHarnessCallbackKind::LifecycleEvent],
                final_message: None,
            };
        }

        match self.run_codex_attempt(attempt) {
            Ok(turn) => AgentHarnessRunOutcome {
                status: "completed".to_owned(),
                emitted_callbacks: vec![
                    AgentHarnessCallbackKind::ModelTurnStarted,
                    AgentHarnessCallbackKind::FinalOutcome,
                ],
                final_message: turn.final_message,
            },
            Err(error) if error.reason_code == "codex_app_server.version_mismatch" => {
                self.record_diagnostic("failed", &error);
                AgentHarnessRunOutcome {
                    status: "deterministic_failure".to_owned(),
                    emitted_callbacks: vec![AgentHarnessCallbackKind::LifecycleEvent],
                    final_message: None,
                }
            }
            Err(error) => {
                self.record_diagnostic("failed", &error);
                AgentHarnessRunOutcome {
                    status: "failed".to_owned(),
                    emitted_callbacks: vec![AgentHarnessCallbackKind::LifecycleEvent],
                    final_message: None,
                }
            }
        }
    }
}

fn command_label(command: &str) -> String {
    command
        .rsplit(['/', '\\'])
        .next()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("<codex-app-server>")
        .to_owned()
}

fn sanitized_env_keys(env: &BTreeMap<String, String>) -> Vec<String> {
    env.keys().filter(|key| !is_sensitive_key(key)).cloned().collect::<Vec<_>>()
}

fn bounded_stderr_tail(stderr: &str, limit_bytes: usize) -> String {
    let redacted = redact_diagnostic_text(stderr);
    if redacted.len() <= limit_bytes {
        return redacted;
    }
    let mut start = redacted.len().saturating_sub(limit_bytes);
    while !redacted.is_char_boundary(start) {
        start = start.saturating_add(1);
    }
    format!("...{}", &redacted[start..])
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, LazyLock, Mutex,
        },
    };

    use serde_json::{json, Value};

    use super::*;
    use crate::application::agent_harness::{
        AgentHarnessCancellation, PreparedAgentAttemptCallbacks,
    };

    #[derive(Debug)]
    struct FakeCodexServer {
        protocol_version: String,
        crash: AtomicBool,
        cancelled_turn: Mutex<Option<String>>,
    }

    impl FakeCodexServer {
        fn new(protocol_version: &str) -> Self {
            Self {
                protocol_version: protocol_version.to_owned(),
                crash: AtomicBool::new(false),
                cancelled_turn: Mutex::new(None),
            }
        }
    }

    impl CodexAppServerRpc for FakeCodexServer {
        fn initialize(
            &self,
            _expected_protocol_version: &str,
        ) -> Result<CodexAppServerInitializeResponse, CodexAppServerError> {
            if self.crash.load(Ordering::Relaxed) {
                return Err(CodexAppServerError::process_crashed(
                    "panic: token=super-secret-value",
                ));
            }
            Ok(CodexAppServerInitializeResponse {
                protocol_version: self.protocol_version.clone(),
                server_name: "fake-codex".to_owned(),
            })
        }

        fn start_thread(
            &self,
            _request: CodexThreadStartRequest,
        ) -> Result<CodexThreadStartResponse, CodexAppServerError> {
            Ok(CodexThreadStartResponse { thread_id: "thread-1".to_owned() })
        }

        fn start_turn(
            &self,
            _request: CodexTurnStartRequest,
        ) -> Result<CodexTurnStartResponse, CodexAppServerError> {
            Ok(CodexTurnStartResponse {
                turn_id: "turn-1".to_owned(),
                final_message: Some("done".to_owned()),
            })
        }

        fn cancel_turn(&self, turn_id: &str) -> Result<(), CodexAppServerError> {
            *self.cancelled_turn.lock().expect("cancel lock should be available") =
                Some(turn_id.to_owned());
            Ok(())
        }
    }

    fn prepared_attempt<'a>(cancellation: AgentHarnessCancellation) -> PreparedAgentAttempt<'a> {
        static AUTH: Value = Value::Null;
        static TRANSCRIPT: [Value; 0] = [];
        static TOOL_SURFACE: LazyLock<Value> = LazyLock::new(|| json!({"tools":[]}));
        static TOOL_POLICY: LazyLock<Value> = LazyLock::new(|| json!({"host_owned":true}));
        PreparedAgentAttempt {
            run_id: "run-1",
            session_id: "session-1",
            provider_id: "openai",
            model_id: "gpt-5.5-codex",
            auth_state_metadata: &AUTH,
            context_token_budget: 8_192,
            reasoning_policy: Some("default"),
            sanitized_transcript_view: &TRANSCRIPT,
            tool_surface: &TOOL_SURFACE,
            tool_policy: &TOOL_POLICY,
            workspace_root: Some(Path::new("workspace")),
            sandbox: "host_owned",
            trace_context: "trace?api_key=secret",
            callbacks: PreparedAgentAttemptCallbacks::host_controlled(),
            cancellation,
        }
    }

    #[test]
    fn fake_app_server_runs_thread_and_turn() {
        let rpc = Arc::new(FakeCodexServer::new(CODEX_APP_SERVER_PROTOCOL_VERSION));
        let adapter = CodexAppServerHarnessAdapter::new(
            CodexAppServerProcessConfig::default(),
            Arc::clone(&rpc),
        );

        let outcome = adapter.run_attempt(prepared_attempt(AgentHarnessCancellation::default()));

        assert_eq!(outcome.status, "completed");
        assert_eq!(outcome.final_message.as_deref(), Some("done"));
        assert!(outcome.emitted_callbacks.contains(&AgentHarnessCallbackKind::FinalOutcome));
    }

    #[test]
    fn process_crash_records_redacted_diagnostics() {
        let rpc = Arc::new(FakeCodexServer::new(CODEX_APP_SERVER_PROTOCOL_VERSION));
        rpc.crash.store(true, Ordering::Relaxed);
        let adapter = CodexAppServerHarnessAdapter::new(
            CodexAppServerProcessConfig::default(),
            Arc::clone(&rpc),
        );

        let outcome = adapter.run_attempt(prepared_attempt(AgentHarnessCancellation::default()));
        let diagnostics = adapter.diagnostics();

        assert_eq!(outcome.status, "failed");
        assert_eq!(diagnostics[0].reason_code, "codex_app_server.process_crashed");
        assert!(!diagnostics[0]
            .stderr_tail_redacted
            .as_deref()
            .unwrap_or_default()
            .contains("super-secret-value"));
    }

    #[test]
    fn version_mismatch_is_deterministic_failure() {
        let rpc = Arc::new(FakeCodexServer::new("codex-app-server.v0"));
        let adapter = CodexAppServerHarnessAdapter::new(
            CodexAppServerProcessConfig::default(),
            Arc::clone(&rpc),
        );

        let outcome = adapter.run_attempt(prepared_attempt(AgentHarnessCancellation::default()));
        let diagnostics = adapter.diagnostics();

        assert_eq!(outcome.status, "deterministic_failure");
        assert_eq!(diagnostics[0].reason_code, "codex_app_server.version_mismatch");
    }

    #[test]
    fn process_plan_filters_secret_environment_keys() {
        let config = CodexAppServerProcessConfig {
            env: BTreeMap::from([
                ("SAFE_FLAG".to_owned(), "1".to_owned()),
                ("OPENAI_API_KEY".to_owned(), "secret".to_owned()),
            ]),
            ..CodexAppServerProcessConfig::default()
        };

        let plan = codex_app_server_process_plan(&config);

        assert_eq!(plan.env_keys, vec!["SAFE_FLAG"]);
        assert_eq!(plan.transport, "stdio_json_rpc");
        assert!(plan.kill_on_drop);
    }

    #[test]
    fn cancellation_terminates_turn_through_rpc() {
        let rpc = Arc::new(FakeCodexServer::new(CODEX_APP_SERVER_PROTOCOL_VERSION));
        let adapter = CodexAppServerHarnessAdapter::new(
            CodexAppServerProcessConfig::default(),
            Arc::clone(&rpc),
        );
        let cancellation = AgentHarnessCancellation::default();
        cancellation.cancel();

        let outcome = adapter.run_attempt(prepared_attempt(cancellation));

        assert_eq!(outcome.status, "cancelled");
        assert!(rpc.cancelled_turn.lock().expect("cancel lock should be available").is_none());
    }
}
