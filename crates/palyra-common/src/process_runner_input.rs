//! Input parsing and normalization for the `palyra.process.run` tool.
//!
//! Models emit predictable argv mistakes — repeating the command as `args[0]` or putting
//! `--cwd` into `args` — so parsing normalizes those instead of failing the run. Accept/
//! reject behavior is exercised by `fuzz/fuzz_targets/process_runner_input_parser.rs`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const INTERPRETER_EXECUTABLE_DENYLIST: &[&str] = &[
    "bash",
    "sh",
    "zsh",
    "fish",
    "powershell",
    "pwsh",
    "cmd",
    "python",
    "python3",
    "node",
    "nodejs",
    "ruby",
    "perl",
    "deno",
];

/// Lifecycle policy for `background=true` process-runner invocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundLifetimeMode {
    /// Tie the process to the current agent run and stop it at terminal cleanup.
    #[default]
    RunOwned,
    /// Leave the process running after terminal cleanup until its bounded lifetime expires.
    Detached,
    /// Keep the process available for an external verifier, with the same bounded handoff as
    /// [`Detached`](Self::Detached).
    UntilVerifier,
}

impl BackgroundLifetimeMode {
    /// Returns the stable JSON spelling used in tool outputs and cleanup summaries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunOwned => "run_owned",
            Self::Detached => "detached",
            Self::UntilVerifier => "until_verifier",
        }
    }

    /// True when the process must not be registered for terminal run cleanup.
    #[must_use]
    pub const fn is_detached_handoff(self) -> bool {
        matches!(self, Self::Detached | Self::UntilVerifier)
    }
}

/// Stream selector for process readiness/watch pattern matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProcessWatchStream {
    /// Scan stdout and stderr tails.
    #[default]
    Both,
    /// Scan stdout only.
    Stdout,
    /// Scan stderr only.
    Stderr,
}

impl ProcessWatchStream {
    /// Returns the stable JSON label used in audit payloads.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Both => "both",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Bounded readiness/completion watch pattern requested for a background process.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessWatchPattern {
    pub name: String,
    pub pattern: String,
    #[serde(default)]
    pub stream: ProcessWatchStream,
    #[serde(default)]
    pub notify_once: bool,
}

/// Audit-safe mapping for model-facing process execution facades.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessRunnerFacadeMapping {
    pub original_tool_name: String,
    pub canonical_tool_name: String,
}

/// Canonical input payload for `palyra.process.run`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessRunnerToolInput {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub prepend_path: Vec<String>,
    #[serde(default)]
    pub requested_egress_hosts: Vec<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub background: bool,
    #[serde(default)]
    pub notify_on_complete: bool,
    #[serde(default)]
    pub watch_patterns: Vec<ProcessWatchPattern>,
    #[serde(default)]
    pub interactive: bool,
    #[serde(default)]
    pub stdin: bool,
    #[serde(default)]
    pub pty: bool,
    #[serde(default)]
    pub port_hints: Vec<u16>,
    #[serde(default)]
    pub lifetime_mode: BackgroundLifetimeMode,
    #[serde(default)]
    pub keep_running_after_run: bool,
    #[serde(default)]
    pub env_profile_id: Option<String>,
    #[serde(default)]
    pub elevated_intent: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub facade_mapping: Option<ProcessRunnerFacadeMapping>,
}

impl ProcessRunnerToolInput {
    /// Returns the effective background lifecycle requested by the input.
    ///
    /// `keep_running_after_run=true` is a compatibility alias for
    /// `lifetime_mode="detached"` so callers can use either shape.
    #[must_use]
    pub const fn effective_lifetime_mode(&self) -> BackgroundLifetimeMode {
        if self.keep_running_after_run {
            BackgroundLifetimeMode::Detached
        } else {
            self.lifetime_mode
        }
    }

    /// Returns whether the caller explicitly requested a stdin-capable
    /// background handle.
    #[must_use]
    pub const fn stdin_requested(&self) -> bool {
        self.stdin || self.interactive
    }
}

/// Parse failure for a `palyra.process.run` payload (malformed JSON or unknown fields).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcessRunnerToolInputParseError {
    #[error("{0}")]
    InvalidJson(String),
}

/// Parse the raw JSON payload for `palyra.process.run`.
///
/// Applies model-output normalization after deserialization: a duplicated command token
/// in `args[0]` is dropped, and a leading `--cwd <dir>` / `--cwd=<dir>` pair is lifted
/// into the `cwd` field. Single-string command lines are kept verbatim.
///
/// # Errors
/// Returns [`ProcessRunnerToolInputParseError::InvalidJson`] for malformed JSON or
/// unknown fields (the schema is closed via `deny_unknown_fields`).
pub fn parse_process_runner_tool_input(
    input_json: &[u8],
) -> Result<ProcessRunnerToolInput, ProcessRunnerToolInputParseError> {
    let mut input = serde_json::from_slice::<ProcessRunnerToolInput>(input_json)
        .map_err(|error| ProcessRunnerToolInputParseError::InvalidJson(error.to_string()))?;
    // The second repeated-command pass is required: stripping `--cwd <dir>` can expose a
    // duplicated command token that was previously not at args[0]
    // (e.g. `args: ["--cwd", "/x", "node", "server.js"]` with command "node").
    normalize_repeated_command_argument(&mut input);
    normalize_leading_cwd_argument(&mut input);
    normalize_repeated_command_argument(&mut input);
    Ok(input)
}

/// Returns whether an executable token names an interpreter that requires explicit policy.
#[must_use]
pub fn process_executable_is_interpreter(command: &str) -> bool {
    let normalized = normalize_executable_token(command);
    INTERPRETER_EXECUTABLE_DENYLIST.contains(&normalized.as_str())
        || normalized.starts_with("python3.")
}

/// Returns whether interpreter arguments request inline code evaluation.
///
/// Python options after a script target belong to that script. Options after a module target
/// remain blocked unless the module/flag pair is known to have non-eval semantics.
#[must_use]
pub fn interpreter_args_contain_blocked_eval_flag(command: &str, args: &[String]) -> bool {
    args.iter().enumerate().any(|(index, arg)| {
        is_blocked_eval_flag(command, arg.as_str())
            && !python_arg_is_safe_downstream_flag(command, args, index)
    })
}

fn normalize_leading_cwd_argument(input: &mut ProcessRunnerToolInput) {
    if input.cwd.is_some() || input.args.is_empty() {
        return;
    }

    let first = input.args[0].trim();
    if first == "--cwd" {
        if input.args.len() < 2 {
            return;
        }
        input.cwd = Some(input.args[1].clone());
        input.args.drain(0..2);
        return;
    }

    if let Some(value) = first.strip_prefix("--cwd=") {
        if value.trim().is_empty() {
            return;
        }
        input.cwd = Some(value.to_owned());
        input.args.remove(0);
    }
}

fn normalize_repeated_command_argument(input: &mut ProcessRunnerToolInput) {
    let command = input.command.trim();
    if command.is_empty() || input.args.is_empty() {
        return;
    }

    if executable_tokens_match(command, input.args[0].as_str()) {
        input.args.remove(0);
    }
}

fn executable_tokens_match(command: &str, candidate: &str) -> bool {
    let command = normalize_executable_token(command);
    let candidate = normalize_executable_token(candidate);
    !command.is_empty() && command == candidate
}

// Reduces an executable spelling to a comparable form: strips quotes, directories, and a
// Windows `.exe` suffix, then lowercases — so `"C:\Tools\node"` matches `node.exe`.
fn normalize_executable_token(value: &str) -> String {
    let trimmed = value.trim().trim_matches('"').trim_matches('\'');
    let file_name = trimmed
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(trimmed)
        .trim()
        .trim_matches('"')
        .trim_matches('\'');
    file_name.strip_suffix(".exe").unwrap_or(file_name).to_ascii_lowercase()
}

fn is_blocked_eval_flag(command: &str, arg: &str) -> bool {
    is_generic_blocked_eval_flag(arg) || node_eval_flag_is_blocked(command, arg)
}

fn is_generic_blocked_eval_flag(arg: &str) -> bool {
    let normalized = arg.trim().to_ascii_lowercase();
    matches!(normalized.as_str(), "-c" | "/c" | "--command" | "-command" | "--eval")
}

fn node_eval_flag_is_blocked(command: &str, arg: &str) -> bool {
    let command = normalize_executable_token(command);
    matches!(command.as_str(), "node" | "nodejs")
        && matches!(arg.trim().to_ascii_lowercase().as_str(), "-e" | "-p")
}

fn python_arg_is_safe_downstream_flag(command: &str, args: &[String], index: usize) -> bool {
    if !is_python_interpreter_command(command) {
        return false;
    }
    let Some(target) = python_execution_target(args) else {
        return false;
    };
    if index <= target.index {
        return false;
    }
    match target.kind {
        PythonExecutionTargetKind::Script => true,
        PythonExecutionTargetKind::Module => {
            python_module_flag_is_known_non_eval(target.value, args[index].as_str())
        }
    }
}

fn is_python_interpreter_command(command: &str) -> bool {
    let command = normalize_executable_token(command);
    matches!(command.as_str(), "python" | "python3" | "py") || command.starts_with("python3.")
}

#[derive(Clone, Copy)]
enum PythonExecutionTargetKind {
    Script,
    Module,
}

struct PythonExecutionTarget<'a> {
    index: usize,
    value: &'a str,
    kind: PythonExecutionTargetKind,
}

fn python_execution_target(args: &[String]) -> Option<PythonExecutionTarget<'_>> {
    let mut index = 0;
    while let Some(arg) = args.get(index).map(|arg| arg.trim()) {
        if arg == "--" {
            let index = index.saturating_add(1);
            return args.get(index).map(|value| PythonExecutionTarget {
                index,
                value: value.trim(),
                kind: PythonExecutionTargetKind::Script,
            });
        }
        if arg.eq_ignore_ascii_case("-m") {
            let index = index.saturating_add(1);
            return args.get(index).map(|value| PythonExecutionTarget {
                index,
                value: value.trim(),
                kind: PythonExecutionTargetKind::Module,
            });
        }
        if is_generic_blocked_eval_flag(arg) {
            return None;
        }
        if !arg.starts_with('-') {
            return Some(PythonExecutionTarget {
                index,
                value: arg,
                kind: PythonExecutionTargetKind::Script,
            });
        }
        index = index.saturating_add(if python_option_consumes_next_value(arg) { 2 } else { 1 });
    }
    None
}

fn python_module_flag_is_known_non_eval(module: &str, flag: &str) -> bool {
    // Unknown modules stay fail-closed because their parsers may assign executable semantics to
    // otherwise familiar flags, as the standard-library `pdb -c` command option does.
    matches!((module, flag.trim()), ("bandit", "-c"))
}

fn python_option_consumes_next_value(arg: &str) -> bool {
    let trimmed = arg.trim();
    matches!(trimmed, "-W" | "-X" | "-Q") || trimmed == "--check-hash-based-pycs"
}

#[cfg(test)]
mod tests {
    use super::{
        interpreter_args_contain_blocked_eval_flag, parse_process_runner_tool_input,
        process_executable_is_interpreter, BackgroundLifetimeMode,
        ProcessRunnerToolInputParseError,
    };

    #[test]
    fn interpreter_policy_classifies_versioned_python_and_inline_eval_flags() {
        assert!(process_executable_is_interpreter("/usr/bin/python3.12"));
        assert!(process_executable_is_interpreter(r"C:\Tools\node.exe"));
        assert!(process_executable_is_interpreter("/usr/bin/nodejs"));
        assert!(process_executable_is_interpreter(r"C:\Tools\nodejs.exe"));
        assert!(!process_executable_is_interpreter("cargo"));

        assert!(interpreter_args_contain_blocked_eval_flag(
            "python",
            &["-c".to_owned(), "print('unsafe')".to_owned()]
        ));
        assert!(interpreter_args_contain_blocked_eval_flag(
            "node",
            &["--eval".to_owned(), "process.exit()".to_owned()]
        ));
        assert!(interpreter_args_contain_blocked_eval_flag(
            "nodejs.exe",
            &["-e".to_owned(), "process.exit()".to_owned()]
        ));
        assert!(!interpreter_args_contain_blocked_eval_flag(
            "python",
            &["scripts/check.py".to_owned(), "-c".to_owned()]
        ));
    }

    #[test]
    fn interpreter_policy_rejects_python_module_eval_flags() {
        assert!(interpreter_args_contain_blocked_eval_flag(
            "python",
            &[
                "-m".to_owned(),
                "pdb".to_owned(),
                "-c".to_owned(),
                "!__import__('os').system('whoami')".to_owned(),
                "scripts/check.py".to_owned(),
            ]
        ));
        assert!(!interpreter_args_contain_blocked_eval_flag(
            "python",
            &[
                "-m".to_owned(),
                "bandit".to_owned(),
                "-c".to_owned(),
                "bandit.yaml".to_owned(),
                "package".to_owned(),
            ]
        ));
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_valid_payload() {
        let input =
            br#"{"command":"uname","args":["-a"],"cwd":"workspace","requested_egress_hosts":["api.example.com"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");
        assert_eq!(parsed.command, "uname");
        assert_eq!(parsed.args, vec!["-a"]);
        assert_eq!(parsed.cwd.as_deref(), Some("workspace"));
        assert!(parsed.env.is_empty());
        assert!(parsed.prepend_path.is_empty());
        assert_eq!(parsed.requested_egress_hosts, vec!["api.example.com"]);
        assert_eq!(parsed.timeout_ms, None);
        assert!(!parsed.background);
        assert!(!parsed.stdin_requested());
        assert!(!parsed.pty);
        assert!(parsed.port_hints.is_empty());
        assert_eq!(parsed.effective_lifetime_mode(), BackgroundLifetimeMode::RunOwned);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_env_map() {
        let input =
            br#"{"command":"node","args":["server.mjs"],"env":{"PALYRA_E2E_HOME":"C:\\tmp\\home"}}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner env payload should parse");

        assert_eq!(parsed.env.get("PALYRA_E2E_HOME").map(String::as_str), Some("C:\\tmp\\home"));
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_prepend_path() {
        let input = br#"{"command":"pnpm","args":["test"],"prepend_path":["C:\\tools\\node","node_modules/.bin"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner prepend_path payload should parse");

        assert_eq!(parsed.prepend_path, vec!["C:\\tools\\node", "node_modules/.bin"]);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_background_flag() {
        let input =
            br#"{"command":"python3","args":["-m","http.server","8765"],"background":true}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid background process-runner payload should parse");

        assert_eq!(parsed.command, "python3");
        assert!(parsed.background);
        assert_eq!(parsed.effective_lifetime_mode(), BackgroundLifetimeMode::RunOwned);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_interactive_handle_fields() {
        let input = br#"{"command":"python3","args":["repl.py"],"background":true,"interactive":true,"stdin":true,"pty":false,"port_hints":[5173,8787]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid interactive process-runner payload should parse");

        assert!(parsed.background);
        assert!(parsed.stdin_requested());
        assert!(!parsed.pty);
        assert_eq!(parsed.port_hints, vec![5173, 8787]);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_background_lifetime_mode() {
        let input = br#"{"command":"python3","args":["server.py"],"background":true,"lifetime_mode":"until_verifier"}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid background lifetime mode should parse");

        assert_eq!(parsed.effective_lifetime_mode(), BackgroundLifetimeMode::UntilVerifier);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_notifications_and_watch_patterns() {
        let input = br#"{"command":"npm","args":["run","dev"],"background":true,"notify_on_complete":true,"watch_patterns":[{"name":"vite_ready","pattern":"Local:","stream":"stdout","notify_once":true}],"env_profile_id":"web-dev","elevated_intent":false}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid notification payload should parse");

        assert!(parsed.notify_on_complete);
        assert_eq!(parsed.watch_patterns.len(), 1);
        assert_eq!(parsed.watch_patterns[0].name, "vite_ready");
        assert_eq!(parsed.watch_patterns[0].stream.as_str(), "stdout");
        assert_eq!(parsed.env_profile_id.as_deref(), Some("web-dev"));
        assert!(!parsed.elevated_intent);
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_facade_mapping() {
        let input = br#"{"command":"pwd","facade_mapping":{"original_tool_name":"palyra.exec.run","canonical_tool_name":"palyra.process.run"}}"#;
        let parsed =
            parse_process_runner_tool_input(input).expect("valid facade mapping should parse");

        let mapping = parsed.facade_mapping.expect("facade mapping should be preserved");
        assert_eq!(mapping.original_tool_name, "palyra.exec.run");
        assert_eq!(mapping.canonical_tool_name, "palyra.process.run");
    }

    #[test]
    fn parse_process_runner_tool_input_accepts_keep_running_alias() {
        let input =
            br#"{"command":"python3","args":["server.py"],"background":true,"keep_running_after_run":true}"#;
        let parsed =
            parse_process_runner_tool_input(input).expect("valid detached alias should parse");

        assert_eq!(parsed.effective_lifetime_mode(), BackgroundLifetimeMode::Detached);
    }

    #[test]
    fn parse_process_runner_tool_input_keeps_single_string_arg_unchanged() {
        let input = br#"{"command":"echo","args":["echo PALYRA_TERMINAL_OK"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.command, "echo");
        assert_eq!(parsed.args, vec!["echo PALYRA_TERMINAL_OK"]);
    }

    #[test]
    fn parse_process_runner_tool_input_does_not_split_single_arg_subexecution() {
        let input = br#"{"command":"find","args":["find . -maxdepth 0 -exec sh -c id +"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.command, "find");
        assert_eq!(parsed.args, vec!["find . -maxdepth 0 -exec sh -c id +"]);
    }

    #[test]
    fn parse_process_runner_tool_input_normalizes_repeated_command_when_split_already() {
        let input = br#"{"command":"echo","args":["echo","PALYRA_TERMINAL_OK"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.args, vec!["PALYRA_TERMINAL_OK"]);
    }

    #[test]
    fn parse_process_runner_tool_input_normalizes_repeated_node_command_when_split_already() {
        let input = br#"{"command":"node","args":["node","e2e-smoke-file-patch/math.test.js"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.command, "node");
        assert_eq!(parsed.args, vec!["e2e-smoke-file-patch/math.test.js"]);
    }

    #[test]
    fn parse_process_runner_tool_input_preserves_complex_single_arg_command_line() {
        let input = br#"{"command":"node","args":["node -e \"(() => console.log('ok'))()\""]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.command, "node");
        assert_eq!(parsed.args, vec!["node -e \"(() => console.log('ok'))()\""]);
    }

    #[test]
    fn parse_process_runner_tool_input_normalizes_repeated_windows_exe_command() {
        let input =
            br#"{"command":"node.exe","args":["C:\\Tools\\node","e2e-smoke-file-patch/math.test.js"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.args, vec!["e2e-smoke-file-patch/math.test.js"]);
    }

    #[test]
    fn parse_process_runner_tool_input_normalizes_leading_cwd_arg() {
        let input = br#"{"command":"node","args":["--cwd","/workspace/app","node","server.js"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.cwd.as_deref(), Some("/workspace/app"));
        assert_eq!(parsed.command, "node");
        assert_eq!(parsed.args, vec!["server.js"]);
    }

    #[test]
    fn parse_process_runner_tool_input_normalizes_leading_cwd_equals_arg() {
        let input = br#"{"command":"npm","args":["--cwd=fixtures/app","test"]}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid process-runner payload should parse");

        assert_eq!(parsed.cwd.as_deref(), Some("fixtures/app"));
        assert_eq!(parsed.args, vec!["test"]);
    }

    #[test]
    fn parse_process_runner_tool_input_rejects_unknown_fields() {
        let input = br#"{"command":"uname","unknown":true}"#;
        let error =
            parse_process_runner_tool_input(input).expect_err("unknown fields must fail parsing");
        assert!(
            matches!(error, ProcessRunnerToolInputParseError::InvalidJson(_)),
            "unknown fields should fail as JSON schema violation"
        );
    }

    #[test]
    fn parse_process_runner_tool_input_rejects_invalid_json() {
        let input = br#"{"command":"uname","#;
        let error = parse_process_runner_tool_input(input)
            .expect_err("invalid JSON payload must fail parsing");
        assert!(
            matches!(error, ProcessRunnerToolInputParseError::InvalidJson(_)),
            "invalid JSON should map to parser error"
        );
    }
}
