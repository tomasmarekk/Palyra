use std::collections::BTreeMap;

use serde::Deserialize;
use thiserror::Error;

/// Canonical input payload for `palyra.process.run`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
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
    pub requested_egress_hosts: Vec<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub background: bool,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProcessRunnerToolInputParseError {
    #[error("{0}")]
    InvalidJson(String),
}

/// Parse the raw JSON payload for `palyra.process.run`.
pub fn parse_process_runner_tool_input(
    input_json: &[u8],
) -> Result<ProcessRunnerToolInput, ProcessRunnerToolInputParseError> {
    let mut input = serde_json::from_slice::<ProcessRunnerToolInput>(input_json)
        .map_err(|error| ProcessRunnerToolInputParseError::InvalidJson(error.to_string()))?;
    normalize_repeated_command_argument(&mut input);
    normalize_leading_cwd_argument(&mut input);
    normalize_repeated_command_argument(&mut input);
    Ok(input)
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

#[cfg(test)]
mod tests {
    use super::{parse_process_runner_tool_input, ProcessRunnerToolInputParseError};

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
        assert_eq!(parsed.requested_egress_hosts, vec!["api.example.com"]);
        assert_eq!(parsed.timeout_ms, None);
        assert!(!parsed.background);
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
    fn parse_process_runner_tool_input_accepts_background_flag() {
        let input =
            br#"{"command":"python3","args":["-m","http.server","8765"],"background":true}"#;
        let parsed = parse_process_runner_tool_input(input)
            .expect("valid background process-runner payload should parse");

        assert_eq!(parsed.command, "python3");
        assert!(parsed.background);
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
