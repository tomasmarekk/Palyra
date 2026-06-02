use std::{fmt, io::Write, process::ExitCode};

use anyhow::{Context, Result};
use palyra_common::redaction::redact_diagnostic_text;
use serde::Serialize;

use crate::{app, args::OutputFormatArg};

pub(crate) mod approvals;
pub(crate) mod channels;
pub(crate) mod skills;
pub(crate) mod support_bundle;

const JSON_ERROR_ENVELOPE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliExitCode {
    Success = 0,
    Validation = 2,
    Auth = 3,
    Connectivity = 4,
    Unsupported = 5,
    Policy = 6,
    Precondition = 7,
    NotFound = 8,
    Cancelled = 130,
    Internal = 1,
}

impl CliExitCode {
    pub(crate) fn as_exit_code(self) -> ExitCode {
        ExitCode::from(self as u8)
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            CliExitCode::Success => "success",
            CliExitCode::Validation => "validation_error",
            CliExitCode::Auth => "auth_failure",
            CliExitCode::Connectivity => "connectivity_failure",
            CliExitCode::Unsupported => "unsupported_capability",
            CliExitCode::Policy => "policy_denial",
            CliExitCode::Precondition => "precondition_failed",
            CliExitCode::NotFound => "not_found",
            CliExitCode::Cancelled => "cancelled",
            CliExitCode::Internal => "internal_error",
        }
    }
}

#[derive(Debug)]
struct AlreadyEmittedCliError {
    exit_code: CliExitCode,
}

impl fmt::Display for AlreadyEmittedCliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CLI output was already emitted")
    }
}

impl std::error::Error for AlreadyEmittedCliError {}

pub(crate) fn already_emitted_error(exit_code: CliExitCode) -> anyhow::Error {
    AlreadyEmittedCliError { exit_code }.into()
}

pub(crate) fn already_emitted_exit_code(error: &anyhow::Error) -> Option<CliExitCode> {
    error.chain().find_map(|cause| {
        cause.downcast_ref::<AlreadyEmittedCliError>().map(|value| value.exit_code)
    })
}

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    version: u32,
    error: ErrorEntry<'a>,
}

#[derive(Serialize)]
struct ErrorEntry<'a> {
    kind: &'a str,
    message: String,
    trace_id: Option<&'a str>,
    profile: Option<&'a str>,
    state_root: Option<String>,
    log_level: Option<String>,
    no_color: Option<bool>,
}

pub(crate) fn print_json_pretty<T>(value: &T, error_context: &'static str) -> Result<()>
where
    T: Serialize,
{
    println!("{}", serde_json::to_string_pretty(value).context(error_context)?);
    Ok(())
}

pub(crate) fn print_json_line<T>(value: &T, error_context: &'static str) -> Result<()>
where
    T: Serialize,
{
    println!("{}", serde_json::to_string(value).context(error_context)?);
    Ok(())
}

pub(crate) fn print_text_line(line: &str) -> Result<()> {
    let line = sanitize_text_output_line(line);
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(line.as_bytes()).context("stdout write failed")?;
    stdout.write_all(b"\n").context("stdout write failed")?;
    Ok(())
}

fn sanitize_text_output_line(line: &str) -> String {
    sanitize_diagnostic_text(line)
}

pub(crate) fn sanitize_diagnostic_text(line: &str) -> String {
    redact_diagnostic_text(line)
}

pub(crate) fn preferred_json(explicit_json: bool) -> bool {
    explicit_json || app::current_root_context().is_some_and(|context| context.prefers_json())
}

pub(crate) fn preferred_ndjson(explicit_json: bool, explicit_ndjson: bool) -> bool {
    if explicit_json {
        return false;
    }
    explicit_ndjson || app::current_root_context().is_some_and(|context| context.prefers_ndjson())
}

pub(crate) fn emit_error(error: &anyhow::Error) -> Result<CliExitCode> {
    let exit_code = classify_error(error);
    let kind = exit_code.kind();
    let context = app::current_root_context();
    let trace_id = context.as_ref().map(|value| value.trace_id());
    let profile = context.as_ref().and_then(|value| value.profile_name());
    let state_root = context.as_ref().map(|value| value.state_root().display().to_string());
    let log_level =
        context.as_ref().map(|value| format!("{:?}", value.log_level()).to_ascii_lowercase());
    let no_color = context.as_ref().map(|value| value.no_color());
    let message = sanitize_text_output_line(format!("{error:#}").as_str());
    let format =
        context.as_ref().map(|value| value.output_format()).unwrap_or(OutputFormatArg::Text);

    match format {
        OutputFormatArg::Text => {
            if let Some(trace_id) = trace_id {
                let profile_suffix =
                    profile.map(|value| format!(" profile={value}")).unwrap_or_default();
                let state_root_suffix = state_root
                    .as_ref()
                    .map(|value| format!(" state_root={value}"))
                    .unwrap_or_default();
                eprintln!(
                    "error[{kind}] trace_id={trace_id}{profile_suffix}{state_root_suffix} {}",
                    message
                );
            } else {
                eprintln!("error[{kind}] {}", message);
            }
        }
        OutputFormatArg::Json => {
            let envelope = ErrorEnvelope {
                version: JSON_ERROR_ENVELOPE_VERSION,
                error: ErrorEntry {
                    kind,
                    message: message.clone(),
                    trace_id,
                    profile,
                    state_root,
                    log_level,
                    no_color,
                },
            };
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&envelope)
                    .context("failed to encode CLI error envelope as JSON")?
            );
        }
        OutputFormatArg::Ndjson => {
            let envelope = ErrorEnvelope {
                version: JSON_ERROR_ENVELOPE_VERSION,
                error: ErrorEntry {
                    kind,
                    message,
                    trace_id,
                    profile,
                    state_root,
                    log_level,
                    no_color,
                },
            };
            eprintln!(
                "{}",
                serde_json::to_string(&envelope)
                    .context("failed to encode CLI error envelope as NDJSON")?
            );
        }
    }

    Ok(exit_code)
}

pub(crate) fn classify_error(error: &anyhow::Error) -> CliExitCode {
    let lower =
        error.chain().map(ToString::to_string).collect::<Vec<_>>().join(": ").to_ascii_lowercase();

    if error.chain().any(|cause| cause.is::<clap::Error>()) {
        return CliExitCode::Validation;
    }
    if let Some(exit_code) = error.chain().find_map(classify_control_plane_error) {
        return exit_code;
    }
    if let Some(exit_code) = error.chain().find_map(classify_tonic_status) {
        return exit_code;
    }
    if let Some(exit_code) = error.chain().find_map(classify_reqwest_status) {
        return exit_code;
    }
    if is_user_cancellation(&lower) {
        return CliExitCode::Cancelled;
    }
    if lower.contains("browser service is disabled")
        || lower.contains("tool_call.browser_service.enabled=false")
    {
        return CliExitCode::Precondition;
    }
    if lower.contains("failed precondition")
        || lower.contains("precondition failed")
        || lower.contains("http 412")
    {
        return CliExitCode::Precondition;
    }
    if is_provider_output_limit_stop(&lower) {
        return CliExitCode::Precondition;
    }
    if is_provider_turn_timeout(&lower) {
        return CliExitCode::Connectivity;
    }
    if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("authentication")
        || lower.contains("authorization")
        || lower.contains("api key")
        || lower.contains("admin token")
        || lower.contains("auth token")
        || lower.contains("token")
    {
        return CliExitCode::Auth;
    }
    if error
        .chain()
        .any(|cause| cause.is::<reqwest::Error>() || cause.is::<tonic::transport::Error>())
        || lower.contains("failed to connect")
        || lower.contains("connection refused")
        || lower.contains("deadline exceeded")
        || lower.contains("timed out")
        || lower.contains("navigation_failed")
        || lower.contains("network_request_failed")
        || lower.contains("browser_runtime_failed")
    {
        return CliExitCode::Connectivity;
    }
    if lower.contains("unsupported")
        || lower.contains("not yet available")
        || lower.contains("unavailable on")
    {
        return CliExitCode::Unsupported;
    }
    if lower.contains("policy") || lower.contains("approval required") || lower.contains("denied") {
        return CliExitCode::Policy;
    }
    if lower.contains("not found") {
        return CliExitCode::NotFound;
    }
    if lower.contains("invalid")
        || lower.contains("must be")
        || lower.contains("cannot be")
        || lower.contains("cannot set")
        || lower.contains("requires")
        || lower.contains("required")
        || lower.contains("missing prompt")
        || lower.contains("prompt from stdin is empty")
    {
        return CliExitCode::Validation;
    }
    CliExitCode::Internal
}

fn is_provider_output_limit_stop(lower_error: &str) -> bool {
    lower_error.contains("finish_reason=length")
        || lower_error.contains("finish reason length")
        || lower_error.contains("output token limit")
        || lower_error.contains("max output tokens")
}

fn is_user_cancellation(lower_error: &str) -> bool {
    lower_error.contains("cancelled by request")
        || lower_error.contains("canceled by request")
        || lower_error.contains("run cancellation requested")
}

fn is_provider_turn_timeout(lower_error: &str) -> bool {
    lower_error.contains("model provider turn timed out")
        || (lower_error.contains("provider")
            && lower_error.contains("timed out")
            && lower_error.contains("no model tokens"))
}

fn classify_control_plane_error(cause: &(dyn std::error::Error + 'static)) -> Option<CliExitCode> {
    match cause.downcast_ref::<palyra_control_plane::ControlPlaneClientError>()? {
        palyra_control_plane::ControlPlaneClientError::Http { status, envelope, .. } => {
            envelope.as_ref().map_or_else(
                || classify_http_status(*status),
                |envelope| classify_control_plane_envelope(*status, envelope),
            )
        }
        palyra_control_plane::ControlPlaneClientError::InvalidBaseUrl(_) => {
            Some(CliExitCode::Validation)
        }
        palyra_control_plane::ControlPlaneClientError::Transport(_) => {
            Some(CliExitCode::Connectivity)
        }
        palyra_control_plane::ControlPlaneClientError::ClientInit(_)
        | palyra_control_plane::ControlPlaneClientError::Decode(_) => None,
    }
}

fn classify_control_plane_envelope(
    status: u16,
    envelope: &palyra_control_plane::ErrorEnvelope,
) -> Option<CliExitCode> {
    use palyra_control_plane::ErrorCategory;

    match envelope.category {
        ErrorCategory::Auth => Some(CliExitCode::Auth),
        ErrorCategory::Validation => Some(CliExitCode::Validation),
        ErrorCategory::Policy => Some(CliExitCode::Policy),
        ErrorCategory::NotFound => Some(CliExitCode::NotFound),
        ErrorCategory::Conflict => Some(CliExitCode::Precondition),
        ErrorCategory::Dependency if status == 412 => Some(CliExitCode::Precondition),
        ErrorCategory::Dependency | ErrorCategory::Availability => Some(CliExitCode::Connectivity),
        ErrorCategory::Internal => classify_http_status(status),
    }
}

fn classify_tonic_status(cause: &(dyn std::error::Error + 'static)) -> Option<CliExitCode> {
    let status = cause.downcast_ref::<tonic::Status>()?;
    Some(match status.code() {
        tonic::Code::InvalidArgument => CliExitCode::Validation,
        tonic::Code::Unauthenticated => CliExitCode::Auth,
        tonic::Code::PermissionDenied => CliExitCode::Policy,
        tonic::Code::NotFound => CliExitCode::NotFound,
        tonic::Code::Cancelled => CliExitCode::Cancelled,
        tonic::Code::FailedPrecondition => CliExitCode::Precondition,
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::ResourceExhausted => CliExitCode::Connectivity,
        _ => return None,
    })
}

fn classify_reqwest_status(cause: &(dyn std::error::Error + 'static)) -> Option<CliExitCode> {
    let status = cause.downcast_ref::<reqwest::Error>()?.status()?;
    classify_http_status(status.as_u16())
}

fn classify_http_status(status: u16) -> Option<CliExitCode> {
    match status {
        400 | 422 => Some(CliExitCode::Validation),
        401 => Some(CliExitCode::Auth),
        403 => Some(CliExitCode::Policy),
        404 => Some(CliExitCode::NotFound),
        409 | 412 => Some(CliExitCode::Precondition),
        408 | 429 | 500..=599 => Some(CliExitCode::Connectivity),
        400..=499 => Some(CliExitCode::Validation),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::*;

    #[test]
    fn classify_error_maps_auth_failures() {
        assert_eq!(
            classify_error(&anyhow!("unauthorized admin token rejected")),
            CliExitCode::Auth
        );
    }

    #[test]
    fn classify_error_maps_provider_length_stop_as_precondition() {
        assert_eq!(
            classify_error(&anyhow!(
                "model provider stopped because of an output token limit before returning a complete final answer or structured tool call (finish_reason=length); provider=anthropic"
            )),
            CliExitCode::Precondition
        );
    }

    #[test]
    fn classify_error_maps_provider_turn_timeout_before_token_auth_match() {
        assert_eq!(
            classify_error(&anyhow!(
                "agent run failed: model provider turn timed out after 60000ms for run 01ARZ3NDEKTSV4RRFFQ69G5FAV; no model tokens, tool proposals, or final answer arrived before the deadline."
            )),
            CliExitCode::Connectivity
        );
    }

    #[test]
    fn classify_error_maps_connectivity_failures() {
        assert_eq!(
            classify_error(&anyhow!("failed to connect to gateway: connection refused")),
            CliExitCode::Connectivity
        );
        assert_eq!(
            classify_error(&anyhow!(
                "browser.tabs.open failed: navigation_failed: navigation returned HTTP 403"
            )),
            CliExitCode::Connectivity
        );
    }

    #[test]
    fn classify_error_maps_policy_denials() {
        assert_eq!(
            classify_error(&anyhow!("approval required by policy before tool execution")),
            CliExitCode::Policy
        );
    }

    #[test]
    fn classify_error_maps_validation_failures() {
        assert_eq!(
            classify_error(&anyhow!("run_id must be a canonical ULID")),
            CliExitCode::Validation
        );
        assert_eq!(
            classify_error(&anyhow!("missing prompt: use --prompt or --prompt-stdin")),
            CliExitCode::Validation
        );
        assert_eq!(
            classify_error(&anyhow!(
                "prompt from stdin is empty; pipe text into stdin or use --prompt"
            )),
            CliExitCode::Validation
        );
    }

    #[test]
    fn classify_error_maps_control_plane_user_errors() {
        let validation = palyra_control_plane::ControlPlaneClientError::Http {
            status: 400,
            message: "supported phrases include daily, hourly".to_owned(),
            envelope: None,
        };
        assert_eq!(classify_error(&anyhow!(validation)), CliExitCode::Validation);

        let precondition = palyra_control_plane::ControlPlaneClientError::Http {
            status: 412,
            message: "browser service is disabled".to_owned(),
            envelope: Some(palyra_control_plane::ErrorEnvelope {
                error: "browser service is disabled".to_owned(),
                code: "failed_precondition".to_owned(),
                category: palyra_control_plane::ErrorCategory::Dependency,
                retryable: false,
                redacted: false,
                validation_errors: Vec::new(),
            }),
        };
        assert_eq!(classify_error(&anyhow!(precondition)), CliExitCode::Precondition);
    }

    #[test]
    fn classify_error_maps_browser_disabled_precondition() {
        assert_eq!(
            classify_error(&anyhow!(
                "browser service is disabled (tool_call.browser_service.enabled=false); enable it with `palyra config set --key tool_call.browser_service.enabled --value true`"
            )),
            CliExitCode::Precondition
        );
        assert_eq!(
            classify_error(&anyhow!(
                "browser service is disabled (tool_call.browser_service.enabled=false); configure tool_call.browser_service.auth_token before use"
            )),
            CliExitCode::Precondition
        );
    }

    #[test]
    fn classify_error_maps_explicit_precondition_before_auth_like_paths() {
        assert_eq!(
            classify_error(&anyhow!(
                "precondition failed: no managed gateway service metadata exists at /tmp/auth-token/gateway-service.json"
            )),
            CliExitCode::Precondition
        );
    }

    #[test]
    fn classify_error_maps_tonic_not_found() {
        let error = anyhow::Error::new(tonic::Status::not_found(
            "orchestrator session not found for selector: missing",
        ))
        .context("failed to call ResolveSession");

        assert_eq!(classify_error(&error), CliExitCode::NotFound);
    }

    #[test]
    fn classify_error_maps_run_cancellation_without_internal_error() {
        assert_eq!(
            classify_error(&anyhow!("agent run failed: cancelled by request")),
            CliExitCode::Cancelled
        );
        assert_eq!(
            classify_error(&anyhow!("agent run failed: canceled by request")),
            CliExitCode::Cancelled
        );

        let error = anyhow::Error::new(tonic::Status::cancelled("run cancellation requested"))
            .context("failed to read run stream");
        assert_eq!(classify_error(&error), CliExitCode::Cancelled);
        assert_eq!(CliExitCode::Cancelled.kind(), "cancelled");
    }

    #[test]
    fn text_output_sanitizer_redacts_auth_material() {
        let sanitized = sanitize_text_output_line(
            "request failed https://example.test/callback?access_token=secret Authorization: Bearer abc",
        );

        assert!(sanitized.contains("access_token=<redacted>"));
        assert!(sanitized.contains("Bearer <redacted>"));
        assert!(!sanitized.contains("access_token=secret"));
        assert!(!sanitized.contains("Bearer abc"));
    }
}
