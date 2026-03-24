use std::{io::Write, process::ExitCode};

use anyhow::{Context, Result};
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
    Internal = 1,
}

impl CliExitCode {
    pub(crate) fn as_exit_code(self) -> ExitCode {
        ExitCode::from(self as u8)
    }
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
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(line.as_bytes()).context("stdout write failed")?;
    stdout.write_all(b"\n").context("stdout write failed")?;
    Ok(())
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
    let kind = match exit_code {
        CliExitCode::Success => "success",
        CliExitCode::Validation => "validation_error",
        CliExitCode::Auth => "auth_failure",
        CliExitCode::Connectivity => "connectivity_failure",
        CliExitCode::Unsupported => "unsupported_capability",
        CliExitCode::Policy => "policy_denial",
        CliExitCode::Internal => "internal_error",
    };
    let context = app::current_root_context();
    let trace_id = context.as_ref().map(|value| value.trace_id());
    let profile = context.as_ref().and_then(|value| value.profile_name());
    let state_root = context.as_ref().map(|value| value.state_root().display().to_string());
    let log_level =
        context.as_ref().map(|value| format!("{:?}", value.log_level()).to_ascii_lowercase());
    let no_color = context.as_ref().map(|value| value.no_color());
    let message = format!("{error:#}");
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
    let lower = error.to_string().to_ascii_lowercase();

    if error.chain().any(|cause| cause.is::<clap::Error>()) {
        return CliExitCode::Validation;
    }
    if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("auth")
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
    if lower.contains("invalid")
        || lower.contains("must be")
        || lower.contains("cannot be")
        || lower.contains("requires")
        || lower.contains("not found")
    {
        return CliExitCode::Validation;
    }
    CliExitCode::Internal
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
    fn classify_error_maps_connectivity_failures() {
        assert_eq!(
            classify_error(&anyhow!("failed to connect to gateway: connection refused")),
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
    }
}
