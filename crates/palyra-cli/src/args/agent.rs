//! Arguments for `palyra agent`: one-off runs and interactive sessions, plus the
//! `agent acp`/`agent acp-shim` compatibility entry points that reuse the
//! bridge/shim structs from `acp.rs`. Help text is pinned by snapshot tests; see
//! the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

use super::{AcpBridgeArgs, AcpShimArgs};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AgentCommand {
    #[command(about = "Start a one-off agent run and stream its events")]
    Run {
        #[arg(long, help = "Override the daemon gRPC endpoint for this run")]
        grpc_url: Option<String>,
        #[arg(long, help = "Use this bearer token for daemon authentication")]
        token: Option<String>,
        #[arg(long, help = "Send the request as this principal")]
        principal: Option<String>,
        #[arg(long, help = "Attach this operator device id to the request")]
        device_id: Option<String>,
        #[arg(long, help = "Route the run through this channel id")]
        channel: Option<String>,
        #[arg(long, help = "Resume or target this exact session id")]
        session_id: Option<String>,
        #[arg(long, help = "Resolve the target session by stable session key")]
        session_key: Option<String>,
        #[arg(long, help = "Create or rename the session with this label")]
        session_label: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Fail if the selected session does not already exist"
        )]
        require_existing: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Clear prior session state before starting the run"
        )]
        reset_session: bool,
        #[arg(long, help = "Canonical 26-character ULID run id. Omit to generate one.")]
        run_id: Option<String>,
        #[arg(
            long,
            help = "Single-line prompt text. Use --prompt-stdin for multi-line or blank-line separated prompts."
        )]
        prompt: Option<String>,
        #[arg(long, default_value_t = false, help = "Read the prompt text from stdin")]
        prompt_stdin: bool,
        #[arg(
            long,
            visible_aliases = ["reasoning-effort", "reasoning-level"],
            help = "Override provider reasoning effort for this run: none, minimal, low, medium, high, or xhigh"
        )]
        reasoning: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["no_fast", "service_tier"],
            help = "Request fast provider processing for this run when supported"
        )]
        fast: bool,
        #[arg(
            long = "no-fast",
            default_value_t = false,
            conflicts_with_all = ["fast", "service_tier"],
            help = "Use the provider default processing tier for this run"
        )]
        no_fast: bool,
        #[arg(
            long,
            value_name = "TIER",
            conflicts_with_all = ["fast", "no_fast"],
            help = "Override provider service tier for this run: auto, default, priority, or flex"
        )]
        service_tier: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Permit tools classified as sensitive for this run"
        )]
        allow_sensitive_tools: bool,
        #[arg(
            long,
            visible_alias = "abort-active-run",
            default_value_t = false,
            help = "Abort the selected active run, then start this prompt in the same session."
        )]
        interrupt_active_run: bool,
        #[arg(
            long,
            value_enum,
            default_value_t = AgentApprovalModeArg::Prompt,
            help = "Handle explicit safe-mode approvals by prompting, denying, allowing one request, or allowing the current run"
        )]
        approval_mode: AgentApprovalModeArg,
        #[arg(long, value_enum, default_value_t = AgentAutoResumeArg::Never, help = "Select whether the CLI automatically starts a continuation run")]
        auto_resume: AgentAutoResumeArg,
        #[arg(
            long,
            default_value_t = 3,
            help = "Maximum automatic continuation runs when --auto-resume allows them"
        )]
        auto_resume_limit: usize,
        #[arg(long, default_value_t = false, help = "Stream run events as newline-delimited JSON")]
        ndjson: bool,
    },
    #[command(about = "Open an interactive terminal agent session")]
    Interactive {
        #[arg(long, help = "Override the daemon gRPC endpoint for this session")]
        grpc_url: Option<String>,
        #[arg(long, help = "Use this bearer token for daemon authentication")]
        token: Option<String>,
        #[arg(long, help = "Send the session request as this principal")]
        principal: Option<String>,
        #[arg(long, help = "Attach this operator device id to the request")]
        device_id: Option<String>,
        #[arg(long, help = "Route the session through this channel id")]
        channel: Option<String>,
        #[arg(long, help = "Resume or target this exact session id")]
        session_id: Option<String>,
        #[arg(long, help = "Resolve the target session by stable session key")]
        session_key: Option<String>,
        #[arg(long, help = "Create or rename the session with this label")]
        session_label: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            help = "Fail if the selected session does not already exist"
        )]
        require_existing: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Permit tools classified as sensitive for this session"
        )]
        allow_sensitive_tools: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Stream interactive events as newline-delimited JSON"
        )]
        ndjson: bool,
    },
    #[command(about = "Run the legacy ACP compatibility shim")]
    AcpShim {
        #[command(flatten)]
        command: AcpShimArgs,
    },
    #[command(about = "Run the ACP stdio bridge")]
    Acp {
        #[command(flatten)]
        command: AcpBridgeArgs,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum AgentApprovalModeArg {
    Prompt,
    Deny,
    AllowOnce,
    #[value(alias = "allow")]
    AllowRun,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum AgentAutoResumeArg {
    Never,
    OnContinuation,
}
