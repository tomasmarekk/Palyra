//! Arguments for `palyra agent`: one-off runs and interactive sessions, plus the
//! `agent acp`/`agent acp-shim` compatibility entry points that reuse the
//! bridge/shim structs from `acp.rs`. Help text is pinned by snapshot tests; see
//! the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

use super::{AcpBridgeArgs, AcpShimArgs};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AgentCommand {
    Run {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        require_existing: bool,
        #[arg(long, default_value_t = false)]
        reset_session: bool,
        #[arg(long, help = "Canonical 26-character ULID run id. Omit to generate one.")]
        run_id: Option<String>,
        #[arg(
            long,
            help = "Single-line prompt text. Use --prompt-stdin for multi-line or blank-line separated prompts."
        )]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(
            long,
            visible_alias = "abort-active-run",
            default_value_t = false,
            help = "Abort the selected active run, then start this prompt in the same session."
        )]
        interrupt_active_run: bool,
        #[arg(long, value_enum, default_value_t = AgentApprovalModeArg::AllowOnce)]
        approval_mode: AgentApprovalModeArg,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    Interactive {
        #[arg(long)]
        grpc_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        device_id: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        require_existing: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    AcpShim {
        #[command(flatten)]
        command: AcpShimArgs,
    },
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
}
