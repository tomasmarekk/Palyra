use clap::Subcommand;

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
        run_id: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
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
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(long, default_value_t = false)]
        ndjson: bool,
    },
    AcpShim {
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
        run_id: Option<String>,
        #[arg(long)]
        prompt: Option<String>,
        #[arg(long, default_value_t = false)]
        prompt_stdin: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["session_id", "run_id", "prompt", "prompt_stdin"]
        )]
        ndjson_stdin: bool,
    },
    Acp {
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
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
    },
}
