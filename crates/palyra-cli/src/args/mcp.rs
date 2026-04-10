use clap::{Args, Subcommand};

use super::{AcpConnectionArgs, AcpSessionDefaultsArgs};

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum McpSubcommand {
    #[command(about = "Run the stdio MCP server")]
    Serve {
        #[command(flatten)]
        connection: AcpConnectionArgs,
        #[command(flatten)]
        session_defaults: AcpSessionDefaultsArgs,
        #[arg(long, default_value_t = false)]
        read_only: bool,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct McpCommand {
    #[command(subcommand)]
    pub subcommand: McpSubcommand,
}
