//! Arguments for `palyra mcp serve`: the stdio MCP server facade. Connection
//! and session defaults reuse the shared ACP structs from `acp.rs`; the facade
//! never imports external MCP servers. Help text is pinned by snapshot tests;
//! see the doc-comment rules in `mod.rs`.

use clap::{Args, Subcommand};

use super::{AcpConnectionArgs, AcpSessionDefaultsArgs};

const MCP_SERVE_AFTER_HELP: &str = "\
Scope:
  `palyra mcp serve` exposes Palyra as a stdio MCP server for MCP clients.
  It does not import external MCP servers or register external MCP client tools such as `ticket.read` into Palyra agent runs.";

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum McpSubcommand {
    #[command(about = "Run the stdio MCP server", after_long_help = MCP_SERVE_AFTER_HELP)]
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
