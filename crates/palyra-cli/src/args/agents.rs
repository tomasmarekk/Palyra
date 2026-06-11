//! Arguments for `palyra agents`: daemon agent registry CRUD, session and
//! operator-context bindings, default-agent selection, and identity resolution.
//! Help text is pinned by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum AgentsCommand {
    #[command(about = "List registered agents")]
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    #[command(about = "Show one registered agent")]
    Show {
        agent_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "List session, principal, and channel agent bindings")]
    Bindings {
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    #[command(about = "Bind an agent to a session or operator context")]
    Bind {
        agent_id: String,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Remove an agent binding from a session or operator context")]
    Unbind {
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Set the default agent")]
    SetDefault {
        agent_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Create a registered agent")]
    Create {
        agent_id: String,
        #[arg(long)]
        display_name: String,
        #[arg(long)]
        agent_dir: Option<String>,
        #[arg(long = "workspace-root")]
        workspace_root: Vec<String>,
        #[arg(long = "model-profile")]
        model_profile: Option<String>,
        #[arg(long = "execution-backend")]
        execution_backend: Option<String>,
        #[arg(long = "tool-allow")]
        tool_allow: Vec<String>,
        #[arg(long = "skill-allow")]
        skill_allow: Vec<String>,
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[arg(long, default_value_t = false)]
        allow_absolute_paths: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Delete a registered agent")]
    Delete {
        agent_id: String,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Resolve the effective agent for an operator context")]
    Identity {
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        preferred_agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        persist_binding: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
