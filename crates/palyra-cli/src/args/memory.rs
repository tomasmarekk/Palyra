use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum MemoryCommand {
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(visible_alias = "reindex")]
    Index {
        #[arg(long)]
        batch_size: Option<u32>,
        #[arg(long, default_value_t = false)]
        until_complete: bool,
        #[arg(long, default_value_t = false)]
        run_maintenance: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Search {
        query: String,
        #[arg(long, value_enum, default_value_t = MemoryScopeArg::Principal)]
        scope: MemoryScopeArg,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long)]
        tag: Vec<String>,
        #[arg(long, value_enum)]
        source: Vec<MemorySourceArg>,
        #[arg(long, default_value_t = false)]
        include_score_breakdown: bool,
        #[arg(long, default_value_t = false)]
        show_metadata: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Purge {
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        principal: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Ingest {
        content: String,
        #[arg(long, value_enum, default_value_t = MemorySourceArg::Manual)]
        source: MemorySourceArg,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        tag: Vec<String>,
        #[arg(long)]
        confidence: Option<String>,
        #[arg(long)]
        ttl_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Workspace {
        #[command(subcommand)]
        command: MemoryWorkspaceCommand,
    },
    Recall {
        query: String,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        memory_top_k: Option<u32>,
        #[arg(long)]
        workspace_top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long)]
        workspace_prefix: Option<String>,
        #[arg(long, default_value_t = false)]
        include_workspace_historical: bool,
        #[arg(long, default_value_t = false)]
        include_workspace_quarantined: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(name = "search-all")]
    SearchAll {
        query: String,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long)]
        workspace_prefix: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Learning {
        #[command(subcommand)]
        command: MemoryLearningCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum MemoryLearningCommand {
    List {
        #[arg(long)]
        candidate_kind: Option<String>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        risk_level: Option<String>,
        #[arg(long)]
        scope_kind: Option<String>,
        #[arg(long)]
        scope_id: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        min_confidence: Option<String>,
        #[arg(long)]
        max_confidence: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    History {
        candidate_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Review {
        candidate_id: String,
        status: String,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long, default_value_t = false)]
        apply_preference: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Preferences {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        scope_kind: Option<String>,
        #[arg(long)]
        scope_id: Option<String>,
        #[arg(long)]
        key: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    PromoteProcedure {
        candidate_id: String,
        #[arg(long)]
        skill_id: Option<String>,
        #[arg(long)]
        version: Option<String>,
        #[arg(long)]
        publisher: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value_t = true)]
        accept_candidate: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum MemoryWorkspaceCommand {
    List {
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        include_deleted: bool,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Get {
        path: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        include_deleted: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Write {
        path: String,
        content: String,
        #[arg(long)]
        title: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = false)]
        manual_override: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Move {
        path: String,
        next_path: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        path: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Pin {
        path: String,
        #[arg(long, default_value_t = true)]
        pinned: bool,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Versions {
        path: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Bootstrap {
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = false)]
        force_repair: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Search {
        query: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long)]
        prefix: Option<String>,
        #[arg(long)]
        top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long, default_value_t = false)]
        include_historical: bool,
        #[arg(long, default_value_t = false)]
        include_quarantined: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MemoryScopeArg {
    Session,
    Channel,
    Principal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum MemorySourceArg {
    TapeUserMessage,
    TapeToolResult,
    Summary,
    Manual,
    Import,
}
