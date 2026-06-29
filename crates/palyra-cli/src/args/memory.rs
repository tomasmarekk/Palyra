//! Arguments for `palyra memory`: durable memory items, indexing and drift
//! reconciliation, recall and cross-surface search, curated workspace
//! documents, and learning candidate review. Help text is pinned by snapshot
//! tests; see the doc-comment rules in `mod.rs`.

use clap::{Subcommand, ValueEnum};

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum MemoryCommand {
    #[command(visible_alias = "list")]
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Doctor {
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
    #[command(name = "index-drift")]
    IndexDrift {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(name = "index-reconcile")]
    IndexReconcile {
        #[arg(long)]
        batch_size: Option<u32>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Search {
        query: String,
        #[arg(long, value_enum, default_value_t = MemoryScopeArg::Principal)]
        scope: MemoryScopeArg,
        #[arg(
            long,
            value_name = "SESSION_ID_OR_KEY",
            help = "Session canonical ULID or user-facing session key for session-scoped memory"
        )]
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
    Get {
        memory_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        memory_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Replace {
        memory_id: String,
        content: String,
        #[arg(long, value_enum)]
        source: Option<MemorySourceArg>,
        #[arg(long)]
        tag: Vec<String>,
        #[arg(
            long,
            value_name = "0.0..1.0",
            help = "Confidence score in the inclusive range 0.0..1.0"
        )]
        confidence: Option<String>,
        #[arg(long)]
        ttl_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Purge {
        #[arg(
            long,
            value_name = "SESSION_ID_OR_KEY",
            help = "Session canonical ULID or user-facing session key for session-scoped purge"
        )]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        principal: bool,
        #[arg(
            long,
            default_value_t = false,
            help = "Confirm irreversible deletion for the selected memory scope"
        )]
        yes: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Ingest {
        content: String,
        #[arg(long, value_enum, default_value_t = MemorySourceArg::Manual)]
        source: MemorySourceArg,
        #[arg(
            long,
            value_name = "SESSION_ID_OR_KEY",
            help = "Session canonical ULID or user-facing session key for session-scoped ingest"
        )]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        tag: Vec<String>,
        #[arg(
            long,
            value_name = "0.0..1.0",
            help = "Confidence score in the inclusive range 0.0..1.0"
        )]
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
        #[arg(
            value_name = "QUERY",
            required_unless_present = "query_option",
            conflicts_with = "query_option"
        )]
        query: Option<String>,
        #[arg(long = "query", value_name = "QUERY", conflicts_with = "query")]
        query_option: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        agent_id: Option<String>,
        #[arg(long, visible_alias = "limit")]
        top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long)]
        workspace_prefix: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(name = "session-search")]
    SessionSearch {
        query: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        top_k: Option<u32>,
        #[arg(long)]
        min_score: Option<String>,
        #[arg(long)]
        window_before: Option<u32>,
        #[arg(long)]
        window_after: Option<u32>,
        #[arg(long)]
        max_windows_per_session: Option<u32>,
        #[arg(long, default_value_t = false)]
        include_archived: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(name = "recall-artifacts")]
    RecallArtifacts {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
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
    Eval {
        candidate_id: String,
        #[arg(long)]
        suite: String,
        #[arg(long)]
        result: String,
        #[arg(long, value_name = "0.0..1.0")]
        threshold: String,
        #[arg(long, value_name = "0.0..1.0")]
        score: String,
        #[arg(long)]
        decision: String,
        #[arg(long)]
        policy_decision: Option<String>,
        #[arg(long)]
        evidence_refs_json: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Apply {
        candidate_id: String,
        #[arg(long)]
        summary: Option<String>,
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
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            value_parser = clap::value_parser!(bool)
        )]
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
    #[command(
        about = "Write a workspace memory document under an allowed curated root",
        long_about = "Write a workspace memory document under an allowed curated root.\n\nAllowed roots: README.md, MEMORY.md, HEARTBEAT.md, context/, daily/, projects/. Run `palyra memory status` to inspect workspace memory."
    )]
    Write {
        #[arg(
            help = "Workspace path. Allowed roots: README.md, MEMORY.md, HEARTBEAT.md, context/, daily/, projects/."
        )]
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
        #[arg(
            long,
            default_value_t = true,
            action = clap::ArgAction::Set,
            value_parser = clap::value_parser!(bool)
        )]
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
