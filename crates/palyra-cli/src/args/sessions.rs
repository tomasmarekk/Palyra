//! Arguments for `palyra sessions`: chat session listing and history, session
//! resolution, queue control, retry/branch, transcript search and export,
//! compaction, checkpoints, and background task management. Help text is pinned
//! by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::{ArgGroup, Subcommand};

use super::AgentApprovalModeArg;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SessionsCommand {
    #[command(about = "List known chat sessions")]
    List {
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        include_archived: bool,
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    #[command(visible_alias = "search", about = "Search or summarize session history")]
    History {
        #[arg(long)]
        query: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        include_archived: bool,
        #[arg(long, default_value_t = false)]
        resume_first: bool,
        #[arg(long, default_value_t = false, conflicts_with = "ndjson")]
        json: bool,
        #[arg(long, default_value_t = false, conflicts_with = "json")]
        ndjson: bool,
    },
    #[command(visible_alias = "resume", about = "Show one session by id or key")]
    Show {
        #[arg(long, help = "Show this exact session id")]
        session_id: Option<String>,
        #[arg(long, help = "Show the session resolved from this stable key")]
        session_key: Option<String>,
        #[arg(long, default_value_t = false, help = "Print the session as JSON")]
        json: bool,
    },
    #[command(about = "Resolve a session selector into a concrete session")]
    Resolve {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        require_existing: bool,
        #[arg(long, default_value_t = false)]
        reset: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Rename a session label")]
    Rename {
        #[arg(value_name = "SESSION_ID", required_unless_present = "session_key")]
        session_id: Option<String>,
        #[arg(long, conflicts_with = "session_id")]
        session_key: Option<String>,
        #[arg(long, alias = "title")]
        session_label: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Reset a session")]
    Reset {
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Clean up session state after confirmation")]
    Cleanup {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Abort a running session run")]
    Abort {
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show queue policy for a session")]
    QueuePolicy {
        #[arg(value_name = "SESSION_ID", required_unless_present = "session_key")]
        session_id: Option<String>,
        #[arg(long, conflicts_with = "session_id")]
        session_key: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Pause queued work for a session")]
    QueuePause {
        session_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Resume queued work for a session")]
    QueueResume {
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Drain queued work for a session")]
    QueueDrain {
        session_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Collect a summary from queued work")]
    QueueCollectSummary {
        session_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Cancel one queued input")]
    QueueCancel {
        session_id: String,
        queued_input_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Retry the latest failed or interrupted turn")]
    Retry {
        session_id: String,
        #[arg(long, default_value_t = false)]
        allow_sensitive_tools: bool,
        #[arg(long, value_enum, default_value_t = AgentApprovalModeArg::AllowOnce)]
        approval_mode: AgentApprovalModeArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Branch a session into a new label")]
    Branch {
        session_id: String,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Search within a session transcript")]
    TranscriptSearch {
        session_id: String,
        #[arg(long)]
        query: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Export a session transcript")]
    Export {
        session_id: String,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Preview session compaction candidates")]
    CompactPreview {
        #[arg(value_name = "SESSION_ID", required_unless_present = "session_key")]
        session_id: Option<String>,
        #[arg(long, conflicts_with = "session_id")]
        session_key: Option<String>,
        #[arg(long)]
        trigger_reason: Option<String>,
        #[arg(long)]
        trigger_policy: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Apply session compaction decisions")]
    CompactApply {
        session_id: String,
        #[arg(long)]
        trigger_reason: Option<String>,
        #[arg(long)]
        trigger_policy: Option<String>,
        #[arg(long = "accept-candidate")]
        accept_candidate_ids: Vec<String>,
        #[arg(long = "reject-candidate")]
        reject_candidate_ids: Vec<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show a compaction artifact")]
    CompactionShow {
        artifact_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Create a session checkpoint")]
    CheckpointCreate {
        session_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        note: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show a session checkpoint")]
    CheckpointShow {
        checkpoint_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Restore a session checkpoint")]
    CheckpointRestore {
        checkpoint_id: String,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Enqueue background work for a session")]
    #[command(group(
        ArgGroup::new("background_enqueue_text_source")
            .required(true)
            .args(["text", "text_stdin"])
    ))]
    BackgroundEnqueue {
        session_id: String,
        #[arg(
            long,
            help = "Single-line task text. Use --text-stdin for multi-line or blank-line separated tasks."
        )]
        text: Option<String>,
        #[arg(long, default_value_t = false)]
        text_stdin: bool,
        #[arg(long)]
        priority: Option<i64>,
        #[arg(long)]
        max_attempts: Option<u64>,
        #[arg(long)]
        budget_tokens: Option<u64>,
        #[arg(long)]
        not_before_unix_ms: Option<i64>,
        #[arg(long)]
        expires_at_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "List background tasks")]
    BackgroundList {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long, default_value_t = false)]
        include_completed: bool,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Show one background task")]
    BackgroundShow {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Pause one background task")]
    BackgroundPause {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Resume one background task")]
    BackgroundResume {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Retry one background task")]
    BackgroundRetry {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Cancel one background task")]
    BackgroundCancel {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
