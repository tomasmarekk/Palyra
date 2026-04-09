use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum SessionsCommand {
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
    #[command(visible_alias = "search")]
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
    #[command(visible_alias = "resume")]
    Show {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        session_key: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
    Rename {
        session_id: String,
        #[arg(long)]
        session_label: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Reset {
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
    Abort {
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Retry {
        session_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Branch {
        session_id: String,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    TranscriptSearch {
        session_id: String,
        #[arg(long)]
        query: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Export {
        session_id: String,
        #[arg(long, default_value = "json")]
        format: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    CompactPreview {
        session_id: String,
        #[arg(long)]
        trigger_reason: Option<String>,
        #[arg(long)]
        trigger_policy: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
    CompactionShow {
        artifact_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
    CheckpointShow {
        checkpoint_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    CheckpointRestore {
        checkpoint_id: String,
        #[arg(long)]
        session_label: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BackgroundEnqueue {
        session_id: String,
        #[arg(long)]
        text: String,
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
    BackgroundShow {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BackgroundPause {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BackgroundResume {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BackgroundRetry {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    BackgroundCancel {
        task_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
