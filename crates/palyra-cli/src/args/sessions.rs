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
}
