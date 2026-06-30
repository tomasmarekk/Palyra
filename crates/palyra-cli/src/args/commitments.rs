//! Arguments for `palyra commitments`: extraction, review, source inspection,
//! and scheduling lifecycle controls. Help text is pinned by snapshot tests.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum CommitmentsCommand {
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        due_before_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        include_terminal: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Sources {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Explain {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Extract {
        #[arg(long)]
        text: String,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        extraction_model: Option<String>,
        #[arg(long, default_value_t = false)]
        include_inferred: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Approve {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        due_at_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Dismiss {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Snooze {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        due_at_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Edit {
        #[arg(long)]
        id: String,
        #[arg(long)]
        user_wording: Option<String>,
        #[arg(long)]
        normalized_action: Option<String>,
        #[arg(long)]
        due_at_unix_ms: Option<i64>,
        #[arg(long)]
        privacy_label: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Schedule {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        due_at_unix_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
