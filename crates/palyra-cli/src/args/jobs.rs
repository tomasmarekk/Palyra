//! Arguments for `palyra jobs`: durable long-running tool job inspection and
//! control (tail, cancel/drain/resume/retry, attach/release, and stale/expired
//! sweeps). Help text is pinned by snapshot tests; see the doc-comment rules in
//! `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Clone, Subcommand, PartialEq, Eq)]
pub enum JobsCommand {
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        include_terminal: bool,
        #[arg(long)]
        json: bool,
    },
    Show {
        id: String,
        #[arg(long)]
        json: bool,
    },
    Tail {
        id: String,
        #[arg(long)]
        offset: Option<i64>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        max_bytes: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    Cancel {
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Drain {
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Resume {
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Retry {
        id: String,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Attach {
        id: String,
        #[arg(long)]
        json: bool,
    },
    Release {
        id: String,
        #[arg(long)]
        json: bool,
    },
    SweepExpired {
        #[arg(long)]
        now_unix_ms: Option<i64>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        json: bool,
    },
    RecoverStale {
        #[arg(long)]
        now_unix_ms: Option<i64>,
        #[arg(long)]
        stale_after_ms: Option<i64>,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        json: bool,
    },
}
