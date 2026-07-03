//! Arguments for `palyra tasks`: unified runtime task timeline and WorkBoard
//! controls. Help text is pinned by snapshot tests; see `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum TasksCommand {
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        state: Option<String>,
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
    Timeline {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Cancel {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Pause {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Retry {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Workboard {
        #[command(subcommand)]
        command: WorkboardCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum WorkboardCommand {
    List {
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        parent_work_item_id: Option<String>,
        #[arg(long)]
        objective_id: Option<String>,
        #[arg(long)]
        routine_id: Option<String>,
        #[arg(long, default_value_t = false)]
        include_terminal: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Create {
        #[arg(long)]
        title: String,
        #[arg(long)]
        summary: Option<String>,
        #[arg(long)]
        priority: Option<i64>,
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        parent_work_item_id: Option<String>,
        #[arg(long)]
        objective_id: Option<String>,
        #[arg(long)]
        routine_id: Option<String>,
        #[arg(long)]
        verification_state: Option<String>,
        #[arg(long)]
        dependencies_json: Option<String>,
        #[arg(long)]
        evidence_refs_json: Option<String>,
        #[arg(long)]
        artifact_refs_json: Option<String>,
        #[arg(long)]
        blocker_json: Option<String>,
        #[arg(long)]
        metadata_json: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Update {
        #[arg(long)]
        id: String,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        priority: Option<i64>,
        #[arg(long)]
        assigned_worker: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_assigned_worker: bool,
        #[arg(long)]
        parent_work_item_id: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_parent_work_item: bool,
        #[arg(long)]
        objective_id: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_objective: bool,
        #[arg(long)]
        routine_id: Option<String>,
        #[arg(long, default_value_t = false)]
        clear_routine: bool,
        #[arg(long)]
        verification_state: Option<String>,
        #[arg(long)]
        dependencies_json: Option<String>,
        #[arg(long)]
        evidence_refs_json: Option<String>,
        #[arg(long)]
        artifact_refs_json: Option<String>,
        #[arg(long)]
        blocker_json: Option<String>,
        #[arg(long)]
        metadata_json: Option<String>,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Block {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        blocker_json: Option<String>,
        #[arg(long)]
        evidence_refs_json: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    LinkArtifact {
        #[arg(long)]
        id: String,
        #[arg(long)]
        artifact_ref_json: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Claim {
        #[arg(long)]
        id: String,
        #[arg(long)]
        worker: Option<String>,
        #[arg(long)]
        lease_ms: Option<i64>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Heartbeat {
        #[arg(long)]
        id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Complete {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        evidence_refs_json: Option<String>,
        #[arg(long)]
        artifact_refs_json: Option<String>,
        #[arg(long)]
        verification_state: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
