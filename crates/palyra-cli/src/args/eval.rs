//! Arguments for `palyra eval`: local eval bundle creation and replay metadata.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum EvalCommand {
    Bundle {
        #[command(subcommand)]
        command: EvalBundleCommand,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum EvalBundleCommand {
    Create {
        #[arg(long, default_value = "palyra-eval-bundle")]
        name: String,
        #[arg(long)]
        output: String,
        #[arg(long)]
        run_id: Vec<String>,
        #[arg(long)]
        run_export: Vec<String>,
        #[arg(long)]
        replay_bundle: Vec<String>,
        #[arg(long)]
        scenario_manifest: Option<String>,
        #[arg(long)]
        memory_fixture: Vec<String>,
        #[arg(long)]
        journal_db: Option<String>,
        #[arg(long, default_value_t = 128)]
        max_events: usize,
        #[arg(long, default_value_t = true)]
        fake_provider: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
