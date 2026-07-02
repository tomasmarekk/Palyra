//! Arguments for `palyra qa`: QA Lab scenario validation and discovery.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum QaCommand {
    #[command(about = "Validate QA Lab scenario manifests")]
    Validate {
        #[arg(long, default_value = "qa/scenarios")]
        path: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Run a local deterministic QA scenario pack")]
    RunPack {
        #[arg(long, default_value = "qa/scenarios")]
        path: String,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long)]
        output: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Validate provider compatibility fixtures")]
    ProviderCompat {
        #[arg(long, default_value = "fixtures/provider_compat")]
        path: String,
        #[arg(long)]
        output: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Run a QA Lab suite gate and maturity scorecard")]
    Gate {
        #[arg(long, default_value = "qa/suites/pr_smoke.yaml")]
        suite: String,
        #[arg(long)]
        output_json: Option<String>,
        #[arg(long)]
        output_markdown: Option<String>,
        #[arg(long, default_value_t = false)]
        allow_live: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
