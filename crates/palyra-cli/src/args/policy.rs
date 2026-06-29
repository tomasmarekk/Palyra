//! Arguments for `palyra policy`: explaining a single principal/action/resource
//! policy decision. Help text is pinned by snapshot tests; see the doc-comment
//! rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum PolicyCommand {
    Explain {
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "tool.execute.shell")]
        action: String,
        #[arg(long, default_value = "tool:shell")]
        resource: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Conformance {
        #[arg(long)]
        baseline: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Diff {
        #[arg(long)]
        baseline: String,
        #[arg(long)]
        candidate: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Posture {
        #[arg(long)]
        session: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ToolPosture {
        #[arg(long)]
        catalog: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
