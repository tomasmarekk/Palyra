//! Arguments for `palyra models`: model-provider status, catalog listing,
//! connectivity tests, live discovery, routing explanation, and default chat or
//! embeddings model selection. Help text is pinned by snapshot tests; see the
//! doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ModelsCommand {
    #[command(about = "Show the effective model-provider configuration")]
    Status {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "List configured and available model catalog entries")]
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Verify model-provider credentials and connectivity")]
    TestConnection {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        refresh: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Discover live provider models with registry fallback")]
    Discover {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        provider: Option<String>,
        #[arg(long, default_value_t = 5_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = false)]
        refresh: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Explain model routing and provider candidate selection")]
    Explain {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value_t = false)]
        json_mode: bool,
        #[arg(long, default_value_t = false)]
        vision: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Set the default chat model")]
    Set {
        model: String,
        #[arg(long)]
        path: Option<String>,
        #[arg(
            long,
            visible_aliases = ["reasoning-effort", "reasoning-level"],
            help = "Set provider reasoning effort with the chat model: none, minimal, low, medium, high, or xhigh"
        )]
        reasoning: Option<String>,
        #[arg(
            long,
            default_value_t = false,
            conflicts_with_all = ["no_fast", "service_tier"],
            help = "Persist fast provider processing for the default chat model when supported"
        )]
        fast: bool,
        #[arg(
            long = "no-fast",
            default_value_t = false,
            conflicts_with_all = ["fast", "service_tier"],
            help = "Persist the provider default processing tier for the default chat model"
        )]
        no_fast: bool,
        #[arg(
            long,
            value_name = "TIER",
            conflicts_with_all = ["fast", "no_fast"],
            help = "Persist provider service tier with the chat model: auto, default, priority, or flex"
        )]
        service_tier: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        allow_custom: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(about = "Set the default embeddings model")]
    SetEmbeddings {
        model: String,
        #[arg(long)]
        dims: Option<u32>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        allow_custom: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
