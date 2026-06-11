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
