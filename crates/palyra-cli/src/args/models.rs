use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum ModelsCommand {
    Status {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
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
    Set {
        model: String,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    SetEmbeddings {
        model: String,
        #[arg(long)]
        dims: Option<u32>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long, default_value_t = 5)]
        backups: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
