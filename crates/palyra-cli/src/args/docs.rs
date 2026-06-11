//! Arguments for `palyra docs`: offline discovery (list/search/show) of
//! committed and bundled CLI help snapshots. Help text is pinned by snapshot
//! tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum DocsCommand {
    List {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Search {
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Show {
        slug_or_path: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
