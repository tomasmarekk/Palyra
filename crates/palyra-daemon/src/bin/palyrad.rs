//! `palyrad` binary entry point.
//!
//! The exact hidden Unix supervisor mode runs before Tokio or daemon initialization; normal
//! invocations construct the async runtime and delegate to `palyra_daemon::run`.

use anyhow::Result;

fn main() -> Result<()> {
    palyra_daemon::dispatch_internal_process_supervisor();
    palyra_daemon::dispatch_internal_codex_app_server_bridge();
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(palyra_daemon::run())
}
