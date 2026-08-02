//! `palyrad` binary entry point.
//!
//! The exact hidden Unix supervisor mode runs before Tokio or daemon initialization; normal
//! invocations construct the async runtime and delegate to `palyra_daemon::run`.

use anyhow::Result;

// Run-stream tool orchestration composes cancellation, journal, provider, and
// transport futures. Keep the daemon's worker-stack contract deterministic in
// the unoptimized QA binary as well as release builds.
const RUNTIME_THREAD_STACK_BYTES: usize = 8 * 1024 * 1024;

fn main() -> Result<()> {
    palyra_daemon::dispatch_internal_process_supervisor();
    palyra_daemon::dispatch_internal_codex_app_server_bridge();
    palyra_daemon::dispatch_internal_document_extractor();
    tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(RUNTIME_THREAD_STACK_BYTES)
        .enable_all()
        .build()?
        .block_on(palyra_daemon::run())
}
