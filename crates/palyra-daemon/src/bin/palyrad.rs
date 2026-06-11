//! `palyrad` binary entry point.
//!
//! All daemon behavior (CLI parsing, config load, listeners, lifecycle) lives in
//! `palyra_daemon::run`; this shim only provides the Tokio runtime and exit code.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    palyra_daemon::run().await
}
