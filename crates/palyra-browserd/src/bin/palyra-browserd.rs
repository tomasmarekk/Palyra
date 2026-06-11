//! Binary entry point for `palyra-browserd`.
//!
//! All daemon logic lives in the library crate; this shim only starts the
//! Tokio runtime and delegates to [`palyra_browserd::run`].

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    palyra_browserd::run().await
}
