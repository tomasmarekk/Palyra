//! Process-wide tracing/log initialization for the daemon.

use tracing_subscriber::EnvFilter;

/// Installs the global JSON tracing subscriber, filtered by `RUST_LOG`
/// (falling back to `info` when the variable is unset or invalid).
///
/// # Panics
///
/// Panics if a global subscriber has already been installed; call exactly once
/// at process startup.
pub(crate) fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
}
