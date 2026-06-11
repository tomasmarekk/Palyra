//! Graceful-shutdown signal future for the daemon's servers.

/// Resolves when the process receives Ctrl+C (SIGINT), signalling graceful shutdown.
pub(crate) async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        // Without a signal handler there is no shutdown trigger to wait for;
        // park forever so the server keeps running instead of exiting early.
        std::future::pending::<()>().await;
    }
}
