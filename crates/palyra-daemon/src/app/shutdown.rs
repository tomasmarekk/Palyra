//! Operating-system signal adapter for the process-wide lifecycle controller.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    application::daemon_lifecycle::{DaemonDrainRequest, DaemonDrainTrigger, DrainAdmissionPolicy},
    gateway::GatewayRuntimeState,
};

const DEFAULT_SIGNAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Starts the sole SIGINT/SIGTERM listener.
///
/// The task translates an operating-system signal into the same durable drain
/// request used by the admin and restart surfaces.
#[must_use]
pub(crate) fn spawn_shutdown_signal_listener(
    runtime: Arc<GatewayRuntimeState>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let trigger = wait_for_shutdown_signal().await;
        let deadline_unix_ms = unix_ms_now().saturating_add(
            i64::try_from(DEFAULT_SIGNAL_DRAIN_TIMEOUT.as_millis()).unwrap_or(i64::MAX),
        );
        let request = DaemonDrainRequest {
            trigger,
            reason_code: match trigger {
                DaemonDrainTrigger::Sigint => "daemon.lifecycle.sigint",
                DaemonDrainTrigger::Sigterm => "daemon.lifecycle.sigterm",
                DaemonDrainTrigger::Admin | DaemonDrainTrigger::ConfigRestart => {
                    "daemon.lifecycle.signal"
                }
            }
            .to_owned(),
            requested_by: "system:signal".to_owned(),
            deadline_unix_ms,
            admission_policy: DrainAdmissionPolicy::RejectNew,
        };
        if let Err(error) = runtime.begin_daemon_drain(request).await {
            tracing::error!(
                code = %error.code(),
                message = %error.message(),
                "operating-system signal could not complete coordinated daemon drain"
            );
        }
    })
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> DaemonDrainTrigger {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(error) => {
            tracing::error!(error = %error, "failed to register SIGTERM handler");
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::error!(error = %error, "failed to register SIGINT handler");
                std::future::pending::<()>().await;
            }
            return DaemonDrainTrigger::Sigint;
        }
    };
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(error) = result {
                tracing::error!(error = %error, "failed to register SIGINT handler");
                terminate.recv().await;
                DaemonDrainTrigger::Sigterm
            } else {
                DaemonDrainTrigger::Sigint
            }
        }
        _ = terminate.recv() => DaemonDrainTrigger::Sigterm,
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> DaemonDrainTrigger {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register SIGINT handler");
        std::future::pending::<()>().await;
    }
    DaemonDrainTrigger::Sigint
}

fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}
