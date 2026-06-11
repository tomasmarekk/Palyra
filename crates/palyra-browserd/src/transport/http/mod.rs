//! Plaintext HTTP admin surface for browserd.
//!
//! Exposes only the `/healthz` liveness probe; all browser operations go
//! through the gRPC service. The probe is unauthenticated, so bootstrap
//! restricts this listener to loopback unless an auth token is configured.

use crate::*;

/// Shared axum state giving handlers access to the daemon runtime.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) runtime: Arc<BrowserRuntimeState>,
}

/// Builds the admin router serving the `/healthz` liveness endpoint.
pub(crate) fn build_router(runtime: Arc<BrowserRuntimeState>) -> Router {
    Router::new().route("/healthz", get(health_handler)).with_state(AppState { runtime })
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyra-browserd", state.runtime.started_at))
}
