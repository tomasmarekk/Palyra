//! Batch loader of per-session project-context previews for session-list
//! console views, wrapping `application::project_context` per session.

use std::{collections::HashMap, sync::Arc};

use tonic::Status;

use crate::{
    application::project_context::{preview_project_context, ProjectContextPreviewEnvelope},
    gateway::{GatewayRuntimeState, RequestContext},
    journal,
};

/// Builds a session-id -> project-context preview map for the given
/// sessions, without deriving or persisting any focus-path changes.
///
/// Sessions whose preview fails with `FailedPrecondition` (no workspace
/// roots configured) or `NotFound` are skipped instead of failing the whole
/// listing: those are expected states for sessions without a bound
/// workspace agent.
///
/// # Errors
/// Propagates any other `Status` from the underlying previews.
pub(crate) async fn load_project_context_summaries(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    sessions: &[journal::OrchestratorSessionRecord],
) -> Result<HashMap<String, ProjectContextPreviewEnvelope>, Status> {
    let mut previews = HashMap::new();
    for session in sessions {
        match preview_project_context(
            runtime_state,
            context,
            session.session_id.as_str(),
            "",
            false,
        )
        .await
        {
            Ok(preview) => {
                previews.insert(session.session_id.clone(), preview);
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::FailedPrecondition | tonic::Code::NotFound
                ) => {}
            Err(status) => return Err(status),
        }
    }
    Ok(previews)
}
