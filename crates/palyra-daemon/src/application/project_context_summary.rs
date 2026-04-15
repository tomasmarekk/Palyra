use std::{collections::HashMap, sync::Arc};

use tonic::Status;

use crate::{
    application::project_context::{preview_project_context, ProjectContextPreviewEnvelope},
    gateway::{GatewayRuntimeState, RequestContext},
    journal,
};

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
