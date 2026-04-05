use crate::{
    access_control::{
        AccessRegistry, AccessRegistryError, ApiTokenCreateRequest, InvitationCreateRequest,
        ResourceShareUpsertRequest, WorkspaceCreateRequest,
    },
    *,
};

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleFeatureFlagMutationRequest {
    enabled: bool,
    stage: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleApiTokenCreatePayload {
    label: String,
    #[serde(default)]
    scopes: Vec<String>,
    principal: String,
    workspace_id: Option<String>,
    role: String,
    expires_at_unix_ms: Option<i64>,
    rate_limit_per_minute: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleWorkspaceCreatePayload {
    team_name: String,
    workspace_name: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleInvitationCreatePayload {
    workspace_id: String,
    invited_identity: String,
    role: String,
    expires_at_unix_ms: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleInvitationAcceptPayload {
    invitation_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleMembershipRolePayload {
    workspace_id: String,
    member_principal: String,
    role: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleMembershipRemovePayload {
    workspace_id: String,
    member_principal: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleShareUpsertPayload {
    resource_kind: String,
    resource_id: String,
    workspace_id: String,
    access_level: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConsoleAccessBackfillPayload {
    #[serde(default)]
    dry_run: bool,
}

pub(crate) async fn console_access_snapshot_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let snapshot = {
        let registry = lock_access_registry(&state.access_registry);
        registry.snapshot(session.context.principal.as_str())
    };
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "snapshot": snapshot,
    })))
}

pub(crate) async fn console_access_backfill_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleAccessBackfillPayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let report = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.run_backfill(session.context.principal.as_str(), payload.dry_run, now)
    }
    .map_err(access_registry_error_response)?;
    let snapshot = {
        let registry = lock_access_registry(&state.access_registry);
        registry.snapshot(session.context.principal.as_str())
    };
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "backfill": report,
        "snapshot": snapshot,
    })))
}

pub(crate) async fn console_access_memberships_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let memberships = {
        let registry = lock_access_registry(&state.access_registry);
        registry.list_visible_workspace_memberships(session.context.principal.as_str())
    };
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "memberships": memberships,
    })))
}

pub(crate) async fn console_access_feature_flag_set_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(feature_key): Path<String>,
    Json(payload): Json<ConsoleFeatureFlagMutationRequest>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let record = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.set_feature_flag(
            feature_key.as_str(),
            payload.enabled,
            payload.stage,
            session.context.principal.as_str(),
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "feature_flag": record,
    })))
}

pub(crate) async fn console_access_api_tokens_list_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, false)?;
    let api_tokens = {
        let registry = lock_access_registry(&state.access_registry);
        registry.list_api_tokens(session.context.principal.as_str())
    };
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "api_tokens": api_tokens,
    })))
}

pub(crate) async fn console_access_api_token_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleApiTokenCreatePayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let created = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.create_api_token(
            session.context.principal.as_str(),
            ApiTokenCreateRequest {
                label: payload.label,
                scopes: payload.scopes,
                principal: payload.principal,
                workspace_id: payload.workspace_id,
                role: payload.role,
                expires_at_unix_ms: payload.expires_at_unix_ms,
                rate_limit_per_minute: payload.rate_limit_per_minute,
            },
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "created": created,
    })))
}

pub(crate) async fn console_access_api_token_rotate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(token_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let rotated = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.rotate_api_token(session.context.principal.as_str(), token_id.as_str(), now)
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "rotated": rotated,
    })))
}

pub(crate) async fn console_access_api_token_revoke_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(token_id): Path<String>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let revoked = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.revoke_api_token(session.context.principal.as_str(), token_id.as_str(), now)
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "revoked": revoked,
    })))
}

pub(crate) async fn console_access_workspace_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleWorkspaceCreatePayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let created = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.create_workspace_bundle(
            session.context.principal.as_str(),
            WorkspaceCreateRequest {
                team_name: payload.team_name,
                workspace_name: payload.workspace_name,
            },
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "created": created,
    })))
}

pub(crate) async fn console_access_invitation_create_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleInvitationCreatePayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let created = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.create_invitation(
            session.context.principal.as_str(),
            InvitationCreateRequest {
                workspace_id: payload.workspace_id,
                invited_identity: payload.invited_identity,
                role: payload.role,
                expires_at_unix_ms: payload.expires_at_unix_ms,
            },
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "created": created,
    })))
}

pub(crate) async fn console_access_invitation_accept_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleInvitationAcceptPayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let membership = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.accept_invitation(
            session.context.principal.as_str(),
            payload.invitation_token.as_str(),
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "membership": membership,
    })))
}

pub(crate) async fn console_access_membership_role_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMembershipRolePayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let membership = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.update_membership_role(
            session.context.principal.as_str(),
            payload.workspace_id.as_str(),
            payload.member_principal.as_str(),
            payload.role.as_str(),
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "membership": membership,
    })))
}

pub(crate) async fn console_access_membership_remove_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleMembershipRemovePayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.remove_membership(
            session.context.principal.as_str(),
            payload.workspace_id.as_str(),
            payload.member_principal.as_str(),
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "removed": true,
    })))
}

pub(crate) async fn console_access_share_upsert_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<ConsoleShareUpsertPayload>,
) -> Result<Json<Value>, Response> {
    let session = authorize_console_session(&state, &headers, true)?;
    let now = unix_ms_now().map_err(internal_clock_error_response)?;
    let share = {
        let mut registry = lock_access_registry(&state.access_registry);
        registry.upsert_resource_share(
            session.context.principal.as_str(),
            ResourceShareUpsertRequest {
                resource_kind: payload.resource_kind,
                resource_id: payload.resource_id,
                workspace_id: payload.workspace_id,
                access_level: payload.access_level,
            },
            now,
        )
    }
    .map_err(access_registry_error_response)?;
    Ok(Json(json!({
        "contract": contract_descriptor(),
        "share": share,
    })))
}

pub(crate) fn lock_access_registry<'a>(
    registry: &'a Arc<Mutex<AccessRegistry>>,
) -> std::sync::MutexGuard<'a, AccessRegistry> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("access registry lock poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

fn internal_clock_error_response(error: impl std::fmt::Display) -> Response {
    runtime_status_response(tonic::Status::internal(format!(
        "failed to read system clock: {error}"
    )))
}

fn access_registry_error_response(error: AccessRegistryError) -> Response {
    match error {
        AccessRegistryError::InvalidField { field, message } => {
            validation_error_response(field, "invalid", message.as_str())
        }
        AccessRegistryError::FeatureFlagNotFound(_)
        | AccessRegistryError::ApiTokenNotFound(_)
        | AccessRegistryError::TeamNotFound(_)
        | AccessRegistryError::WorkspaceNotFound(_)
        | AccessRegistryError::InvitationNotFound => {
            runtime_status_response(tonic::Status::not_found(error.to_string()))
        }
        AccessRegistryError::InvitationExpired
        | AccessRegistryError::InvitationAlreadyAccepted
        | AccessRegistryError::AccessDenied(_)
        | AccessRegistryError::FeatureDisabled(_)
        | AccessRegistryError::InvalidApiToken
        | AccessRegistryError::MissingScope(_) => {
            runtime_status_response(tonic::Status::permission_denied(error.to_string()))
        }
        AccessRegistryError::ReadRegistry { .. }
        | AccessRegistryError::WriteRegistry { .. }
        | AccessRegistryError::ParseRegistry { .. }
        | AccessRegistryError::SerializeRegistry(_) => {
            runtime_status_response(tonic::Status::internal(error.to_string()))
        }
    }
}
