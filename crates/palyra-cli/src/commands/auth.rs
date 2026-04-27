use crate::*;
use palyra_control_plane as control_plane;

pub(crate) fn run_auth(command: AuthCommand) -> Result<()> {
    match command {
        AuthCommand::Profiles { command } => {
            let runtime = build_runtime()?;
            if auth_profiles_command_uses_control_plane(&command) {
                runtime.block_on(run_auth_profiles_control_plane_async(command))
            } else {
                let root_context = app::current_root_context()
                    .ok_or_else(|| anyhow!("CLI root context is unavailable for auth command"))?;
                let connection = root_context.resolve_grpc_connection(
                    app::ConnectionOverrides::default(),
                    app::ConnectionDefaults::ADMIN,
                )?;
                runtime.block_on(run_auth_profiles_async(
                    AuthCommand::Profiles { command },
                    connection,
                ))
            }
        }
        AuthCommand::Access { .. } => {
            let runtime = build_runtime()?;
            runtime.block_on(run_auth_access_async(command))
        }
        AuthCommand::Openai { command } => {
            let runtime = build_runtime()?;
            runtime.block_on(run_auth_openai_async(command))
        }
    }
}

fn auth_profiles_command_uses_control_plane(command: &AuthProfilesCommand) -> bool {
    matches!(
        command,
        AuthProfilesCommand::Doctor { .. }
            | AuthProfilesCommand::Audit { .. }
            | AuthProfilesCommand::CooldownClear { .. }
            | AuthProfilesCommand::OrderSet { .. }
            | AuthProfilesCommand::ExplainSelection { .. }
    )
}

pub(crate) async fn run_auth_profiles_async(
    command: AuthCommand,
    connection: AgentConnection,
) -> Result<()> {
    let mut client =
        auth_v1::auth_service_client::AuthServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect auth gRPC endpoint {}", connection.grpc_url)
            })?;

    let AuthCommand::Profiles { command } = command else {
        anyhow::bail!("auth profiles command dispatch received an incompatible auth command");
    };
    match command {
        AuthProfilesCommand::List {
            after,
            limit,
            provider,
            provider_name,
            scope,
            agent_id,
            json,
        } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(auth_v1::ListAuthProfilesRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_profile_id: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                provider_kind: provider
                    .map(auth_provider_arg_to_proto)
                    .unwrap_or(auth_v1::AuthProviderKind::Unspecified as i32),
                provider_custom_name: provider_name.unwrap_or_default(),
                scope_kind: scope
                    .map(auth_scope_arg_to_proto)
                    .unwrap_or(auth_v1::AuthScopeKind::Unspecified as i32),
                scope_agent_id: agent_id.unwrap_or_default(),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.list_profiles(request).await.context("failed to call auth ListProfiles")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "profiles": payload.profiles.iter().map(auth_profile_to_json).collect::<Vec<_>>(),
                        "next_after_profile_id": empty_to_none(payload.next_after_profile_id),
                    }))?
                );
            } else {
                println!(
                    "auth.profiles.list count={} next_after={}",
                    payload.profiles.len(),
                    if payload.next_after_profile_id.is_empty() {
                        "none"
                    } else {
                        payload.next_after_profile_id.as_str()
                    }
                );
                for profile in &payload.profiles {
                    println!(
                        "auth.profile id={} provider={} scope={} credential={}",
                        profile.profile_id,
                        auth_provider_to_text(profile.provider.as_ref()),
                        auth_scope_to_text(profile.scope.as_ref()),
                        auth_profile_credential_type(profile)
                    );
                }
            }
        }
        AuthProfilesCommand::Show { profile_id, json } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(auth_v1::GetAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile_id,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_profile(request).await.context("failed to call auth GetProfile")?;
            let profile = response
                .into_inner()
                .profile
                .context("auth GetProfile returned empty profile payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&auth_profile_to_json(&profile))?);
            } else {
                println!(
                    "auth.profiles.show id={} provider={} scope={} credential={} updated_at_ms={}",
                    profile.profile_id,
                    auth_provider_to_text(profile.provider.as_ref()),
                    auth_scope_to_text(profile.scope.as_ref()),
                    auth_profile_credential_type(&profile),
                    profile.updated_at_unix_ms
                );
            }
        }
        AuthProfilesCommand::Set {
            profile_id,
            provider,
            provider_name,
            profile_name,
            scope,
            agent_id,
            credential,
            api_key_ref,
            access_token_ref,
            refresh_token_ref,
            token_endpoint,
            client_id,
            client_secret_ref,
            scope_value,
            expires_at_unix_ms,
            json,
        } => {
            let json = output::preferred_json(json);
            let provider_message = auth_v1::AuthProvider {
                kind: auth_provider_arg_to_proto(provider),
                custom_name: provider_name.unwrap_or_default(),
            };
            let scope_message = match scope {
                AuthScopeArg::Global => auth_v1::AuthScope {
                    kind: auth_v1::AuthScopeKind::Global as i32,
                    agent_id: String::new(),
                },
                AuthScopeArg::Agent => auth_v1::AuthScope {
                    kind: auth_v1::AuthScopeKind::Agent as i32,
                    agent_id: agent_id.context("--agent-id is required when --scope=agent")?,
                },
            };
            let credential_message = match credential {
                AuthCredentialArg::ApiKey => auth_v1::AuthCredential {
                    kind: Some(auth_v1::auth_credential::Kind::ApiKey(auth_v1::ApiKeyCredential {
                        api_key_vault_ref: api_key_ref
                            .context("--api-key-ref is required when --credential=api-key")?,
                    })),
                },
                AuthCredentialArg::Oauth => auth_v1::AuthCredential {
                    kind: Some(auth_v1::auth_credential::Kind::Oauth(auth_v1::OAuthCredential {
                        access_token_vault_ref: access_token_ref
                            .context("--access-token-ref is required when --credential=oauth")?,
                        refresh_token_vault_ref: refresh_token_ref
                            .context("--refresh-token-ref is required when --credential=oauth")?,
                        token_endpoint: token_endpoint
                            .context("--token-endpoint is required when --credential=oauth")?,
                        client_id: client_id.unwrap_or_default(),
                        client_secret_vault_ref: client_secret_ref.unwrap_or_default(),
                        scopes: scope_value,
                        expires_at_unix_ms: expires_at_unix_ms.unwrap_or_default(),
                        refresh_state: None,
                    })),
                },
            };
            let mut request = Request::new(auth_v1::SetAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile: Some(auth_v1::AuthProfile {
                    profile_id,
                    provider: Some(provider_message),
                    profile_name,
                    scope: Some(scope_message),
                    credential: Some(credential_message),
                    created_at_unix_ms: 0,
                    updated_at_unix_ms: 0,
                }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.set_profile(request).await.context("failed to call auth SetProfile")?;
            let profile = response
                .into_inner()
                .profile
                .context("auth SetProfile returned empty profile payload")?;
            if json {
                println!("{}", serde_json::to_string_pretty(&auth_profile_to_json(&profile))?);
            } else {
                println!(
                    "auth.profiles.set id={} provider={} scope={} credential={}",
                    profile.profile_id,
                    auth_provider_to_text(profile.provider.as_ref()),
                    auth_scope_to_text(profile.scope.as_ref()),
                    auth_profile_credential_type(&profile)
                );
            }
        }
        AuthProfilesCommand::Delete { profile_id, json } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(auth_v1::DeleteAuthProfileRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                profile_id,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .delete_profile(request)
                .await
                .context("failed to call auth DeleteProfile")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({ "deleted": payload.deleted }))?
                );
            } else {
                println!("auth.profiles.delete deleted={}", payload.deleted);
            }
        }
        AuthProfilesCommand::Health { agent_id, include_profiles, json } => {
            let json = output::preferred_json(json);
            let mut request = Request::new(auth_v1::GetAuthHealthRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                agent_id: agent_id.unwrap_or_default(),
                include_profiles,
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response =
                client.get_health(request).await.context("failed to call auth GetHealth")?;
            let payload = response.into_inner();
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "summary": payload.summary.as_ref().map(auth_health_summary_to_json),
                        "expiry_distribution": payload
                            .expiry_distribution
                            .as_ref()
                            .map(auth_expiry_distribution_to_json),
                        "refresh_metrics": payload.refresh_metrics.as_ref().map(auth_refresh_metrics_to_json),
                        "profiles": payload.profiles.iter().map(auth_health_profile_to_json).collect::<Vec<_>>(),
                    }))?
                );
            } else {
                let summary = payload.summary.unwrap_or_default();
                println!(
                    "auth.profiles.health total={} ok={} expiring={} expired={} missing={} static={}",
                    summary.total,
                    summary.ok,
                    summary.expiring,
                    summary.expired,
                    summary.missing,
                    summary.static_count
                );
                let refresh = payload.refresh_metrics.unwrap_or_default();
                println!(
                    "auth.refresh attempts={} successes={} failures={}",
                    refresh.attempts, refresh.successes, refresh.failures
                );
                if include_profiles {
                    for profile in &payload.profiles {
                        println!(
                            "auth.health.profile id={} provider={} state={} reason={}",
                            profile.profile_id,
                            profile.provider,
                            auth_health_state_to_text(profile.state),
                            profile.reason
                        );
                    }
                }
            }
        }
        AuthProfilesCommand::Doctor { .. }
        | AuthProfilesCommand::Audit { .. }
        | AuthProfilesCommand::CooldownClear { .. }
        | AuthProfilesCommand::OrderSet { .. }
        | AuthProfilesCommand::ExplainSelection { .. } => {
            anyhow::bail!("auth profiles command requires control-plane dispatch")
        }
    }

    std::io::stdout().flush().context("stdout flush failed")
}

async fn run_auth_profiles_control_plane_async(command: AuthProfilesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        AuthProfilesCommand::Doctor { agent_id, json } => {
            let payload = context
                .client
                .get_auth_doctor(agent_id.as_deref())
                .await
                .context("failed to fetch auth doctor")?;
            emit_auth_runtime_payload(payload, json, "auth.profiles.doctor")?;
        }
        AuthProfilesCommand::Audit { agent_id, provider, provider_name, json } => {
            let provider_kind = provider.map(auth_provider_arg_to_control_plane);
            let payload = context
                .client
                .get_auth_audit(
                    agent_id.as_deref(),
                    provider_kind.as_deref(),
                    provider_name.as_deref(),
                )
                .await
                .context("failed to fetch auth audit")?;
            emit_auth_runtime_payload(payload, json, "auth.profiles.audit")?;
        }
        AuthProfilesCommand::CooldownClear { profile_id, json } => {
            let payload = context
                .client
                .clear_auth_profile_cooldown(profile_id.as_str())
                .await
                .with_context(|| {
                    format!("failed to clear cooldown for auth profile {profile_id}")
                })?;
            emit_auth_runtime_payload(payload, json, "auth.profiles.cooldown_clear")?;
        }
        AuthProfilesCommand::OrderSet { provider, provider_name, agent_id, profile_id, json } => {
            let payload = context
                .client
                .set_auth_profile_order(&json!({
                    "agent_id": agent_id,
                    "provider_kind": provider.map(auth_provider_arg_to_control_plane),
                    "provider_custom_name": provider_name,
                    "profile_ids": profile_id,
                }))
                .await
                .context("failed to set auth profile order")?;
            emit_auth_runtime_payload(payload, json, "auth.profiles.order_set")?;
        }
        AuthProfilesCommand::ExplainSelection {
            provider,
            provider_name,
            agent_id,
            profile_id,
            credential,
            policy_denied_profile_id,
            json,
        } => {
            let payload = context
                .client
                .explain_auth_profile_selection(&json!({
                    "agent_id": agent_id,
                    "provider_kind": provider.map(auth_provider_arg_to_control_plane),
                    "provider_custom_name": provider_name,
                    "explicit_profile_order": profile_id,
                    "allowed_credential_types": credential
                        .into_iter()
                        .map(auth_credential_arg_to_control_plane)
                        .collect::<Vec<_>>(),
                    "policy_denied_profile_ids": policy_denied_profile_id,
                }))
                .await
                .context("failed to explain auth profile selection")?;
            emit_auth_runtime_payload(payload, json, "auth.profiles.explain_selection")?;
        }
        AuthProfilesCommand::List { .. }
        | AuthProfilesCommand::Show { .. }
        | AuthProfilesCommand::Set { .. }
        | AuthProfilesCommand::Delete { .. }
        | AuthProfilesCommand::Health { .. } => {
            anyhow::bail!("auth profiles command requires gRPC dispatch")
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

async fn run_auth_access_async(command: AuthCommand) -> Result<()> {
    let AuthCommand::Access { command } = command else {
        anyhow::bail!("auth access dispatch received an incompatible auth command");
    };
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        AuthAccessCommand::Status { json } => {
            let payload = context
                .client
                .get_access_snapshot()
                .await
                .context("failed to fetch access snapshot")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::Backfill { dry_run, json } => {
            let payload = context
                .client
                .run_access_backfill(&json!({ "dry_run": dry_run }))
                .await
                .context("failed to run access backfill")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::Feature { feature_key, enabled, stage, json } => {
            let payload = context
                .client
                .set_access_feature_flag(
                    feature_key.as_str(),
                    &json!({
                        "enabled": enabled,
                        "stage": stage,
                    }),
                )
                .await
                .with_context(|| format!("failed to set access feature flag {feature_key}"))?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::TokenList { json } => {
            let payload = context
                .client
                .list_access_api_tokens()
                .await
                .context("failed to list access API tokens")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::TokenCreate {
            label,
            principal,
            workspace_id,
            role,
            scope,
            expires_at_unix_ms,
            rate_limit_per_minute,
            json,
        } => {
            let payload = context
                .client
                .create_access_api_token(&json!({
                    "label": label,
                    "principal": principal,
                    "workspace_id": workspace_id,
                    "role": workspace_role_arg_to_text(role),
                    "scopes": scope,
                    "expires_at_unix_ms": expires_at_unix_ms,
                    "rate_limit_per_minute": rate_limit_per_minute,
                }))
                .await
                .context("failed to create access API token")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::TokenRotate { token_id, json } => {
            let payload = context
                .client
                .rotate_access_api_token(token_id.as_str())
                .await
                .with_context(|| format!("failed to rotate access API token {token_id}"))?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::TokenRevoke { token_id, json } => {
            let payload = context
                .client
                .revoke_access_api_token(token_id.as_str())
                .await
                .with_context(|| format!("failed to revoke access API token {token_id}"))?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::WorkspaceCreate { team_name, workspace_name, json } => {
            let payload = context
                .client
                .create_access_workspace(&json!({
                    "team_name": team_name,
                    "workspace_name": workspace_name,
                }))
                .await
                .context("failed to create access workspace")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::InviteCreate {
            workspace_id,
            invited_identity,
            role,
            expires_at_unix_ms,
            json,
        } => {
            let payload = context
                .client
                .create_access_invitation(&json!({
                    "workspace_id": workspace_id,
                    "invited_identity": invited_identity,
                    "role": workspace_role_arg_to_text(role),
                    "expires_at_unix_ms": expires_at_unix_ms,
                }))
                .await
                .context("failed to create access invitation")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::InviteAccept { invitation_token, json } => {
            let payload = context
                .client
                .accept_access_invitation(&json!({ "invitation_token": invitation_token }))
                .await
                .context("failed to accept access invitation")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::MembershipRole { workspace_id, member_principal, role, json } => {
            let payload = context
                .client
                .update_access_membership_role(&json!({
                    "workspace_id": workspace_id,
                    "member_principal": member_principal,
                    "role": workspace_role_arg_to_text(role),
                }))
                .await
                .context("failed to update workspace membership role")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::MembershipRemove { workspace_id, member_principal, json } => {
            let payload = context
                .client
                .remove_access_membership(&json!({
                    "workspace_id": workspace_id,
                    "member_principal": member_principal,
                }))
                .await
                .context("failed to remove workspace membership")?;
            emit_access_payload(payload, json)?;
        }
        AuthAccessCommand::ShareUpsert {
            workspace_id,
            resource_kind,
            resource_id,
            access_level,
            json,
        } => {
            let payload = context
                .client
                .upsert_access_share(&json!({
                    "workspace_id": workspace_id,
                    "resource_kind": resource_kind,
                    "resource_id": resource_id,
                    "access_level": access_level,
                }))
                .await
                .context("failed to upsert access share")?;
            emit_access_payload(payload, json)?;
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct OpenAiAuthHealthSummary {
    #[serde(default)]
    total: u64,
    #[serde(default)]
    ok: u64,
    #[serde(default)]
    expiring: u64,
    #[serde(default)]
    expired: u64,
    #[serde(default)]
    missing: u64,
    #[serde(default)]
    static_count: u64,
}

#[derive(Debug, Deserialize, Default)]
struct OpenAiRefreshMetricsValue {
    #[serde(default)]
    provider: String,
    #[serde(default)]
    attempts: u64,
    #[serde(default)]
    successes: u64,
    #[serde(default)]
    failures: u64,
}

#[derive(Debug, Deserialize, Default)]
struct OpenAiRefreshMetricsEnvelope {
    #[serde(default)]
    attempts: u64,
    #[serde(default)]
    successes: u64,
    #[serde(default)]
    failures: u64,
    #[serde(default)]
    by_provider: Vec<OpenAiRefreshMetricsValue>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct OpenAiAuthHealthProfile {
    #[serde(default)]
    profile_id: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct OpenAiAuthStatusPayload {
    provider: String,
    provider_state: String,
    note: Option<String>,
    default_profile_id: Option<String>,
    summary: OpenAiAuthHealthSummary,
    refresh: OpenAiRefreshSnapshot,
    profiles: Vec<OpenAiAuthProfilePayload>,
}

#[derive(Debug, Serialize)]
struct OpenAiRefreshSnapshot {
    attempts: u64,
    successes: u64,
    failures: u64,
}

#[derive(Debug, Serialize)]
struct OpenAiAuthProfilePayload {
    profile_id: String,
    profile_name: String,
    scope: String,
    credential_type: &'static str,
    health_state: String,
    health_reason: String,
    expires_at_unix_ms: Option<i64>,
    is_default: bool,
}

#[derive(Debug, Serialize)]
struct OpenAiActionPayload {
    action: String,
    state: String,
    message: String,
    profile_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct OpenAiOAuthLaunchPayload {
    attempt_id: String,
    authorization_url: String,
    expires_at_unix_ms: i64,
    profile_id: Option<String>,
    opened: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct OpenAiOAuthStatePayload {
    attempt_id: String,
    state: String,
    message: String,
    profile_id: Option<String>,
    completed_at_unix_ms: Option<i64>,
    expires_at_unix_ms: Option<i64>,
}

async fn run_auth_openai_async(command: AuthOpenAiCommand) -> Result<()> {
    match command {
        AuthOpenAiCommand::Status { json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let provider_state = context
                .client
                .get_openai_provider_state()
                .await
                .context("failed to fetch OpenAI provider state")?;
            let auth_health = context
                .client
                .get_auth_health(true, None)
                .await
                .context("failed to fetch OpenAI auth health")?;
            let profiles = context
                .client
                .list_auth_profiles("provider_kind=openai&limit=100")
                .await
                .context("failed to list OpenAI auth profiles")?;
            let payload = build_openai_status_payload(provider_state, auth_health, profiles)?;
            emit_openai_status(payload, output::preferred_json(json))
        }
        AuthOpenAiCommand::ApiKey {
            profile_id,
            profile_name,
            scope,
            agent_id,
            api_key_env,
            api_key_stdin,
            api_key_prompt,
            set_default,
            json,
        } => {
            let api_key =
                load_secret_input(api_key_env, api_key_stdin, api_key_prompt, "OpenAI API key: ")?;
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .connect_openai_api_key(&control_plane::OpenAiApiKeyUpsertRequest {
                    profile_id,
                    profile_name,
                    scope: build_control_plane_scope(scope, agent_id)?,
                    api_key,
                    set_default,
                })
                .await
                .context("failed to configure OpenAI API key profile")?;
            emit_openai_action(
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthOpenAiCommand::OauthStart {
            profile_id,
            profile_name,
            scope,
            agent_id,
            client_id,
            client_secret_env,
            client_secret_stdin,
            client_secret_prompt,
            scope_value,
            set_default,
            open,
            json,
        } => {
            let client_secret =
                if client_secret_env.is_some() || client_secret_stdin || client_secret_prompt {
                    Some(load_secret_input(
                        client_secret_env,
                        client_secret_stdin,
                        client_secret_prompt,
                        "OpenAI OAuth client secret: ",
                    )?)
                } else {
                    None
                };
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .start_openai_oauth_bootstrap(&control_plane::OpenAiOAuthBootstrapRequest {
                    profile_id,
                    profile_name,
                    scope: Some(build_control_plane_scope(scope, agent_id)?),
                    client_id: Some(client_id),
                    client_secret,
                    scopes: scope_value,
                    set_default,
                })
                .await
                .context("failed to start OpenAI OAuth bootstrap")?;
            let mut payload = OpenAiOAuthLaunchPayload {
                attempt_id: response.attempt_id,
                authorization_url: response.authorization_url,
                expires_at_unix_ms: response.expires_at_unix_ms,
                profile_id: response.profile_id,
                opened: false,
                message: response.message,
            };
            if open {
                open_url_in_default_browser(payload.authorization_url.as_str()).with_context(
                    || {
                        format!(
                            "failed to open OpenAI OAuth authorization URL {}",
                            payload.authorization_url
                        )
                    },
                )?;
                payload.opened = true;
            }
            emit_openai_oauth_launch(payload, output::preferred_json(json))
        }
        AuthOpenAiCommand::OauthState { attempt_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .get_openai_oauth_callback_state(attempt_id.as_str())
                .await
                .context("failed to fetch OpenAI OAuth callback state")?;
            emit_openai_oauth_state(
                OpenAiOAuthStatePayload {
                    attempt_id: response.attempt_id,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                    completed_at_unix_ms: response.completed_at_unix_ms,
                    expires_at_unix_ms: response.expires_at_unix_ms,
                },
                output::preferred_json(json),
            )
        }
        AuthOpenAiCommand::Refresh { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_openai_provider_action(
                    "refresh",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to refresh OpenAI auth profile")?;
            emit_openai_action(
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthOpenAiCommand::Reconnect { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_openai_provider_action(
                    "reconnect",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to reconnect OpenAI auth profile")?;
            emit_openai_action(
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthOpenAiCommand::Revoke { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_openai_provider_action(
                    "revoke",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to revoke OpenAI auth profile")?;
            emit_openai_action(
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthOpenAiCommand::UseProfile { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_openai_provider_action(
                    "default-profile",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to select default OpenAI auth profile")?;
            emit_openai_action(
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
    }
}

fn build_openai_status_payload(
    provider_state: control_plane::ProviderAuthStateEnvelope,
    auth_health: control_plane::AuthHealthEnvelope,
    profiles: control_plane::AuthProfileListEnvelope,
) -> Result<OpenAiAuthStatusPayload> {
    let summary = serde_json::from_value::<OpenAiAuthHealthSummary>(auth_health.summary)
        .context("failed to decode OpenAI auth health summary")?;
    let refresh_metrics =
        serde_json::from_value::<OpenAiRefreshMetricsEnvelope>(auth_health.refresh_metrics)
            .context("failed to decode OpenAI refresh metrics")?;
    let refresh = refresh_metrics
        .by_provider
        .into_iter()
        .find(|entry| entry.provider.eq_ignore_ascii_case("openai"))
        .map(|entry| OpenAiRefreshSnapshot {
            attempts: entry.attempts,
            successes: entry.successes,
            failures: entry.failures,
        })
        .unwrap_or(OpenAiRefreshSnapshot {
            attempts: refresh_metrics.attempts,
            successes: refresh_metrics.successes,
            failures: refresh_metrics.failures,
        });
    let health_profiles = auth_health
        .profiles
        .into_iter()
        .filter_map(|value| serde_json::from_value::<OpenAiAuthHealthProfile>(value).ok())
        .filter(|profile| profile.provider.eq_ignore_ascii_case("openai"))
        .map(|profile| (profile.profile_id.clone(), profile))
        .collect::<std::collections::BTreeMap<_, _>>();
    let profiles = profiles
        .profiles
        .into_iter()
        .map(|profile| {
            let health = health_profiles.get(profile.profile_id.as_str());
            OpenAiAuthProfilePayload {
                profile_id: profile.profile_id.clone(),
                profile_name: profile.profile_name,
                scope: format_control_plane_scope(&profile.scope),
                credential_type: match profile.credential {
                    control_plane::AuthCredentialView::ApiKey { .. } => "api_key",
                    control_plane::AuthCredentialView::Oauth { .. } => "oauth",
                },
                health_state: health
                    .map(|value| normalize_openai_health_state(value.state.as_str()))
                    .unwrap_or_else(|| "unknown".to_owned()),
                health_reason: health
                    .map(|value| sanitize_auth_message(value.reason.as_str()))
                    .unwrap_or_else(|| "No health report available.".to_owned()),
                expires_at_unix_ms: health.and_then(|value| value.expires_at_unix_ms),
                is_default: provider_state
                    .default_profile_id
                    .as_deref()
                    .is_some_and(|value| value == profile.profile_id),
            }
        })
        .collect::<Vec<_>>();
    Ok(OpenAiAuthStatusPayload {
        provider: provider_state.provider,
        provider_state: provider_state.state,
        note: provider_state.note.map(|value| sanitize_auth_message(value.as_str())),
        default_profile_id: provider_state.default_profile_id,
        summary,
        refresh,
        profiles,
    })
}

fn emit_openai_status(payload: OpenAiAuthStatusPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(&payload, "failed to encode OpenAI auth status as JSON")?;
    } else {
        println!(
            "auth.openai.status provider={} state={} default_profile_id={} note={}",
            payload.provider,
            payload.provider_state,
            payload.default_profile_id.as_deref().unwrap_or("none"),
            payload.note.as_deref().unwrap_or("none")
        );
        println!(
            "auth.openai.summary total={} ok={} expiring={} expired={} missing={} static={} refresh_attempts={} refresh_successes={} refresh_failures={}",
            payload.summary.total,
            payload.summary.ok,
            payload.summary.expiring,
            payload.summary.expired,
            payload.summary.missing,
            payload.summary.static_count,
            payload.refresh.attempts,
            payload.refresh.successes,
            payload.refresh.failures
        );
        for profile in payload.profiles {
            println!(
                "auth.openai.profile id={} name={} scope={} credential={} health={} default={} expires_at_unix_ms={} reason=\"{}\"",
                profile.profile_id,
                profile.profile_name,
                profile.scope,
                profile.credential_type,
                profile.health_state,
                profile.is_default,
                profile
                    .expires_at_unix_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
                profile.health_reason.replace('"', "'")
            );
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_openai_action(payload: OpenAiActionPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(&payload, "failed to encode OpenAI action as JSON")?;
    } else {
        println!(
            "auth.openai.action action={} state={} profile_id={} message=\"{}\"",
            payload.action,
            payload.state,
            payload.profile_id.as_deref().unwrap_or("none"),
            payload.message.replace('"', "'")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_openai_oauth_launch(payload: OpenAiOAuthLaunchPayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(&payload, "failed to encode OpenAI OAuth launch as JSON")?;
    } else {
        println!(
            "auth.openai.oauth.start attempt_id={} profile_id={} expires_at_unix_ms={} authorization_url={} opened={}",
            payload.attempt_id,
            payload.profile_id.as_deref().unwrap_or("none"),
            payload.expires_at_unix_ms,
            payload.authorization_url,
            payload.opened
        );
        println!("auth.openai.oauth.message=\"{}\"", payload.message.replace('"', "'"));
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn emit_openai_oauth_state(payload: OpenAiOAuthStatePayload, json_output: bool) -> Result<()> {
    if json_output {
        output::print_json_pretty(
            &payload,
            "failed to encode OpenAI OAuth callback state as JSON",
        )?;
    } else {
        println!(
            "auth.openai.oauth.state attempt_id={} state={} profile_id={} completed_at_unix_ms={} expires_at_unix_ms={} message=\"{}\"",
            payload.attempt_id,
            payload.state,
            payload.profile_id.as_deref().unwrap_or("none"),
            payload
                .completed_at_unix_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            payload
                .expires_at_unix_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            payload.message.replace('"', "'")
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn build_control_plane_scope(
    scope: AuthScopeArg,
    agent_id: Option<String>,
) -> Result<control_plane::AuthProfileScope> {
    match scope {
        AuthScopeArg::Global => {
            Ok(control_plane::AuthProfileScope { kind: "global".to_owned(), agent_id: None })
        }
        AuthScopeArg::Agent => Ok(control_plane::AuthProfileScope {
            kind: "agent".to_owned(),
            agent_id: Some(agent_id.context("--agent-id is required when --scope=agent")?),
        }),
    }
}

fn auth_provider_arg_to_control_plane(value: AuthProviderArg) -> String {
    match value {
        AuthProviderArg::Openai => "openai",
        AuthProviderArg::Anthropic => "anthropic",
        AuthProviderArg::Telegram => "telegram",
        AuthProviderArg::Slack => "slack",
        AuthProviderArg::Discord => "discord",
        AuthProviderArg::Webhook => "webhook",
        AuthProviderArg::Custom => "custom",
    }
    .to_owned()
}

fn auth_credential_arg_to_control_plane(value: AuthCredentialArg) -> String {
    match value {
        AuthCredentialArg::ApiKey => "api_key",
        AuthCredentialArg::Oauth => "oauth",
    }
    .to_owned()
}

fn emit_auth_runtime_payload(payload: Value, json_output: bool, label: &str) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }
    let status = payload.get("status").and_then(Value::as_str).unwrap_or("ok");
    let profile_count = payload
        .pointer("/summary/profile_count")
        .and_then(Value::as_u64)
        .or_else(|| {
            payload.get("runtime_records").and_then(Value::as_array).map(|rows| rows.len() as u64)
        })
        .unwrap_or_default();
    let selected_profile =
        payload.pointer("/selection/selected_profile_id").and_then(Value::as_str).unwrap_or("none");
    let event_count =
        payload.get("events").and_then(Value::as_array).map(std::vec::Vec::len).unwrap_or_default();
    println!(
        "{label} status={} profiles={} selected_profile={} events={}",
        status, profile_count, selected_profile, event_count
    );
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn emit_access_payload(payload: Value, json_output: bool) -> Result<()> {
    if json_output {
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }
    let count = payload
        .get("api_tokens")
        .and_then(Value::as_array)
        .map(std::vec::Vec::len)
        .unwrap_or_default();
    let membership_count = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("memberships"))
        .and_then(Value::as_array)
        .map(std::vec::Vec::len)
        .unwrap_or_default();
    let feature_count = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("feature_flags"))
        .and_then(Value::as_array)
        .map(std::vec::Vec::len)
        .unwrap_or_default();
    let migration_backfill_required = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("migration"))
        .and_then(|migration| migration.get("backfill_required"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let migration_blockers = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("migration"))
        .and_then(|migration| migration.get("blocking_issues"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let external_api_safe_mode = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("rollout"))
        .and_then(|rollout| rollout.get("external_api_safe_mode"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let team_mode_safe_mode = payload
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("rollout"))
        .and_then(|rollout| rollout.get("team_mode_safe_mode"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let backfill_changes = payload
        .get("backfill")
        .and_then(|backfill| backfill.get("changed_records"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    println!(
        "auth.access result tokens={} memberships={} feature_flags={} backfill_required={} migration_blockers={} external_api_safe_mode={} team_mode_safe_mode={} backfill_changes={}",
        count,
        membership_count,
        feature_count,
        migration_backfill_required,
        migration_blockers,
        external_api_safe_mode,
        team_mode_safe_mode,
        backfill_changes
    );
    println!("{}", serde_json::to_string_pretty(&payload)?);
    Ok(())
}

fn workspace_role_arg_to_text(role: WorkspaceRoleArg) -> &'static str {
    match role {
        WorkspaceRoleArg::Owner => "owner",
        WorkspaceRoleArg::Admin => "admin",
        WorkspaceRoleArg::Operator => "operator",
    }
}

fn format_control_plane_scope(scope: &control_plane::AuthProfileScope) -> String {
    match scope.kind.trim().to_ascii_lowercase().as_str() {
        "agent" => scope
            .agent_id
            .as_deref()
            .map(|value| format!("agent:{value}"))
            .unwrap_or_else(|| "agent".to_owned()),
        "global" => "global".to_owned(),
        _ => scope.kind.clone(),
    }
}

fn normalize_openai_health_state(raw: &str) -> String {
    let lowered = raw.trim().to_ascii_lowercase();
    if lowered.is_empty() {
        "unknown".to_owned()
    } else {
        lowered
    }
}

fn sanitize_auth_message(raw: &str) -> String {
    raw.trim().replace(['\n', '\r'], " ")
}

fn load_secret_input(
    env_name: Option<String>,
    from_stdin: bool,
    from_prompt: bool,
    prompt: &str,
) -> Result<String> {
    let selected_sources =
        usize::from(env_name.is_some()) + usize::from(from_stdin) + usize::from(from_prompt);
    if selected_sources != 1 {
        anyhow::bail!("select exactly one secret source: --*-env, --*-stdin, or --*-prompt");
    }
    if let Some(env_name) = env_name {
        let value = env::var(env_name.as_str())
            .with_context(|| format!("environment variable {env_name} is not set"))?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("environment variable {env_name} does not contain a usable secret value");
        }
        return Ok(trimmed.to_owned());
    }
    if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read secret value from stdin")?;
        let trimmed = value.trim();
        if trimmed.is_empty() {
            anyhow::bail!("stdin did not contain a usable secret value");
        }
        return Ok(trimmed.to_owned());
    }
    let value = rpassword::prompt_password(prompt).context("failed to read secret from prompt")?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("prompt did not contain a usable secret value");
    }
    Ok(trimmed.to_owned())
}
