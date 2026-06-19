//! Auth commands: profile registry CRUD/health, runtime auth diagnostics, access
//! control (tokens, workspaces, invitations), and OpenAI provider auth flows.
//!
//! Credentials are handled as vault references only; raw secrets enter exclusively
//! through load_secret_input (env/stdin/prompt). OAuth launch text prints the
//! user-facing URL because device-login flows depend on copy/paste access to it.
//! Output lines are pinned by CLI parity tests.

use crate::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use palyra_control_plane as control_plane;
use ring::rand::{SecureRandom, SystemRandom};

const AUTH_PROFILES_EMPTY_REGISTRY_NOTE: &str = "This command lists auth-profile registry entries only. Model-provider credentials configured with `palyra configure --section auth-model` can be active vault refs even when this registry is empty; use the model-provider diagnostics commands for MiniMax/model-provider auth state.";
const AUTH_PROFILES_MODEL_PROVIDER_SOURCES: &[&str] = &[
    "palyra models status --json",
    "palyra models test-connection --provider minimax --refresh --json",
    "palyra secrets inventory --json",
];
const ANTHROPIC_OAUTH_AUTHORIZE_URL: &str = "https://claude.ai/oauth/authorize";
const ANTHROPIC_OAUTH_TOKEN_URL: &str = "https://console.anthropic.com/v1/oauth/token";
const ANTHROPIC_OAUTH_REDIRECT_URI: &str = "https://console.anthropic.com/oauth/code/callback";
const ANTHROPIC_OAUTH_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const ANTHROPIC_OAUTH_SCOPES: &str = "org:create_api_key user:profile user:inference";
const ANTHROPIC_OAUTH_USER_AGENT: &str = "claude-cli/2.1.74 (external, cli)";
const ANTHROPIC_OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
const XAI_OAUTH_DISCOVERY_URL: &str = "https://auth.x.ai/.well-known/openid-configuration";
const XAI_OAUTH_CLIENT_ID: &str = "b1a00492-073a-47ea-816f-4c329264a828";
const XAI_OAUTH_SCOPE: &str = "openid profile email offline_access grok-cli:access api:access";
const XAI_OAUTH_REDIRECT_URI: &str = "http://127.0.0.1:56121/callback";
const XAI_OAUTH_CALLBACK_HOST: &str = "127.0.0.1";
const XAI_OAUTH_CALLBACK_PORT: u16 = 56_121;
const XAI_OAUTH_CALLBACK_PATH: &str = "/callback";
const XAI_OAUTH_HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const XAI_OAUTH_CALLBACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const XAI_OAUTH_USER_AGENT: &str = "palyra-cli/0.1.0";

/// Runs a `palyra auth` subcommand on a fresh Tokio runtime.
///
/// # Errors
/// Returns an error when the runtime cannot be built, connection resolution
/// fails, or the dispatched subcommand fails.
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
        AuthCommand::Anthropic { command } => {
            let runtime = build_runtime()?;
            runtime.block_on(run_auth_anthropic_async(command))
        }
        AuthCommand::Xai { command } => {
            let runtime = build_runtime()?;
            runtime.block_on(run_auth_xai_async(command))
        }
    }
}

// Registry CRUD and health ride the auth gRPC service; the newer runtime
// diagnostics (doctor/audit/cooldown/order/selection) only exist on the daemon
// admin-console HTTP API, hence the split dispatch.
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

/// Dispatches registry-backed `auth profiles` subcommands over the auth gRPC service.
///
/// Accepts the full [`AuthCommand`] because callers share dispatch plumbing; any
/// non-`Profiles` command is rejected.
///
/// # Errors
/// Returns an error when the gRPC connection or call fails, a response is missing
/// its payload, or the command is not a gRPC-backed profiles subcommand.
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
                let profiles =
                    payload.profiles.iter().map(auth_profile_to_json).collect::<Vec<_>>();
                println!(
                    "{}",
                    serde_json::to_string_pretty(&build_auth_profiles_list_json_payload(
                        profiles,
                        empty_to_none(payload.next_after_profile_id),
                    ))?
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
                if payload.profiles.is_empty() {
                    println!("auth.profiles.note {}", AUTH_PROFILES_EMPTY_REGISTRY_NOTE);
                    println!(
                        "auth.profiles.model_provider_sources {}",
                        AUTH_PROFILES_MODEL_PROVIDER_SOURCES.join(" | ")
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

fn build_auth_profiles_list_json_payload(
    profiles: Vec<Value>,
    next_after_profile_id: Option<String>,
) -> Value {
    let is_empty = profiles.is_empty();
    let mut payload = json!({
        "profiles": profiles,
        "next_after_profile_id": next_after_profile_id,
    });
    if is_empty {
        payload["empty_registry_note"] = json!(AUTH_PROFILES_EMPTY_REGISTRY_NOTE);
        payload["model_provider_auth_sources"] = json!(AUTH_PROFILES_MODEL_PROVIDER_SOURCES);
    }
    payload
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

// Serde views over loosely typed console JSON payloads. Every field carries
// #[serde(default)] so older daemons that omit fields still deserialize cleanly.
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

#[derive(Debug)]
struct AnthropicOAuthTokens {
    access_token: String,
    refresh_token: String,
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct AnthropicOAuthTokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
}

#[derive(Debug, Clone)]
struct XaiOAuthDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct XaiOAuthDiscoveryResponse {
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
}

#[derive(Debug)]
struct XaiOAuthCallback {
    code: String,
}

#[derive(Debug)]
struct XaiOAuthTokens {
    access_token: String,
    refresh_token: String,
    expires_at_unix_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct XaiOAuthTokenResponse {
    access_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
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
            let payload =
                build_provider_status_payload("openai", provider_state, auth_health, profiles)?;
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
                    client_id,
                    client_secret,
                    scopes: scope_value,
                    set_default,
                })
                .await
                .context("failed to start OpenAI OAuth bootstrap")?;
            let mut payload = build_openai_oauth_launch_payload(response);
            if open {
                open_url_in_default_browser(payload.authorization_url.as_str())
                    .with_context(|| "failed to open OpenAI OAuth authorization URL".to_owned())?;
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
                .reconnect_openai_oauth(&control_plane::ProviderAuthActionRequest {
                    profile_id: Some(profile_id),
                })
                .await
                .context("failed to reconnect OpenAI auth profile")?;
            emit_openai_oauth_launch(
                build_openai_oauth_launch_payload(response),
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

async fn run_auth_anthropic_async(command: AuthAnthropicCommand) -> Result<()> {
    match command {
        AuthAnthropicCommand::Status { json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let provider_state = context
                .client
                .get_provider_auth_state("anthropic")
                .await
                .context("failed to fetch Anthropic provider state")?;
            let auth_health = context
                .client
                .get_auth_health(true, None)
                .await
                .context("failed to fetch Anthropic auth health")?;
            let profiles = context
                .client
                .list_auth_profiles("provider_kind=anthropic&limit=100")
                .await
                .context("failed to list Anthropic auth profiles")?;
            let payload =
                build_provider_status_payload("anthropic", provider_state, auth_health, profiles)?;
            emit_provider_status("anthropic", "Anthropic", payload, output::preferred_json(json))
        }
        AuthAnthropicCommand::ApiKey {
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
            let api_key = load_secret_input(
                api_key_env,
                api_key_stdin,
                api_key_prompt,
                "Anthropic API key: ",
            )?;
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .connect_provider_api_key(
                    "anthropic",
                    &control_plane::ProviderApiKeyUpsertRequest {
                        profile_id,
                        profile_name,
                        scope: build_control_plane_scope(scope, agent_id)?,
                        api_key,
                        set_default,
                    },
                )
                .await
                .context("failed to configure Anthropic API key profile")?;
            emit_provider_action(
                "anthropic",
                "Anthropic",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthAnthropicCommand::OauthStart {
            profile_id,
            profile_name,
            scope,
            agent_id,
            authorization_code_env,
            authorization_code_stdin,
            set_default,
            open,
            json,
        } => {
            let verifier = generate_oauth_pkce_verifier()?;
            let challenge = oauth_pkce_challenge(verifier.as_str());
            let state = generate_oauth_state()?;
            let authorization_url =
                build_anthropic_oauth_authorization_url(challenge.as_str(), state.as_str())?;
            let opened = if open {
                open_url_in_default_browser(authorization_url.as_str()).with_context(|| {
                    "failed to open Anthropic OAuth authorization URL".to_owned()
                })?;
                true
            } else {
                false
            };
            emit_anthropic_oauth_instructions(
                authorization_url.as_str(),
                opened,
                output::preferred_json(json),
            )?;
            let authorization_input = load_authorization_code_input(
                authorization_code_env,
                authorization_code_stdin,
                "Anthropic authorization code: ",
            )?;
            let authorization_code =
                parse_anthropic_authorization_code(authorization_input.as_str(), state.as_str())?;
            let tokens = exchange_anthropic_oauth_code(
                authorization_code.as_str(),
                state.as_str(),
                verifier.as_str(),
            )
            .await?;
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .connect_provider_oauth_tokens(
                    "anthropic",
                    &control_plane::ProviderOAuthTokenUpsertRequest {
                        profile_id,
                        profile_name: profile_name.unwrap_or_else(|| "Anthropic OAuth".to_owned()),
                        scope: build_control_plane_scope(scope, agent_id)?,
                        access_token: tokens.access_token,
                        refresh_token: tokens.refresh_token,
                        token_endpoint: ANTHROPIC_OAUTH_TOKEN_URL.to_owned(),
                        client_id: Some(ANTHROPIC_OAUTH_CLIENT_ID.to_owned()),
                        scopes: ANTHROPIC_OAUTH_SCOPES
                            .split_whitespace()
                            .map(str::to_owned)
                            .collect(),
                        expires_at_unix_ms: tokens.expires_at_unix_ms,
                        set_default,
                    },
                )
                .await
                .context("failed to store Anthropic OAuth profile")?;
            emit_provider_action(
                "anthropic",
                "Anthropic",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthAnthropicCommand::Refresh { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "anthropic",
                    "refresh",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to refresh Anthropic auth profile")?;
            emit_provider_action(
                "anthropic",
                "Anthropic",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthAnthropicCommand::Revoke { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "anthropic",
                    "revoke",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to revoke Anthropic auth profile")?;
            emit_provider_action(
                "anthropic",
                "Anthropic",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthAnthropicCommand::UseProfile { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "anthropic",
                    "default-profile",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to select default Anthropic auth profile")?;
            emit_provider_action(
                "anthropic",
                "Anthropic",
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

async fn run_auth_xai_async(command: AuthXaiCommand) -> Result<()> {
    match command {
        AuthXaiCommand::Status { json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let provider_state = context
                .client
                .get_provider_auth_state("xai")
                .await
                .context("failed to fetch xAI provider state")?;
            let auth_health = context
                .client
                .get_auth_health(true, None)
                .await
                .context("failed to fetch xAI auth health")?;
            let profiles = context
                .client
                .list_auth_profiles("provider_kind=custom&provider_custom_name=xai&limit=100")
                .await
                .context("failed to list xAI auth profiles")?;
            let payload =
                build_provider_status_payload("xai", provider_state, auth_health, profiles)?;
            emit_provider_status("xai", "xAI", payload, output::preferred_json(json))
        }
        AuthXaiCommand::OauthStart {
            profile_id,
            profile_name,
            scope,
            agent_id,
            set_default,
            open,
            manual_paste,
            callback_url_env,
            callback_url_stdin,
            json,
        } => {
            let discovery = discover_xai_oauth_endpoints().await?;
            let verifier = generate_oauth_pkce_verifier()?;
            let challenge = oauth_pkce_challenge(verifier.as_str());
            let state = generate_oauth_state()?;
            let nonce = generate_oauth_state()?;
            let authorization_url = build_xai_oauth_authorization_url(
                discovery.authorization_endpoint.as_str(),
                challenge.as_str(),
                state.as_str(),
                nonce.as_str(),
            )?;
            let opened = if open {
                open_url_in_default_browser(authorization_url.as_str())
                    .with_context(|| "failed to open xAI OAuth authorization URL".to_owned())?;
                true
            } else {
                false
            };
            let callback = if manual_paste || callback_url_env.is_some() || callback_url_stdin {
                emit_xai_oauth_instructions(
                    authorization_url.as_str(),
                    opened,
                    true,
                    output::preferred_json(json),
                )?;
                let callback_url = load_callback_url_input(
                    callback_url_env,
                    callback_url_stdin,
                    "xAI callback URL: ",
                )?;
                parse_xai_callback_url(callback_url.as_str(), state.as_str())?
            } else {
                let callback_waiter = start_xai_loopback_callback_waiter(state.clone())?;
                emit_xai_oauth_instructions(
                    authorization_url.as_str(),
                    opened,
                    false,
                    output::preferred_json(json),
                )?;
                callback_waiter.await.context("xAI OAuth callback worker failed")??
            };
            let tokens = exchange_xai_oauth_code(
                discovery.token_endpoint.as_str(),
                callback.code.as_str(),
                verifier.as_str(),
                challenge.as_str(),
            )
            .await?;
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .connect_provider_oauth_tokens(
                    "xai",
                    &control_plane::ProviderOAuthTokenUpsertRequest {
                        profile_id,
                        profile_name: profile_name.unwrap_or_else(|| "xAI OAuth".to_owned()),
                        scope: build_control_plane_scope(scope, agent_id)?,
                        access_token: tokens.access_token,
                        refresh_token: tokens.refresh_token,
                        token_endpoint: discovery.token_endpoint,
                        client_id: Some(XAI_OAUTH_CLIENT_ID.to_owned()),
                        scopes: XAI_OAUTH_SCOPE.split_whitespace().map(str::to_owned).collect(),
                        expires_at_unix_ms: tokens.expires_at_unix_ms,
                        set_default,
                    },
                )
                .await
                .context("failed to store xAI OAuth profile")?;
            emit_provider_action(
                "xai",
                "xAI",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthXaiCommand::Refresh { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "xai",
                    "refresh",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to refresh xAI auth profile")?;
            emit_provider_action(
                "xai",
                "xAI",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthXaiCommand::Revoke { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "xai",
                    "revoke",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to revoke xAI auth profile")?;
            emit_provider_action(
                "xai",
                "xAI",
                OpenAiActionPayload {
                    action: response.action,
                    state: response.state,
                    message: response.message,
                    profile_id: response.profile_id,
                },
                output::preferred_json(json),
            )
        }
        AuthXaiCommand::UseProfile { profile_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let response = context
                .client
                .run_provider_auth_action(
                    "xai",
                    "default-profile",
                    &control_plane::ProviderAuthActionRequest { profile_id: Some(profile_id) },
                )
                .await
                .context("failed to select default xAI auth profile")?;
            emit_provider_action(
                "xai",
                "xAI",
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

/// Merges three console endpoints (provider state, auth health, profile list) into
/// the single status payload shown by provider auth status commands.
fn build_provider_status_payload(
    provider_key: &str,
    provider_state: control_plane::ProviderAuthStateEnvelope,
    auth_health: control_plane::AuthHealthEnvelope,
    profiles: control_plane::AuthProfileListEnvelope,
) -> Result<OpenAiAuthStatusPayload> {
    let refresh_metrics =
        serde_json::from_value::<OpenAiRefreshMetricsEnvelope>(auth_health.refresh_metrics)
            .context("failed to decode OpenAI refresh metrics")?;
    let refresh = refresh_metrics
        .by_provider
        .into_iter()
        .find(|entry| entry.provider.eq_ignore_ascii_case(provider_key))
        .map(|entry| OpenAiRefreshSnapshot {
            attempts: entry.attempts,
            successes: entry.successes,
            failures: entry.failures,
        })
        .unwrap_or_else(|| {
            if provider_key.eq_ignore_ascii_case("openai") {
                OpenAiRefreshSnapshot {
                    attempts: refresh_metrics.attempts,
                    successes: refresh_metrics.successes,
                    failures: refresh_metrics.failures,
                }
            } else {
                OpenAiRefreshSnapshot { attempts: 0, successes: 0, failures: 0 }
            }
        });
    let health_profiles = auth_health
        .profiles
        .into_iter()
        .filter_map(|value| serde_json::from_value::<OpenAiAuthHealthProfile>(value).ok())
        .filter(|profile| profile.provider.eq_ignore_ascii_case(provider_key))
        .map(|profile| (profile.profile_id.clone(), profile))
        .collect::<std::collections::BTreeMap<_, _>>();
    let summary = provider_health_summary(health_profiles.values());
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

fn provider_health_summary<'a>(
    profiles: impl Iterator<Item = &'a OpenAiAuthHealthProfile>,
) -> OpenAiAuthHealthSummary {
    let mut summary = OpenAiAuthHealthSummary::default();
    for profile in profiles {
        summary.total += 1;
        match normalize_openai_health_state(profile.state.as_str()).as_str() {
            "ok" => summary.ok += 1,
            "expiring" => summary.expiring += 1,
            "expired" => summary.expired += 1,
            "missing" => summary.missing += 1,
            "static" => summary.static_count += 1,
            _ => summary.missing += 1,
        }
    }
    summary
}

fn build_openai_oauth_launch_payload(
    response: control_plane::OpenAiOAuthBootstrapEnvelope,
) -> OpenAiOAuthLaunchPayload {
    OpenAiOAuthLaunchPayload {
        attempt_id: response.attempt_id,
        authorization_url: response.authorization_url,
        expires_at_unix_ms: response.expires_at_unix_ms,
        profile_id: response.profile_id,
        opened: false,
        message: response.message,
    }
}

fn emit_openai_status(payload: OpenAiAuthStatusPayload, json_output: bool) -> Result<()> {
    emit_provider_status("openai", "OpenAI", payload, json_output)
}

fn emit_provider_status(
    provider_key: &str,
    _provider_label: &str,
    payload: OpenAiAuthStatusPayload,
    json_output: bool,
) -> Result<()> {
    if json_output {
        let error_label = match provider_key {
            "anthropic" => "failed to encode Anthropic auth status as JSON",
            _ => "failed to encode OpenAI auth status as JSON",
        };
        output::print_json_pretty(&payload, error_label)?;
    } else {
        println!(
            "auth.{provider_key}.status provider={} state={} default_profile_id={} note={}",
            payload.provider,
            payload.provider_state,
            payload.default_profile_id.as_deref().unwrap_or("none"),
            payload.note.as_deref().unwrap_or("none")
        );
        println!(
            "auth.{provider_key}.summary total={} ok={} expiring={} expired={} missing={} static={} refresh_attempts={} refresh_successes={} refresh_failures={}",
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
                "auth.{provider_key}.profile id={} name={} scope={} credential={} health={} default={} expires_at_unix_ms={} reason=\"{}\"",
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
    emit_provider_action("openai", "OpenAI", payload, json_output)
}

fn emit_provider_action(
    provider_key: &str,
    _provider_label: &str,
    payload: OpenAiActionPayload,
    json_output: bool,
) -> Result<()> {
    if json_output {
        let error_label = match provider_key {
            "anthropic" => "failed to encode Anthropic action as JSON",
            _ => "failed to encode OpenAI action as JSON",
        };
        output::print_json_pretty(&payload, error_label)?;
    } else {
        println!(
            "auth.{provider_key}.action action={} state={} profile_id={} message=\"{}\"",
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
        for line in openai_oauth_launch_text_lines(&payload) {
            output::print_text_line(line.as_str())?;
        }
    }
    std::io::stdout().flush().context("stdout flush failed")
}

// OpenAI's ChatGPT/Codex device flow requires the operator to copy or open the
// verification URL, so text output includes it explicitly.
fn openai_oauth_launch_text_lines(payload: &OpenAiOAuthLaunchPayload) -> [String; 3] {
    let authorization_url = payload.authorization_url.replace('"', "'");
    [
        format!(
            "auth.openai.oauth.start attempt_id={} profile_id={} expires_at_unix_ms={} authorization_url_present={} opened={}",
            payload.attempt_id,
            payload.profile_id.as_deref().unwrap_or("none"),
            payload.expires_at_unix_ms,
            !payload.authorization_url.trim().is_empty(),
            payload.opened
        ),
        format!("auth.openai.oauth.authorization_url=\"{authorization_url}\""),
        format!("auth.openai.oauth.message=\"{}\"", payload.message.replace('"', "'")),
    ]
}

fn emit_anthropic_oauth_instructions(
    authorization_url: &str,
    opened: bool,
    json_output: bool,
) -> Result<()> {
    let safe_url = authorization_url.replace('"', "'");
    if json_output {
        eprintln!(
            "auth.anthropic.oauth.authorization_url=\"{safe_url}\" opened={opened} message=\"Open this URL, authorize Palyra, then paste the code shown by Anthropic.\""
        );
    } else {
        output::print_text_line(
            format!(
                "auth.anthropic.oauth.start authorization_url_present={} opened={opened}",
                !authorization_url.trim().is_empty()
            )
            .as_str(),
        )?;
        output::print_text_line(
            format!("auth.anthropic.oauth.authorization_url=\"{safe_url}\"").as_str(),
        )?;
        output::print_text_line(
            "auth.anthropic.oauth.message=\"Open this URL, authorize Palyra, then paste the code shown by Anthropic.\"",
        )?;
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    Ok(())
}

fn generate_oauth_random_urlsafe(byte_len: usize, label: &str) -> Result<String> {
    let rng = SystemRandom::new();
    let mut bytes = vec![0_u8; byte_len];
    rng.fill(bytes.as_mut_slice()).map_err(|_| anyhow!("failed to generate {label} randomness"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn generate_oauth_pkce_verifier() -> Result<String> {
    generate_oauth_random_urlsafe(32, "PKCE verifier")
}

fn generate_oauth_state() -> Result<String> {
    generate_oauth_random_urlsafe(32, "OAuth state")
}

fn oauth_pkce_challenge(verifier: &str) -> String {
    use sha2::Digest as _;

    let digest = sha2::Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn build_anthropic_oauth_authorization_url(code_challenge: &str, state: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(ANTHROPIC_OAUTH_AUTHORIZE_URL)
        .context("Anthropic OAuth authorization endpoint is invalid")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("code", "true");
        pairs.append_pair("client_id", ANTHROPIC_OAUTH_CLIENT_ID);
        pairs.append_pair("response_type", "code");
        pairs.append_pair("redirect_uri", ANTHROPIC_OAUTH_REDIRECT_URI);
        pairs.append_pair("scope", ANTHROPIC_OAUTH_SCOPES);
        pairs.append_pair("code_challenge", code_challenge);
        pairs.append_pair("code_challenge_method", "S256");
        pairs.append_pair("state", state);
    }
    Ok(url.to_string())
}

fn load_authorization_code_input(
    env_name: Option<String>,
    from_stdin: bool,
    prompt: &str,
) -> Result<String> {
    let selected_sources = usize::from(env_name.is_some()) + usize::from(from_stdin);
    if selected_sources > 1 {
        anyhow::bail!("select at most one authorization code source");
    }
    let value = if let Some(env_name) = env_name {
        env::var(env_name.as_str())
            .with_context(|| format!("environment variable {env_name} is not set"))?
    } else if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read authorization code from stdin")?;
        value
    } else {
        print!("{prompt}");
        std::io::stdout().flush().context("stdout flush failed")?;
        let mut value = String::new();
        std::io::stdin().read_line(&mut value).context("failed to read authorization code")?;
        value
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("authorization code input was empty");
    }
    Ok(trimmed.to_owned())
}

fn parse_anthropic_authorization_code(input: &str, expected_state: &str) -> Result<String> {
    let trimmed = input.trim();
    let (code, received_state) = if let Ok(url) = reqwest::Url::parse(trimmed) {
        (
            url.query_pairs()
                .find(|(key, _)| key == "code")
                .map(|(_, value)| value.to_string())
                .unwrap_or_default(),
            url.query_pairs()
                .find(|(key, _)| key == "state")
                .map(|(_, value)| value.to_string())
                .or_else(|| url.fragment().map(str::to_owned))
                .unwrap_or_default(),
        )
    } else {
        let mut parts = trimmed.splitn(2, '#');
        (
            parts.next().unwrap_or_default().trim().to_owned(),
            parts.next().unwrap_or_default().trim().to_owned(),
        )
    };
    if code.is_empty() {
        anyhow::bail!("Anthropic authorization code was empty");
    }
    if received_state != expected_state {
        anyhow::bail!("Anthropic OAuth state mismatch; restart the login flow");
    }
    Ok(code)
}

async fn exchange_anthropic_oauth_code(
    code: &str,
    state: &str,
    code_verifier: &str,
) -> Result<AnthropicOAuthTokens> {
    let client = reqwest::Client::builder()
        .timeout(ANTHROPIC_OAUTH_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build Anthropic OAuth HTTP client")?;
    let response = client
        .post(ANTHROPIC_OAUTH_TOKEN_URL)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("user-agent", ANTHROPIC_OAUTH_USER_AGENT)
        .json(&serde_json::json!({
            "grant_type": "authorization_code",
            "client_id": ANTHROPIC_OAUTH_CLIENT_ID,
            "code": code,
            "state": state,
            "redirect_uri": ANTHROPIC_OAUTH_REDIRECT_URI,
            "code_verifier": code_verifier,
        }))
        .send()
        .await
        .context("Anthropic OAuth token exchange request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let sanitized = redact_auth_error(redact_url_segments_in_text(body.as_str()).as_str());
        anyhow::bail!(
            "Anthropic OAuth token exchange failed with HTTP {}: {}",
            status.as_u16(),
            sanitize_auth_message(sanitized.as_str())
        );
    }
    let parsed: AnthropicOAuthTokenResponse = serde_json::from_str(body.as_str())
        .context("Anthropic OAuth token response was not JSON")?;
    let access_token = parsed
        .access_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Anthropic OAuth token response did not include access_token"))?;
    let refresh_token = parsed
        .refresh_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Anthropic OAuth token response did not include refresh_token"))?;
    let expires_at_unix_ms = parsed
        .expires_in
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|duration_ms| now_unix_ms_i64().ok()?.checked_add(duration_ms));
    Ok(AnthropicOAuthTokens { access_token, refresh_token, expires_at_unix_ms })
}

async fn discover_xai_oauth_endpoints() -> Result<XaiOAuthDiscovery> {
    let response = reqwest::Client::builder()
        .timeout(XAI_OAUTH_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build xAI OAuth discovery client")?
        .get(XAI_OAUTH_DISCOVERY_URL)
        .header("accept", "application/json")
        .header("user-agent", XAI_OAUTH_USER_AGENT)
        .send()
        .await
        .context("xAI OAuth discovery request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!(
            "xAI OAuth discovery failed with HTTP {}: {}",
            status.as_u16(),
            sanitize_auth_message(redact_auth_error(body.as_str()).as_str())
        );
    }
    let parsed: XaiOAuthDiscoveryResponse =
        serde_json::from_str(body.as_str()).context("xAI OAuth discovery response was not JSON")?;
    let authorization_endpoint = normalize_xai_oauth_endpoint(
        parsed.authorization_endpoint.as_deref().unwrap_or_default(),
        "authorization_endpoint",
    )?;
    let token_endpoint = normalize_xai_oauth_endpoint(
        parsed.token_endpoint.as_deref().unwrap_or_default(),
        "token_endpoint",
    )?;
    Ok(XaiOAuthDiscovery { authorization_endpoint, token_endpoint })
}

fn normalize_xai_oauth_endpoint(raw: &str, field: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("xAI OAuth discovery is missing {field}");
    }
    let parsed = reqwest::Url::parse(trimmed).with_context(|| format!("invalid xAI {field}"))?;
    if parsed.scheme() != "https" {
        anyhow::bail!("xAI {field} must use https");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        anyhow::bail!("xAI {field} must not contain embedded credentials");
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        anyhow::bail!("xAI {field} must not contain query or fragment components");
    }
    let host = parsed.host_str().unwrap_or_default();
    if !host.eq_ignore_ascii_case("auth.x.ai") && !host.to_ascii_lowercase().ends_with(".x.ai") {
        anyhow::bail!("xAI {field} host is not trusted");
    }
    Ok(parsed.to_string())
}

fn build_xai_oauth_authorization_url(
    authorization_endpoint: &str,
    code_challenge: &str,
    state: &str,
    nonce: &str,
) -> Result<String> {
    let mut url = reqwest::Url::parse(authorization_endpoint)
        .context("xAI OAuth authorization endpoint is invalid")?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("response_type", "code");
        pairs.append_pair("client_id", XAI_OAUTH_CLIENT_ID);
        pairs.append_pair("redirect_uri", XAI_OAUTH_REDIRECT_URI);
        pairs.append_pair("scope", XAI_OAUTH_SCOPE);
        pairs.append_pair("state", state);
        pairs.append_pair("nonce", nonce);
        pairs.append_pair("code_challenge", code_challenge);
        pairs.append_pair("code_challenge_method", "S256");
        pairs.append_pair("plan", "generic");
        pairs.append_pair("referrer", "palyra");
    }
    Ok(url.to_string())
}

fn emit_xai_oauth_instructions(
    authorization_url: &str,
    opened: bool,
    manual_paste: bool,
    json_output: bool,
) -> Result<()> {
    let safe_url = authorization_url.replace('"', "'");
    let mode = if manual_paste { "manual_paste" } else { "loopback" };
    if json_output {
        eprintln!(
            "auth.xai.oauth.authorization_url=\"{safe_url}\" opened={opened} callback_mode={mode} message=\"Open this URL to authorize Palyra with xAI.\""
        );
    } else {
        output::print_text_line(
            format!(
                "auth.xai.oauth.start authorization_url_present={} opened={opened} callback_mode={mode}",
                !authorization_url.trim().is_empty()
            )
            .as_str(),
        )?;
        output::print_text_line(
            format!("auth.xai.oauth.authorization_url=\"{safe_url}\"").as_str(),
        )?;
        if manual_paste {
            output::print_text_line(
                "auth.xai.oauth.message=\"Open this URL, authorize Palyra, then paste the full 127.0.0.1 callback URL.\"",
            )?;
        } else {
            output::print_text_line(
                "auth.xai.oauth.message=\"Open this URL to authorize Palyra; waiting on http://127.0.0.1:56121/callback.\"",
            )?;
        }
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    Ok(())
}

fn start_xai_loopback_callback_waiter(
    expected_state: String,
) -> Result<tokio::task::JoinHandle<Result<XaiOAuthCallback>>> {
    let listener = std::net::TcpListener::bind((XAI_OAUTH_CALLBACK_HOST, XAI_OAUTH_CALLBACK_PORT))
        .with_context(|| {
            format!(
                "failed to bind xAI OAuth callback listener on {XAI_OAUTH_CALLBACK_HOST}:{XAI_OAUTH_CALLBACK_PORT}"
            )
        })?;
    listener
        .set_nonblocking(true)
        .context("failed to set xAI OAuth callback listener nonblocking")?;
    Ok(tokio::task::spawn_blocking(move || {
        wait_for_xai_loopback_callback(listener, expected_state.as_str())
    }))
}

fn wait_for_xai_loopback_callback(
    listener: std::net::TcpListener,
    expected_state: &str,
) -> Result<XaiOAuthCallback> {
    let deadline = std::time::Instant::now() + XAI_OAUTH_CALLBACK_TIMEOUT;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Some(callback) = handle_xai_callback_stream(stream, expected_state)? {
                    return Ok(callback);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!("timed out waiting for xAI OAuth callback");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error).context("failed to accept xAI OAuth callback"),
        }
    }
}

fn handle_xai_callback_stream(
    stream: std::net::TcpStream,
    expected_state: &str,
) -> Result<Option<XaiOAuthCallback>> {
    let mut reader = std::io::BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line).context("failed to read xAI OAuth callback request")?;
    let mut header_line = String::new();
    loop {
        header_line.clear();
        if reader
            .read_line(&mut header_line)
            .context("failed to read xAI OAuth callback headers")?
            == 0
        {
            break;
        }
        if header_line == "\r\n" || header_line == "\n" {
            break;
        }
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let mut stream = reader.into_inner();
    if method.eq_ignore_ascii_case("OPTIONS") {
        write_xai_callback_response_best_effort(
            &mut stream,
            "HTTP/1.1 204 No Content",
            "",
            Some("https://auth.x.ai"),
        );
        return Ok(None);
    }
    if !method.eq_ignore_ascii_case("GET") {
        write_xai_callback_response_best_effort(
            &mut stream,
            "HTTP/1.1 405 Method Not Allowed",
            "Method not allowed.",
            None,
        );
        return Ok(None);
    }
    let callback = parse_xai_callback_target(target, expected_state);
    match callback {
        Ok(callback) => {
            write_xai_callback_response_best_effort(
                &mut stream,
                "HTTP/1.1 200 OK",
                "xAI authentication completed. You can close this window.",
                None,
            );
            Ok(Some(callback))
        }
        Err(error) => {
            write_xai_callback_response_best_effort(
                &mut stream,
                "HTTP/1.1 400 Bad Request",
                "xAI authentication did not complete.",
                None,
            );
            Err(error)
        }
    }
}

fn write_xai_callback_response_best_effort(
    stream: &mut impl std::io::Write,
    status_line: &str,
    body: &str,
    cors_origin: Option<&str>,
) {
    // Browser clients can close the loopback connection after delivering the
    // callback. The parsed callback is the OAuth result; the response body is UX.
    let _ = write_xai_callback_response(stream, status_line, body, cors_origin);
}

fn write_xai_callback_response(
    stream: &mut impl std::io::Write,
    status_line: &str,
    body: &str,
    cors_origin: Option<&str>,
) -> Result<()> {
    let mut headers = format!(
        "{status_line}\r\ncontent-type: text/html; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    if let Some(origin) = cors_origin {
        headers.push_str(format!("access-control-allow-origin: {origin}\r\n").as_str());
        headers.push_str("access-control-allow-methods: GET, OPTIONS\r\n");
        headers.push_str("access-control-allow-headers: content-type\r\n");
    }
    headers.push_str("\r\n");
    stream.write_all(headers.as_bytes()).context("failed to write xAI OAuth callback headers")?;
    stream.write_all(body.as_bytes()).context("failed to write xAI OAuth callback body")?;
    Ok(())
}

fn load_callback_url_input(
    env_name: Option<String>,
    from_stdin: bool,
    prompt: &str,
) -> Result<String> {
    let selected_sources = usize::from(env_name.is_some()) + usize::from(from_stdin);
    if selected_sources > 1 {
        anyhow::bail!("select at most one callback URL source");
    }
    let value = if let Some(env_name) = env_name {
        env::var(env_name.as_str())
            .with_context(|| format!("environment variable {env_name} is not set"))?
    } else if from_stdin {
        let mut value = String::new();
        std::io::stdin()
            .read_to_string(&mut value)
            .context("failed to read callback URL from stdin")?;
        value
    } else {
        print!("{prompt}");
        std::io::stdout().flush().context("stdout flush failed")?;
        let mut value = String::new();
        std::io::stdin().read_line(&mut value).context("failed to read callback URL")?;
        value
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("callback URL input was empty");
    }
    Ok(trimmed.to_owned())
}

fn parse_xai_callback_url(raw: &str, expected_state: &str) -> Result<XaiOAuthCallback> {
    let parsed = reqwest::Url::parse(raw.trim()).context("xAI callback URL is invalid")?;
    if parsed.scheme() != "http"
        || parsed.host_str() != Some(XAI_OAUTH_CALLBACK_HOST)
        || parsed.port_or_known_default() != Some(XAI_OAUTH_CALLBACK_PORT)
        || parsed.path() != XAI_OAUTH_CALLBACK_PATH
    {
        anyhow::bail!("xAI callback URL does not match {XAI_OAUTH_REDIRECT_URI}");
    }
    parse_xai_callback_query(&parsed, expected_state)
}

fn parse_xai_callback_target(target: &str, expected_state: &str) -> Result<XaiOAuthCallback> {
    let parsed = reqwest::Url::parse(format!("http://{XAI_OAUTH_CALLBACK_HOST}{target}").as_str())
        .context("xAI callback request target is invalid")?;
    if parsed.path() != XAI_OAUTH_CALLBACK_PATH {
        anyhow::bail!("xAI callback route not found");
    }
    parse_xai_callback_query(&parsed, expected_state)
}

fn parse_xai_callback_query(
    parsed: &reqwest::Url,
    expected_state: &str,
) -> Result<XaiOAuthCallback> {
    if let Some(error) = parsed.query_pairs().find(|(key, _)| key == "error") {
        anyhow::bail!("xAI OAuth returned error: {}", error.1);
    }
    let code = parsed
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.to_string())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("xAI OAuth callback did not include code"))?;
    let received_state = parsed
        .query_pairs()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value.to_string())
        .unwrap_or_default();
    if received_state != expected_state {
        anyhow::bail!("xAI OAuth state mismatch; restart the login flow");
    }
    Ok(XaiOAuthCallback { code })
}

async fn exchange_xai_oauth_code(
    token_endpoint: &str,
    code: &str,
    code_verifier: &str,
    code_challenge: &str,
) -> Result<XaiOAuthTokens> {
    let token_endpoint = normalize_xai_oauth_endpoint(token_endpoint, "token_endpoint")?;
    let response = reqwest::Client::builder()
        .timeout(XAI_OAUTH_HTTP_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("failed to build xAI OAuth token client")?
        .post(token_endpoint.as_str())
        .header("content-type", "application/x-www-form-urlencoded")
        .header("accept", "application/json")
        .header("user-agent", XAI_OAUTH_USER_AGENT)
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", XAI_OAUTH_REDIRECT_URI),
            ("client_id", XAI_OAUTH_CLIENT_ID),
            ("code_verifier", code_verifier),
            ("code_challenge", code_challenge),
            ("code_challenge_method", "S256"),
        ])
        .send()
        .await
        .context("xAI OAuth token exchange request failed")?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let sanitized = redact_auth_error(redact_url_segments_in_text(body.as_str()).as_str());
        anyhow::bail!(
            "xAI OAuth token exchange failed with HTTP {}: {}",
            status.as_u16(),
            sanitize_auth_message(sanitized.as_str())
        );
    }
    let parsed: XaiOAuthTokenResponse =
        serde_json::from_str(body.as_str()).context("xAI OAuth token response was not JSON")?;
    let access_token = parsed
        .access_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("xAI OAuth token response did not include access_token"))?;
    let refresh_token = parsed
        .refresh_token
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("xAI OAuth token response did not include refresh_token"))?;
    let expires_at_unix_ms = parsed
        .expires_in
        .filter(|seconds| *seconds > 0)
        .and_then(|seconds| seconds.checked_mul(1_000))
        .and_then(|duration_ms| now_unix_ms_i64().ok()?.checked_add(duration_ms));
    Ok(XaiOAuthTokens { access_token, refresh_token, expires_at_unix_ms })
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
    let json_output = output::preferred_json(json_output);
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
    let json_output = output::preferred_json(json_output);
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

// Daemon messages can be multi-line; flatten them so the line-oriented
// `key=value` text output stays parseable.
fn sanitize_auth_message(raw: &str) -> String {
    raw.trim().replace(['\n', '\r'], " ")
}

/// Reads a credential from exactly one source: a named environment variable,
/// stdin, or a hidden interactive prompt. Secrets are never accepted as plain
/// command-line arguments, which would leak via shell history and process lists.
///
/// # Errors
/// Returns an error when zero or multiple sources are selected, the source cannot
/// be read, or the value is empty after trimming.
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

#[cfg(test)]
mod tests {
    use super::{
        build_anthropic_oauth_authorization_url, build_auth_profiles_list_json_payload,
        build_openai_oauth_launch_payload, build_provider_status_payload,
        build_xai_oauth_authorization_url, normalize_xai_oauth_endpoint,
        openai_oauth_launch_text_lines, parse_anthropic_authorization_code, parse_xai_callback_url,
        write_xai_callback_response_best_effort, AnthropicOAuthTokenResponse,
        OpenAiOAuthLaunchPayload, ANTHROPIC_OAUTH_AUTHORIZE_URL, ANTHROPIC_OAUTH_CLIENT_ID,
        ANTHROPIC_OAUTH_REDIRECT_URI, ANTHROPIC_OAUTH_SCOPES, AUTH_PROFILES_EMPTY_REGISTRY_NOTE,
        AUTH_PROFILES_MODEL_PROVIDER_SOURCES, XAI_OAUTH_CLIENT_ID, XAI_OAUTH_REDIRECT_URI,
        XAI_OAUTH_SCOPE,
    };
    use palyra_control_plane as control_plane;
    use serde_json::json;

    #[test]
    fn empty_auth_profiles_json_points_to_model_provider_auth_sources() {
        let payload = build_auth_profiles_list_json_payload(Vec::new(), None);

        assert_eq!(payload.get("profiles"), Some(&json!([])));
        assert!(
            payload.get("empty_registry_note").and_then(|value| value.as_str()).is_some_and(
                |note| {
                    note.contains("configure --section auth-model")
                        && note.contains("MiniMax/model-provider auth state")
                }
            ),
            "empty auth profile registry should explain model-provider auth diagnostics: {payload}"
        );
        assert_eq!(
            payload.get("model_provider_auth_sources"),
            Some(&json!(AUTH_PROFILES_MODEL_PROVIDER_SOURCES))
        );
    }

    #[test]
    fn non_empty_auth_profiles_json_stays_focused_on_profiles() {
        let payload = build_auth_profiles_list_json_payload(
            vec![json!({"profile_id": "openai-default"})],
            Some("next-profile".to_owned()),
        );

        assert!(payload.get("empty_registry_note").is_none());
        assert!(payload.get("model_provider_auth_sources").is_none());
        assert_eq!(
            payload.get("next_after_profile_id").and_then(|value| value.as_str()),
            Some("next-profile")
        );
        assert!(
            AUTH_PROFILES_EMPTY_REGISTRY_NOTE.contains("auth-profile registry"),
            "empty-registry note should keep the command boundary explicit"
        );
    }

    #[test]
    fn provider_status_payload_summarizes_only_requested_provider() {
        let contract = control_plane::ContractDescriptor {
            contract_version: control_plane::CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
        };
        let payload = build_provider_status_payload(
            "xai",
            control_plane::ProviderAuthStateEnvelope {
                contract: contract.clone(),
                provider: "xai".to_owned(),
                oauth_supported: true,
                bootstrap_supported: false,
                callback_supported: false,
                reconnect_supported: true,
                revoke_supported: true,
                default_selection_supported: true,
                default_profile_id: None,
                available_profile_ids: Vec::new(),
                state: "not_configured".to_owned(),
                note: None,
            },
            control_plane::AuthHealthEnvelope {
                contract: contract.clone(),
                summary: json!({
                    "total": 1,
                    "ok": 1,
                }),
                expiry_distribution: json!({}),
                profiles: vec![json!({
                    "profile_id": "openai-default",
                    "provider": "openai",
                    "state": "ok",
                    "reason": "oauth access token is healthy",
                })],
                refresh_metrics: json!({
                    "attempts": 4,
                    "successes": 4,
                    "failures": 0,
                    "by_provider": [
                        {
                            "provider": "openai",
                            "attempts": 4,
                            "successes": 4,
                            "failures": 0,
                        }
                    ],
                }),
            },
            control_plane::AuthProfileListEnvelope {
                contract,
                profiles: Vec::new(),
                page: control_plane::PageInfo {
                    limit: 50,
                    returned: 0,
                    next_cursor: None,
                    has_more: false,
                },
            },
        )
        .expect("fixture payload is valid");

        assert_eq!(payload.provider, "xai");
        assert_eq!(payload.summary.total, 0);
        assert_eq!(payload.summary.ok, 0);
        assert_eq!(payload.refresh.attempts, 0);
        assert_eq!(payload.refresh.successes, 0);
        assert_eq!(payload.refresh.failures, 0);
        assert!(payload.profiles.is_empty());
    }

    #[test]
    fn openai_oauth_bootstrap_response_builds_launch_payload() {
        let payload =
            build_openai_oauth_launch_payload(control_plane::OpenAiOAuthBootstrapEnvelope {
                contract: control_plane::ContractDescriptor {
                    contract_version: control_plane::CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
                },
                provider: "openai".to_owned(),
                attempt_id: "attempt-1".to_owned(),
                authorization_url: "https://auth.openai.example/authorize".to_owned(),
                expires_at_unix_ms: 1_772_000_000_000,
                profile_id: Some("openai-default".to_owned()),
                message: "Open the authorization URL to reconnect OpenAI.".to_owned(),
            });

        assert_eq!(payload.attempt_id, "attempt-1");
        assert_eq!(payload.authorization_url, "https://auth.openai.example/authorize");
        assert_eq!(payload.expires_at_unix_ms, 1_772_000_000_000);
        assert_eq!(payload.profile_id.as_deref(), Some("openai-default"));
        assert!(!payload.opened);
        assert_eq!(payload.message, "Open the authorization URL to reconnect OpenAI.");
    }

    #[test]
    fn openai_oauth_launch_text_output_includes_authorization_url() {
        let payload = OpenAiOAuthLaunchPayload {
            attempt_id: "attempt-1".to_owned(),
            authorization_url: "https://auth.openai.example/authorize?state=attempt-1".to_owned(),
            expires_at_unix_ms: 1_772_000_000_000,
            profile_id: Some("openai-default".to_owned()),
            opened: false,
            message: "Open the authorization URL to reconnect OpenAI.".to_owned(),
        };
        let output = openai_oauth_launch_text_lines(&payload).join("\n");

        assert!(output.contains("authorization_url_present=true"), "{output}");
        assert!(
            output.contains(
                "auth.openai.oauth.authorization_url=\"https://auth.openai.example/authorize?state=attempt-1\""
            ),
            "{output}"
        );
    }

    #[test]
    fn anthropic_oauth_authorization_url_uses_public_claude_client() {
        let url = build_anthropic_oauth_authorization_url("challenge", "state-123")
            .expect("authorization URL should build");
        let parsed = reqwest::Url::parse(url.as_str()).expect("authorization URL should parse");

        assert_eq!(
            format!("{}://{}{}", parsed.scheme(), parsed.host_str().unwrap_or(""), parsed.path()),
            ANTHROPIC_OAUTH_AUTHORIZE_URL
        );
        let params = parsed.query_pairs().collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            params.get("client_id").map(|value| value.as_ref()),
            Some(ANTHROPIC_OAUTH_CLIENT_ID)
        );
        assert_eq!(
            params.get("redirect_uri").map(|value| value.as_ref()),
            Some(ANTHROPIC_OAUTH_REDIRECT_URI)
        );
        assert_eq!(params.get("scope").map(|value| value.as_ref()), Some(ANTHROPIC_OAUTH_SCOPES));
        assert_eq!(params.get("code_challenge").map(|value| value.as_ref()), Some("challenge"));
        assert_eq!(params.get("state").map(|value| value.as_ref()), Some("state-123"));
    }

    #[test]
    fn anthropic_authorization_code_requires_matching_state() {
        let code = parse_anthropic_authorization_code("auth-code#expected-state", "expected-state")
            .expect("matching state should be accepted");
        assert_eq!(code, "auth-code");

        let error = parse_anthropic_authorization_code("auth-code#other-state", "expected-state")
            .expect_err("mismatched state should be rejected");
        assert!(error.to_string().contains("state mismatch"));
    }

    #[test]
    fn anthropic_oauth_token_response_allows_missing_expiry() {
        let parsed: AnthropicOAuthTokenResponse =
            serde_json::from_value(json!({"access_token": "at", "refresh_token": "rt"}))
                .expect("token response should decode without expires_in");

        assert_eq!(parsed.access_token.as_deref(), Some("at"));
        assert_eq!(parsed.refresh_token.as_deref(), Some("rt"));
        assert_eq!(parsed.expires_in, None);
    }

    #[test]
    fn xai_oauth_authorization_url_uses_public_grok_client() {
        let url = build_xai_oauth_authorization_url(
            "https://auth.x.ai/oauth/authorize",
            "challenge",
            "state-123",
            "nonce-123",
        )
        .expect("authorization URL should build");
        let parsed = reqwest::Url::parse(url.as_str()).expect("authorization URL should parse");

        assert_eq!(
            format!("{}://{}{}", parsed.scheme(), parsed.host_str().unwrap_or(""), parsed.path()),
            "https://auth.x.ai/oauth/authorize"
        );
        let params = parsed.query_pairs().collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(params.get("client_id").map(|value| value.as_ref()), Some(XAI_OAUTH_CLIENT_ID));
        assert_eq!(
            params.get("redirect_uri").map(|value| value.as_ref()),
            Some(XAI_OAUTH_REDIRECT_URI)
        );
        assert_eq!(params.get("scope").map(|value| value.as_ref()), Some(XAI_OAUTH_SCOPE));
        assert_eq!(params.get("code_challenge").map(|value| value.as_ref()), Some("challenge"));
        assert_eq!(params.get("code_challenge_method").map(|value| value.as_ref()), Some("S256"));
        assert_eq!(params.get("state").map(|value| value.as_ref()), Some("state-123"));
        assert_eq!(params.get("nonce").map(|value| value.as_ref()), Some("nonce-123"));
        assert_eq!(params.get("plan").map(|value| value.as_ref()), Some("generic"));
        assert_eq!(params.get("referrer").map(|value| value.as_ref()), Some("palyra"));
    }

    #[test]
    fn xai_callback_url_requires_loopback_redirect_and_matching_state() {
        let callback = parse_xai_callback_url(
            "http://127.0.0.1:56121/callback?code=auth-code&state=expected-state",
            "expected-state",
        )
        .expect("matching loopback callback should be accepted");
        assert_eq!(callback.code, "auth-code");

        let state_error = parse_xai_callback_url(
            "http://127.0.0.1:56121/callback?code=auth-code&state=other-state",
            "expected-state",
        )
        .expect_err("mismatched state should be rejected");
        let host_error = parse_xai_callback_url(
            "http://localhost:56121/callback?code=auth-code&state=expected-state",
            "expected-state",
        )
        .expect_err("unexpected host should be rejected");

        assert!(state_error.to_string().contains("state mismatch"));
        assert!(host_error.to_string().contains("does not match"));
    }

    #[test]
    fn xai_callback_response_write_failure_is_best_effort() {
        struct ResetWriter;

        impl std::io::Write for ResetWriter {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::ConnectionReset.into())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut writer = ResetWriter;
        write_xai_callback_response_best_effort(
            &mut writer,
            "HTTP/1.1 200 OK",
            "xAI authentication completed. You can close this window.",
            None,
        );
    }

    #[test]
    fn xai_oauth_endpoint_normalizer_rejects_untrusted_hosts() {
        let trusted =
            normalize_xai_oauth_endpoint("https://auth.x.ai/oauth/token", "token_endpoint")
                .expect("xAI auth endpoint should be trusted");
        let subdomain =
            normalize_xai_oauth_endpoint("https://accounts.x.ai/oauth/token", "token_endpoint")
                .expect("xAI subdomain should be trusted");
        let hostile =
            normalize_xai_oauth_endpoint("https://attacker.example/token", "token_endpoint")
                .expect_err("hostile endpoint should be rejected");
        let credentials = normalize_xai_oauth_endpoint(
            "https://user:secret@auth.x.ai/oauth/token",
            "token_endpoint",
        )
        .expect_err("embedded credentials should be rejected");

        assert_eq!(trusted, "https://auth.x.ai/oauth/token");
        assert_eq!(subdomain, "https://accounts.x.ai/oauth/token");
        assert!(hostile.to_string().contains("host is not trusted"));
        assert!(credentials.to_string().contains("embedded credentials"));
    }
}
