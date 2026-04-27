use std::sync::Arc;

use palyra_auth::{
    AuthCredential, AuthCredentialType, AuthExpiryDistribution, AuthHealthSummary,
    AuthProfileError, AuthProfileHealthState, AuthProfileListFilter, AuthProfileRecord,
    AuthProfileScope, AuthProfileSetRequest, AuthProvider, AuthProviderKind, AuthScopeFilter,
    OAuthRefreshOutcome,
};
use serde_json::json;
use tonic::Status;
use ulid::Ulid;

use crate::{
    gateway::{current_unix_ms, non_empty, AuthRefreshMetricsSnapshot, GatewayRuntimeState},
    journal::JournalAppendRequest,
    transport::grpc::{
        auth::RequestContext,
        proto::palyra::{auth::v1 as auth_v1, common::v1 as common_v1},
    },
};

pub(crate) fn map_auth_profile_error(error: AuthProfileError) -> Status {
    match error {
        AuthProfileError::InvalidField { .. } | AuthProfileError::InvalidPath { .. } => {
            Status::invalid_argument(error.to_string())
        }
        AuthProfileError::UnsupportedVersion(_) => Status::failed_precondition(error.to_string()),
        AuthProfileError::ProfileNotFound(_) => Status::not_found(error.to_string()),
        AuthProfileError::RegistryLimitExceeded => Status::resource_exhausted(error.to_string()),
        AuthProfileError::ReadRegistry { .. }
        | AuthProfileError::ParseRegistry { .. }
        | AuthProfileError::WriteRegistry { .. }
        | AuthProfileError::SerializeRegistry(_)
        | AuthProfileError::LockPoisoned
        | AuthProfileError::InvalidSystemTime(_) => Status::internal(error.to_string()),
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn auth_list_filter_from_proto(
    payload: auth_v1::ListAuthProfilesRequest,
) -> Result<AuthProfileListFilter, Status> {
    let provider_kind = auth_v1::AuthProviderKind::try_from(payload.provider_kind)
        .unwrap_or(auth_v1::AuthProviderKind::Unspecified);
    let provider = match provider_kind {
        auth_v1::AuthProviderKind::Unspecified => None,
        auth_v1::AuthProviderKind::Custom => {
            let custom_name = payload.provider_custom_name.trim();
            if custom_name.is_empty() {
                return Err(Status::invalid_argument(
                    "provider_custom_name is required when provider_kind=custom",
                ));
            }
            Some(AuthProvider {
                kind: AuthProviderKind::Custom,
                custom_name: Some(custom_name.to_owned()),
            })
        }
        _ => Some(AuthProvider {
            kind: auth_provider_kind_from_proto(provider_kind)?,
            custom_name: None,
        }),
    };
    let scope_kind = auth_v1::AuthScopeKind::try_from(payload.scope_kind)
        .unwrap_or(auth_v1::AuthScopeKind::Unspecified);
    let scope = match scope_kind {
        auth_v1::AuthScopeKind::Unspecified => None,
        auth_v1::AuthScopeKind::Global => Some(AuthScopeFilter::Global),
        auth_v1::AuthScopeKind::Agent => {
            let agent_id = payload.scope_agent_id.trim();
            if agent_id.is_empty() {
                return Err(Status::invalid_argument(
                    "scope_agent_id is required when scope_kind=agent",
                ));
            }
            Some(AuthScopeFilter::Agent { agent_id: agent_id.to_owned() })
        }
    };
    Ok(AuthProfileListFilter {
        after_profile_id: non_empty(payload.after_profile_id),
        limit: if payload.limit == 0 { None } else { Some(payload.limit as usize) },
        provider,
        scope,
    })
}

#[allow(clippy::result_large_err)]
pub(crate) fn auth_set_request_from_proto(
    profile: auth_v1::AuthProfile,
) -> Result<AuthProfileSetRequest, Status> {
    let provider = auth_provider_from_proto(
        profile.provider.ok_or_else(|| Status::invalid_argument("profile.provider is required"))?,
    )?;
    let scope = auth_scope_from_proto(
        profile.scope.ok_or_else(|| Status::invalid_argument("profile.scope is required"))?,
    )?;
    let credential = auth_credential_from_proto(
        profile
            .credential
            .ok_or_else(|| Status::invalid_argument("profile.credential is required"))?,
    )?;
    Ok(AuthProfileSetRequest {
        profile_id: profile.profile_id,
        provider,
        profile_name: profile.profile_name,
        scope,
        credential,
    })
}

#[allow(clippy::result_large_err)]
fn auth_provider_from_proto(provider: auth_v1::AuthProvider) -> Result<AuthProvider, Status> {
    let kind = auth_v1::AuthProviderKind::try_from(provider.kind)
        .unwrap_or(auth_v1::AuthProviderKind::Unspecified);
    if kind == auth_v1::AuthProviderKind::Unspecified {
        return Err(Status::invalid_argument("profile.provider.kind must be specified"));
    }
    if kind == auth_v1::AuthProviderKind::Custom {
        let custom_name = provider.custom_name.trim();
        if custom_name.is_empty() {
            return Err(Status::invalid_argument(
                "profile.provider.custom_name is required for custom providers",
            ));
        }
        return Ok(AuthProvider {
            kind: AuthProviderKind::Custom,
            custom_name: Some(custom_name.to_owned()),
        });
    }
    Ok(AuthProvider { kind: auth_provider_kind_from_proto(kind)?, custom_name: None })
}

#[allow(clippy::result_large_err)]
fn auth_scope_from_proto(scope: auth_v1::AuthScope) -> Result<AuthProfileScope, Status> {
    match auth_v1::AuthScopeKind::try_from(scope.kind)
        .unwrap_or(auth_v1::AuthScopeKind::Unspecified)
    {
        auth_v1::AuthScopeKind::Global => Ok(AuthProfileScope::Global),
        auth_v1::AuthScopeKind::Agent => {
            let agent_id = scope.agent_id.trim();
            if agent_id.is_empty() {
                return Err(Status::invalid_argument(
                    "profile.scope.agent_id is required for agent scope",
                ));
            }
            Ok(AuthProfileScope::Agent { agent_id: agent_id.to_owned() })
        }
        auth_v1::AuthScopeKind::Unspecified => {
            Err(Status::invalid_argument("profile.scope.kind must be specified"))
        }
    }
}

#[allow(clippy::result_large_err)]
fn auth_credential_from_proto(
    credential: auth_v1::AuthCredential,
) -> Result<AuthCredential, Status> {
    match credential.kind {
        Some(auth_v1::auth_credential::Kind::ApiKey(value)) => {
            Ok(AuthCredential::ApiKey { api_key_vault_ref: value.api_key_vault_ref })
        }
        Some(auth_v1::auth_credential::Kind::Oauth(value)) => Ok(AuthCredential::Oauth {
            access_token_vault_ref: value.access_token_vault_ref,
            refresh_token_vault_ref: value.refresh_token_vault_ref,
            token_endpoint: value.token_endpoint,
            client_id: non_empty(value.client_id),
            client_secret_vault_ref: non_empty(value.client_secret_vault_ref),
            scopes: value.scopes,
            expires_at_unix_ms: if value.expires_at_unix_ms > 0 {
                Some(value.expires_at_unix_ms)
            } else {
                None
            },
            refresh_state: if let Some(refresh_state) = value.refresh_state {
                palyra_auth::OAuthRefreshState {
                    failure_count: refresh_state.failure_count,
                    last_error: non_empty(refresh_state.last_error),
                    last_attempt_unix_ms: if refresh_state.last_attempt_unix_ms > 0 {
                        Some(refresh_state.last_attempt_unix_ms)
                    } else {
                        None
                    },
                    last_success_unix_ms: if refresh_state.last_success_unix_ms > 0 {
                        Some(refresh_state.last_success_unix_ms)
                    } else {
                        None
                    },
                    next_allowed_refresh_unix_ms: if refresh_state.next_allowed_refresh_unix_ms > 0
                    {
                        Some(refresh_state.next_allowed_refresh_unix_ms)
                    } else {
                        None
                    },
                }
            } else {
                palyra_auth::OAuthRefreshState::default()
            },
        }),
        None => Err(Status::invalid_argument("profile.credential.kind is required")),
    }
}

#[allow(clippy::result_large_err)]
fn auth_provider_kind_from_proto(
    kind: auth_v1::AuthProviderKind,
) -> Result<AuthProviderKind, Status> {
    match kind {
        auth_v1::AuthProviderKind::Openai => Ok(AuthProviderKind::Openai),
        auth_v1::AuthProviderKind::Anthropic => Ok(AuthProviderKind::Anthropic),
        auth_v1::AuthProviderKind::Telegram => Ok(AuthProviderKind::Telegram),
        auth_v1::AuthProviderKind::Slack => Ok(AuthProviderKind::Slack),
        auth_v1::AuthProviderKind::Discord => Ok(AuthProviderKind::Discord),
        auth_v1::AuthProviderKind::Webhook => Ok(AuthProviderKind::Webhook),
        auth_v1::AuthProviderKind::Custom => Ok(AuthProviderKind::Custom),
        auth_v1::AuthProviderKind::Unspecified => {
            Err(Status::invalid_argument("provider kind must be specified"))
        }
    }
}

pub(crate) fn auth_profile_to_proto(profile: &AuthProfileRecord) -> auth_v1::AuthProfile {
    auth_v1::AuthProfile {
        profile_id: profile.profile_id.clone(),
        provider: Some(auth_provider_to_proto(&profile.provider)),
        profile_name: profile.profile_name.clone(),
        scope: Some(auth_scope_to_proto(&profile.scope)),
        credential: Some(auth_credential_to_proto(&profile.credential)),
        created_at_unix_ms: profile.created_at_unix_ms,
        updated_at_unix_ms: profile.updated_at_unix_ms,
    }
}

fn auth_provider_to_proto(provider: &AuthProvider) -> auth_v1::AuthProvider {
    auth_v1::AuthProvider {
        kind: match provider.kind {
            AuthProviderKind::Openai => auth_v1::AuthProviderKind::Openai as i32,
            AuthProviderKind::Anthropic => auth_v1::AuthProviderKind::Anthropic as i32,
            AuthProviderKind::Telegram => auth_v1::AuthProviderKind::Telegram as i32,
            AuthProviderKind::Slack => auth_v1::AuthProviderKind::Slack as i32,
            AuthProviderKind::Discord => auth_v1::AuthProviderKind::Discord as i32,
            AuthProviderKind::Webhook => auth_v1::AuthProviderKind::Webhook as i32,
            AuthProviderKind::Custom => auth_v1::AuthProviderKind::Custom as i32,
        },
        custom_name: provider.custom_name.clone().unwrap_or_default(),
    }
}

fn auth_scope_to_proto(scope: &AuthProfileScope) -> auth_v1::AuthScope {
    match scope {
        AuthProfileScope::Global => auth_v1::AuthScope {
            kind: auth_v1::AuthScopeKind::Global as i32,
            agent_id: String::new(),
        },
        AuthProfileScope::Agent { agent_id } => auth_v1::AuthScope {
            kind: auth_v1::AuthScopeKind::Agent as i32,
            agent_id: agent_id.clone(),
        },
    }
}

fn auth_credential_to_proto(credential: &AuthCredential) -> auth_v1::AuthCredential {
    match credential {
        AuthCredential::ApiKey { api_key_vault_ref } => auth_v1::AuthCredential {
            kind: Some(auth_v1::auth_credential::Kind::ApiKey(auth_v1::ApiKeyCredential {
                api_key_vault_ref: api_key_vault_ref.clone(),
            })),
        },
        AuthCredential::Oauth {
            access_token_vault_ref,
            refresh_token_vault_ref,
            token_endpoint,
            client_id,
            client_secret_vault_ref,
            scopes,
            expires_at_unix_ms,
            refresh_state,
        } => auth_v1::AuthCredential {
            kind: Some(auth_v1::auth_credential::Kind::Oauth(auth_v1::OAuthCredential {
                access_token_vault_ref: access_token_vault_ref.clone(),
                refresh_token_vault_ref: refresh_token_vault_ref.clone(),
                token_endpoint: token_endpoint.clone(),
                client_id: client_id.clone().unwrap_or_default(),
                client_secret_vault_ref: client_secret_vault_ref.clone().unwrap_or_default(),
                scopes: scopes.clone(),
                expires_at_unix_ms: expires_at_unix_ms.unwrap_or_default(),
                refresh_state: Some(auth_v1::OAuthRefreshState {
                    failure_count: refresh_state.failure_count,
                    last_error: refresh_state.last_error.clone().unwrap_or_default(),
                    last_attempt_unix_ms: refresh_state.last_attempt_unix_ms.unwrap_or_default(),
                    last_success_unix_ms: refresh_state.last_success_unix_ms.unwrap_or_default(),
                    next_allowed_refresh_unix_ms: refresh_state
                        .next_allowed_refresh_unix_ms
                        .unwrap_or_default(),
                }),
            })),
        },
    }
}

pub(crate) fn auth_health_summary_to_proto(
    summary: &AuthHealthSummary,
) -> auth_v1::AuthHealthSummary {
    auth_v1::AuthHealthSummary {
        total: summary.total,
        ok: summary.ok,
        expiring: summary.expiring,
        expired: summary.expired,
        missing: summary.missing,
        static_count: summary.static_count,
    }
}

pub(crate) fn auth_expiry_distribution_to_proto(
    distribution: &AuthExpiryDistribution,
) -> auth_v1::AuthExpiryDistribution {
    auth_v1::AuthExpiryDistribution {
        expired: distribution.expired,
        under_5m: distribution.under_5m,
        between_5m_15m: distribution.between_5m_15m,
        between_15m_60m: distribution.between_15m_60m,
        between_1h_24h: distribution.between_1h_24h,
        over_24h: distribution.over_24h,
        unknown: distribution.unknown,
        static_count: distribution.static_count,
        missing: distribution.missing,
    }
}

pub(crate) fn auth_health_profile_to_proto(
    health: &palyra_auth::AuthProfileHealthRecord,
) -> auth_v1::AuthProfileHealth {
    auth_v1::AuthProfileHealth {
        profile_id: health.profile_id.clone(),
        provider: health.provider.clone(),
        profile_name: health.profile_name.clone(),
        scope: health.scope.clone(),
        credential_type: match health.credential_type {
            AuthCredentialType::ApiKey => "api_key".to_owned(),
            AuthCredentialType::Oauth => "oauth".to_owned(),
        },
        state: auth_health_state_to_proto(health.state),
        reason: health.reason.clone(),
        expires_at_unix_ms: health.expires_at_unix_ms.unwrap_or_default(),
    }
}

fn auth_health_state_to_proto(state: AuthProfileHealthState) -> i32 {
    match state {
        AuthProfileHealthState::Ok => auth_v1::AuthHealthState::Ok as i32,
        AuthProfileHealthState::Expiring => auth_v1::AuthHealthState::Expiring as i32,
        AuthProfileHealthState::Expired => auth_v1::AuthHealthState::Expired as i32,
        AuthProfileHealthState::Missing => auth_v1::AuthHealthState::Missing as i32,
        AuthProfileHealthState::Static => auth_v1::AuthHealthState::Static as i32,
    }
}

pub(crate) fn auth_refresh_metrics_to_proto(
    metrics: &AuthRefreshMetricsSnapshot,
) -> auth_v1::AuthRefreshMetrics {
    auth_v1::AuthRefreshMetrics {
        attempts: metrics.attempts,
        successes: metrics.successes,
        failures: metrics.failures,
        by_provider: metrics
            .by_provider
            .iter()
            .map(|provider| auth_v1::ProviderRefreshMetric {
                provider: provider.provider.clone(),
                attempts: provider.attempts,
                successes: provider.successes,
                failures: provider.failures,
            })
            .collect(),
    }
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_auth_profile_saved_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    profile: &AuthProfileRecord,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "auth.profile.saved",
                "profile_id": profile.profile_id,
                "provider": profile.provider.label(),
                "scope": profile.scope.scope_key(),
                "credential_type": match profile.credential.credential_type() {
                    AuthCredentialType::ApiKey => "api_key",
                    AuthCredentialType::Oauth => "oauth",
                },
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_auth_profile_deleted_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    profile_id: &str,
    profile: Option<&AuthProfileRecord>,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": "auth.profile.deleted",
                "profile_id": profile_id,
                "provider": profile.map(|value| value.provider.label()),
                "scope": profile.map(|value| value.scope.scope_key()),
                "credential_type": profile.map(|value| match value.credential.credential_type() {
                    AuthCredentialType::ApiKey => "api_key",
                    AuthCredentialType::Oauth => "oauth",
                }),
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_auth_refresh_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    outcome: &OAuthRefreshOutcome,
) -> Result<(), Status> {
    if !outcome.kind.attempted() {
        return Ok(());
    }
    let event_name =
        if outcome.kind.success() { "auth.token.refreshed" } else { "auth.refresh.failed" };
    let redacted_reason = crate::model_provider::sanitize_remote_error(outcome.reason.as_str());
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event_name,
                "profile_id": outcome.profile_id,
                "provider": outcome.provider,
                "reason": redacted_reason,
                "next_allowed_refresh_unix_ms": outcome.next_allowed_refresh_unix_ms,
                "expires_at_unix_ms": outcome.expires_at_unix_ms,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn record_auth_runtime_operation_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    event_name: &str,
    payload: serde_json::Value,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: Ulid::new().to_string(),
            run_id: Ulid::new().to_string(),
            kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: json!({
                "event": event_name,
                "details": payload,
            })
            .to_string()
            .into_bytes(),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}
