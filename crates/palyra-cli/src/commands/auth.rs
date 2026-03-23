use crate::*;

pub(crate) fn run_auth(command: AuthCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for auth command"))?;
    let connection = root_context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::ADMIN)?;
    let runtime = build_runtime()?;
    runtime.block_on(run_auth_async(command, connection))
}

pub(crate) async fn run_auth_async(
    command: AuthCommand,
    connection: AgentConnection,
) -> Result<()> {
    let mut client =
        auth_v1::auth_service_client::AuthServiceClient::connect(connection.grpc_url.clone())
            .await
            .with_context(|| {
                format!("failed to connect auth gRPC endpoint {}", connection.grpc_url)
            })?;

    let AuthCommand::Profiles { command } = command;
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
    }

    std::io::stdout().flush().context("stdout flush failed")
}
