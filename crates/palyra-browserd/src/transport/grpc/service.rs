use crate::*;

#[derive(Debug, Clone, Copy)]
struct RelayPrivateTargetBlock;

#[derive(Clone)]
pub(crate) struct BrowserServiceImpl {
    pub(crate) runtime: Arc<BrowserRuntimeState>,
}

#[tonic::async_trait]
impl browser_v1::browser_service_server::BrowserService for BrowserServiceImpl {
    async fn health(
        &self,
        request: Request<browser_v1::BrowserHealthRequest>,
    ) -> Result<Response<browser_v1::BrowserHealthResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let active_sessions = self.runtime.sessions.lock().await.len();
        Ok(Response::new(browser_v1::BrowserHealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            status: "ok".to_owned(),
            uptime_seconds: self.runtime.started_at.elapsed().as_secs(),
            active_sessions: u32::try_from(active_sessions).unwrap_or(u32::MAX),
        }))
    }

    async fn create_session(
        &self,
        request: Request<browser_v1::CreateSessionRequest>,
    ) -> Result<Response<browser_v1::CreateSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = payload.principal.trim();
        if principal.is_empty() {
            return Err(Status::invalid_argument("principal is required"));
        }
        let channel = normalize_optional_string(payload.channel.as_str());
        let requested_profile_id = parse_optional_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let mut profile = resolve_session_profile(
            self.runtime.as_ref(),
            principal,
            requested_profile_id.as_deref(),
        )
        .await
        .map_err(Status::internal)?;

        let mut private_profile = payload.private_profile;
        let mut persistence_enabled = payload.persistence_enabled;
        let mut persistence_id = if payload.persistence_enabled {
            let Some(value) = sanitize_persistence_id(payload.persistence_id.as_str()) else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id",
                ));
            };
            Some(value)
        } else {
            None
        };
        let mut profile_id = None;
        if let Some(resolved_profile) = profile.as_ref() {
            profile_id = Some(resolved_profile.profile_id.clone());
            private_profile = private_profile || resolved_profile.private_profile;
            if resolved_profile.persistence_enabled && !private_profile {
                persistence_enabled = true;
                persistence_id = Some(resolved_profile.profile_id.clone());
            } else {
                persistence_enabled = false;
                persistence_id = None;
            }
        }

        let restored_snapshot = if persistence_enabled {
            let Some(store) = self.runtime.state_store.as_ref() else {
                return Err(Status::failed_precondition(
                    "state persistence requires PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
                ));
            };
            let Some(state_id) = persistence_id.as_ref() else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id",
                ));
            };
            store.load_snapshot(state_id.as_str(), profile_id.as_deref()).map_err(|error| {
                Status::internal(format!("failed to load persisted state: {error}"))
            })?
        } else {
            None
        };

        let session_id = Ulid::new().to_string();
        let now = Instant::now();
        let idle_ttl = if payload.idle_ttl_ms == 0 {
            self.runtime.default_idle_ttl
        } else {
            Duration::from_millis(payload.idle_ttl_ms)
        };
        let requested_budget = payload.budget.as_ref();
        let clamp_u64_budget = |requested: Option<u64>, default: u64| {
            requested.filter(|value| *value > 0).map(|value| value.min(default)).unwrap_or(default)
        };
        let clamp_usize_budget = |requested: Option<usize>, default: usize| {
            requested.filter(|value| *value > 0).map(|value| value.min(default)).unwrap_or(default)
        };
        let budget = SessionBudget {
            max_navigation_timeout_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_navigation_timeout_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_navigation_timeout_ms),
            max_session_lifetime_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_session_lifetime_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_session_lifetime_ms),
            max_screenshot_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_screenshot_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_screenshot_bytes),
            max_response_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_response_bytes),
                self.runtime.default_budget.max_response_bytes,
            ),
            max_action_timeout_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_action_timeout_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_action_timeout_ms),
            max_type_input_bytes: clamp_u64_budget(
                requested_budget.map(|value| value.max_type_input_bytes),
                self.runtime.default_budget.max_type_input_bytes,
            ),
            max_actions_per_session: clamp_u64_budget(
                requested_budget.map(|value| value.max_actions_per_session),
                self.runtime.default_budget.max_actions_per_session,
            ),
            max_actions_per_window: payload
                .budget
                .as_ref()
                .map(|value| value.max_actions_per_window)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_actions_per_window),
            action_rate_window_ms: payload
                .budget
                .as_ref()
                .map(|value| value.action_rate_window_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.action_rate_window_ms),
            max_action_log_entries: clamp_usize_budget(
                requested_budget
                    .map(|value| value.max_action_log_entries)
                    .and_then(|value| usize::try_from(value).ok()),
                self.runtime.default_budget.max_action_log_entries,
            ),
            max_observe_snapshot_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_observe_snapshot_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_observe_snapshot_bytes),
            max_visible_text_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_visible_text_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_visible_text_bytes),
            max_network_log_entries: payload
                .budget
                .as_ref()
                .map(|value| value.max_network_log_entries)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_network_log_entries),
            max_network_log_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_network_log_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_network_log_bytes),
            max_tabs_per_session: self.runtime.default_budget.max_tabs_per_session,
            max_title_bytes: self.runtime.default_budget.max_title_bytes,
        };
        let action_allowed_domains =
            normalize_action_allowed_domains(payload.action_allowed_domains.as_slice());
        let mut session = BrowserSessionRecord::with_defaults(BrowserSessionInit {
            principal: principal.to_owned(),
            channel: channel.clone(),
            now,
            idle_ttl,
            budget: budget.clone(),
            allow_private_targets: payload.allow_private_targets,
            allow_downloads: payload.allow_downloads,
            action_allowed_domains: action_allowed_domains.clone(),
            profile_id: profile_id.clone(),
            private_profile,
            persistence: SessionPersistenceState {
                enabled: persistence_enabled,
                persistence_id: persistence_id.clone(),
                state_restored: false,
            },
        });
        if let Some(restored_snapshot) = restored_snapshot {
            if let Some(profile_record) = profile.as_ref() {
                validate_restored_snapshot_against_profile(
                    &restored_snapshot.snapshot,
                    Some(restored_snapshot.raw_hash_sha256.as_str()),
                    profile_record,
                )
                .map_err(|error| {
                    Status::failed_precondition(format!(
                        "persisted state integrity validation failed: {error}"
                    ))
                })?;
            }
            let snapshot = restored_snapshot.snapshot;
            if snapshot.principal != principal {
                return Err(Status::permission_denied(
                    "persisted state principal does not match session principal",
                ));
            }
            if normalize_optional_string(snapshot.channel.as_deref().unwrap_or_default()) != channel
            {
                return Err(Status::permission_denied(
                    "persisted state channel does not match session channel",
                ));
            }
            session.apply_snapshot(snapshot);
            session.persistence.state_restored = true;
        }
        if let Some(record) = profile.as_mut() {
            record.last_used_unix_ms = current_unix_ms();
            record.updated_at_unix_ms = record.last_used_unix_ms;
            refresh_profile_record_hash(record);
            if let Some(store) = self.runtime.state_store.as_ref() {
                upsert_profile_record(
                    store,
                    &self.runtime.profile_registry_lock,
                    record.clone(),
                    false,
                )
                .await
                .map_err(|error| {
                    Status::internal(format!("failed to update browser profile usage: {error}"))
                })?;
            }
        }
        let state_restored = session.persistence.state_restored;
        let persist_on_create = persistence_enabled;
        let mut session_for_persist = None;
        {
            let mut sessions = self.runtime.sessions.lock().await;
            if sessions.len() >= self.runtime.max_sessions {
                return Err(Status::resource_exhausted("browser session capacity reached"));
            }
            sessions.insert(session_id.clone(), session.clone());
            if persist_on_create {
                session_for_persist = Some(session);
            }
        }
        if let (Some(store), Some(record)) =
            (self.runtime.state_store.as_ref(), session_for_persist)
        {
            persist_session_snapshot(store, &record)
                .map_err(|error| Status::internal(format!("failed to persist state: {error}")))?;
        }
        if payload.allow_downloads {
            let sandbox = DownloadSandboxSession::new().map_err(Status::internal)?;
            self.runtime.download_sessions.lock().await.insert(session_id.clone(), sandbox);
        }
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            let session_snapshot = {
                let sessions = self.runtime.sessions.lock().await;
                sessions.get(session_id.as_str()).cloned()
            }
            .ok_or_else(|| Status::internal("session registration race during engine init"))?;
            if let Err(error) = initialize_chromium_session_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                &session_snapshot,
            )
            .await
            {
                self.runtime.sessions.lock().await.remove(session_id.as_str());
                self.runtime.download_sessions.lock().await.remove(session_id.as_str());
                return Err(Status::internal(format!(
                    "failed to initialize chromium session runtime: {error}"
                )));
            }
        }

        Ok(Response::new(browser_v1::CreateSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            created_at_unix_ms: current_unix_ms(),
            effective_budget: Some(browser_v1::SessionBudget {
                max_navigation_timeout_ms: budget.max_navigation_timeout_ms,
                max_session_lifetime_ms: budget.max_session_lifetime_ms,
                max_screenshot_bytes: budget.max_screenshot_bytes,
                max_response_bytes: budget.max_response_bytes,
                max_action_timeout_ms: budget.max_action_timeout_ms,
                max_type_input_bytes: budget.max_type_input_bytes,
                max_actions_per_session: budget.max_actions_per_session,
                max_actions_per_window: budget.max_actions_per_window,
                action_rate_window_ms: budget.action_rate_window_ms,
                max_action_log_entries: budget.max_action_log_entries as u64,
                max_observe_snapshot_bytes: budget.max_observe_snapshot_bytes,
                max_visible_text_bytes: budget.max_visible_text_bytes,
                max_network_log_entries: budget.max_network_log_entries as u64,
                max_network_log_bytes: budget.max_network_log_bytes,
            }),
            downloads_enabled: payload.allow_downloads,
            action_allowed_domains,
            persistence_enabled,
            persistence_id: persistence_id.unwrap_or_default(),
            state_restored,
            profile_id: profile_id
                .clone()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
            private_profile,
        }))
    }

    async fn close_session(
        &self,
        request: Request<browser_v1::CloseSessionRequest>,
    ) -> Result<Response<browser_v1::CloseSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let session_id = parse_session_id_from_proto(request.into_inner().session_id)
            .map_err(Status::invalid_argument)?;
        let removed = self.runtime.sessions.lock().await.remove(session_id.as_str());
        self.runtime.chromium_sessions.lock().await.remove(session_id.as_str());
        self.runtime.download_sessions.lock().await.remove(session_id.as_str());
        if let (Some(store), Some(record)) = (self.runtime.state_store.as_ref(), removed.as_ref()) {
            if record.persistence.enabled {
                persist_session_snapshot(store, record).map_err(|error| {
                    Status::internal(format!(
                        "failed to persist state while closing session: {error}"
                    ))
                })?;
            }
        }
        Ok(Response::new(browser_v1::CloseSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            closed: removed.is_some(),
            reason: if removed.is_some() {
                "closed".to_owned()
            } else {
                "session_not_found".to_owned()
            },
        }))
    }

    async fn list_profiles(
        &self,
        request: Request<browser_v1::ListProfilesRequest>,
    ) -> Result<Response<browser_v1::ListProfilesResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let active_profile_id =
            registry.active_profile_by_principal.get(principal.as_str()).cloned();
        let mut profiles = registry
            .profiles
            .drain(..)
            .filter(|profile| profile.principal == principal)
            .collect::<Vec<_>>();
        profiles.sort_by(|left, right| right.last_used_unix_ms.cmp(&left.last_used_unix_ms));
        Ok(Response::new(browser_v1::ListProfilesResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profiles: profiles
                .iter()
                .map(|profile| {
                    profile_record_to_proto(
                        profile,
                        active_profile_id
                            .as_deref()
                            .map(|value| value == profile.profile_id.as_str())
                            .unwrap_or(false),
                    )
                })
                .collect(),
            active_profile_id: active_profile_id
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn create_profile(
        &self,
        request: Request<browser_v1::CreateProfileRequest>,
    ) -> Result<Response<browser_v1::CreateProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let theme = normalize_profile_theme(payload.theme_color.as_str())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        prune_profiles_for_principal(&mut registry, principal.as_str());
        let now = current_unix_ms();
        let mut profile = BrowserProfileRecord {
            profile_id: Ulid::new().to_string(),
            principal: principal.clone(),
            name,
            theme_color: theme,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            last_used_unix_ms: now,
            persistence_enabled: payload.persistence_enabled && !payload.private_profile,
            private_profile: payload.private_profile,
            state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
            state_revision: 0,
            state_hash_sha256: None,
            record_hash_sha256: String::new(),
        };
        refresh_profile_record_hash(&mut profile);
        registry.profiles.push(profile.clone());
        registry
            .active_profile_by_principal
            .entry(principal.clone())
            .or_insert_with(|| profile.profile_id.clone());
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile.profile_id)
            .unwrap_or(false);
        Ok(Response::new(browser_v1::CreateProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(profile_record_to_proto(&profile, active)),
        }))
    }

    async fn rename_profile(
        &self,
        request: Request<browser_v1::RenameProfileRequest>,
    ) -> Result<Response<browser_v1::RenameProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.name = name;
        profile.updated_at_unix_ms = current_unix_ms();
        profile.last_used_unix_ms = profile.updated_at_unix_ms;
        refresh_profile_record_hash(profile);
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile_id)
            .unwrap_or(false);
        let output = profile_record_to_proto(profile, active);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::RenameProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn delete_profile(
        &self,
        request: Request<browser_v1::DeleteProfileRequest>,
    ) -> Result<Response<browser_v1::DeleteProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let before = registry.profiles.len();
        registry.profiles.retain(|profile| {
            !(profile.profile_id == profile_id && profile.principal == principal)
        });
        let deleted = registry.profiles.len() != before;
        if deleted {
            if registry
                .active_profile_by_principal
                .get(principal.as_str())
                .map(|value| value == &profile_id)
                .unwrap_or(false)
            {
                let replacement = registry
                    .profiles
                    .iter()
                    .filter(|profile| profile.principal == principal)
                    .max_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
                    .map(|profile| profile.profile_id.clone());
                if let Some(value) = replacement {
                    registry.active_profile_by_principal.insert(principal.clone(), value);
                } else {
                    registry.active_profile_by_principal.remove(principal.as_str());
                }
            }
            prune_profile_registry(&mut registry);
            store.save_profile_registry(&registry).map_err(|error| {
                Status::internal(format!("failed to save browser profiles after delete: {error}"))
            })?;
            store.delete_snapshot(profile_id.as_str()).map_err(|error| {
                Status::internal(format!("failed to delete browser profile snapshot: {error}"))
            })?;
        }
        Ok(Response::new(browser_v1::DeleteProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
            active_profile_id: registry
                .active_profile_by_principal
                .get(principal.as_str())
                .cloned()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn set_active_profile(
        &self,
        request: Request<browser_v1::SetActiveProfileRequest>,
    ) -> Result<Response<browser_v1::SetActiveProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.last_used_unix_ms = current_unix_ms();
        profile.updated_at_unix_ms = profile.last_used_unix_ms;
        refresh_profile_record_hash(profile);
        let output = profile_record_to_proto(profile, true);
        registry.active_profile_by_principal.insert(principal, profile_id);
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::SetActiveProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn navigate(
        &self,
        request: Request<browser_v1::NavigateRequest>,
    ) -> Result<Response<browser_v1::NavigateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let url = payload.url.trim().to_owned();
        if url.is_empty() {
            return Err(Status::invalid_argument("navigate requires non-empty url"));
        }
        let (timeout_ms, max_response_bytes, allow_private_targets, cookie_header) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Err(Status::not_found("browser session not found"));
            };
            session.last_active = Instant::now();
            let timeout_ms =
                payload.timeout_ms.max(1).min(session.budget.max_navigation_timeout_ms);
            let cookie_header = cookie_header_for_url(session, url.as_str());
            (
                timeout_ms,
                session.budget.max_response_bytes,
                payload.allow_private_targets || session.allow_private_targets,
                cookie_header,
            )
        };

        let outcome = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                navigate_with_guards(
                    url.as_str(),
                    timeout_ms,
                    payload.allow_redirects,
                    if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                    allow_private_targets,
                    max_response_bytes,
                    cookie_header.as_deref(),
                )
                .await
            }
            BrowserEngineMode::Chromium => {
                navigate_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    ChromiumNavigateParams {
                        raw_url: url.clone(),
                        timeout_ms,
                        allow_redirects: payload.allow_redirects,
                        max_redirects: if payload.max_redirects == 0 {
                            3
                        } else {
                            payload.max_redirects
                        },
                        allow_private_targets,
                        max_response_bytes,
                        cookie_header: cookie_header.clone(),
                    },
                )
                .await
            }
        };
        let network_log_entries = outcome.network_log.clone();
        let cookie_updates = outcome.cookie_updates.clone();
        let mut session_for_persist = None;

        let mut sessions = self.runtime.sessions.lock().await;
        if let Some(session) = sessions.get_mut(session_id.as_str()) {
            let max_network_log_entries = session.budget.max_network_log_entries;
            let max_network_log_bytes = session.budget.max_network_log_bytes;
            if let Some(tab) = session.active_tab_mut() {
                if outcome.success {
                    tab.last_title = outcome.title.clone();
                    tab.last_url = Some(outcome.final_url.clone());
                    tab.last_page_body = outcome.page_body.clone();
                    tab.scroll_x = 0;
                    tab.scroll_y = 0;
                    tab.typed_inputs.clear();
                }
                append_network_log_entries(
                    tab,
                    network_log_entries.as_slice(),
                    max_network_log_entries,
                    max_network_log_bytes,
                );
            }
            apply_cookie_updates(session, cookie_updates.as_slice());
            session.last_active = Instant::now();
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
        }
        drop(sessions);
        if let (Some(store), Some(record)) =
            (self.runtime.state_store.as_ref(), session_for_persist)
        {
            persist_session_snapshot(store, &record).map_err(|error| {
                Status::internal(format!("failed to persist state after navigate: {error}"))
            })?;
        }

        Ok(Response::new(browser_v1::NavigateResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: outcome.success,
            final_url: outcome.final_url,
            status_code: u32::from(outcome.status_code),
            title: truncate_utf8_bytes(
                outcome.title.as_str(),
                self.runtime.default_budget.max_title_bytes as usize,
            ),
            body_bytes: outcome.body_bytes,
            latency_ms: outcome.latency_ms,
            error: outcome.error,
        }))
    }

    async fn click(
        &self,
        request: Request<browser_v1::ClickRequest>,
    ) -> Result<Response<browser_v1::ClickResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("click requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ClickResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    artifact: None,
                }));
            }
        };

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let max_attempts = payload.max_retries.clamp(0, 16).saturating_add(1);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{}' was not found", selector);
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if is_download_like_tag(tag.as_str()) && !context.allow_downloads {
                            outcome = "download_blocked".to_owned();
                            error =
                                "download-like click is blocked by session policy (allow_downloads=false)"
                                    .to_owned();
                            break;
                        }
                        success = true;
                        outcome = if is_download_like_tag(tag.as_str()) {
                            "download_allowed".to_owned()
                        } else {
                            "clicked".to_owned()
                        };
                        error.clear();
                        break;
                    }
                    if attempts >= max_attempts
                        || started_at.elapsed() >= Duration::from_millis(timeout_ms)
                    {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = click_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    timeout_ms,
                    max_attempts,
                    context.allow_downloads,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };
        let mut success = success;
        let mut outcome = outcome;
        let mut error = error;
        let mut artifact = None;
        if success && outcome == "download_allowed" {
            match capture_download_artifact_for_click(
                self.runtime.as_ref(),
                session_id.as_str(),
                selector,
                &context,
                timeout_ms,
            )
            .await
            {
                Ok(record) => {
                    if record.quarantined {
                        outcome = "download_quarantined".to_owned();
                    }
                    artifact = Some(download_artifact_to_proto(&record));
                }
                Err(download_error) => {
                    success = false;
                    outcome = "download_failed".to_owned();
                    error = download_error;
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "click",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "click")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ClickResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            artifact,
        }))
    }

    async fn r#type(
        &self,
        request: Request<browser_v1::TypeRequest>,
    ) -> Result<Response<browser_v1::TypeResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("type requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::TypeResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    typed_bytes: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let text = payload.text;
        if (text.len() as u64) > context.budget.max_type_input_bytes {
            let error = format!(
                "type input exceeds max_type_input_bytes ({} > {})",
                text.len(),
                context.budget.max_type_input_bytes
            );
            let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
                finalize_session_action(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    FinalizeActionRequest {
                        action_name: "type",
                        selector,
                        success: false,
                        outcome: "input_too_large",
                        error: error.as_str(),
                        started_at_unix_ms: current_unix_ms(),
                        attempts: 1,
                        capture_failure_screenshot: payload.capture_failure_screenshot,
                        max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                    },
                )
                .await;
            return Ok(Response::new(browser_v1::TypeResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                typed_bytes: 0,
                error,
                action_log,
                failure_screenshot_bytes,
                failure_screenshot_mime_type,
            }));
        }

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{}' was not found", selector);
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if !is_typable_tag(tag.as_str()) {
                            outcome = "selector_not_typable".to_owned();
                            error = format!(
                                "selector '{}' does not target an input-like element",
                                selector
                            );
                            break;
                        }
                        success = true;
                        outcome = "typed".to_owned();
                        error.clear();
                        break;
                    }
                    if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = type_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    text.as_str(),
                    payload.clear_existing,
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        if success {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                let mut origin = None;
                if let Some(tab) = session.active_tab_mut() {
                    let field = tab.typed_inputs.entry(selector.to_owned()).or_default();
                    if payload.clear_existing {
                        *field = text.clone();
                    } else {
                        field.push_str(text.as_str());
                    }
                    origin = tab.last_url.as_deref().and_then(url_origin_key);
                }
                if let Some(origin_key) = origin {
                    apply_storage_entry_update(
                        session,
                        origin_key.as_str(),
                        selector,
                        text.as_str(),
                        payload.clear_existing,
                    );
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "type",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "type")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::TypeResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            typed_bytes: if success { text.len() as u64 } else { 0 },
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn scroll(
        &self,
        request: Request<browser_v1::ScrollRequest>,
    ) -> Result<Response<browser_v1::ScrollResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;

        let _context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            false,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ScrollResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    scroll_x: 0,
                    scroll_y: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let (success, scroll_x, scroll_y, error) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let mut scroll_x = 0_i64;
                let mut scroll_y = 0_i64;
                {
                    let mut sessions = self.runtime.sessions.lock().await;
                    if let Some(session) = sessions.get_mut(session_id.as_str()) {
                        if let Some(tab) = session.active_tab_mut() {
                            tab.scroll_x = tab.scroll_x.saturating_add(payload.delta_x);
                            tab.scroll_y = tab.scroll_y.saturating_add(payload.delta_y);
                            scroll_x = tab.scroll_x;
                            scroll_y = tab.scroll_y;
                        }
                    }
                }
                (true, scroll_x, scroll_y, String::new())
            }
            BrowserEngineMode::Chromium => {
                let result = scroll_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    payload.delta_x,
                    payload.delta_y,
                )
                .await;
                (result.success, result.scroll_x, result.scroll_y, result.error)
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "scroll",
                    selector: "",
                    success,
                    outcome: if success { "scrolled" } else { "scroll_failed" },
                    error: error.as_str(),
                    started_at_unix_ms: current_unix_ms(),
                    attempts: 1,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "scroll")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ScrollResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            scroll_x,
            scroll_y,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn wait_for(
        &self,
        request: Request<browser_v1::WaitForRequest>,
    ) -> Result<Response<browser_v1::WaitForResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim().to_owned();
        let text = payload.text;
        if selector.is_empty() && text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "wait_for requires non-empty selector or non-empty text",
            ));
        }
        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::WaitForResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    waited_ms: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    matched_selector: String::new(),
                    matched_text: String::new(),
                }));
            }
        };

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let poll_interval_ms = payload.poll_interval_ms.clamp(25, 1_000);
        let started_at_unix_ms = current_unix_ms();
        let (success, matched_selector, matched_text, attempts, waited_ms, error) =
            match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => {
                    let started = Instant::now();
                    let mut attempts = 0_u32;
                    let mut matched_selector = String::new();
                    let mut matched_text = String::new();
                    let mut success = false;
                    loop {
                        attempts = attempts.saturating_add(1);
                        if !selector.is_empty()
                            && find_matching_html_tag(selector.as_str(), context.page_body.as_str())
                                .is_some()
                        {
                            matched_selector = selector.clone();
                            success = true;
                            break;
                        }
                        if !text.trim().is_empty() && context.page_body.contains(text.as_str()) {
                            matched_text = text.clone();
                            success = true;
                            break;
                        }
                        if started.elapsed() >= Duration::from_millis(timeout_ms) {
                            break;
                        }
                        let remaining_ms =
                            timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
                        let sleep_ms = poll_interval_ms.min(remaining_ms.max(1));
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    }
                    let waited_ms = started.elapsed().as_millis() as u64;
                    let error = if success {
                        String::new()
                    } else {
                        "wait_for condition was not satisfied before timeout".to_owned()
                    };
                    (success, matched_selector, matched_text, attempts, waited_ms, error)
                }
                BrowserEngineMode::Chromium => {
                    let result = wait_for_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        selector.as_str(),
                        text.as_str(),
                        timeout_ms,
                        poll_interval_ms,
                    )
                    .await;
                    (
                        result.success,
                        result.matched_selector,
                        result.matched_text,
                        result.attempts,
                        result.waited_ms,
                        result.error,
                    )
                }
            };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "wait_for",
                    selector: selector.as_str(),
                    success,
                    outcome: if success { "condition_matched" } else { "condition_timeout" },
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "wait_for")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::WaitForResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            waited_ms,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            matched_selector,
            matched_text,
        }))
    }

    async fn get_title(
        &self,
        request: Request<browser_v1::GetTitleRequest>,
    ) -> Result<Response<browser_v1::GetTitleResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let max_title_bytes = usize::try_from(payload.max_title_bytes)
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(self.runtime.default_budget.max_title_bytes as usize);
        let active_tab_id = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            tab.tab_id.clone()
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Ok(title) = chromium_get_title(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                        tab.last_title = title;
                    }
                }
            }
        }
        let title = {
            let sessions = self.runtime.sessions.lock().await;
            sessions
                .get(session_id.as_str())
                .and_then(|session| session.tabs.get(active_tab_id.as_str()))
                .map(|tab| tab.last_title.clone())
                .unwrap_or_default()
        };
        Ok(Response::new(browser_v1::GetTitleResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            title: truncate_utf8_bytes(title.as_str(), max_title_bytes),
            error: String::new(),
        }))
    }

    async fn screenshot(
        &self,
        request: Request<browser_v1::ScreenshotRequest>,
    ) -> Result<Response<browser_v1::ScreenshotResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        if !payload.format.trim().is_empty() && !payload.format.trim().eq_ignore_ascii_case("png") {
            return Err(Status::invalid_argument("screenshot format must be empty or 'png'"));
        }
        let max_bytes = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ScreenshotResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    image_bytes: Vec::new(),
                    mime_type: "image/png".to_owned(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            payload.max_bytes.max(1).min(session.budget.max_screenshot_bytes)
        };
        let image_bytes = if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            match chromium_screenshot(self.runtime.as_ref(), session_id.as_str()).await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(Response::new(browser_v1::ScreenshotResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        image_bytes: Vec::new(),
                        mime_type: "image/png".to_owned(),
                        error,
                    }));
                }
            }
        } else {
            ONE_BY_ONE_PNG.to_vec()
        };
        if (image_bytes.len() as u64) > max_bytes {
            return Ok(Response::new(browser_v1::ScreenshotResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                image_bytes: Vec::new(),
                mime_type: "image/png".to_owned(),
                error: format!(
                    "screenshot output exceeds max_bytes ({} > {max_bytes})",
                    image_bytes.len()
                ),
            }));
        }
        Ok(Response::new(browser_v1::ScreenshotResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            image_bytes,
            mime_type: "image/png".to_owned(),
            error: String::new(),
        }))
    }

    async fn observe(
        &self,
        request: Request<browser_v1::ObserveRequest>,
    ) -> Result<Response<browser_v1::ObserveResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let include_dom_snapshot = if payload.include_dom_snapshot
            || payload.include_accessibility_tree
            || payload.include_visible_text
        {
            payload.include_dom_snapshot
        } else {
            true
        };
        let include_accessibility_tree = if payload.include_dom_snapshot
            || payload.include_accessibility_tree
            || payload.include_visible_text
        {
            payload.include_accessibility_tree
        } else {
            true
        };
        let include_visible_text = payload.include_visible_text;

        let (
            active_tab_id,
            max_dom_snapshot_bytes,
            max_accessibility_tree_bytes,
            max_visible_text_bytes,
        ) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            (
                tab.tab_id.clone(),
                payload.max_dom_snapshot_bytes.max(1).min(session.budget.max_observe_snapshot_bytes)
                    as usize,
                payload
                    .max_accessibility_tree_bytes
                    .max(1)
                    .min(session.budget.max_observe_snapshot_bytes) as usize,
                payload.max_visible_text_bytes.max(1).min(session.budget.max_visible_text_bytes)
                    as usize,
            )
        };

        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Ok(snapshot) = chromium_observe_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                        tab.last_page_body = snapshot.page_body;
                        tab.last_title = snapshot.title;
                        tab.last_url = Some(snapshot.page_url);
                    }
                }
            }
        }

        let (page_body, page_url) = {
            let sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            let Some(tab) = session.tabs.get(active_tab_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            (tab.last_page_body.clone(), tab.last_url.clone().unwrap_or_default())
        };
        if page_body.trim().is_empty() {
            return Ok(Response::new(browser_v1::ObserveResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                dom_snapshot: String::new(),
                accessibility_tree: String::new(),
                visible_text: String::new(),
                dom_truncated: false,
                accessibility_tree_truncated: false,
                visible_text_truncated: false,
                page_url: String::new(),
                error: "navigate must succeed before observe".to_owned(),
            }));
        }

        let (dom_snapshot, dom_truncated) = if include_dom_snapshot {
            build_dom_snapshot(page_body.as_str(), max_dom_snapshot_bytes)
        } else {
            (String::new(), false)
        };
        let (accessibility_tree, accessibility_tree_truncated) = if include_accessibility_tree {
            build_accessibility_tree_snapshot(page_body.as_str(), max_accessibility_tree_bytes)
        } else {
            (String::new(), false)
        };
        let (visible_text, visible_text_truncated) = if include_visible_text {
            build_visible_text_snapshot(page_body.as_str(), max_visible_text_bytes)
        } else {
            (String::new(), false)
        };

        Ok(Response::new(browser_v1::ObserveResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            dom_snapshot,
            accessibility_tree,
            visible_text,
            dom_truncated,
            accessibility_tree_truncated,
            visible_text_truncated,
            page_url: normalize_url_with_redaction(page_url.as_str()),
            error: String::new(),
        }))
    }

    async fn network_log(
        &self,
        request: Request<browser_v1::NetworkLogRequest>,
    ) -> Result<Response<browser_v1::NetworkLogResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        let Some(tab) = session.active_tab() else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "active_tab_not_found".to_owned(),
            }));
        };
        let limit = if payload.limit == 0 {
            session.budget.max_network_log_entries
        } else {
            usize::try_from(payload.limit).unwrap_or(usize::MAX)
        }
        .min(session.budget.max_network_log_entries)
        .max(1);
        let max_payload_bytes =
            payload.max_payload_bytes.max(1).min(session.budget.max_network_log_bytes) as usize;

        let start = tab.network_log.len().saturating_sub(limit);
        let mut truncated = start > 0;
        let mut entries = tab
            .network_log
            .iter()
            .skip(start)
            .cloned()
            .map(|entry| network_log_entry_to_proto(entry, payload.include_headers))
            .collect::<Vec<_>>();
        truncated = truncate_network_log_payload(&mut entries, max_payload_bytes) || truncated;

        Ok(Response::new(browser_v1::NetworkLogResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            entries,
            truncated,
            error: String::new(),
        }))
    }

    async fn reset_state(
        &self,
        request: Request<browser_v1::ResetStateRequest>,
    ) -> Result<Response<browser_v1::ResetStateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let default_reset = !payload.clear_cookies
            && !payload.clear_storage
            && !payload.reset_tabs
            && !payload.reset_permissions;
        let clear_cookies = payload.clear_cookies || default_reset;
        let clear_storage = payload.clear_storage || default_reset;
        let mut session_for_persist = None;

        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ResetStateResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    cookies_cleared: 0,
                    storage_entries_cleared: 0,
                    tabs_closed: 0,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let mut cookies_cleared = 0_u32;
            let mut storage_entries_cleared = 0_u32;
            let mut tabs_closed = 0_u32;
            if clear_cookies {
                cookies_cleared =
                    session.cookie_jar.values().map(|cookies| cookies.len() as u32).sum::<u32>();
                session.cookie_jar.clear();
            }
            if clear_storage {
                storage_entries_cleared = session
                    .storage_entries
                    .values()
                    .map(|entries| entries.len() as u32)
                    .sum::<u32>();
                session.storage_entries.clear();
                if let Some(tab) = session.active_tab_mut() {
                    tab.typed_inputs.clear();
                }
            }
            if payload.reset_tabs && !session.tab_order.is_empty() {
                tabs_closed = session.tab_order.len().saturating_sub(1) as u32;
                let active_tab_id = session.active_tab_id.clone();
                session.tabs.clear();
                session
                    .tabs
                    .insert(active_tab_id.clone(), BrowserTabRecord::new(active_tab_id.clone()));
                session.tab_order = vec![active_tab_id];
            }
            if payload.reset_permissions {
                session.permissions = SessionPermissionsInternal::default();
            }
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
            browser_v1::ResetStateResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                cookies_cleared,
                storage_entries_cleared,
                tabs_closed,
                permissions: Some(session.permissions.to_proto()),
                error: String::new(),
            }
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "reset_state")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn list_tabs(
        &self,
        request: Request<browser_v1::ListTabsRequest>,
    ) -> Result<Response<browser_v1::ListTabsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ListTabsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tabs: Vec::new(),
                active_tab_id: None,
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::ListTabsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            tabs: session.list_tabs(),
            active_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                ulid: session.active_tab_id.clone(),
            }),
            error: String::new(),
        }))
    }

    async fn open_tab(
        &self,
        request: Request<browser_v1::OpenTabRequest>,
    ) -> Result<Response<browser_v1::OpenTabResponse>, Status> {
        let relay_private_target_block =
            request.extensions().get::<RelayPrivateTargetBlock>().is_some();
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let url = payload.url.trim().to_owned();
        let (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            if !session.can_create_tab() {
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: "tab_limit_reached".to_owned(),
                }));
            }
            let created_tab_id = session.create_tab();
            if payload.activate {
                session.active_tab_id = created_tab_id.clone();
            }
            let timeout_ms =
                payload.timeout_ms.max(1).min(session.budget.max_navigation_timeout_ms);
            let max_response_bytes = session.budget.max_response_bytes;
            let allow_private_targets = if relay_private_target_block {
                false
            } else {
                payload.allow_private_targets || session.allow_private_targets
            };
            let cookie_header = cookie_header_for_url(session, url.as_str());
            (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header)
        };
        let mut session_for_persist = None;
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Err(error) = chromium_open_tab_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                created_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if session.tabs.remove(created_tab_id.as_str()).is_some() {
                        session.tab_order.retain(|value| value != created_tab_id.as_str());
                        if session.tab_order.is_empty() {
                            let fallback_id = session.create_tab();
                            session.active_tab_id = fallback_id;
                        } else if session.active_tab_id == created_tab_id {
                            if let Some(first) = session.tab_order.first() {
                                session.active_tab_id = first.clone();
                            }
                        }
                    }
                }
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: format!("failed to create chromium tab runtime: {error}"),
                }));
            }
        }

        let mut navigated = false;
        let mut status_code = 0_u32;
        let mut success = true;
        let mut error = String::new();
        if !url.is_empty() {
            navigated = true;
            let outcome = match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => {
                    navigate_with_guards(
                        url.as_str(),
                        timeout_ms,
                        payload.allow_redirects,
                        if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                        allow_private_targets,
                        max_response_bytes,
                        cookie_header.as_deref(),
                    )
                    .await
                }
                BrowserEngineMode::Chromium => {
                    navigate_tab_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        created_tab_id.as_str(),
                        &ChromiumNavigateParams {
                            raw_url: url.clone(),
                            timeout_ms,
                            allow_redirects: payload.allow_redirects,
                            max_redirects: if payload.max_redirects == 0 {
                                3
                            } else {
                                payload.max_redirects
                            },
                            allow_private_targets,
                            max_response_bytes,
                            cookie_header: cookie_header.clone(),
                        },
                    )
                    .await
                }
            };
            status_code = outcome.status_code as u32;
            success = outcome.success;
            if !success {
                error = outcome.error.clone();
            }
            let network_log_entries = outcome.network_log.clone();
            let cookie_updates = outcome.cookie_updates.clone();
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                let max_network_log_entries = session.budget.max_network_log_entries;
                let max_network_log_bytes = session.budget.max_network_log_bytes;
                if let Some(tab) = session.tabs.get_mut(created_tab_id.as_str()) {
                    if outcome.success {
                        tab.last_title = outcome.title;
                        tab.last_url = Some(outcome.final_url);
                        tab.last_page_body = outcome.page_body;
                        tab.scroll_x = 0;
                        tab.scroll_y = 0;
                        tab.typed_inputs.clear();
                    }
                    append_network_log_entries(
                        tab,
                        network_log_entries.as_slice(),
                        max_network_log_entries,
                        max_network_log_bytes,
                    );
                }
                apply_cookie_updates(session, cookie_updates.as_slice());
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        } else {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "open_tab")
            .map_err(map_persist_error_to_status)?;

        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::OpenTabResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tab: None,
                navigated,
                status_code,
                error: "session_not_found".to_owned(),
            }));
        };
        let tab = session.tab_to_proto(created_tab_id.as_str());
        Ok(Response::new(browser_v1::OpenTabResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            tab,
            navigated,
            status_code,
            error,
        }))
    }

    async fn switch_tab(
        &self,
        request: Request<browser_v1::SwitchTabRequest>,
    ) -> Result<Response<browser_v1::SwitchTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let tab_id =
            parse_tab_id_from_proto(payload.tab_id.take()).map_err(Status::invalid_argument)?;
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            if !session.tabs.contains_key(tab_id.as_str()) {
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "tab_not_found".to_owned(),
                }
            } else {
                session.active_tab_id = tab_id;
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    error: String::new(),
                }
            }
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "switch_tab")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn close_tab(
        &self,
        request: Request<browser_v1::CloseTabRequest>,
    ) -> Result<Response<browser_v1::CloseTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let requested_tab_id = match payload.tab_id.take() {
            Some(value) if !value.ulid.trim().is_empty() => {
                parse_tab_id(Some(value.ulid.trim())).map_err(Status::invalid_argument)?
            }
            _ => String::new(),
        };
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: None,
                    tabs_remaining: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let tab_id_to_close = if requested_tab_id.is_empty() {
                session.active_tab_id.clone()
            } else {
                requested_tab_id.clone()
            };
            match session.close_tab(tab_id_to_close.as_str()) {
                Ok((closed_tab_id, _)) => {
                    if session.persistence.enabled {
                        session_for_persist = Some(session.clone());
                    }
                    browser_v1::CloseTabResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: true,
                        closed_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                            ulid: closed_tab_id,
                        }),
                        active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                        tabs_remaining: session.tabs.len() as u32,
                        error: String::new(),
                    }
                }
                Err(error) => browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    tabs_remaining: session.tabs.len() as u32,
                    error,
                },
            }
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium && response.success {
            if let Some(closed_tab_id) = response.closed_tab_id.as_ref() {
                let _ = chromium_close_tab_runtime(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    closed_tab_id.ulid.as_str(),
                )
                .await;
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "close_tab")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn get_permissions(
        &self,
        request: Request<browser_v1::GetPermissionsRequest>,
    ) -> Result<Response<browser_v1::GetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::GetPermissionsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                permissions: Some(SessionPermissionsInternal::default().to_proto()),
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::GetPermissionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            permissions: Some(session.permissions.to_proto()),
            error: String::new(),
        }))
    }

    async fn set_permissions(
        &self,
        request: Request<browser_v1::SetPermissionsRequest>,
    ) -> Result<Response<browser_v1::SetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SetPermissionsResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            session.permissions.apply_update(
                payload.camera,
                payload.microphone,
                payload.location,
                payload.reset_to_default,
            );
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
            browser_v1::SetPermissionsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                permissions: Some(session.permissions.to_proto()),
                error: String::new(),
            }
        };
        persist_session_after_mutation(
            self.runtime.as_ref(),
            session_for_persist,
            "set_permissions",
        )
        .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn relay_action(
        &self,
        request: Request<browser_v1::RelayActionRequest>,
    ) -> Result<Response<browser_v1::RelayActionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let auth_header = request.metadata().get(AUTHORIZATION_HEADER).cloned();
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let extension_id = payload.extension_id.trim();
        if extension_id.is_empty() {
            return Err(Status::invalid_argument("extension_id is required"));
        }
        if extension_id.len() > MAX_RELAY_EXTENSION_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "extension_id exceeds {MAX_RELAY_EXTENSION_ID_BYTES} bytes"
            )));
        }
        if !extension_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'-' | b'_'))
        {
            return Err(Status::invalid_argument("extension_id contains unsupported characters"));
        }
        if payload.max_payload_bytes > MAX_RELAY_PAYLOAD_BYTES {
            return Err(Status::invalid_argument(format!(
                "relay max_payload_bytes exceeds {} bytes",
                MAX_RELAY_PAYLOAD_BYTES
            )));
        }

        let action = browser_v1::RelayActionKind::try_from(payload.action)
            .unwrap_or(browser_v1::RelayActionKind::Unspecified);
        match action {
            browser_v1::RelayActionKind::OpenTab => {
                let Some(browser_v1::relay_action_request::Payload::OpenTab(open_tab)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::OpenTab as i32,
                        error: "relay open_tab payload is required".to_owned(),
                        result: None,
                    }));
                };
                let mut open_request = Request::new(browser_v1::OpenTabRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(proto::palyra::common::v1::CanonicalId {
                        ulid: session_id.clone(),
                    }),
                    url: open_tab.url,
                    activate: open_tab.activate,
                    timeout_ms: open_tab.timeout_ms,
                    allow_redirects: true,
                    max_redirects: 3,
                    allow_private_targets: false,
                });
                if let Some(value) = auth_header.clone() {
                    open_request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
                }
                open_request.extensions_mut().insert(RelayPrivateTargetBlock);
                let open_response = self.open_tab(open_request).await?;
                let output = open_response.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: output.success,
                    action: browser_v1::RelayActionKind::OpenTab as i32,
                    error: output.error.clone(),
                    result: output.tab.map(browser_v1::relay_action_response::Result::OpenedTab),
                }))
            }
            browser_v1::RelayActionKind::CaptureSelection => {
                let Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                    selection_payload,
                )) = payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection payload is required".to_owned(),
                        result: None,
                    }));
                };
                let selector = selection_payload.selector.trim();
                if selector.is_empty() {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection selector is required".to_owned(),
                        result: None,
                    }));
                }
                let max_selection_bytes = if selection_payload.max_selection_bytes == 0 {
                    MAX_RELAY_SELECTION_BYTES
                } else {
                    selection_payload.max_selection_bytes.min(MAX_RELAY_SELECTION_BYTES as u64)
                        as usize
                };
                let (selected_text, truncated) = {
                    let mut sessions = self.runtime.sessions.lock().await;
                    let Some(session) = sessions.get_mut(session_id.as_str()) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: "session_not_found".to_owned(),
                            result: None,
                        }));
                    };
                    session.last_active = Instant::now();
                    let Some(tag) = find_matching_html_tag(
                        selector,
                        session
                            .active_tab()
                            .map(|tab| tab.last_page_body.as_str())
                            .unwrap_or_default(),
                    ) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: format!("selector '{selector}' was not found"),
                            result: None,
                        }));
                    };
                    truncate_utf8_bytes_with_flag(tag.as_str(), max_selection_bytes)
                };
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    action: browser_v1::RelayActionKind::CaptureSelection as i32,
                    error: String::new(),
                    result: Some(browser_v1::relay_action_response::Result::Selection(
                        browser_v1::RelaySelectionResult {
                            selector: selector.to_owned(),
                            selected_text,
                            truncated,
                        },
                    )),
                }))
            }
            browser_v1::RelayActionKind::SendPageSnapshot => {
                let Some(browser_v1::relay_action_request::Payload::PageSnapshot(snapshot_payload)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                        error: "relay page_snapshot payload is required".to_owned(),
                        result: None,
                    }));
                };
                let mut observe_request = Request::new(browser_v1::ObserveRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(proto::palyra::common::v1::CanonicalId {
                        ulid: session_id.clone(),
                    }),
                    include_dom_snapshot: snapshot_payload.include_dom_snapshot,
                    include_accessibility_tree: false,
                    include_visible_text: snapshot_payload.include_visible_text,
                    max_dom_snapshot_bytes: snapshot_payload.max_dom_snapshot_bytes,
                    max_accessibility_tree_bytes: 0,
                    max_visible_text_bytes: snapshot_payload.max_visible_text_bytes,
                });
                if let Some(value) = auth_header {
                    observe_request.metadata_mut().insert(AUTHORIZATION_HEADER, value);
                }
                let observe = self.observe(observe_request).await?;
                let observe = observe.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: observe.success,
                    action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                    error: observe.error.clone(),
                    result: if observe.success {
                        Some(browser_v1::relay_action_response::Result::Snapshot(
                            browser_v1::RelayPageSnapshotResult {
                                dom_snapshot: observe.dom_snapshot,
                                visible_text: observe.visible_text,
                                dom_truncated: observe.dom_truncated,
                                visible_text_truncated: observe.visible_text_truncated,
                                page_url: observe.page_url,
                            },
                        ))
                    } else {
                        None
                    },
                }))
            }
            _ => Ok(Response::new(browser_v1::RelayActionResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                action: browser_v1::RelayActionKind::Unspecified as i32,
                error: "unsupported relay action".to_owned(),
                result: None,
            })),
        }
    }

    async fn list_download_artifacts(
        &self,
        request: Request<browser_v1::ListDownloadArtifactsRequest>,
    ) -> Result<Response<browser_v1::ListDownloadArtifactsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let limit = if payload.limit == 0 {
            MAX_DOWNLOAD_ARTIFACTS_PER_SESSION
        } else {
            usize::try_from(payload.limit).unwrap_or(MAX_DOWNLOAD_ARTIFACTS_PER_SESSION)
        }
        .clamp(1, MAX_DOWNLOAD_ARTIFACTS_PER_SESSION);
        let quarantined_only = payload.quarantined_only;
        let guard = self.runtime.download_sessions.lock().await;
        let Some(download_session) = guard.get(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                artifacts: Vec::new(),
                truncated: false,
                error: "session_not_found".to_owned(),
            }));
        };
        let filtered = download_session
            .artifacts
            .iter()
            .filter(|artifact| !quarantined_only || artifact.quarantined)
            .cloned()
            .collect::<Vec<_>>();
        let truncated = filtered.len() > limit;
        let artifacts = filtered
            .into_iter()
            .rev()
            .take(limit)
            .map(|record| download_artifact_to_proto(&record))
            .collect::<Vec<_>>();
        Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            artifacts,
            truncated,
            error: String::new(),
        }))
    }
}
