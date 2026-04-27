use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use palyra_vault::Vault;

use crate::{
    constants::{
        DEFAULT_EXPIRING_WINDOW_MS, DEFAULT_REFRESH_WINDOW_MS, MAX_PROFILE_COUNT,
        MAX_PROFILE_PAGE_LIMIT, RUNTIME_STATE_VERSION,
    },
    error::AuthProfileError,
    models::{
        AuthCredential, AuthExpiryDistribution, AuthHealthReport, AuthHealthSummary,
        AuthProfileDoctorHint, AuthProfileDoctorSeverity, AuthProfileEligibility,
        AuthProfileFailureKind, AuthProfileHealthRecord, AuthProfileHealthState,
        AuthProfileListFilter, AuthProfileOrderRecord, AuthProfileRecord, AuthProfileRuntimeRecord,
        AuthProfileScope, AuthProfileSelectionCandidate, AuthProfileSelectionRequest,
        AuthProfileSelectionResult, AuthProfileSetRequest, AuthProfilesPage, AuthProvider,
        AuthTokenExpiryState, OAuthRefreshRequest,
    },
    refresh::{
        compute_backoff_ms, evaluate_profile_health, load_secret_utf8, oauth_expires_at,
        persist_secret_utf8, prepare_oauth_refresh_snapshot, sanitize_refresh_error,
        should_attempt_oauth_refresh, update_expiry_distribution, OAuthRefreshAdapter,
        OAuthRefreshOutcome, OAuthRefreshOutcomeKind, OAuthRefreshSnapshot, PreparedOAuthRefresh,
    },
    storage::{
        persist_registry, persist_runtime_state, resolve_registry_path, resolve_runtime_state_path,
        resolve_state_root, unix_ms_now, RegistryDocument, RuntimeStateDocument,
    },
    validation::{
        next_profile_updated_at, normalize_agent_id, normalize_document, normalize_profile_id,
        normalize_set_request, profile_matches_filter, profile_merge_key,
    },
};

#[derive(Debug)]
pub struct AuthProfileRegistry {
    registry_path: PathBuf,
    runtime_state_path: PathBuf,
    state: Mutex<RegistryDocument>,
    runtime_state: Mutex<RuntimeStateDocument>,
}

impl AuthProfileRegistry {
    pub fn open(identity_store_root: &Path) -> Result<Self, AuthProfileError> {
        let state_root = resolve_state_root(identity_store_root)?;
        let registry_path = resolve_registry_path(state_root.as_path())?;
        let runtime_state_path = resolve_runtime_state_path(state_root.as_path());
        if let Some(parent) = registry_path.parent() {
            fs::create_dir_all(parent).map_err(|source| AuthProfileError::WriteRegistry {
                path: parent.to_path_buf(),
                source,
            })?;
        }

        let mut document = if registry_path.exists() {
            let raw = fs::read_to_string(&registry_path).map_err(|source| {
                AuthProfileError::ReadRegistry { path: registry_path.clone(), source }
            })?;
            toml::from_str::<RegistryDocument>(&raw).map_err(|source| {
                AuthProfileError::ParseRegistry {
                    path: registry_path.clone(),
                    source: Box::new(source),
                }
            })?
        } else {
            RegistryDocument::default()
        };
        normalize_document(&mut document)?;
        persist_registry(registry_path.as_path(), &document)?;

        let mut runtime_document = if runtime_state_path.exists() {
            let raw = fs::read_to_string(&runtime_state_path).map_err(|source| {
                AuthProfileError::ReadRegistry { path: runtime_state_path.clone(), source }
            })?;
            toml::from_str::<RuntimeStateDocument>(&raw).map_err(|source| {
                AuthProfileError::ParseRegistry {
                    path: runtime_state_path.clone(),
                    source: Box::new(source),
                }
            })?
        } else {
            RuntimeStateDocument::default()
        };
        normalize_runtime_document(&mut runtime_document)?;
        persist_runtime_state(runtime_state_path.as_path(), &runtime_document)?;

        Ok(Self {
            registry_path,
            runtime_state_path,
            state: Mutex::new(document),
            runtime_state: Mutex::new(runtime_document),
        })
    }

    pub fn list_profiles(
        &self,
        filter: AuthProfileListFilter,
    ) -> Result<AuthProfilesPage, AuthProfileError> {
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let limit = filter.limit.unwrap_or(100).clamp(1, MAX_PROFILE_PAGE_LIMIT);
        let mut filtered = guard
            .profiles
            .iter()
            .filter(|profile| profile_matches_filter(profile, &filter))
            .cloned()
            .collect::<Vec<_>>();
        let start = if let Some(after) = filter.after_profile_id.as_deref() {
            filtered
                .iter()
                .position(|profile| profile.profile_id == after)
                .map(|index| index.saturating_add(1))
                .ok_or_else(|| AuthProfileError::InvalidField {
                    field: "after_profile_id",
                    message: "cursor does not exist in current result set".to_owned(),
                })?
        } else {
            0
        };
        let mut page = filtered.drain(start..).take(limit.saturating_add(1)).collect::<Vec<_>>();
        let has_more = page.len() > limit;
        if has_more {
            page.truncate(limit);
        }
        Ok(AuthProfilesPage {
            next_after_profile_id: if has_more {
                page.last().map(|profile| profile.profile_id.clone())
            } else {
                None
            },
            profiles: page,
        })
    }

    pub fn get_profile(
        &self,
        profile_id: &str,
    ) -> Result<Option<AuthProfileRecord>, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        Ok(guard.profiles.iter().find(|profile| profile.profile_id == profile_id).cloned())
    }

    pub fn set_profile(
        &self,
        request: AuthProfileSetRequest,
    ) -> Result<AuthProfileRecord, AuthProfileError> {
        let normalized = normalize_set_request(request)?;
        let now = unix_ms_now()?;

        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let mut record = AuthProfileRecord {
            profile_id: normalized.profile_id,
            provider: normalized.provider,
            profile_name: normalized.profile_name,
            scope: normalized.scope,
            credential: normalized.credential,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        if let Some(existing) =
            next.profiles.iter_mut().find(|profile| profile.profile_id == record.profile_id)
        {
            record.created_at_unix_ms = existing.created_at_unix_ms;
            *existing = record.clone();
        } else {
            if next.profiles.len() >= MAX_PROFILE_COUNT {
                return Err(AuthProfileError::RegistryLimitExceeded);
            }
            next.profiles.push(record.clone());
        }
        next.profiles.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
        persist_registry(self.registry_path.as_path(), &next)?;
        *guard = next;
        Ok(record)
    }

    pub fn delete_profile(&self, profile_id: &str) -> Result<bool, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;
        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let before = next.profiles.len();
        next.profiles.retain(|profile| profile.profile_id != profile_id);
        let deleted = next.profiles.len() != before;
        if deleted {
            persist_registry(self.registry_path.as_path(), &next)?;
            *guard = next;
            self.remove_runtime_record(profile_id.as_str())?;
        }
        Ok(deleted)
    }

    pub fn merged_profiles_for_agent(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<AuthProfileRecord>, AuthProfileError> {
        let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        if let Some(agent_id_raw) = agent_id {
            let normalized_agent_id = normalize_agent_id(agent_id_raw)?;
            let mut merged = BTreeMap::<String, AuthProfileRecord>::new();
            for profile in &guard.profiles {
                if matches!(profile.scope, AuthProfileScope::Global) {
                    merged.insert(profile_merge_key(profile), profile.clone());
                }
            }
            for profile in &guard.profiles {
                if matches!(
                    profile.scope,
                    AuthProfileScope::Agent { ref agent_id } if agent_id == &normalized_agent_id
                ) {
                    merged.insert(profile_merge_key(profile), profile.clone());
                }
            }
            return Ok(merged.into_values().collect());
        }
        Ok(guard.profiles.clone())
    }

    pub fn health_report(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
    ) -> Result<AuthHealthReport, AuthProfileError> {
        self.health_report_with_clock(vault, agent_id, unix_ms_now()?, DEFAULT_EXPIRING_WINDOW_MS)
    }

    pub fn health_report_with_clock(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        expiring_window_ms: i64,
    ) -> Result<AuthHealthReport, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let mut report = AuthHealthReport {
            summary: AuthHealthSummary::default(),
            expiry_distribution: AuthExpiryDistribution::default(),
            profiles: Vec::with_capacity(profiles.len()),
        };

        for profile in profiles {
            let health = evaluate_profile_health(&profile, vault, now_unix_ms, expiring_window_ms);
            report.summary.total = report.summary.total.saturating_add(1);
            match health.state {
                AuthProfileHealthState::Ok => {
                    report.summary.ok = report.summary.ok.saturating_add(1)
                }
                AuthProfileHealthState::Expiring => {
                    report.summary.expiring = report.summary.expiring.saturating_add(1)
                }
                AuthProfileHealthState::Expired => {
                    report.summary.expired = report.summary.expired.saturating_add(1)
                }
                AuthProfileHealthState::Missing => {
                    report.summary.missing = report.summary.missing.saturating_add(1)
                }
                AuthProfileHealthState::Static => {
                    report.summary.static_count = report.summary.static_count.saturating_add(1)
                }
            }
            update_expiry_distribution(
                &mut report.expiry_distribution,
                health.state,
                health.expires_at_unix_ms,
                now_unix_ms,
            );
            report.profiles.push(health);
        }

        Ok(report)
    }

    pub fn runtime_records_for_agent(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
    ) -> Result<Vec<AuthProfileRuntimeRecord>, AuthProfileError> {
        self.runtime_records_for_agent_with_clock(
            vault,
            agent_id,
            unix_ms_now()?,
            DEFAULT_EXPIRING_WINDOW_MS,
        )
    }

    pub fn runtime_records_for_agent_with_clock(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        expiring_window_ms: i64,
    ) -> Result<Vec<AuthProfileRuntimeRecord>, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let mut records = Vec::with_capacity(profiles.len());
        let mut runtime_guard =
            self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = runtime_guard.clone();
        for profile in profiles {
            let health = evaluate_profile_health(&profile, vault, now_unix_ms, expiring_window_ms);
            let existing =
                next.records.iter().find(|record| record.profile_id == profile.profile_id);
            let record = runtime_record_from_health(&profile, &health, existing, now_unix_ms);
            upsert_runtime_record(&mut next.records, record.clone());
            records.push(record);
        }
        next.records.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
        if next.records != runtime_guard.records {
            persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
            *runtime_guard = next;
        }
        records.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
        Ok(records)
    }

    pub fn runtime_records_for_agent_readonly(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
    ) -> Result<Vec<AuthProfileRuntimeRecord>, AuthProfileError> {
        self.runtime_records_for_agent_readonly_with_clock(
            vault,
            agent_id,
            unix_ms_now()?,
            DEFAULT_EXPIRING_WINDOW_MS,
        )
    }

    pub fn runtime_records_for_agent_readonly_with_clock(
        &self,
        vault: &Vault,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        expiring_window_ms: i64,
    ) -> Result<Vec<AuthProfileRuntimeRecord>, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let runtime_guard =
            self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut records = Vec::with_capacity(profiles.len());
        for profile in profiles {
            let health = evaluate_profile_health(&profile, vault, now_unix_ms, expiring_window_ms);
            let existing =
                runtime_guard.records.iter().find(|record| record.profile_id == profile.profile_id);
            records.push(runtime_record_from_health(&profile, &health, existing, now_unix_ms));
        }
        records.sort_by(|left, right| left.profile_id.cmp(&right.profile_id));
        Ok(records)
    }

    pub fn record_profile_success(&self, profile_id: &str) -> Result<(), AuthProfileError> {
        self.record_profile_success_with_clock(profile_id, unix_ms_now()?)
    }

    pub fn record_profile_success_with_clock(
        &self,
        profile_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), AuthProfileError> {
        let profile = self.profile_or_not_found(profile_id)?;
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let existing = next.records.iter().find(|record| record.profile_id == profile.profile_id);
        let mut record = runtime_record_from_profile(&profile, existing, now_unix_ms);
        record.last_used_unix_ms = Some(now_unix_ms);
        record.last_success_unix_ms = Some(now_unix_ms);
        record.last_failure_kind = None;
        record.failure_count = 0;
        record.cooldown_until_unix_ms = None;
        if !matches!(
            record.token_expiry_state,
            AuthTokenExpiryState::Missing | AuthTokenExpiryState::Expired
        ) {
            record.eligibility = AuthProfileEligibility::Eligible;
            record.doctor_hint = None;
        }
        record.updated_at_unix_ms = now_unix_ms;
        upsert_runtime_record(&mut next.records, record);
        persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
        *guard = next;
        Ok(())
    }

    pub fn record_profile_failure(
        &self,
        profile_id: &str,
        kind: AuthProfileFailureKind,
    ) -> Result<AuthProfileRuntimeRecord, AuthProfileError> {
        self.record_profile_failure_with_clock(profile_id, kind, unix_ms_now()?)
    }

    pub fn record_profile_failure_with_clock(
        &self,
        profile_id: &str,
        kind: AuthProfileFailureKind,
        now_unix_ms: i64,
    ) -> Result<AuthProfileRuntimeRecord, AuthProfileError> {
        let profile = self.profile_or_not_found(profile_id)?;
        self.record_profile_failure_from_profile(&profile, kind, now_unix_ms, None)
    }

    pub fn clear_profile_cooldown(
        &self,
        profile_id: &str,
    ) -> Result<AuthProfileRuntimeRecord, AuthProfileError> {
        self.clear_profile_cooldown_with_clock(profile_id, unix_ms_now()?)
    }

    pub fn clear_profile_cooldown_with_clock(
        &self,
        profile_id: &str,
        now_unix_ms: i64,
    ) -> Result<AuthProfileRuntimeRecord, AuthProfileError> {
        let profile = self.profile_or_not_found(profile_id)?;
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let existing = next.records.iter().find(|record| record.profile_id == profile.profile_id);
        let mut record = runtime_record_from_profile(&profile, existing, now_unix_ms);
        record.failure_count = 0;
        record.cooldown_until_unix_ms = None;
        record.last_failure_kind = None;
        record.eligibility = if matches!(record.token_expiry_state, AuthTokenExpiryState::Expired) {
            AuthProfileEligibility::Expired
        } else if matches!(record.token_expiry_state, AuthTokenExpiryState::Missing) {
            AuthProfileEligibility::MissingCredential
        } else {
            AuthProfileEligibility::Eligible
        };
        record.doctor_hint =
            doctor_hint_for_state(record.eligibility, record.token_expiry_state, None);
        record.updated_at_unix_ms = now_unix_ms;
        upsert_runtime_record(&mut next.records, record.clone());
        persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
        *guard = next;
        Ok(record)
    }

    pub fn profile_order(
        &self,
        provider: Option<&AuthProvider>,
        agent_id: Option<&str>,
    ) -> Result<Option<AuthProfileOrderRecord>, AuthProfileError> {
        let guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let scope = profile_order_scope_key(agent_id)?;
        let provider_key = provider.map(AuthProvider::canonical_key);
        Ok(guard
            .profile_orders
            .iter()
            .find(|record| {
                record.scope == scope && record.provider.as_deref() == provider_key.as_deref()
            })
            .cloned())
    }

    pub fn set_profile_order(
        &self,
        provider: Option<AuthProvider>,
        agent_id: Option<&str>,
        profile_ids: Vec<String>,
    ) -> Result<AuthProfileOrderRecord, AuthProfileError> {
        self.set_profile_order_with_clock(provider, agent_id, profile_ids, unix_ms_now()?)
    }

    pub fn set_profile_order_with_clock(
        &self,
        provider: Option<AuthProvider>,
        agent_id: Option<&str>,
        profile_ids: Vec<String>,
        now_unix_ms: i64,
    ) -> Result<AuthProfileOrderRecord, AuthProfileError> {
        let normalized_profile_ids = normalize_profile_order(profile_ids.as_slice())?;
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let profiles_by_id = profiles
            .iter()
            .map(|profile| (profile.profile_id.as_str(), profile))
            .collect::<BTreeMap<_, _>>();
        for profile_id in &normalized_profile_ids {
            let profile = profiles_by_id
                .get(profile_id.as_str())
                .ok_or_else(|| AuthProfileError::ProfileNotFound(profile_id.clone()))?;
            if let Some(provider) = provider.as_ref() {
                if profile.provider.canonical_key() != provider.canonical_key() {
                    return Err(AuthProfileError::InvalidField {
                        field: "profile_ids",
                        message: format!(
                            "profile '{profile_id}' does not belong to provider {}",
                            provider.label()
                        ),
                    });
                }
            }
        }
        let scope = profile_order_scope_key(agent_id)?;
        let provider_key = provider.as_ref().map(AuthProvider::canonical_key);
        let record = AuthProfileOrderRecord {
            scope: scope.clone(),
            provider: provider_key.clone(),
            profile_ids: normalized_profile_ids,
            updated_at_unix_ms: now_unix_ms,
        };
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        upsert_profile_order(&mut next.profile_orders, record.clone());
        persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
        *guard = next;
        Ok(record)
    }

    pub fn select_auth_profile(
        &self,
        vault: &Vault,
        request: AuthProfileSelectionRequest,
    ) -> Result<AuthProfileSelectionResult, AuthProfileError> {
        self.select_auth_profile_with_clock(vault, request, unix_ms_now()?)
    }

    pub fn select_auth_profile_with_clock(
        &self,
        vault: &Vault,
        request: AuthProfileSelectionRequest,
        now_unix_ms: i64,
    ) -> Result<AuthProfileSelectionResult, AuthProfileError> {
        let agent_id = request.agent_id.as_deref();
        let records = self.runtime_records_for_agent_with_clock(
            vault,
            agent_id,
            now_unix_ms,
            DEFAULT_EXPIRING_WINDOW_MS,
        )?;
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let records_by_profile = records
            .into_iter()
            .map(|record| (record.profile_id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let explicit_order = if request.explicit_profile_order.is_empty() {
            self.profile_order(request.provider.as_ref(), agent_id)?
                .map(|record| record.profile_ids)
                .unwrap_or_default()
        } else {
            request.explicit_profile_order
        };
        let explicit_order = normalize_profile_order(explicit_order.as_slice())?;
        let explicit_positions = explicit_order
            .iter()
            .enumerate()
            .map(|(index, profile_id)| (profile_id.clone(), index))
            .collect::<BTreeMap<_, _>>();
        let denied = normalize_profile_set(request.policy_denied_profile_ids.as_slice())?;
        let allowed_credentials =
            request.allowed_credential_types.iter().copied().collect::<BTreeSet<_>>();

        let mut candidates = Vec::new();
        for profile in profiles {
            let Some(record) = records_by_profile.get(profile.profile_id.as_str()) else {
                continue;
            };
            let provider_matches = request.provider.as_ref().is_none_or(|provider| {
                provider.canonical_key() == profile.provider.canonical_key()
            });
            let credential_allowed = allowed_credentials.is_empty()
                || allowed_credentials.contains(&profile.credential.credential_type());
            let in_explicit_order = explicit_positions.is_empty()
                || explicit_positions.contains_key(&profile.profile_id);
            let policy_denied = denied.contains(profile.profile_id.as_str());
            let reason_code = selection_reason_code(
                provider_matches,
                credential_allowed,
                in_explicit_order,
                policy_denied,
                record.eligibility,
            );
            candidates.push(AuthProfileSelectionCandidate {
                profile_id: profile.profile_id.clone(),
                provider: profile.provider.label(),
                scope: profile.scope.scope_key(),
                credential_type: profile.credential.credential_type(),
                token_expiry_state: record.token_expiry_state,
                eligibility: if policy_denied {
                    AuthProfileEligibility::PolicyDenied
                } else {
                    record.eligibility
                },
                failure_count: record.failure_count,
                cooldown_until_unix_ms: record.cooldown_until_unix_ms,
                last_used_unix_ms: record.last_used_unix_ms,
                selected: false,
                reason_code,
            });
        }

        sort_selection_candidates(&mut candidates, &explicit_positions);
        let selected_index =
            candidates.iter().position(|candidate| candidate.reason_code == "eligible");
        let selected_profile_id = selected_index.map(|index| {
            candidates[index].selected = true;
            candidates[index].profile_id.clone()
        });
        let reason_code =
            selected_profile_id.as_ref().map(|_| "selected".to_owned()).unwrap_or_else(|| {
                if candidates.is_empty() {
                    "no_candidates".to_owned()
                } else {
                    "no_eligible_candidates".to_owned()
                }
            });

        Ok(AuthProfileSelectionResult {
            selected_profile_id,
            reason_code,
            candidates,
            generated_at_unix_ms: now_unix_ms,
        })
    }

    pub fn refresh_due_oauth_profiles(
        &self,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        agent_id: Option<&str>,
    ) -> Result<Vec<OAuthRefreshOutcome>, AuthProfileError> {
        self.refresh_due_oauth_profiles_with_clock(
            vault,
            adapter,
            agent_id,
            unix_ms_now()?,
            DEFAULT_REFRESH_WINDOW_MS,
        )
    }

    pub fn refresh_due_oauth_profiles_with_clock(
        &self,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        agent_id: Option<&str>,
        now_unix_ms: i64,
        refresh_window_ms: i64,
    ) -> Result<Vec<OAuthRefreshOutcome>, AuthProfileError> {
        let profiles = self.merged_profiles_for_agent(agent_id)?;
        let mut outcomes = Vec::new();
        for profile in profiles {
            let profile_id = profile.profile_id.clone();
            if let AuthCredential::Oauth { .. } = profile.credential {
                if !should_attempt_oauth_refresh(&profile, vault, now_unix_ms, refresh_window_ms) {
                    outcomes.push(OAuthRefreshOutcome {
                        profile_id: profile_id.clone(),
                        provider: profile.provider.label(),
                        kind: OAuthRefreshOutcomeKind::SkippedNotDue,
                        reason: "refresh skipped because token is not yet due".to_owned(),
                        next_allowed_refresh_unix_ms: None,
                        expires_at_unix_ms: oauth_expires_at(&profile),
                    });
                    continue;
                }
                match prepare_oauth_refresh_snapshot(&profile, now_unix_ms) {
                    PreparedOAuthRefresh::Snapshot(snapshot) => {
                        outcomes.push(self.refresh_oauth_profile_snapshot_with_clock(
                            snapshot,
                            vault,
                            adapter,
                            now_unix_ms,
                        )?)
                    }
                    PreparedOAuthRefresh::Outcome(outcome) => outcomes.push(outcome),
                }
            }
        }
        Ok(outcomes)
    }

    pub fn refresh_oauth_profile(
        &self,
        profile_id: &str,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        self.refresh_oauth_profile_with_clock(profile_id, vault, adapter, unix_ms_now()?)
    }

    pub fn refresh_oauth_profile_with_clock(
        &self,
        profile_id: &str,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        now_unix_ms: i64,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;

        let prepared = {
            let guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
            let profile = guard
                .profiles
                .iter()
                .find(|profile| profile.profile_id == profile_id)
                .ok_or_else(|| AuthProfileError::ProfileNotFound(profile_id.clone()))?;
            prepare_oauth_refresh_snapshot(profile, now_unix_ms)
        };
        match prepared {
            PreparedOAuthRefresh::Snapshot(snapshot) => self
                .refresh_oauth_profile_snapshot_with_clock(snapshot, vault, adapter, now_unix_ms),
            PreparedOAuthRefresh::Outcome(outcome) => Ok(outcome),
        }
    }

    fn refresh_oauth_profile_snapshot_with_clock(
        &self,
        snapshot: OAuthRefreshSnapshot,
        vault: &Vault,
        adapter: &dyn OAuthRefreshAdapter,
        now_unix_ms: i64,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        let refresh_token = match load_secret_utf8(vault, snapshot.refresh_token_vault_ref.as_str())
        {
            Ok(token) => token,
            Err(_) => {
                return self.persist_refresh_failure(
                    snapshot,
                    now_unix_ms,
                    "refresh token reference is missing or unreadable".to_owned(),
                );
            }
        };

        let client_secret =
            if let Some(client_secret_ref) = snapshot.client_secret_vault_ref.as_deref() {
                match load_secret_utf8(vault, client_secret_ref) {
                    Ok(secret) => Some(secret),
                    Err(_) => {
                        return self.persist_refresh_failure(
                            snapshot,
                            now_unix_ms,
                            "client secret reference is missing or unreadable".to_owned(),
                        );
                    }
                }
            } else {
                None
            };

        let response = adapter.refresh_access_token(&OAuthRefreshRequest {
            provider: snapshot.provider.clone(),
            token_endpoint: snapshot.token_endpoint.clone(),
            client_id: snapshot.client_id.clone(),
            client_secret,
            refresh_token,
            scopes: snapshot.scopes.clone(),
        });
        match response {
            Ok(payload) => {
                if let Some(refresh_token) = payload.refresh_token.as_deref() {
                    if persist_secret_utf8(
                        vault,
                        snapshot.refresh_token_vault_ref.as_str(),
                        refresh_token,
                    )
                    .is_err()
                    {
                        return self.persist_refresh_failure(
                            snapshot,
                            now_unix_ms,
                            "failed to persist rotated refresh token into vault".to_owned(),
                        );
                    }
                }
                if persist_secret_utf8(
                    vault,
                    snapshot.access_token_vault_ref.as_str(),
                    payload.access_token.as_str(),
                )
                .is_err()
                {
                    return self.persist_refresh_failure(
                        snapshot,
                        now_unix_ms,
                        "failed to persist refreshed access token into vault".to_owned(),
                    );
                }
                let computed_expires_at = payload
                    .expires_in_seconds
                    .map(|seconds| {
                        now_unix_ms.saturating_add((seconds as i64).saturating_mul(1_000))
                    })
                    .or(snapshot.expires_at_unix_ms);
                let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
                let mut next = guard.clone();
                let (profile_for_runtime, profile_id, provider, expires_at_unix_ms) = {
                    let profile = next
                        .profiles
                        .iter_mut()
                        .find(|profile| profile.profile_id == snapshot.profile_id)
                        .ok_or_else(|| {
                            AuthProfileError::ProfileNotFound(snapshot.profile_id.clone())
                        })?;
                    let provider = profile.provider.label();
                    let profile_id = profile.profile_id.clone();
                    let AuthCredential::Oauth { expires_at_unix_ms, refresh_state, .. } =
                        &mut profile.credential
                    else {
                        return Ok(OAuthRefreshOutcome {
                            profile_id,
                            provider,
                            kind: OAuthRefreshOutcomeKind::SkippedNotOauth,
                            reason: "profile credential type changed before refresh completed"
                                .to_owned(),
                            next_allowed_refresh_unix_ms: None,
                            expires_at_unix_ms: None,
                        });
                    };
                    *expires_at_unix_ms = computed_expires_at;
                    refresh_state.failure_count = 0;
                    refresh_state.last_error = None;
                    refresh_state.last_attempt_unix_ms = Some(now_unix_ms);
                    refresh_state.last_success_unix_ms = Some(now_unix_ms);
                    refresh_state.next_allowed_refresh_unix_ms = None;
                    profile.updated_at_unix_ms =
                        next_profile_updated_at(profile.updated_at_unix_ms, now_unix_ms);
                    let expires_at_value = *expires_at_unix_ms;
                    let profile_for_runtime = profile.clone();
                    (profile_for_runtime, profile_id, provider, expires_at_value)
                };
                persist_registry(self.registry_path.as_path(), &next)?;
                *guard = next;
                drop(guard);
                self.record_profile_success_from_profile(&profile_for_runtime, now_unix_ms)?;
                Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::Succeeded,
                    reason: "oauth access token refreshed".to_owned(),
                    next_allowed_refresh_unix_ms: None,
                    expires_at_unix_ms,
                })
            }
            Err(error) => {
                self.persist_refresh_failure(snapshot, now_unix_ms, sanitize_refresh_error(&error))
            }
        }
    }

    fn persist_refresh_failure(
        &self,
        snapshot: OAuthRefreshSnapshot,
        now_unix_ms: i64,
        reason: String,
    ) -> Result<OAuthRefreshOutcome, AuthProfileError> {
        let mut guard = self.state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let profile = next
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == snapshot.profile_id)
            .ok_or_else(|| AuthProfileError::ProfileNotFound(snapshot.profile_id.clone()))?;
        let provider = profile.provider.label();
        let profile_id = profile.profile_id.clone();
        let (profile_for_runtime, next_allowed, expires_at_unix_ms, failure_count) = {
            let AuthCredential::Oauth { refresh_state, expires_at_unix_ms, .. } =
                &mut profile.credential
            else {
                return Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::SkippedNotOauth,
                    reason: "profile credential type changed before refresh failure persisted"
                        .to_owned(),
                    next_allowed_refresh_unix_ms: None,
                    expires_at_unix_ms: None,
                });
            };
            if profile.updated_at_unix_ms != snapshot.observed_updated_at_unix_ms {
                return Ok(OAuthRefreshOutcome {
                    profile_id,
                    provider,
                    kind: OAuthRefreshOutcomeKind::SkippedCooldown,
                    reason: "stale refresh failure ignored because profile state changed"
                        .to_owned(),
                    next_allowed_refresh_unix_ms: refresh_state.next_allowed_refresh_unix_ms,
                    expires_at_unix_ms: *expires_at_unix_ms,
                });
            }
            let failure_count = snapshot.failure_count.saturating_add(1);
            let backoff_ms = compute_backoff_ms(&snapshot.provider, failure_count);
            let next_allowed = now_unix_ms.saturating_add(backoff_ms as i64);

            refresh_state.failure_count = failure_count;
            refresh_state.last_error = Some(reason.clone());
            refresh_state.last_attempt_unix_ms = Some(now_unix_ms);
            refresh_state.next_allowed_refresh_unix_ms = Some(next_allowed);
            let expires_at_value = *expires_at_unix_ms;
            let profile_for_runtime = profile.clone();
            (profile_for_runtime, next_allowed, expires_at_value, failure_count)
        };
        profile.updated_at_unix_ms =
            next_profile_updated_at(profile.updated_at_unix_ms, now_unix_ms);

        persist_registry(self.registry_path.as_path(), &next)?;
        *guard = next;
        drop(guard);
        self.record_profile_failure_from_profile(
            &profile_for_runtime,
            AuthProfileFailureKind::RefreshFailed,
            now_unix_ms,
            Some((failure_count, Some(next_allowed))),
        )?;

        Ok(OAuthRefreshOutcome {
            profile_id,
            provider,
            kind: OAuthRefreshOutcomeKind::Failed,
            reason,
            next_allowed_refresh_unix_ms: Some(next_allowed),
            expires_at_unix_ms,
        })
    }

    fn remove_runtime_record(&self, profile_id: &str) -> Result<(), AuthProfileError> {
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let before = next.records.len();
        next.records.retain(|record| record.profile_id != profile_id);
        if before != next.records.len() {
            persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
            *guard = next;
        }
        Ok(())
    }

    fn profile_or_not_found(
        &self,
        profile_id: &str,
    ) -> Result<AuthProfileRecord, AuthProfileError> {
        let profile_id = normalize_profile_id(profile_id)?;
        self.get_profile(profile_id.as_str())?.ok_or(AuthProfileError::ProfileNotFound(profile_id))
    }

    fn record_profile_success_from_profile(
        &self,
        profile: &AuthProfileRecord,
        now_unix_ms: i64,
    ) -> Result<(), AuthProfileError> {
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let existing = next.records.iter().find(|record| record.profile_id == profile.profile_id);
        let mut record = runtime_record_from_profile(profile, existing, now_unix_ms);
        record.last_used_unix_ms = Some(now_unix_ms);
        record.last_success_unix_ms = Some(now_unix_ms);
        record.last_failure_kind = None;
        record.failure_count = 0;
        record.cooldown_until_unix_ms = None;
        record.eligibility = if matches!(record.token_expiry_state, AuthTokenExpiryState::Expired) {
            AuthProfileEligibility::Expired
        } else if matches!(record.token_expiry_state, AuthTokenExpiryState::Missing) {
            AuthProfileEligibility::MissingCredential
        } else {
            AuthProfileEligibility::Eligible
        };
        record.doctor_hint =
            doctor_hint_for_state(record.eligibility, record.token_expiry_state, None);
        record.updated_at_unix_ms = now_unix_ms;
        upsert_runtime_record(&mut next.records, record);
        persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
        *guard = next;
        Ok(())
    }

    fn record_profile_failure_from_profile(
        &self,
        profile: &AuthProfileRecord,
        kind: AuthProfileFailureKind,
        now_unix_ms: i64,
        override_state: Option<(u32, Option<i64>)>,
    ) -> Result<AuthProfileRuntimeRecord, AuthProfileError> {
        let mut guard = self.runtime_state.lock().map_err(|_| AuthProfileError::LockPoisoned)?;
        let mut next = guard.clone();
        let existing = next.records.iter().find(|record| record.profile_id == profile.profile_id);
        let mut record = runtime_record_from_profile(profile, existing, now_unix_ms);
        let failure_count = override_state
            .map(|(failure_count, _)| failure_count)
            .unwrap_or_else(|| record.failure_count.saturating_add(1));
        let cooldown_until =
            override_state.and_then(|(_, cooldown_until)| cooldown_until).or_else(|| {
                failure_cooldown_ms(kind, &profile.provider, failure_count)
                    .map(|duration_ms| now_unix_ms.saturating_add(duration_ms as i64))
            });
        record.last_used_unix_ms = Some(now_unix_ms);
        record.last_failure_unix_ms = Some(now_unix_ms);
        record.last_failure_kind = Some(kind);
        record.failure_count = failure_count;
        record.cooldown_until_unix_ms = cooldown_until;
        record.eligibility = if cooldown_until.is_some_and(|value| value > now_unix_ms) {
            AuthProfileEligibility::CoolingDown
        } else {
            match kind {
                AuthProfileFailureKind::AuthInvalid | AuthProfileFailureKind::ConfigMissing => {
                    AuthProfileEligibility::MissingCredential
                }
                _ => record.eligibility,
            }
        };
        record.doctor_hint =
            doctor_hint_for_state(record.eligibility, record.token_expiry_state, Some(kind));
        record.updated_at_unix_ms = now_unix_ms;
        upsert_runtime_record(&mut next.records, record.clone());
        persist_runtime_state(self.runtime_state_path.as_path(), &next)?;
        *guard = next;
        Ok(record)
    }
}

fn normalize_runtime_document(document: &mut RuntimeStateDocument) -> Result<(), AuthProfileError> {
    if document.version != RUNTIME_STATE_VERSION {
        return Err(AuthProfileError::UnsupportedVersion(document.version));
    }
    let mut deduped = BTreeMap::<String, AuthProfileRuntimeRecord>::new();
    for mut record in std::mem::take(&mut document.records) {
        record.profile_id = normalize_profile_id(record.profile_id.as_str())?;
        deduped.insert(record.profile_id.clone(), record);
    }
    document.records = deduped.into_values().collect();
    Ok(())
}

fn runtime_record_from_health(
    profile: &AuthProfileRecord,
    health: &AuthProfileHealthRecord,
    existing: Option<&AuthProfileRuntimeRecord>,
    now_unix_ms: i64,
) -> AuthProfileRuntimeRecord {
    let mut record = runtime_record_from_profile(profile, existing, now_unix_ms);
    record.token_expiry_state = token_expiry_state_from_health(health.state);
    let refresh_cooldown = oauth_refresh_cooldown_until(profile);
    if record.cooldown_until_unix_ms.is_none() {
        record.cooldown_until_unix_ms = refresh_cooldown;
    }
    record.eligibility =
        eligibility_from_health(health.state, record.cooldown_until_unix_ms, now_unix_ms);
    record.doctor_hint = doctor_hint_for_state(
        record.eligibility,
        record.token_expiry_state,
        record.last_failure_kind,
    );
    record.updated_at_unix_ms = now_unix_ms;
    record
}

fn runtime_record_from_profile(
    profile: &AuthProfileRecord,
    existing: Option<&AuthProfileRuntimeRecord>,
    now_unix_ms: i64,
) -> AuthProfileRuntimeRecord {
    AuthProfileRuntimeRecord {
        profile_id: profile.profile_id.clone(),
        provider: profile.provider.label(),
        scope: profile.scope.scope_key(),
        credential_type: profile.credential.credential_type(),
        last_used_unix_ms: existing.and_then(|record| record.last_used_unix_ms),
        last_success_unix_ms: existing.and_then(|record| record.last_success_unix_ms),
        last_failure_unix_ms: existing.and_then(|record| record.last_failure_unix_ms),
        last_failure_kind: existing.and_then(|record| record.last_failure_kind),
        failure_count: existing.map_or(0, |record| record.failure_count),
        cooldown_until_unix_ms: existing.and_then(|record| record.cooldown_until_unix_ms),
        token_expiry_state: existing
            .map_or(AuthTokenExpiryState::Unknown, |record| record.token_expiry_state),
        eligibility: existing.map_or(AuthProfileEligibility::Eligible, |record| record.eligibility),
        doctor_hint: existing.and_then(|record| record.doctor_hint.clone()),
        created_at_unix_ms: existing.map_or(now_unix_ms, |record| record.created_at_unix_ms),
        updated_at_unix_ms: now_unix_ms,
    }
}

fn upsert_runtime_record(
    records: &mut Vec<AuthProfileRuntimeRecord>,
    record: AuthProfileRuntimeRecord,
) {
    if let Some(existing) =
        records.iter_mut().find(|existing| existing.profile_id == record.profile_id)
    {
        *existing = record;
    } else {
        records.push(record);
    }
}

fn token_expiry_state_from_health(state: AuthProfileHealthState) -> AuthTokenExpiryState {
    match state {
        AuthProfileHealthState::Ok => AuthTokenExpiryState::Valid,
        AuthProfileHealthState::Expiring => AuthTokenExpiryState::Expiring,
        AuthProfileHealthState::Expired => AuthTokenExpiryState::Expired,
        AuthProfileHealthState::Missing => AuthTokenExpiryState::Missing,
        AuthProfileHealthState::Static => AuthTokenExpiryState::Static,
    }
}

fn oauth_refresh_cooldown_until(profile: &AuthProfileRecord) -> Option<i64> {
    let AuthCredential::Oauth { refresh_state, .. } = &profile.credential else {
        return None;
    };
    refresh_state.next_allowed_refresh_unix_ms
}

fn eligibility_from_health(
    state: AuthProfileHealthState,
    cooldown_until_unix_ms: Option<i64>,
    now_unix_ms: i64,
) -> AuthProfileEligibility {
    if cooldown_until_unix_ms.is_some_and(|value| value > now_unix_ms) {
        return AuthProfileEligibility::CoolingDown;
    }
    match state {
        AuthProfileHealthState::Ok
        | AuthProfileHealthState::Expiring
        | AuthProfileHealthState::Static => AuthProfileEligibility::Eligible,
        AuthProfileHealthState::Expired => AuthProfileEligibility::Expired,
        AuthProfileHealthState::Missing => AuthProfileEligibility::MissingCredential,
    }
}

fn doctor_hint_for_state(
    eligibility: AuthProfileEligibility,
    expiry: AuthTokenExpiryState,
    failure: Option<AuthProfileFailureKind>,
) -> Option<AuthProfileDoctorHint> {
    let (code, severity, message) = match (eligibility, expiry, failure) {
        (AuthProfileEligibility::CoolingDown, _, Some(kind)) => (
            format!("{}_cooldown", kind.as_str()),
            AuthProfileDoctorSeverity::Warning,
            "profile is in cooldown after a recent credential or provider failure",
        ),
        (AuthProfileEligibility::CoolingDown, _, None) => (
            "cooldown_active".to_owned(),
            AuthProfileDoctorSeverity::Warning,
            "profile is in cooldown and will be retried after the recorded deadline",
        ),
        (AuthProfileEligibility::Expired, _, _) => (
            "token_expired".to_owned(),
            AuthProfileDoctorSeverity::Error,
            "OAuth access token is expired; refresh or reconnect the profile",
        ),
        (
            AuthProfileEligibility::MissingCredential,
            _,
            Some(AuthProfileFailureKind::AuthInvalid),
        ) => (
            "auth_invalid".to_owned(),
            AuthProfileDoctorSeverity::Error,
            "credential was rejected by the provider; rotate or reconnect the profile",
        ),
        (AuthProfileEligibility::MissingCredential, _, _) => (
            "credential_missing".to_owned(),
            AuthProfileDoctorSeverity::Error,
            "credential vault reference is missing or unreadable",
        ),
        (_, AuthTokenExpiryState::Expiring, _) => (
            "refresh_due".to_owned(),
            AuthProfileDoctorSeverity::Warning,
            "OAuth access token is nearing expiration",
        ),
        _ => return None,
    };
    Some(AuthProfileDoctorHint { code, severity, message: message.to_owned() })
}

fn failure_cooldown_ms(
    kind: AuthProfileFailureKind,
    provider: &AuthProvider,
    failure_count: u32,
) -> Option<u64> {
    match kind {
        AuthProfileFailureKind::RefreshFailed => Some(compute_backoff_ms(provider, failure_count)),
        AuthProfileFailureKind::AuthInvalid => Some(5 * 60 * 1_000),
        AuthProfileFailureKind::RefreshDue => Some(30_000),
        AuthProfileFailureKind::Quota => Some(30 * 60 * 1_000),
        AuthProfileFailureKind::RateLimit => Some(60_000),
        AuthProfileFailureKind::Transient => Some(15_000),
        AuthProfileFailureKind::ConfigMissing => None,
    }
}

fn normalize_profile_order(values: &[String]) -> Result<Vec<String>, AuthProfileError> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::<String>::new();
    for value in values {
        let profile_id = normalize_profile_id(value.as_str())?;
        if seen.insert(profile_id.clone()) {
            normalized.push(profile_id);
        }
    }
    Ok(normalized)
}

fn profile_order_scope_key(agent_id: Option<&str>) -> Result<String, AuthProfileError> {
    match agent_id.map(str::trim).filter(|value| !value.is_empty()) {
        Some(agent_id) => {
            let normalized = normalize_agent_id(agent_id)?;
            Ok(format!("agent:{normalized}"))
        }
        None => Ok("global".to_owned()),
    }
}

fn upsert_profile_order(records: &mut Vec<AuthProfileOrderRecord>, record: AuthProfileOrderRecord) {
    if let Some(existing) = records.iter_mut().find(|existing| {
        existing.scope == record.scope && existing.provider.as_deref() == record.provider.as_deref()
    }) {
        *existing = record;
    } else {
        records.push(record);
        records.sort_by(|left, right| {
            left.scope.cmp(&right.scope).then_with(|| left.provider.cmp(&right.provider))
        });
    }
}

fn normalize_profile_set(values: &[String]) -> Result<BTreeSet<String>, AuthProfileError> {
    values.iter().map(|value| normalize_profile_id(value.as_str())).collect()
}

fn selection_reason_code(
    provider_matches: bool,
    credential_allowed: bool,
    in_explicit_order: bool,
    policy_denied: bool,
    eligibility: AuthProfileEligibility,
) -> String {
    if !provider_matches {
        return "provider_mismatch".to_owned();
    }
    if !credential_allowed {
        return "credential_mode_restricted".to_owned();
    }
    if !in_explicit_order {
        return "not_in_explicit_order".to_owned();
    }
    if policy_denied {
        return "policy_denied".to_owned();
    }
    match eligibility {
        AuthProfileEligibility::Eligible => "eligible".to_owned(),
        AuthProfileEligibility::CoolingDown => "cooldown_active".to_owned(),
        AuthProfileEligibility::Expired => "token_expired".to_owned(),
        AuthProfileEligibility::Revoked => "credential_revoked".to_owned(),
        AuthProfileEligibility::MissingCredential => "credential_missing".to_owned(),
        AuthProfileEligibility::Unsupported => "unsupported".to_owned(),
        AuthProfileEligibility::PolicyDenied => "policy_denied".to_owned(),
    }
}

fn sort_selection_candidates(
    candidates: &mut [AuthProfileSelectionCandidate],
    explicit_positions: &BTreeMap<String, usize>,
) {
    candidates.sort_by(|left, right| {
        let left_explicit = explicit_positions.get(left.profile_id.as_str()).copied();
        let right_explicit = explicit_positions.get(right.profile_id.as_str()).copied();
        match (left_explicit, right_explicit) {
            (Some(left_index), Some(right_index)) => {
                left_index.cmp(&right_index).then_with(|| left.profile_id.cmp(&right.profile_id))
            }
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => {
                let left_rank = if left.reason_code == "eligible" { 0 } else { 1 };
                let right_rank = if right.reason_code == "eligible" { 0 } else { 1 };
                left_rank
                    .cmp(&right_rank)
                    .then_with(|| {
                        left.last_used_unix_ms
                            .unwrap_or_default()
                            .cmp(&right.last_used_unix_ms.unwrap_or_default())
                    })
                    .then_with(|| left.profile_id.cmp(&right.profile_id))
            }
        }
    });
}
