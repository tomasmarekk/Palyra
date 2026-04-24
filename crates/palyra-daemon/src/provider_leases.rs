use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use serde::Serialize;

const DEFAULT_PROVIDER_MAX_ACTIVE: u16 = 4;
const DEFAULT_CREDENTIAL_MAX_ACTIVE: u16 = 2;
const RECENT_LEASE_EVENTS_LIMIT: usize = 24;
const LEASE_WAIT_POLL_MS: u64 = 25;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeasePriority {
    Foreground,
    Background,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LeasePreviewState {
    Ready,
    Waiting,
    Deferred,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderLeasePreviewSnapshot {
    pub state: LeasePreviewState,
    pub priority: LeasePriority,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_wait_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    pub active_provider_leases: u16,
    pub active_credential_leases: u16,
    pub foreground_waiters: u16,
    pub background_waiters: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl ProviderLeasePreviewSnapshot {
    pub(crate) fn ready(priority: LeasePriority) -> Self {
        Self {
            state: LeasePreviewState::Ready,
            priority,
            estimated_wait_ms: None,
            retry_after_ms: None,
            active_provider_leases: 0,
            active_credential_leases: 0,
            foreground_waiters: 0,
            background_waiters: 0,
            credential_state: None,
            reason: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderLeaseEventSnapshot {
    pub event: String,
    pub provider_id: String,
    pub credential_id: String,
    pub priority: LeasePriority,
    pub task_label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub waited_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProviderCredentialFeedbackKind {
    Success,
    RateLimited,
    QuotaExhausted,
    AuthFailed,
    TransientFailure,
}

impl ProviderCredentialFeedbackKind {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::RateLimited => "rate_limited",
            Self::QuotaExhausted => "quota_exhausted",
            Self::AuthFailed => "auth_failed",
            Self::TransientFailure => "transient_failure",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProviderCredentialFeedbackRequest {
    pub provider_id: String,
    pub credential_id: String,
    pub kind: ProviderCredentialFeedbackKind,
    pub retry_after_ms: Option<u64>,
    pub reason: String,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderCredentialFeedbackSnapshot {
    pub provider_id: String,
    pub credential_id: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_until_unix_ms: Option<i64>,
    pub reason: String,
    pub observed_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct ProviderLeaseManagerSnapshot {
    pub provider_limit: u16,
    pub credential_limit: u16,
    pub active_leases: u16,
    pub foreground_active: u16,
    pub background_active: u16,
    pub foreground_waiters: u16,
    pub background_waiters: u16,
    pub deferred_total: u64,
    pub timed_out_total: u64,
    pub wait_events_total: u64,
    pub recent_events: Vec<ProviderLeaseEventSnapshot>,
    pub credential_feedback: Vec<ProviderCredentialFeedbackSnapshot>,
}

#[derive(Debug, Clone)]
pub(crate) struct ProviderLeaseExecutionContext {
    pub provider_id: String,
    pub credential_id: String,
    pub priority: LeasePriority,
    pub task_label: String,
    pub max_wait_ms: u64,
    pub session_id: Option<String>,
    pub run_id: Option<String>,
}

pub(crate) struct ProviderLeasePreviewRequest<'a> {
    pub provider_id: &'a str,
    pub credential_id: &'a str,
    pub priority: LeasePriority,
    pub max_wait_ms: u64,
}

pub(crate) struct ProviderLeaseAcquireRequest<'a> {
    pub provider_id: &'a str,
    pub credential_id: &'a str,
    pub priority: LeasePriority,
    pub task_label: &'a str,
    pub max_wait_ms: u64,
    pub session_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub(crate) enum ProviderLeaseAcquireError {
    Deferred(ProviderLeasePreviewSnapshot),
    TimedOut { waited_ms: u64, preview: ProviderLeasePreviewSnapshot },
}

#[derive(Debug, Clone)]
pub(crate) struct ProviderLeaseManager {
    inner: Arc<ProviderLeaseManagerInner>,
}

impl Default for ProviderLeaseManager {
    fn default() -> Self {
        Self::new(DEFAULT_PROVIDER_MAX_ACTIVE, DEFAULT_CREDENTIAL_MAX_ACTIVE)
    }
}

impl ProviderLeaseManager {
    pub(crate) fn new(provider_limit: u16, credential_limit: u16) -> Self {
        Self {
            inner: Arc::new(ProviderLeaseManagerInner {
                provider_limit: provider_limit.max(1),
                credential_limit: credential_limit.max(1),
                state: Mutex::new(ProviderLeaseState::default()),
            }),
        }
    }

    pub(crate) fn preview(
        &self,
        request: ProviderLeasePreviewRequest<'_>,
    ) -> ProviderLeasePreviewSnapshot {
        let guard = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
        self.inner.preview_locked(
            &guard,
            request.provider_id,
            request.credential_id,
            request.priority,
            request.max_wait_ms,
        )
    }

    pub(crate) fn snapshot(&self) -> ProviderLeaseManagerSnapshot {
        let guard = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
        let mut foreground_active = 0_u16;
        let mut background_active = 0_u16;
        let mut foreground_waiters = 0_u16;
        let mut background_waiters = 0_u16;
        for bucket in guard.providers.values() {
            foreground_active = foreground_active.saturating_add(bucket.active_foreground);
            background_active = background_active.saturating_add(bucket.active_background);
            foreground_waiters = foreground_waiters.max(bucket.waiting_foreground);
            background_waiters = background_waiters.max(bucket.waiting_background);
        }
        ProviderLeaseManagerSnapshot {
            provider_limit: self.inner.provider_limit,
            credential_limit: self.inner.credential_limit,
            active_leases: foreground_active.saturating_add(background_active),
            foreground_active,
            background_active,
            foreground_waiters,
            background_waiters,
            deferred_total: guard.deferred_total,
            timed_out_total: guard.timed_out_total,
            wait_events_total: guard.wait_events_total,
            recent_events: guard.recent_events.iter().cloned().collect(),
            credential_feedback: guard
                .credential_feedback
                .values()
                .filter(|feedback| feedback.is_active(crate::gateway::current_unix_ms()))
                .map(ProviderCredentialFeedbackState::snapshot)
                .collect(),
        }
    }

    pub(crate) fn record_credential_feedback(&self, request: ProviderCredentialFeedbackRequest) {
        let mut guard = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
        if request.kind == ProviderCredentialFeedbackKind::Success {
            guard.credential_feedback.remove(request.credential_id.as_str());
            self.inner.push_event_locked(
                &mut guard,
                ProviderLeaseEventSnapshot {
                    event: "credential_feedback_cleared".to_owned(),
                    provider_id: request.provider_id,
                    credential_id: request.credential_id,
                    priority: LeasePriority::Foreground,
                    task_label: "provider_feedback".to_owned(),
                    session_id: None,
                    run_id: None,
                    waited_ms: 0,
                    held_ms: None,
                    reason: Some(request.reason),
                    observed_at_unix_ms: request.observed_at_unix_ms,
                },
            );
            return;
        }
        let feedback = ProviderCredentialFeedbackState::from_request(request);
        self.inner.push_event_locked(
            &mut guard,
            ProviderLeaseEventSnapshot {
                event: "credential_feedback_recorded".to_owned(),
                provider_id: feedback.provider_id.clone(),
                credential_id: feedback.credential_id.clone(),
                priority: LeasePriority::Foreground,
                task_label: "provider_feedback".to_owned(),
                session_id: None,
                run_id: None,
                waited_ms: 0,
                held_ms: None,
                reason: Some(format!("{}:{}", feedback.state, feedback.reason)),
                observed_at_unix_ms: feedback.observed_at_unix_ms,
            },
        );
        guard.credential_feedback.insert(feedback.credential_id.clone(), feedback);
    }

    pub(crate) async fn acquire(
        &self,
        request: ProviderLeaseAcquireRequest<'_>,
    ) -> Result<ProviderLeaseGuard, ProviderLeaseAcquireError> {
        let provider_id = request.provider_id.to_owned();
        let credential_id = request.credential_id.to_owned();
        let session_id = request.session_id.map(ToOwned::to_owned);
        let run_id = request.run_id.map(ToOwned::to_owned);
        let started = Instant::now();
        let poll_ms = LEASE_WAIT_POLL_MS.max(1).min(request.max_wait_ms.max(1));
        let mut waiting_registered = false;

        loop {
            {
                let mut guard = self.inner.state.lock().unwrap_or_else(|error| error.into_inner());
                let preview = self.inner.preview_locked(
                    &guard,
                    provider_id.as_str(),
                    credential_id.as_str(),
                    request.priority,
                    request.max_wait_ms,
                );
                match preview.state {
                    LeasePreviewState::Ready => {
                        if waiting_registered {
                            decrement_waiting(
                                &mut guard.providers,
                                provider_id.as_str(),
                                request.priority,
                            );
                            decrement_waiting(
                                &mut guard.credentials,
                                credential_id.as_str(),
                                request.priority,
                            );
                        }
                        increment_active(
                            &mut guard.providers,
                            provider_id.as_str(),
                            request.priority,
                        );
                        increment_active(
                            &mut guard.credentials,
                            credential_id.as_str(),
                            request.priority,
                        );
                        let waited_ms =
                            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
                        if waited_ms > 0 {
                            guard.wait_events_total = guard.wait_events_total.saturating_add(1);
                        }
                        self.inner.push_event_locked(
                            &mut guard,
                            ProviderLeaseEventSnapshot {
                                event: "acquired".to_owned(),
                                provider_id: provider_id.clone(),
                                credential_id: credential_id.clone(),
                                priority: request.priority,
                                task_label: request.task_label.to_owned(),
                                session_id: session_id.clone(),
                                run_id: run_id.clone(),
                                waited_ms,
                                held_ms: None,
                                reason: preview.reason.clone(),
                                observed_at_unix_ms: crate::gateway::current_unix_ms(),
                            },
                        );
                        return Ok(ProviderLeaseGuard {
                            manager: Arc::downgrade(&self.inner),
                            provider_id,
                            credential_id,
                            priority: request.priority,
                            task_label: request.task_label.to_owned(),
                            session_id,
                            run_id,
                            waited_ms,
                            acquired_at: Instant::now(),
                        });
                    }
                    LeasePreviewState::Deferred => {
                        if waiting_registered {
                            decrement_waiting(
                                &mut guard.providers,
                                provider_id.as_str(),
                                request.priority,
                            );
                            decrement_waiting(
                                &mut guard.credentials,
                                credential_id.as_str(),
                                request.priority,
                            );
                        }
                        guard.deferred_total = guard.deferred_total.saturating_add(1);
                        self.inner.push_event_locked(
                            &mut guard,
                            ProviderLeaseEventSnapshot {
                                event: "deferred".to_owned(),
                                provider_id: provider_id.clone(),
                                credential_id: credential_id.clone(),
                                priority: request.priority,
                                task_label: request.task_label.to_owned(),
                                session_id: session_id.clone(),
                                run_id: run_id.clone(),
                                waited_ms: started
                                    .elapsed()
                                    .as_millis()
                                    .try_into()
                                    .unwrap_or(u64::MAX),
                                held_ms: None,
                                reason: preview.reason.clone(),
                                observed_at_unix_ms: crate::gateway::current_unix_ms(),
                            },
                        );
                        return Err(ProviderLeaseAcquireError::Deferred(preview));
                    }
                    LeasePreviewState::Waiting => {
                        let waited_ms =
                            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX);
                        if waited_ms >= request.max_wait_ms {
                            if waiting_registered {
                                decrement_waiting(
                                    &mut guard.providers,
                                    provider_id.as_str(),
                                    request.priority,
                                );
                                decrement_waiting(
                                    &mut guard.credentials,
                                    credential_id.as_str(),
                                    request.priority,
                                );
                            }
                            guard.timed_out_total = guard.timed_out_total.saturating_add(1);
                            guard.wait_events_total = guard.wait_events_total.saturating_add(1);
                            self.inner.push_event_locked(
                                &mut guard,
                                ProviderLeaseEventSnapshot {
                                    event: "timed_out".to_owned(),
                                    provider_id: provider_id.clone(),
                                    credential_id: credential_id.clone(),
                                    priority: request.priority,
                                    task_label: request.task_label.to_owned(),
                                    session_id: session_id.clone(),
                                    run_id: run_id.clone(),
                                    waited_ms,
                                    held_ms: None,
                                    reason: preview.reason.clone(),
                                    observed_at_unix_ms: crate::gateway::current_unix_ms(),
                                },
                            );
                            return Err(ProviderLeaseAcquireError::TimedOut { waited_ms, preview });
                        }
                        if !waiting_registered {
                            increment_waiting(
                                &mut guard.providers,
                                provider_id.as_str(),
                                request.priority,
                            );
                            increment_waiting(
                                &mut guard.credentials,
                                credential_id.as_str(),
                                request.priority,
                            );
                            waiting_registered = true;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct ProviderLeaseGuard {
    manager: Weak<ProviderLeaseManagerInner>,
    provider_id: String,
    credential_id: String,
    priority: LeasePriority,
    task_label: String,
    session_id: Option<String>,
    run_id: Option<String>,
    waited_ms: u64,
    acquired_at: Instant,
}

impl ProviderLeaseGuard {
    #[cfg(test)]
    pub(crate) const fn waited_ms(&self) -> u64 {
        self.waited_ms
    }
}

impl Drop for ProviderLeaseGuard {
    fn drop(&mut self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut guard = manager.state.lock().unwrap_or_else(|error| error.into_inner());
        decrement_active(&mut guard.providers, self.provider_id.as_str(), self.priority);
        decrement_active(&mut guard.credentials, self.credential_id.as_str(), self.priority);
        manager.push_event_locked(
            &mut guard,
            ProviderLeaseEventSnapshot {
                event: "released".to_owned(),
                provider_id: self.provider_id.clone(),
                credential_id: self.credential_id.clone(),
                priority: self.priority,
                task_label: self.task_label.clone(),
                session_id: self.session_id.clone(),
                run_id: self.run_id.clone(),
                waited_ms: self.waited_ms,
                held_ms: Some(
                    self.acquired_at.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                ),
                reason: None,
                observed_at_unix_ms: crate::gateway::current_unix_ms(),
            },
        );
    }
}

#[derive(Debug)]
struct ProviderLeaseManagerInner {
    provider_limit: u16,
    credential_limit: u16,
    state: Mutex<ProviderLeaseState>,
}

impl ProviderLeaseManagerInner {
    fn preview_locked(
        &self,
        state: &ProviderLeaseState,
        provider_id: &str,
        credential_id: &str,
        priority: LeasePriority,
        max_wait_ms: u64,
    ) -> ProviderLeasePreviewSnapshot {
        let provider = state.providers.get(provider_id).copied().unwrap_or_default();
        let credential = state.credentials.get(credential_id).copied().unwrap_or_default();
        let foreground_waiters = provider.waiting_foreground.max(credential.waiting_foreground);
        let background_waiters = provider.waiting_background.max(credential.waiting_background);
        let provider_active = provider.active_total();
        let credential_active = credential.active_total();
        if let Some(feedback) = state.active_credential_feedback(credential_id) {
            let retry_after_ms = feedback.retry_after_ms(crate::gateway::current_unix_ms());
            let state_kind = if retry_after_ms.is_some_and(|value| value <= max_wait_ms.max(1)) {
                LeasePreviewState::Waiting
            } else {
                LeasePreviewState::Deferred
            };
            return ProviderLeasePreviewSnapshot {
                state: state_kind,
                priority,
                estimated_wait_ms: retry_after_ms,
                retry_after_ms,
                active_provider_leases: provider_active,
                active_credential_leases: credential_active,
                foreground_waiters,
                background_waiters,
                credential_state: Some(feedback.state.clone()),
                reason: Some(format!("credential_feedback:{}", feedback.reason)),
            };
        }
        let provider_reason =
            bucket_reason(provider, priority, self.provider_limit, foreground_waiters);
        let credential_reason =
            bucket_reason(credential, priority, self.credential_limit, foreground_waiters);
        let reason = provider_reason.or(credential_reason);
        let state_kind = if reason.is_none() {
            LeasePreviewState::Ready
        } else if priority == LeasePriority::Background
            && matches!(reason, Some("foreground_waiters_present" | "foreground_capacity_reserved"))
        {
            LeasePreviewState::Deferred
        } else {
            LeasePreviewState::Waiting
        };
        let estimated_wait_ms = match state_kind {
            LeasePreviewState::Ready | LeasePreviewState::Deferred => None,
            LeasePreviewState::Waiting => Some(
                (u64::from(foreground_waiters.max(background_waiters)).saturating_add(1))
                    .saturating_mul(LEASE_WAIT_POLL_MS.max(1))
                    .min(max_wait_ms.max(LEASE_WAIT_POLL_MS)),
            ),
        };
        ProviderLeasePreviewSnapshot {
            state: state_kind,
            priority,
            estimated_wait_ms,
            retry_after_ms: None,
            active_provider_leases: provider_active,
            active_credential_leases: credential_active,
            foreground_waiters,
            background_waiters,
            credential_state: None,
            reason: reason.map(str::to_owned),
        }
    }

    fn push_event_locked(&self, state: &mut ProviderLeaseState, event: ProviderLeaseEventSnapshot) {
        state.recent_events.push_front(event);
        while state.recent_events.len() > RECENT_LEASE_EVENTS_LIMIT {
            state.recent_events.pop_back();
        }
    }
}

#[derive(Debug, Default)]
struct ProviderLeaseState {
    providers: HashMap<String, LeaseBucketState>,
    credentials: HashMap<String, LeaseBucketState>,
    credential_feedback: HashMap<String, ProviderCredentialFeedbackState>,
    recent_events: VecDeque<ProviderLeaseEventSnapshot>,
    deferred_total: u64,
    timed_out_total: u64,
    wait_events_total: u64,
}

impl ProviderLeaseState {
    fn active_credential_feedback(
        &self,
        credential_id: &str,
    ) -> Option<&ProviderCredentialFeedbackState> {
        let feedback = self.credential_feedback.get(credential_id)?;
        if feedback.is_active(crate::gateway::current_unix_ms()) {
            Some(feedback)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
struct ProviderCredentialFeedbackState {
    provider_id: String,
    credential_id: String,
    state: String,
    retry_after_ms: Option<u64>,
    blocked_until_unix_ms: Option<i64>,
    reason: String,
    observed_at_unix_ms: i64,
}

impl ProviderCredentialFeedbackState {
    fn from_request(request: ProviderCredentialFeedbackRequest) -> Self {
        let default_retry_after_ms = match request.kind {
            ProviderCredentialFeedbackKind::RateLimited => Some(30_000),
            ProviderCredentialFeedbackKind::TransientFailure => Some(5_000),
            ProviderCredentialFeedbackKind::QuotaExhausted
            | ProviderCredentialFeedbackKind::AuthFailed => None,
            ProviderCredentialFeedbackKind::Success => None,
        };
        let retry_after_ms = request.retry_after_ms.or(default_retry_after_ms);
        let blocked_until_unix_ms =
            retry_after_ms.map(|value| request.observed_at_unix_ms.saturating_add(value as i64));
        Self {
            provider_id: request.provider_id,
            credential_id: request.credential_id,
            state: request.kind.as_str().to_owned(),
            retry_after_ms,
            blocked_until_unix_ms,
            reason: request.reason,
            observed_at_unix_ms: request.observed_at_unix_ms,
        }
    }

    fn is_active(&self, now_unix_ms: i64) -> bool {
        match self.blocked_until_unix_ms {
            Some(blocked_until_unix_ms) => blocked_until_unix_ms > now_unix_ms,
            None => true,
        }
    }

    fn retry_after_ms(&self, now_unix_ms: i64) -> Option<u64> {
        self.blocked_until_unix_ms
            .map(|blocked_until| blocked_until.saturating_sub(now_unix_ms) as u64)
            .or(self.retry_after_ms)
    }

    fn snapshot(&self) -> ProviderCredentialFeedbackSnapshot {
        ProviderCredentialFeedbackSnapshot {
            provider_id: self.provider_id.clone(),
            credential_id: self.credential_id.clone(),
            state: self.state.clone(),
            retry_after_ms: self.retry_after_ms,
            blocked_until_unix_ms: self.blocked_until_unix_ms,
            reason: self.reason.clone(),
            observed_at_unix_ms: self.observed_at_unix_ms,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct LeaseBucketState {
    active_foreground: u16,
    active_background: u16,
    waiting_foreground: u16,
    waiting_background: u16,
}

impl LeaseBucketState {
    const fn active_total(self) -> u16 {
        self.active_foreground.saturating_add(self.active_background)
    }

    const fn is_empty(self) -> bool {
        self.active_foreground == 0
            && self.active_background == 0
            && self.waiting_foreground == 0
            && self.waiting_background == 0
    }
}

fn bucket_reason(
    bucket: LeaseBucketState,
    priority: LeasePriority,
    limit: u16,
    foreground_waiters: u16,
) -> Option<&'static str> {
    if priority == LeasePriority::Background && foreground_waiters > 0 {
        return Some("foreground_waiters_present");
    }
    if priority == LeasePriority::Background && bucket.active_background >= background_limit(limit)
    {
        return Some("foreground_capacity_reserved");
    }
    if bucket.active_total() >= limit {
        return Some("shared_capacity_exhausted");
    }
    None
}

const fn background_limit(limit: u16) -> u16 {
    if limit > 1 {
        limit - 1
    } else {
        1
    }
}

fn increment_active(
    map: &mut HashMap<String, LeaseBucketState>,
    key: &str,
    priority: LeasePriority,
) {
    let entry = map.entry(key.to_owned()).or_default();
    match priority {
        LeasePriority::Foreground => {
            entry.active_foreground = entry.active_foreground.saturating_add(1);
        }
        LeasePriority::Background => {
            entry.active_background = entry.active_background.saturating_add(1);
        }
    }
}

fn decrement_active(
    map: &mut HashMap<String, LeaseBucketState>,
    key: &str,
    priority: LeasePriority,
) {
    let remove = if let Some(entry) = map.get_mut(key) {
        match priority {
            LeasePriority::Foreground => {
                entry.active_foreground = entry.active_foreground.saturating_sub(1);
            }
            LeasePriority::Background => {
                entry.active_background = entry.active_background.saturating_sub(1);
            }
        }
        entry.is_empty()
    } else {
        false
    };
    if remove {
        map.remove(key);
    }
}

fn increment_waiting(
    map: &mut HashMap<String, LeaseBucketState>,
    key: &str,
    priority: LeasePriority,
) {
    let entry = map.entry(key.to_owned()).or_default();
    match priority {
        LeasePriority::Foreground => {
            entry.waiting_foreground = entry.waiting_foreground.saturating_add(1);
        }
        LeasePriority::Background => {
            entry.waiting_background = entry.waiting_background.saturating_add(1);
        }
    }
}

fn decrement_waiting(
    map: &mut HashMap<String, LeaseBucketState>,
    key: &str,
    priority: LeasePriority,
) {
    let remove = if let Some(entry) = map.get_mut(key) {
        match priority {
            LeasePriority::Foreground => {
                entry.waiting_foreground = entry.waiting_foreground.saturating_sub(1);
            }
            LeasePriority::Background => {
                entry.waiting_background = entry.waiting_background.saturating_sub(1);
            }
        }
        entry.is_empty()
    } else {
        false
    };
    if remove {
        map.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LeasePreviewState, LeasePriority, ProviderCredentialFeedbackKind,
        ProviderCredentialFeedbackRequest, ProviderLeaseAcquireError, ProviderLeaseAcquireRequest,
        ProviderLeaseManager, ProviderLeasePreviewRequest,
    };
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn lease_manager_coordinates_parallel_foreground_waiters() {
        let manager = ProviderLeaseManager::new(1, 1);
        let first = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive",
                max_wait_ms: 0,
                session_id: Some("session-1"),
                run_id: Some("run-1"),
            })
            .await
            .expect("first foreground lease should acquire immediately");

        let manager_clone = manager.clone();
        let waiter = tokio::spawn(async move {
            manager_clone
                .acquire(ProviderLeaseAcquireRequest {
                    provider_id: "openai",
                    credential_id: "cred-a",
                    priority: LeasePriority::Foreground,
                    task_label: "primary_interactive",
                    max_wait_ms: 300,
                    session_id: Some("session-2"),
                    run_id: Some("run-2"),
                })
                .await
        });

        sleep(Duration::from_millis(60)).await;
        drop(first);
        let second = waiter
            .await
            .expect("waiter join should succeed")
            .expect("second foreground lease should acquire after the first releases");
        assert!(second.waited_ms() >= 25, "second lease should record shared wait pressure");
        drop(second);

        let snapshot = manager.snapshot();
        assert!(
            snapshot.recent_events.iter().any(|entry| entry.event == "acquired"),
            "acquire events should be retained for observability"
        );
        assert!(
            snapshot.recent_events.iter().any(|entry| entry.event == "released"),
            "release events should be retained for observability"
        );
    }

    #[tokio::test]
    async fn lease_manager_defers_background_when_foreground_waiter_is_present() {
        let manager = ProviderLeaseManager::new(2, 2);
        let first_foreground = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive",
                max_wait_ms: 0,
                session_id: Some("session-1"),
                run_id: Some("run-1"),
            })
            .await
            .expect("first foreground lease should acquire");
        let background = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Background,
                task_label: "background_automation",
                max_wait_ms: 0,
                session_id: Some("session-bg"),
                run_id: Some("run-bg"),
            })
            .await
            .expect("background lease should use the remaining fair-share slot");

        let manager_clone = manager.clone();
        let foreground_waiter = tokio::spawn(async move {
            manager_clone
                .acquire(ProviderLeaseAcquireRequest {
                    provider_id: "openai",
                    credential_id: "cred-a",
                    priority: LeasePriority::Foreground,
                    task_label: "primary_interactive",
                    max_wait_ms: 300,
                    session_id: Some("session-2"),
                    run_id: Some("run-2"),
                })
                .await
        });
        sleep(Duration::from_millis(50)).await;

        let preview = manager.preview(ProviderLeasePreviewRequest {
            provider_id: "openai",
            credential_id: "cred-a",
            priority: LeasePriority::Background,
            max_wait_ms: 150,
        });
        assert_eq!(
            preview.state,
            LeasePreviewState::Deferred,
            "background work should defer while a foreground waiter is queued"
        );

        let deferred = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Background,
                task_label: "background_automation",
                max_wait_ms: 150,
                session_id: Some("session-bg-2"),
                run_id: Some("run-bg-2"),
            })
            .await
            .expect_err("background acquire should defer while foreground waiter is present");
        assert!(
            matches!(deferred, ProviderLeaseAcquireError::Deferred(_)),
            "fairness posture should surface as a defer outcome"
        );

        drop(background);
        drop(first_foreground);
        let acquired = foreground_waiter
            .await
            .expect("foreground waiter join should succeed")
            .expect("foreground waiter should eventually acquire");
        drop(acquired);
    }

    #[tokio::test]
    async fn lease_manager_records_timeout_events() {
        let manager = ProviderLeaseManager::new(1, 1);
        let held = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive",
                max_wait_ms: 0,
                session_id: Some("session-1"),
                run_id: Some("run-1"),
            })
            .await
            .expect("initial foreground lease should acquire");

        let error = manager
            .acquire(ProviderLeaseAcquireRequest {
                provider_id: "openai",
                credential_id: "cred-a",
                priority: LeasePriority::Foreground,
                task_label: "primary_interactive",
                max_wait_ms: 40,
                session_id: Some("session-2"),
                run_id: Some("run-2"),
            })
            .await
            .expect_err("second foreground lease should time out while capacity stays exhausted");
        assert!(
            matches!(error, ProviderLeaseAcquireError::TimedOut { .. }),
            "capacity exhaustion should publish timeout events for observability"
        );

        drop(held);
        let snapshot = manager.snapshot();
        assert_eq!(snapshot.timed_out_total, 1, "timeout totals should be counted");
        assert!(
            snapshot.recent_events.iter().any(|entry| entry.event == "timed_out"),
            "timeout events should stay visible in recent lease telemetry"
        );
    }

    #[tokio::test]
    async fn lease_manager_defers_rate_limited_credentials_until_feedback_clears() {
        let manager = ProviderLeaseManager::new(2, 2);
        manager.record_credential_feedback(ProviderCredentialFeedbackRequest {
            provider_id: "openai".to_owned(),
            credential_id: "cred-a".to_owned(),
            kind: ProviderCredentialFeedbackKind::RateLimited,
            retry_after_ms: Some(250),
            reason: "provider returned 429".to_owned(),
            observed_at_unix_ms: crate::gateway::current_unix_ms(),
        });

        let preview = manager.preview(ProviderLeasePreviewRequest {
            provider_id: "openai",
            credential_id: "cred-a",
            priority: LeasePriority::Foreground,
            max_wait_ms: 100,
        });
        assert_eq!(
            preview.state,
            LeasePreviewState::Deferred,
            "foreground work should not acquire a credential during a rate-limit cooldown"
        );
        assert_eq!(preview.credential_state.as_deref(), Some("rate_limited"));
        assert!(
            preview.retry_after_ms.is_some(),
            "rate-limit feedback should expose scheduler retry-after timing"
        );

        let snapshot = manager.snapshot();
        assert_eq!(snapshot.credential_feedback.len(), 1);
        assert_eq!(snapshot.credential_feedback[0].state, "rate_limited");

        manager.record_credential_feedback(ProviderCredentialFeedbackRequest {
            provider_id: "openai".to_owned(),
            credential_id: "cred-a".to_owned(),
            kind: ProviderCredentialFeedbackKind::Success,
            retry_after_ms: None,
            reason: "successful provider call".to_owned(),
            observed_at_unix_ms: crate::gateway::current_unix_ms(),
        });
        let ready = manager.preview(ProviderLeasePreviewRequest {
            provider_id: "openai",
            credential_id: "cred-a",
            priority: LeasePriority::Foreground,
            max_wait_ms: 100,
        });
        assert_eq!(ready.state, LeasePreviewState::Ready);
        assert!(manager.snapshot().credential_feedback.is_empty());
    }
}
