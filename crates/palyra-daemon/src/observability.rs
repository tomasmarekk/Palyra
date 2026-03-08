use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
};

use serde::Serialize;

const RECENT_FAILURE_LIMIT: usize = 24;

#[derive(Debug, Clone, Default, Serialize)]
pub struct CorrelationSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub envelope_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onboarding_flow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_session_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    #[serde(rename = "product_failure")]
    Product,
    #[serde(rename = "config_failure")]
    Config,
    #[serde(rename = "upstream_provider_failure")]
    UpstreamProvider,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailureSnapshot {
    pub operation: String,
    pub failure_class: FailureClass,
    pub message: String,
    pub observed_at_unix_ms: i64,
    pub correlation: CorrelationSnapshot,
}

#[derive(Debug, Default)]
pub struct ObservabilityState {
    provider_auth_attempts: AtomicU64,
    provider_auth_failures: AtomicU64,
    provider_refresh_failures: AtomicU64,
    dashboard_mutation_attempts: AtomicU64,
    dashboard_mutation_successes: AtomicU64,
    dashboard_mutation_failures: AtomicU64,
    support_bundle_exports_started: AtomicU64,
    support_bundle_exports_succeeded: AtomicU64,
    support_bundle_exports_failed: AtomicU64,
    recent_failures: Mutex<VecDeque<FailureSnapshot>>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CounterSnapshot {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub failure_rate_bps: u32,
}

impl ObservabilityState {
    pub fn record_provider_auth_attempt(&self) {
        self.provider_auth_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_provider_auth_failure(
        &self,
        operation: impl Into<String>,
        failure_class: FailureClass,
        message: impl Into<String>,
        observed_at_unix_ms: i64,
        correlation: CorrelationSnapshot,
    ) {
        self.provider_auth_failures.fetch_add(1, Ordering::Relaxed);
        self.push_failure(FailureSnapshot {
            operation: operation.into(),
            failure_class,
            message: message.into(),
            observed_at_unix_ms,
            correlation,
        });
    }

    pub fn record_provider_refresh_failure(
        &self,
        operation: impl Into<String>,
        failure_class: FailureClass,
        message: impl Into<String>,
        observed_at_unix_ms: i64,
        correlation: CorrelationSnapshot,
    ) {
        self.provider_refresh_failures.fetch_add(1, Ordering::Relaxed);
        self.push_failure(FailureSnapshot {
            operation: operation.into(),
            failure_class,
            message: message.into(),
            observed_at_unix_ms,
            correlation,
        });
    }

    pub fn record_dashboard_mutation_result(
        &self,
        success: bool,
        operation: impl Into<String>,
        failure_class: FailureClass,
        message: impl Into<String>,
        observed_at_unix_ms: i64,
        correlation: CorrelationSnapshot,
    ) {
        self.dashboard_mutation_attempts.fetch_add(1, Ordering::Relaxed);
        if success {
            self.dashboard_mutation_successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.dashboard_mutation_failures.fetch_add(1, Ordering::Relaxed);
            self.push_failure(FailureSnapshot {
                operation: operation.into(),
                failure_class,
                message: message.into(),
                observed_at_unix_ms,
                correlation,
            });
        }
    }

    pub fn record_support_bundle_export_started(&self) {
        self.support_bundle_exports_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_support_bundle_export_result(
        &self,
        success: bool,
        operation: impl Into<String>,
        message: impl Into<String>,
        observed_at_unix_ms: i64,
        correlation: CorrelationSnapshot,
    ) {
        if success {
            self.support_bundle_exports_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.support_bundle_exports_failed.fetch_add(1, Ordering::Relaxed);
            self.push_failure(FailureSnapshot {
                operation: operation.into(),
                failure_class: FailureClass::Product,
                message: message.into(),
                observed_at_unix_ms,
                correlation,
            });
        }
    }

    pub fn provider_auth_snapshot(&self) -> CounterSnapshot {
        let attempts = self.provider_auth_attempts.load(Ordering::Relaxed);
        let failures = self.provider_auth_failures.load(Ordering::Relaxed);
        CounterSnapshot {
            attempts,
            successes: attempts.saturating_sub(failures),
            failures,
            failure_rate_bps: ratio_bps(failures, attempts),
        }
    }

    pub fn dashboard_mutation_snapshot(&self) -> CounterSnapshot {
        let attempts = self.dashboard_mutation_attempts.load(Ordering::Relaxed);
        let successes = self.dashboard_mutation_successes.load(Ordering::Relaxed);
        let failures = self.dashboard_mutation_failures.load(Ordering::Relaxed);
        CounterSnapshot {
            attempts,
            successes,
            failures,
            failure_rate_bps: ratio_bps(failures, attempts),
        }
    }

    pub fn support_bundle_snapshot(&self) -> CounterSnapshot {
        let attempts = self.support_bundle_exports_started.load(Ordering::Relaxed);
        let successes = self.support_bundle_exports_succeeded.load(Ordering::Relaxed);
        let failures = self.support_bundle_exports_failed.load(Ordering::Relaxed);
        CounterSnapshot {
            attempts,
            successes,
            failures,
            failure_rate_bps: ratio_bps(failures, attempts),
        }
    }

    pub fn provider_refresh_failures(&self) -> u64 {
        self.provider_refresh_failures.load(Ordering::Relaxed)
    }

    pub fn recent_failures(&self) -> Vec<FailureSnapshot> {
        self.recent_failures
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect()
    }

    fn push_failure(&self, failure: FailureSnapshot) {
        let mut guard = self.recent_failures.lock().unwrap_or_else(|error| error.into_inner());
        guard.push_front(failure);
        while guard.len() > RECENT_FAILURE_LIMIT {
            guard.pop_back();
        }
    }
}

fn ratio_bps(numerator: u64, denominator: u64) -> u32 {
    if denominator == 0 {
        return 0;
    }
    let scaled = numerator.saturating_mul(10_000) / denominator;
    u32::try_from(scaled).unwrap_or(u32::MAX)
}
