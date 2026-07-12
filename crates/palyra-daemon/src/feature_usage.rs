//! Bounded process-local evidence for feature-rollout hot-path usage.
//!
//! Run identifiers become domain-separated fingerprints at ingestion. Usage
//! and terminal-order evidence use separate FIFO windows, while diagnostics
//! expose only aggregate counts and stable redacted reasons.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Mutex, MutexGuard},
};

use serde::Serialize;
use sha2::{Digest, Sha256};

const FEATURE_USAGE_CAPACITY: usize = 4_096;
const MAX_RUN_ID_BYTES: usize = 128;
const RUN_FINGERPRINT_DOMAIN: &[u8] = b"palyra.feature_usage.run_fingerprint.v1\0";
const OBSERVATION_SCOPE: &str = "process_local_retained_run_window";

/// A rollout capability with instrumented direct and fallback paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FeatureUsageCapability {
    VerificationRuntime,
    CompactionSafeguard,
}

impl FeatureUsageCapability {
    pub(crate) const ALL: [Self; 2] = [Self::VerificationRuntime, Self::CompactionSafeguard];

    /// Returns the stable diagnostics identifier for this capability.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::VerificationRuntime => "verification_runtime",
            Self::CompactionSafeguard => "compaction_safeguard",
        }
    }
}

impl Serialize for FeatureUsageCapability {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// A bounded, redaction-safe reason for taking a fallback path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FeatureUsageReason {
    RolloutDisabled,
}

impl FeatureUsageReason {
    /// Returns the stable diagnostics reason code.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RolloutDisabled => "rollout_disabled",
        }
    }
}

impl Serialize for FeatureUsageReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// A redaction-safe reason why a usage observation was not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FeatureUsageDropReason {
    EmptyRunId,
    RunIdTooLong,
    AfterTerminal,
}

impl FeatureUsageDropReason {
    /// Returns the stable diagnostics reason code.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EmptyRunId => "empty_run_id",
            Self::RunIdTooLong => "run_id_too_long",
            Self::AfterTerminal => "after_terminal",
        }
    }
}

impl Serialize for FeatureUsageDropReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

/// The execution path observed for a capability during one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FeatureUsagePath {
    Direct,
    Fallback { reason: FeatureUsageReason },
}

/// Aggregate evidence for one capability in the retained run window.
///
/// Direct and fallback counts are inclusive, so a mixed run contributes to
/// both counters and to the corresponding mixed counter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FeatureUsageCapabilitySnapshot {
    pub(crate) capability: FeatureUsageCapability,
    pub(crate) observed_runs: usize,
    pub(crate) active_runs: usize,
    pub(crate) direct_runs: usize,
    pub(crate) fallback_runs: usize,
    pub(crate) mixed_runs: usize,
    pub(crate) terminal_observed_runs: usize,
    pub(crate) terminal_direct_runs: usize,
    pub(crate) terminal_fallback_runs: usize,
    pub(crate) terminal_mixed_runs: usize,
    pub(crate) reason_counts: BTreeMap<FeatureUsageReason, usize>,
    pub(crate) window_truncated: bool,
    pub(crate) evicted_runs: u64,
    pub(crate) dropped_observations: u64,
    pub(crate) dropped_observation_reason_counts: BTreeMap<FeatureUsageDropReason, u64>,
}

/// A redacted point-in-time view of the process-local usage registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct FeatureUsageSnapshot {
    pub(crate) observation_scope: &'static str,
    pub(crate) resets_on_restart: bool,
    pub(crate) capacity: usize,
    pub(crate) retained_runs: usize,
    pub(crate) active_runs: usize,
    pub(crate) terminal_runs: usize,
    pub(crate) window_truncated: bool,
    pub(crate) evicted_runs: u64,
    pub(crate) terminal_fence_capacity: usize,
    pub(crate) retained_terminal_fences: usize,
    pub(crate) terminal_fence_window_truncated: bool,
    pub(crate) evicted_terminal_fences: u64,
    pub(crate) dropped_observations: u64,
    pub(crate) dropped_observation_reason_counts: BTreeMap<FeatureUsageDropReason, u64>,
    pub(crate) capabilities: Vec<FeatureUsageCapabilitySnapshot>,
}

/// Thread-safe, bounded evidence registry for rollout hot-path diagnostics.
///
/// The registry retains the most recently first-observed runs. Duplicate
/// observations do not refresh FIFO position, and terminal runs reject later
/// observations so completed evidence cannot be rewritten by retries.
pub(crate) struct FeatureUsageRegistry {
    state: Mutex<FeatureUsageState>,
}

impl FeatureUsageRegistry {
    /// Creates a registry with the production run-window capacity.
    pub(crate) fn new() -> Self {
        Self::with_capacity(FEATURE_USAGE_CAPACITY)
    }

    /// Creates a registry with a reduced deterministic capacity for tests.
    #[cfg(test)]
    pub(crate) fn with_test_capacity(capacity: usize) -> Self {
        Self::with_capacity(capacity)
    }

    /// Records one direct or fallback observation for a run and capability.
    ///
    /// Repeated observations are idempotent. An observation that arrives after
    /// terminalization is ignored, preserving the completed run's evidence.
    pub(crate) fn record(
        &self,
        run_id: &str,
        capability: FeatureUsageCapability,
        path: FeatureUsagePath,
    ) {
        let fingerprint = match RunFingerprint::from_run_id(run_id) {
            Ok(fingerprint) => fingerprint,
            Err(reason) => {
                self.lock_state().record_dropped_observation(capability, reason);
                return;
            }
        };
        let mut state = self.lock_state();
        if state.terminal_fences.contains(&fingerprint)
            || state.runs.get(&fingerprint).is_some_and(|run| run.is_terminal)
        {
            state.record_dropped_observation(capability, FeatureUsageDropReason::AfterTerminal);
            return;
        }
        let run = state.ensure_run(fingerprint);
        let usage = run.capabilities.entry(capability).or_default();
        match path {
            FeatureUsagePath::Direct => usage.direct = true,
            FeatureUsagePath::Fallback { reason } => {
                usage.fallback_reasons.insert(reason);
            }
        }
    }

    /// Marks a run terminal and freezes its current evidence.
    ///
    /// Terminalization is idempotent. Unknown runs enter a separate bounded
    /// fence without contributing to usage counts.
    pub(crate) fn mark_terminal(&self, run_id: &str) {
        let Ok(fingerprint) = RunFingerprint::from_run_id(run_id) else {
            return;
        };
        let mut state = self.lock_state();
        if let Some(run) = state.runs.get_mut(&fingerprint) {
            run.is_terminal = true;
        }
        state.remember_terminal_fence(fingerprint);
    }

    /// Returns aggregate evidence without exposing run identifiers or fingerprints.
    pub(crate) fn snapshot(&self) -> FeatureUsageSnapshot {
        let state = self.lock_state();
        state.snapshot()
    }

    fn lock_state(&self) -> MutexGuard<'_, FeatureUsageState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(error) => {
                tracing::warn!(
                    "feature usage registry mutex was poisoned; recovering retained evidence"
                );
                error.into_inner()
            }
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "feature usage registry capacity must be positive");
        Self {
            state: Mutex::new(FeatureUsageState {
                capacity,
                runs: HashMap::with_capacity(capacity),
                fifo: VecDeque::with_capacity(capacity),
                evicted_runs: 0,
                capability_evicted_runs: BTreeMap::new(),
                terminal_fences: HashSet::with_capacity(capacity),
                terminal_fence_fifo: VecDeque::with_capacity(capacity),
                evicted_terminal_fences: 0,
                dropped_observations: 0,
                dropped_observation_reason_counts: BTreeMap::new(),
                capability_dropped_observations: BTreeMap::new(),
                capability_dropped_observation_reason_counts: BTreeMap::new(),
            }),
        }
    }
}

impl Default for FeatureUsageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct RunFingerprint([u8; 32]);

impl RunFingerprint {
    fn from_run_id(run_id: &str) -> Result<Self, FeatureUsageDropReason> {
        let run_id = run_id.trim();
        if run_id.is_empty() {
            return Err(FeatureUsageDropReason::EmptyRunId);
        }
        if run_id.len() > MAX_RUN_ID_BYTES {
            return Err(FeatureUsageDropReason::RunIdTooLong);
        }
        let run_id_len = u64::try_from(run_id.len())
            .expect("bounded run identifier length fits in u64 on supported targets");
        let mut hasher = Sha256::new();
        hasher.update(RUN_FINGERPRINT_DOMAIN);
        hasher.update(run_id_len.to_be_bytes());
        hasher.update(run_id.as_bytes());
        Ok(Self(hasher.finalize().into()))
    }
}

struct FeatureUsageState {
    capacity: usize,
    runs: HashMap<RunFingerprint, RunUsage>,
    fifo: VecDeque<RunFingerprint>,
    evicted_runs: u64,
    capability_evicted_runs: BTreeMap<FeatureUsageCapability, u64>,
    terminal_fences: HashSet<RunFingerprint>,
    terminal_fence_fifo: VecDeque<RunFingerprint>,
    evicted_terminal_fences: u64,
    dropped_observations: u64,
    dropped_observation_reason_counts: BTreeMap<FeatureUsageDropReason, u64>,
    capability_dropped_observations: BTreeMap<FeatureUsageCapability, u64>,
    capability_dropped_observation_reason_counts:
        BTreeMap<FeatureUsageCapability, BTreeMap<FeatureUsageDropReason, u64>>,
}

impl FeatureUsageState {
    fn ensure_run(&mut self, fingerprint: RunFingerprint) -> &mut RunUsage {
        if !self.runs.contains_key(&fingerprint) {
            while self.runs.len() >= self.capacity {
                let oldest =
                    self.fifo.pop_front().expect("feature usage FIFO contains every retained run");
                if let Some(evicted) = self.runs.remove(&oldest) {
                    self.evicted_runs = self.evicted_runs.saturating_add(1);
                    for capability in evicted.capabilities.keys() {
                        let count = self.capability_evicted_runs.entry(*capability).or_default();
                        *count = count.saturating_add(1);
                    }
                    if evicted.is_terminal {
                        self.remember_terminal_fence(oldest);
                    }
                }
            }
            self.fifo.push_back(fingerprint);
            self.runs.insert(fingerprint, RunUsage::default());
        }

        self.runs
            .get_mut(&fingerprint)
            .expect("feature usage run was inserted before mutable lookup")
    }

    fn remember_terminal_fence(&mut self, fingerprint: RunFingerprint) {
        if self.terminal_fences.contains(&fingerprint) {
            return;
        }

        // The fence has its own FIFO so unrelated terminal runs cannot evict
        // retained usage evidence or distort capability-local truncation.
        while self.terminal_fences.len() >= self.capacity {
            let oldest = self
                .terminal_fence_fifo
                .pop_front()
                .expect("feature usage terminal fence FIFO contains every retained fence");
            if self.terminal_fences.remove(&oldest) {
                self.evicted_terminal_fences = self.evicted_terminal_fences.saturating_add(1);
            }
        }
        self.terminal_fence_fifo.push_back(fingerprint);
        self.terminal_fences.insert(fingerprint);
    }

    fn record_dropped_observation(
        &mut self,
        capability: FeatureUsageCapability,
        reason: FeatureUsageDropReason,
    ) {
        self.dropped_observations = self.dropped_observations.saturating_add(1);
        let reason_count = self.dropped_observation_reason_counts.entry(reason).or_default();
        *reason_count = reason_count.saturating_add(1);
        let capability_count = self.capability_dropped_observations.entry(capability).or_default();
        *capability_count = capability_count.saturating_add(1);
        let capability_reason_count = self
            .capability_dropped_observation_reason_counts
            .entry(capability)
            .or_default()
            .entry(reason)
            .or_default();
        *capability_reason_count = capability_reason_count.saturating_add(1);
    }

    fn snapshot(&self) -> FeatureUsageSnapshot {
        let terminal_runs = self.runs.values().filter(|run| run.is_terminal).count();
        let mut capability_snapshots = FeatureUsageCapability::ALL
            .into_iter()
            .map(|capability| {
                (
                    capability,
                    FeatureUsageCapabilitySnapshot {
                        capability,
                        observed_runs: 0,
                        active_runs: 0,
                        direct_runs: 0,
                        fallback_runs: 0,
                        mixed_runs: 0,
                        terminal_observed_runs: 0,
                        terminal_direct_runs: 0,
                        terminal_fallback_runs: 0,
                        terminal_mixed_runs: 0,
                        reason_counts: BTreeMap::new(),
                        window_truncated: self
                            .capability_evicted_runs
                            .get(&capability)
                            .is_some_and(|count| *count > 0),
                        evicted_runs: self
                            .capability_evicted_runs
                            .get(&capability)
                            .copied()
                            .unwrap_or(0),
                        dropped_observations: self
                            .capability_dropped_observations
                            .get(&capability)
                            .copied()
                            .unwrap_or(0),
                        dropped_observation_reason_counts: self
                            .capability_dropped_observation_reason_counts
                            .get(&capability)
                            .cloned()
                            .unwrap_or_default(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        for run in self.runs.values() {
            for (capability, usage) in &run.capabilities {
                let snapshot = capability_snapshots
                    .get_mut(capability)
                    .expect("every instrumented capability has a snapshot bucket");
                snapshot.observe(usage, run.is_terminal);
            }
        }

        FeatureUsageSnapshot {
            observation_scope: OBSERVATION_SCOPE,
            resets_on_restart: true,
            capacity: self.capacity,
            retained_runs: self.runs.len(),
            active_runs: self.runs.len() - terminal_runs,
            terminal_runs,
            window_truncated: self.evicted_runs > 0,
            evicted_runs: self.evicted_runs,
            terminal_fence_capacity: self.capacity,
            retained_terminal_fences: self.terminal_fences.len(),
            terminal_fence_window_truncated: self.evicted_terminal_fences > 0,
            evicted_terminal_fences: self.evicted_terminal_fences,
            dropped_observations: self.dropped_observations,
            dropped_observation_reason_counts: self.dropped_observation_reason_counts.clone(),
            capabilities: capability_snapshots.into_values().collect(),
        }
    }
}

impl FeatureUsageCapabilitySnapshot {
    fn observe(&mut self, usage: &CapabilityUsage, is_terminal: bool) {
        let has_direct = usage.direct;
        let has_fallback = !usage.fallback_reasons.is_empty();
        self.observed_runs += 1;
        self.direct_runs += usize::from(has_direct);
        self.fallback_runs += usize::from(has_fallback);
        self.mixed_runs += usize::from(has_direct && has_fallback);

        if is_terminal {
            self.terminal_observed_runs += 1;
            self.terminal_direct_runs += usize::from(has_direct);
            self.terminal_fallback_runs += usize::from(has_fallback);
            self.terminal_mixed_runs += usize::from(has_direct && has_fallback);
        } else {
            self.active_runs += 1;
        }

        for reason in &usage.fallback_reasons {
            *self.reason_counts.entry(*reason).or_default() += 1;
        }
    }
}

#[derive(Default)]
struct RunUsage {
    capabilities: BTreeMap<FeatureUsageCapability, CapabilityUsage>,
    is_terminal: bool,
}

#[derive(Default)]
struct CapabilityUsage {
    direct: bool,
    fallback_reasons: BTreeSet<FeatureUsageReason>,
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Barrier},
        thread,
    };

    use serde_json::Value;
    use sha2::{Digest, Sha256};

    use super::{
        FeatureUsageCapability, FeatureUsageDropReason, FeatureUsagePath, FeatureUsageReason,
        FeatureUsageRegistry, RunFingerprint,
    };

    fn capability_snapshot(
        registry: &FeatureUsageRegistry,
        capability: FeatureUsageCapability,
    ) -> super::FeatureUsageCapabilitySnapshot {
        registry
            .snapshot()
            .capabilities
            .into_iter()
            .find(|snapshot| snapshot.capability == capability)
            .expect("all capabilities are present in every snapshot")
    }

    #[test]
    fn duplicate_observations_count_each_run_once() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        for _ in 0..3 {
            registry.record(
                "run-1",
                FeatureUsageCapability::VerificationRuntime,
                FeatureUsagePath::Direct,
            );
            registry.record(
                "run-1",
                FeatureUsageCapability::VerificationRuntime,
                FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
            );
        }

        let snapshot = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(snapshot.observed_runs, 1);
        assert_eq!(snapshot.direct_runs, 1);
        assert_eq!(snapshot.fallback_runs, 1);
        assert_eq!(snapshot.mixed_runs, 1);
        assert_eq!(snapshot.reason_counts.get(&FeatureUsageReason::RolloutDisabled), Some(&1));
    }

    #[test]
    fn production_registry_uses_bounded_4096_run_window() {
        let snapshot = FeatureUsageRegistry::new().snapshot();
        assert_eq!(snapshot.capacity, 4_096);
        assert_eq!(snapshot.terminal_fence_capacity, 4_096);
        assert_eq!(snapshot.retained_runs, 0);
        assert!(!snapshot.window_truncated);
    }

    #[test]
    fn invalid_run_identifiers_do_not_consume_window_capacity() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        let oversized = "x".repeat(129);
        for run_id in ["", " \t\r\n", oversized.as_str()] {
            registry.record(
                run_id,
                FeatureUsageCapability::VerificationRuntime,
                FeatureUsagePath::Direct,
            );
            registry.mark_terminal(run_id);
        }

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.retained_runs, 0);
        assert_eq!(snapshot.terminal_runs, 0);
        assert_eq!(snapshot.evicted_runs, 0);
        assert_eq!(snapshot.dropped_observations, 3);
        assert_eq!(
            snapshot.dropped_observation_reason_counts.get(&FeatureUsageDropReason::EmptyRunId),
            Some(&2)
        );
        assert_eq!(
            snapshot.dropped_observation_reason_counts.get(&FeatureUsageDropReason::RunIdTooLong),
            Some(&1)
        );
        let verification =
            capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(verification.dropped_observations, 3);

        let max_length_utf8_id = "\u{00E9}".repeat(64);
        registry.record(
            max_length_utf8_id.as_str(),
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        assert_eq!(registry.snapshot().retained_runs, 1);
    }

    #[test]
    fn invalid_terminal_identifiers_are_not_usage_observation_drops() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        registry.mark_terminal("");
        registry.mark_terminal(" ");
        registry.mark_terminal("x".repeat(129).as_str());

        let snapshot = registry.snapshot();
        assert_eq!(snapshot.retained_runs, 0);
        assert_eq!(snapshot.retained_terminal_fences, 0);
        assert_eq!(snapshot.dropped_observations, 0);
    }

    #[test]
    fn terminal_counts_distinguish_direct_fallback_and_mixed_runs() {
        let registry = FeatureUsageRegistry::with_capacity(8);
        registry.record(
            "direct-terminal",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("direct-terminal");
        registry.record(
            "fallback-terminal",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.mark_terminal("fallback-terminal");
        registry.record(
            "mixed-terminal",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "mixed-terminal",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.mark_terminal("mixed-terminal");
        registry.record(
            "direct-active",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );

        let snapshot = capability_snapshot(&registry, FeatureUsageCapability::CompactionSafeguard);
        assert_eq!(snapshot.observed_runs, 4);
        assert_eq!(snapshot.direct_runs, 3);
        assert_eq!(snapshot.fallback_runs, 2);
        assert_eq!(snapshot.mixed_runs, 1);
        assert_eq!(snapshot.terminal_observed_runs, 3);
        assert_eq!(snapshot.terminal_direct_runs, 2);
        assert_eq!(snapshot.terminal_fallback_runs, 2);
        assert_eq!(snapshot.terminal_mixed_runs, 1);
        assert_eq!(snapshot.reason_counts.get(&FeatureUsageReason::RolloutDisabled), Some(&2));
    }

    #[test]
    fn terminalization_is_idempotent_and_freezes_evidence() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        registry.record(
            "terminal-run",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("terminal-run");
        registry.mark_terminal("terminal-run");
        registry.record(
            "terminal-run",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );

        let registry_snapshot = registry.snapshot();
        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(registry_snapshot.retained_runs, 1);
        assert_eq!(registry_snapshot.active_runs, 0);
        assert_eq!(registry_snapshot.terminal_runs, 1);
        assert_eq!(usage.direct_runs, 1);
        assert_eq!(usage.fallback_runs, 0);
        assert_eq!(usage.terminal_direct_runs, 1);
        assert_eq!(usage.dropped_observations, 1);
        assert_eq!(
            usage.dropped_observation_reason_counts.get(&FeatureUsageDropReason::AfterTerminal),
            Some(&1)
        );
    }

    #[test]
    fn terminal_before_first_observation_is_fenced_without_usage_counts() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        registry.mark_terminal("already-finished");
        registry.record(
            "already-finished",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );

        let registry_snapshot = registry.snapshot();
        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(registry_snapshot.retained_runs, 0);
        assert_eq!(registry_snapshot.terminal_runs, 0);
        assert_eq!(registry_snapshot.retained_terminal_fences, 1);
        assert_eq!(usage.observed_runs, 0);
        assert_eq!(usage.dropped_observations, 1);
        assert_eq!(
            usage.dropped_observation_reason_counts.get(&FeatureUsageDropReason::AfterTerminal),
            Some(&1)
        );
    }

    #[test]
    fn terminal_fence_is_bounded_fifo_without_truncating_usage_window() {
        let registry = FeatureUsageRegistry::with_capacity(2);
        registry.mark_terminal("terminal-oldest");
        registry.mark_terminal("terminal-newer");
        registry.mark_terminal("terminal-oldest");
        registry.mark_terminal("terminal-newest");

        let fence_snapshot = registry.snapshot();
        assert_eq!(fence_snapshot.retained_runs, 0);
        assert_eq!(fence_snapshot.retained_terminal_fences, 2);
        assert!(fence_snapshot.terminal_fence_window_truncated);
        assert_eq!(fence_snapshot.evicted_terminal_fences, 1);
        assert!(!fence_snapshot.window_truncated);
        assert_eq!(fence_snapshot.evicted_runs, 0);

        registry.record(
            "terminal-oldest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "terminal-newer",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "terminal-newest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );

        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(usage.observed_runs, 1, "only the evicted oldest fence may admit evidence");
        assert_eq!(usage.dropped_observations, 2);
    }

    #[test]
    fn fifo_capacity_evicts_oldest_first_observed_run() {
        let registry = FeatureUsageRegistry::with_capacity(2);
        registry.record(
            "oldest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.record(
            "newer",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "oldest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.record(
            "newest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );

        let registry_snapshot = registry.snapshot();
        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(registry_snapshot.capacity, 2);
        assert_eq!(registry_snapshot.retained_runs, 2);
        assert!(registry_snapshot.window_truncated);
        assert_eq!(registry_snapshot.evicted_runs, 1);
        assert_eq!(usage.observed_runs, 2);
        assert_eq!(usage.active_runs, 2);
        assert_eq!(usage.direct_runs, 2);
        assert_eq!(usage.fallback_runs, 0);
        assert!(usage.window_truncated);
        assert_eq!(usage.evicted_runs, 1);
        assert!(usage.reason_counts.is_empty());
    }

    #[test]
    fn capability_eviction_does_not_truncate_unaffected_capability() {
        let registry = FeatureUsageRegistry::with_capacity(2);
        registry.record(
            "verification-oldest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        registry.record(
            "compaction-retained",
            FeatureUsageCapability::CompactionSafeguard,
            FeatureUsagePath::Direct,
        );
        registry.mark_terminal("compaction-retained");
        registry.record(
            "verification-newest",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );

        let global = registry.snapshot();
        let verification =
            capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        let compaction =
            capability_snapshot(&registry, FeatureUsageCapability::CompactionSafeguard);
        assert!(global.window_truncated);
        assert_eq!(verification.evicted_runs, 1);
        assert!(verification.window_truncated);
        assert_eq!(compaction.evicted_runs, 0);
        assert!(!compaction.window_truncated);
        assert_eq!(compaction.active_runs, 0);
        assert_eq!(compaction.terminal_direct_runs, 1);
    }

    #[test]
    fn fallback_reason_counts_unique_retained_runs() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        for run_id in ["run-a", "run-b"] {
            registry.record(
                run_id,
                FeatureUsageCapability::VerificationRuntime,
                FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
            );
        }
        registry.record(
            "run-b",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );

        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(usage.reason_counts.get(&FeatureUsageReason::RolloutDisabled), Some(&2));
    }

    #[test]
    fn snapshot_serialization_never_contains_run_identifiers_or_fingerprints() {
        let registry = FeatureUsageRegistry::with_capacity(4);
        let raw_run_id = "secret-run-id-token-like-value";
        registry.record(
            raw_run_id,
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Fallback { reason: FeatureUsageReason::RolloutDisabled },
        );
        registry.mark_terminal(raw_run_id);

        let fingerprint =
            RunFingerprint::from_run_id(raw_run_id).expect("test run identifier should be valid");
        let fingerprint_hex = hex::encode(fingerprint.0);
        let serialized = serde_json::to_string(&registry.snapshot())
            .expect("feature usage snapshot should serialize");
        assert!(!serialized.contains(raw_run_id));
        assert!(!serialized.contains(fingerprint_hex.as_str()));

        let value: Value =
            serde_json::from_str(&serialized).expect("feature usage snapshot should be valid JSON");
        assert_eq!(value["observation_scope"], "process_local_retained_run_window");
        assert_eq!(value["resets_on_restart"], true);
        assert_eq!(value["capabilities"][0]["capability"], "verification_runtime");
        assert_eq!(value["capabilities"][0]["reason_counts"]["rollout_disabled"], 1);
    }

    #[test]
    fn run_fingerprints_are_deterministic_and_domain_separated() {
        let fingerprint =
            RunFingerprint::from_run_id("run-1").expect("test run identifier should be valid");
        assert!(RunFingerprint::from_run_id(" run-1 ") == Ok(fingerprint));
        assert!(RunFingerprint::from_run_id("run-1") == Ok(fingerprint));
        assert!(RunFingerprint::from_run_id("run-2") != Ok(fingerprint));

        let raw_digest: [u8; 32] = Sha256::digest(b"run-1").into();
        assert_ne!(fingerprint.0, raw_digest);
    }

    #[test]
    fn poisoned_mutex_recovers_without_losing_registry_availability() {
        let registry = Arc::new(FeatureUsageRegistry::with_capacity(4));
        let poisoning_registry = Arc::clone(&registry);
        let panic_result = thread::spawn(move || {
            let _state = poisoning_registry
                .state
                .lock()
                .expect("test registry mutex should initially be healthy");
            panic!("poison feature usage registry for recovery test");
        })
        .join();
        assert!(panic_result.is_err());

        registry.record(
            "recovered-run",
            FeatureUsageCapability::VerificationRuntime,
            FeatureUsagePath::Direct,
        );
        let snapshot = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(snapshot.observed_runs, 1);
        assert_eq!(snapshot.direct_runs, 1);
    }

    #[test]
    fn concurrent_record_and_terminal_calls_preserve_unique_counts() {
        const THREADS: usize = 16;
        let registry = Arc::new(FeatureUsageRegistry::with_capacity(THREADS * 2));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles = (0..THREADS)
            .map(|index| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let run_id = format!("concurrent-run-{index}");
                    barrier.wait();
                    registry.record(
                        run_id.as_str(),
                        FeatureUsageCapability::VerificationRuntime,
                        FeatureUsagePath::Direct,
                    );
                    registry.record(
                        run_id.as_str(),
                        FeatureUsageCapability::VerificationRuntime,
                        FeatureUsagePath::Direct,
                    );
                    registry.mark_terminal(run_id.as_str());
                    registry.mark_terminal(run_id.as_str());
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().expect("feature usage worker should not panic");
        }

        let registry_snapshot = registry.snapshot();
        let usage = capability_snapshot(&registry, FeatureUsageCapability::VerificationRuntime);
        assert_eq!(registry_snapshot.retained_runs, THREADS);
        assert_eq!(registry_snapshot.terminal_runs, THREADS);
        assert_eq!(registry_snapshot.evicted_runs, 0);
        assert_eq!(usage.observed_runs, THREADS);
        assert_eq!(usage.direct_runs, THREADS);
        assert_eq!(usage.terminal_direct_runs, THREADS);
    }

    #[test]
    fn enum_identifiers_match_serialized_diagnostics_values() {
        for capability in FeatureUsageCapability::ALL {
            let serialized = serde_json::to_value(capability)
                .expect("feature usage capability should serialize");
            assert_eq!(serialized, Value::String(capability.as_str().to_owned()));
        }
        let reason = FeatureUsageReason::RolloutDisabled;
        let serialized =
            serde_json::to_value(reason).expect("feature usage reason should serialize");
        assert_eq!(serialized, Value::String(reason.as_str().to_owned()));
        for reason in [
            FeatureUsageDropReason::EmptyRunId,
            FeatureUsageDropReason::RunIdTooLong,
            FeatureUsageDropReason::AfterTerminal,
        ] {
            let serialized =
                serde_json::to_value(reason).expect("feature usage drop reason should serialize");
            assert_eq!(serialized, Value::String(reason.as_str().to_owned()));
        }
    }
}
