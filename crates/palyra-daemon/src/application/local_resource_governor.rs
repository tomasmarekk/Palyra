//! Durable local admission and pressure policy for host runtime resources.
//! The governor accounts leases and selects revocation candidates, but never
//! owns process, PTY, socket, or service handles.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const RESOURCE_LEASE_SCHEMA_VERSION: u32 = 1;
const RESOURCE_REGISTRY_SCHEMA_VERSION: u32 = 2;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 512;
const MAX_PRESSURE_ACTIONS: usize = 32;

/// Counted resource dimensions governed as one atomic allocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceUnitsV1 {
    /// Operating-system process slots.
    pub processes: u64,
    /// Estimated resident memory bytes.
    pub memory_bytes: u64,
    /// File-descriptor or handle slots.
    pub file_descriptors: u64,
    /// Socket slots.
    pub sockets: u64,
    /// Durable and resident spool bytes.
    pub spool_bytes: u64,
    /// Service-specific concurrent operation slots.
    pub concurrency: u64,
}

impl ResourceUnitsV1 {
    /// Returns whether every dimension is zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.processes == 0
            && self.memory_bytes == 0
            && self.file_descriptors == 0
            && self.sockets == 0
            && self.spool_bytes == 0
            && self.concurrency == 0
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            processes: self.processes.checked_add(other.processes)?,
            memory_bytes: self.memory_bytes.checked_add(other.memory_bytes)?,
            file_descriptors: self.file_descriptors.checked_add(other.file_descriptors)?,
            sockets: self.sockets.checked_add(other.sockets)?,
            spool_bytes: self.spool_bytes.checked_add(other.spool_bytes)?,
            concurrency: self.concurrency.checked_add(other.concurrency)?,
        })
    }

    fn saturating_sub(self, other: Self) -> Self {
        Self {
            processes: self.processes.saturating_sub(other.processes),
            memory_bytes: self.memory_bytes.saturating_sub(other.memory_bytes),
            file_descriptors: self.file_descriptors.saturating_sub(other.file_descriptors),
            sockets: self.sockets.saturating_sub(other.sockets),
            spool_bytes: self.spool_bytes.saturating_sub(other.spool_bytes),
            concurrency: self.concurrency.saturating_sub(other.concurrency),
        }
    }

    fn fits_within(self, limit: Self) -> bool {
        self.processes <= limit.processes
            && self.memory_bytes <= limit.memory_bytes
            && self.file_descriptors <= limit.file_descriptors
            && self.sockets <= limit.sockets
            && self.spool_bytes <= limit.spool_bytes
            && self.concurrency <= limit.concurrency
    }
}

/// Service category charged by a resource lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceServiceKind {
    /// Generic supervised child process.
    Process,
    /// Container runtime process admitted through an isolated backend.
    Container,
    /// Interactive pseudoterminal.
    Pty,
    /// Language server workspace.
    Lsp,
    /// Model Context Protocol server.
    Mcp,
    /// Managed external harness or ACP runtime.
    ExternalRuntime,
    /// Git worktree mutation or snapshot operation.
    Worktree,
    /// Host-authoritative WorkGraph worker execution.
    WorkGraph,
}

/// Retention priority used only for pressure planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePriority {
    /// User-attended terminal or equivalent interactive operation.
    Interactive,
    /// Foreground task whose completion gates the active run.
    Foreground,
    /// Warm service that can be restarted, such as an idle LSP or MCP server.
    IdleService,
    /// Speculative or parallel fanout work.
    BackgroundFanout,
}

/// Durable lifecycle state for a resource allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLeaseState {
    /// Capacity remains charged.
    Active,
    /// Pressure policy revoked capacity from the owner.
    Revoked,
    /// The owner released capacity normally.
    Released,
    /// Restart or deadline reconciliation expired capacity.
    Expired,
}

impl ResourceLeaseState {
    const fn is_active(self) -> bool {
        matches!(self, Self::Active)
    }
}

/// Durable resource allocation metadata without any OS-handle authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLeaseV1 {
    /// Lease schema version.
    pub schema_version: u32,
    /// Host-issued lease identity.
    pub lease_id: String,
    /// Owning session or service identity.
    pub owner_id: String,
    /// Owning process/runtime generation for CAS operations.
    pub generation: u64,
    /// Service charged by this lease.
    pub service: ResourceServiceKind,
    /// Pressure-retention priority.
    pub priority: ResourcePriority,
    /// Atomic resource grant.
    pub granted: ResourceUnitsV1,
    /// Issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Expiry timestamp.
    pub expires_at_unix_ms: i64,
    /// Most recent state-change timestamp.
    pub updated_at_unix_ms: i64,
    /// Durable lease state.
    pub state: ResourceLeaseState,
    /// Stable state-transition reason.
    pub reason_code: String,
}

/// Admission request evaluated atomically across all resource dimensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLeaseRequestV1 {
    /// Owning session or service identity.
    pub owner_id: String,
    /// Owning runtime generation.
    pub generation: u64,
    /// Service requesting capacity.
    pub service: ResourceServiceKind,
    /// Pressure-retention priority.
    pub priority: ResourcePriority,
    /// Requested atomic resource units.
    pub requested: ResourceUnitsV1,
    /// Requested lease lifetime.
    pub duration: Duration,
}

/// Durable governor policy and storage location.
#[derive(Debug, Clone)]
pub struct LocalResourceGovernorConfig {
    /// Absolute registry file path.
    pub registry_path: PathBuf,
    /// Global resource ceiling.
    pub global_limit: ResourceUnitsV1,
    /// Per-owner resource ceiling.
    pub per_owner_limit: ResourceUnitsV1,
    /// Maximum retained active and terminal lease records.
    pub max_records: usize,
}

impl LocalResourceGovernorConfig {
    fn validate(&self) -> Result<(), LocalResourceGovernorError> {
        if !self.registry_path.is_absolute()
            || self.registry_path.file_name().is_none()
            || self.global_limit.is_zero()
            || self.per_owner_limit.is_zero()
            || self.max_records == 0
            || !self.per_owner_limit.fits_within(self.global_limit)
        {
            return Err(LocalResourceGovernorError::InvalidConfiguration);
        }
        Ok(())
    }
}

/// Point-in-time governor state for diagnostics and admission explanations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalResourceSnapshotV1 {
    /// Total capacity currently charged.
    pub used: ResourceUnitsV1,
    /// Configured global capacity.
    pub limit: ResourceUnitsV1,
    /// Number of active leases.
    pub active_leases: usize,
    /// Usage aggregated by owner.
    pub owner_usage: BTreeMap<String, ResourceUnitsV1>,
}

/// One deterministic pressure action proposed without mutating lease state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceEvictionDecisionV1 {
    /// Lease selected for revocation.
    pub lease_id: String,
    /// Redacted service owner identity.
    pub owner_id: String,
    /// Service class.
    pub service: ResourceServiceKind,
    /// Retention priority used by the ordering policy.
    pub priority: ResourcePriority,
    /// Capacity released if the owner acknowledges revocation.
    pub released: ResourceUnitsV1,
    /// Stable diagnostics reason.
    pub reason_code: String,
}

/// Bounded resource-pressure diagnostics and deterministic relief plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePressureSnapshotV1 {
    /// Snapshot schema version.
    pub schema_version: u32,
    /// Current resource accounting.
    pub resources: LocalResourceSnapshotV1,
    /// Additional capacity that triggered pressure evaluation.
    pub required: ResourceUnitsV1,
    /// Whether the request fits without revocation.
    pub capacity_available: bool,
    /// Ordered background-first eviction proposal.
    pub eviction_plan: Vec<ResourceEvictionDecisionV1>,
    /// Stable state reason.
    pub reason_code: String,
    /// Observation timestamp.
    pub observed_at_unix_ms: i64,
}

/// Result of one pressure-relief action attempted by a service coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePressureActionStateV1 {
    /// The selected owner acknowledged revocation and released capacity.
    Applied,
    /// The coordinator could not safely control the selected service.
    Skipped,
    /// The coordinator owned the service, but cleanup or persistence failed.
    Failed,
}

/// Durable, bounded evidence for one pressure-relief attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcePressureActionV1 {
    /// Action schema version.
    pub schema_version: u32,
    /// Deterministic eviction decision that triggered the attempt.
    pub decision: ResourceEvictionDecisionV1,
    /// Applied, skipped, or failed outcome.
    pub state: ResourcePressureActionStateV1,
    /// Stable outcome reason.
    pub reason_code: String,
    /// Observation timestamp.
    pub observed_at_unix_ms: i64,
}

/// Resource admission, persistence, or generation-fencing failure.
#[derive(Debug, Error)]
pub enum LocalResourceGovernorError {
    /// Configuration is unbounded, inconsistent, or not durable.
    #[error("local resource governor configuration is invalid")]
    InvalidConfiguration,
    /// Lease request is empty, unbounded, or malformed.
    #[error("resource lease request is invalid: {0}")]
    InvalidRequest(String),
    /// Global capacity cannot admit the request.
    #[error("global resource capacity is exhausted")]
    GlobalCapacityExhausted,
    /// The owner reached its configured capacity.
    #[error("per-owner resource capacity is exhausted")]
    OwnerCapacityExhausted,
    /// The bounded durable registry reached its record limit.
    #[error("resource lease registry is full")]
    RegistryFull,
    /// No matching lease exists.
    #[error("resource lease was not found")]
    LeaseNotFound,
    /// A stale generation attempted to mutate a live lease.
    #[error("resource lease generation does not match")]
    GenerationMismatch,
    /// The lease is already terminal.
    #[error("resource lease is not active")]
    LeaseNotActive,
    /// Durable registry access failed.
    #[error("resource governor persistence failed: {0}")]
    Persistence(String),
    /// In-memory governor state was poisoned by a panic.
    #[error("resource governor state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceRegistryV2 {
    schema_version: u32,
    updated_at_unix_ms: i64,
    leases: Vec<ResourceLeaseV1>,
    last_pressure: Option<ResourcePressureSnapshotV1>,
    pressure_actions: Vec<ResourcePressureActionV1>,
}

struct GovernorState {
    registry: ResourceRegistryV2,
    used: ResourceUnitsV1,
    owner_usage: HashMap<String, ResourceUnitsV1>,
}

/// Cloneable admission authority shared by processes, PTYs, LSPs, and worktrees.
#[derive(Clone)]
pub struct LocalResourceGovernor {
    config: LocalResourceGovernorConfig,
    state: Arc<Mutex<GovernorState>>,
}

impl LocalResourceGovernor {
    /// Opens durable state and expires any lease whose live authority cannot survive restart.
    ///
    /// # Errors
    /// Returns an error when configuration, decoding, or durable reconciliation fails.
    pub fn open(config: LocalResourceGovernorConfig) -> Result<Self, LocalResourceGovernorError> {
        config.validate()?;
        if let Some(parent) = config.registry_path.parent() {
            create_private_dir(parent)?;
        }
        let mut registry = if config.registry_path.exists() {
            read_registry(config.registry_path.as_path())?
        } else {
            ResourceRegistryV2 {
                schema_version: RESOURCE_REGISTRY_SCHEMA_VERSION,
                updated_at_unix_ms: unix_time_ms(),
                leases: Vec::new(),
                last_pressure: None,
                pressure_actions: Vec::new(),
            }
        };
        let now = unix_time_ms();
        let mut changed = false;
        for lease in &mut registry.leases {
            if lease.state.is_active() {
                lease.state = ResourceLeaseState::Expired;
                lease.reason_code = "resource.restart_requires_reacquire".to_owned();
                lease.updated_at_unix_ms = now;
                changed = true;
            }
        }
        registry.updated_at_unix_ms = now;
        trim_terminal_records(&mut registry, config.max_records);
        if changed || !config.registry_path.exists() {
            write_registry(config.registry_path.as_path(), &registry)?;
        }
        Ok(Self {
            config,
            state: Arc::new(Mutex::new(GovernorState {
                registry,
                used: ResourceUnitsV1::default(),
                owner_usage: HashMap::new(),
            })),
        })
    }

    /// Atomically admits a bounded resource request and persists the grant.
    ///
    /// # Errors
    /// Returns an error for malformed requests, exhausted capacity, or persistence failure.
    pub fn acquire(
        &self,
        request: ResourceLeaseRequestV1,
    ) -> Result<ResourceLeaseV1, LocalResourceGovernorError> {
        validate_request(&request)?;
        let mut state = self.lock_state()?;
        reap_expired(&self.config, &mut state)?;
        if state.registry.leases.len() >= self.config.max_records {
            trim_terminal_records(&mut state.registry, self.config.max_records.saturating_sub(1));
        }
        if state.registry.leases.len() >= self.config.max_records {
            return Err(LocalResourceGovernorError::RegistryFull);
        }
        let next_global = state
            .used
            .checked_add(request.requested)
            .ok_or(LocalResourceGovernorError::GlobalCapacityExhausted)?;
        if !next_global.fits_within(self.config.global_limit) {
            return Err(LocalResourceGovernorError::GlobalCapacityExhausted);
        }
        let owner_used =
            state.owner_usage.get(request.owner_id.as_str()).copied().unwrap_or_default();
        let next_owner = owner_used
            .checked_add(request.requested)
            .ok_or(LocalResourceGovernorError::OwnerCapacityExhausted)?;
        if !next_owner.fits_within(self.config.per_owner_limit) {
            return Err(LocalResourceGovernorError::OwnerCapacityExhausted);
        }
        let issued_at_unix_ms = unix_time_ms();
        let duration_ms = i64::try_from(request.duration.as_millis()).unwrap_or(i64::MAX);
        let lease = ResourceLeaseV1 {
            schema_version: RESOURCE_LEASE_SCHEMA_VERSION,
            lease_id: format!("resource_{}", ulid::Ulid::new()),
            owner_id: request.owner_id,
            generation: request.generation,
            service: request.service,
            priority: request.priority,
            granted: request.requested,
            issued_at_unix_ms,
            expires_at_unix_ms: issued_at_unix_ms.saturating_add(duration_ms),
            updated_at_unix_ms: issued_at_unix_ms,
            state: ResourceLeaseState::Active,
            reason_code: "resource.admitted".to_owned(),
        };
        state.used = next_global;
        state.owner_usage.insert(lease.owner_id.clone(), next_owner);
        state.registry.updated_at_unix_ms = issued_at_unix_ms;
        state.registry.leases.push(lease.clone());
        if let Err(error) = write_registry(self.config.registry_path.as_path(), &state.registry) {
            state.registry.leases.pop();
            state.used = state.used.saturating_sub(lease.granted);
            restore_owner_usage_after_release(&mut state, &lease);
            return Err(error);
        }
        Ok(lease)
    }

    /// Releases an active lease using exact generation comparison.
    ///
    /// # Errors
    /// Returns an error when the lease is absent, stale, terminal, or cannot be persisted.
    pub fn release(
        &self,
        lease_id: &str,
        generation: u64,
    ) -> Result<ResourceLeaseV1, LocalResourceGovernorError> {
        self.set_terminal_state(
            lease_id,
            generation,
            ResourceLeaseState::Released,
            "resource.released",
        )
    }

    /// Revokes an active lease selected by pressure policy.
    ///
    /// # Errors
    /// Returns an error when the lease is absent, stale, terminal, or cannot be persisted.
    pub fn revoke(
        &self,
        lease_id: &str,
        generation: u64,
        reason_code: &str,
    ) -> Result<ResourceLeaseV1, LocalResourceGovernorError> {
        validate_reason(reason_code)?;
        self.set_terminal_state(lease_id, generation, ResourceLeaseState::Revoked, reason_code)
    }

    /// Extends an active lease without changing its resource grant.
    ///
    /// # Errors
    /// Returns an error for stale, terminal, invalid, or non-durable renewal.
    pub fn renew(
        &self,
        lease_id: &str,
        generation: u64,
        duration: Duration,
    ) -> Result<ResourceLeaseV1, LocalResourceGovernorError> {
        if duration.is_zero() {
            return Err(LocalResourceGovernorError::InvalidRequest(
                "renewal duration must be non-zero".to_owned(),
            ));
        }
        let mut state = self.lock_state()?;
        reap_expired(&self.config, &mut state)?;
        let index = find_lease_index(&state, lease_id)?;
        validate_mutation(&state.registry.leases[index], generation)?;
        let previous = state.registry.leases[index].clone();
        let now = unix_time_ms();
        let duration_ms = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        state.registry.leases[index].expires_at_unix_ms = now.saturating_add(duration_ms);
        state.registry.leases[index].updated_at_unix_ms = now;
        state.registry.leases[index].reason_code = "resource.renewed".to_owned();
        state.registry.updated_at_unix_ms = now;
        if let Err(error) = write_registry(self.config.registry_path.as_path(), &state.registry) {
            state.registry.leases[index] = previous;
            return Err(error);
        }
        Ok(state.registry.leases[index].clone())
    }

    /// Plans least-disruptive revocations without changing state.
    ///
    /// Background fanout is selected before idle services, foreground work,
    /// and interactive PTYs. The returned sequence is deterministic.
    #[must_use]
    pub fn plan_pressure_relief(&self, required: ResourceUnitsV1) -> Vec<ResourceLeaseV1> {
        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        let Some(projected) = state.used.checked_add(required) else {
            return active_leases_by_eviction_order(&state);
        };
        if projected.fits_within(self.config.global_limit) {
            return Vec::new();
        }
        let mut selected = Vec::new();
        let mut remaining = state.used;
        for lease in active_leases_by_eviction_order(&state) {
            remaining = remaining.saturating_sub(lease.granted);
            selected.push(lease);
            if remaining
                .checked_add(required)
                .is_some_and(|usage| usage.fits_within(self.config.global_limit))
            {
                break;
            }
        }
        selected
    }

    /// Returns a bounded point-in-time accounting snapshot.
    #[must_use]
    pub fn snapshot(&self) -> LocalResourceSnapshotV1 {
        let Ok(state) = self.state.lock() else {
            return LocalResourceSnapshotV1 {
                used: ResourceUnitsV1::default(),
                limit: self.config.global_limit,
                active_leases: 0,
                owner_usage: BTreeMap::new(),
            };
        };
        LocalResourceSnapshotV1 {
            used: state.used,
            limit: self.config.global_limit,
            active_leases: state
                .registry
                .leases
                .iter()
                .filter(|lease| lease.state.is_active())
                .count(),
            owner_usage: state
                .owner_usage
                .iter()
                .map(|(owner, usage)| (owner.clone(), *usage))
                .collect(),
        }
    }

    /// Returns active lease metadata without exposing any OS authority.
    ///
    /// # Errors
    /// Returns an error when in-memory governor state is unavailable.
    pub fn active_leases(&self) -> Result<Vec<ResourceLeaseV1>, LocalResourceGovernorError> {
        Ok(self
            .lock_state()?
            .registry
            .leases
            .iter()
            .filter(|lease| lease.state.is_active())
            .cloned()
            .collect())
    }

    /// Produces fail-closed pressure diagnostics without revoking owners.
    ///
    /// # Errors
    /// Returns an error when the in-memory accounting state is unavailable.
    pub fn pressure_snapshot(
        &self,
        required: ResourceUnitsV1,
    ) -> Result<ResourcePressureSnapshotV1, LocalResourceGovernorError> {
        let mut state = self.lock_state()?;
        let capacity_available = state
            .used
            .checked_add(required)
            .is_some_and(|projected| projected.fits_within(self.config.global_limit));
        let eviction_plan = if capacity_available {
            Vec::new()
        } else {
            let mut remaining = state.used;
            let mut decisions = Vec::new();
            for lease in active_leases_by_eviction_order(&state) {
                remaining = remaining.saturating_sub(lease.granted);
                decisions.push(ResourceEvictionDecisionV1 {
                    lease_id: lease.lease_id,
                    owner_id: lease.owner_id,
                    service: lease.service,
                    priority: lease.priority,
                    released: lease.granted,
                    reason_code: "resource.pressure_revocation_proposed".to_owned(),
                });
                if remaining
                    .checked_add(required)
                    .is_some_and(|projected| projected.fits_within(self.config.global_limit))
                {
                    break;
                }
            }
            decisions
        };
        let snapshot = ResourcePressureSnapshotV1 {
            schema_version: 1,
            resources: LocalResourceSnapshotV1 {
                used: state.used,
                limit: self.config.global_limit,
                active_leases: state
                    .registry
                    .leases
                    .iter()
                    .filter(|lease| lease.state.is_active())
                    .count(),
                owner_usage: state
                    .owner_usage
                    .iter()
                    .map(|(owner, usage)| (owner.clone(), *usage))
                    .collect(),
            },
            required,
            capacity_available,
            eviction_plan,
            reason_code: if capacity_available {
                "resource.capacity_available".to_owned()
            } else {
                "resource.pressure_detected".to_owned()
            },
            observed_at_unix_ms: unix_time_ms(),
        };
        if !snapshot.capacity_available {
            let previous = state.registry.last_pressure.replace(snapshot.clone());
            let previous_updated_at_unix_ms = state.registry.updated_at_unix_ms;
            state.registry.updated_at_unix_ms = snapshot.observed_at_unix_ms;
            if let Err(error) = write_registry(self.config.registry_path.as_path(), &state.registry)
            {
                state.registry.last_pressure = previous;
                state.registry.updated_at_unix_ms = previous_updated_at_unix_ms;
                return Err(error);
            }
        }
        Ok(snapshot)
    }

    /// Records one bounded pressure-relief outcome for operator diagnostics.
    ///
    /// # Errors
    /// Returns an error for malformed reasons or failed durable persistence.
    pub fn record_pressure_action(
        &self,
        decision: ResourceEvictionDecisionV1,
        state_value: ResourcePressureActionStateV1,
        reason_code: &str,
    ) -> Result<ResourcePressureActionV1, LocalResourceGovernorError> {
        validate_reason(reason_code)?;
        let mut state = self.lock_state()?;
        let action = ResourcePressureActionV1 {
            schema_version: 1,
            decision,
            state: state_value,
            reason_code: bounded_reason(reason_code),
            observed_at_unix_ms: unix_time_ms(),
        };
        let previous_actions = state.registry.pressure_actions.clone();
        let previous_updated_at_unix_ms = state.registry.updated_at_unix_ms;
        state.registry.pressure_actions.push(action.clone());
        let excess = state.registry.pressure_actions.len().saturating_sub(MAX_PRESSURE_ACTIONS);
        if excess > 0 {
            state.registry.pressure_actions.drain(..excess);
        }
        state.registry.updated_at_unix_ms = action.observed_at_unix_ms;
        if let Err(error) = write_registry(self.config.registry_path.as_path(), &state.registry) {
            state.registry.pressure_actions = previous_actions;
            state.registry.updated_at_unix_ms = previous_updated_at_unix_ms;
            return Err(error);
        }
        Ok(action)
    }

    /// Returns the most recent pressure evaluation and bounded action evidence.
    ///
    /// # Errors
    /// Returns an error when in-memory governor state is unavailable.
    pub fn pressure_evidence(
        &self,
    ) -> Result<
        (Option<ResourcePressureSnapshotV1>, Vec<ResourcePressureActionV1>),
        LocalResourceGovernorError,
    > {
        let state = self.lock_state()?;
        Ok((state.registry.last_pressure.clone(), state.registry.pressure_actions.clone()))
    }

    fn set_terminal_state(
        &self,
        lease_id: &str,
        generation: u64,
        terminal_state: ResourceLeaseState,
        reason_code: &str,
    ) -> Result<ResourceLeaseV1, LocalResourceGovernorError> {
        let mut state = self.lock_state()?;
        reap_expired(&self.config, &mut state)?;
        let index = find_lease_index(&state, lease_id)?;
        validate_mutation(&state.registry.leases[index], generation)?;
        let previous = state.registry.leases[index].clone();
        let now = unix_time_ms();
        state.registry.leases[index].state = terminal_state;
        state.registry.leases[index].reason_code = bounded_reason(reason_code);
        state.registry.leases[index].updated_at_unix_ms = now;
        state.registry.updated_at_unix_ms = now;
        state.used = state.used.saturating_sub(previous.granted);
        restore_owner_usage_after_release(&mut state, &previous);
        if let Err(error) = write_registry(self.config.registry_path.as_path(), &state.registry) {
            state.registry.leases[index] = previous.clone();
            state.used =
                state.used.checked_add(previous.granted).unwrap_or(self.config.global_limit);
            let owner = state.owner_usage.entry(previous.owner_id.clone()).or_default();
            *owner = owner.checked_add(previous.granted).unwrap_or(self.config.per_owner_limit);
            return Err(error);
        }
        Ok(state.registry.leases[index].clone())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, GovernorState>, LocalResourceGovernorError> {
        self.state.lock().map_err(|_| LocalResourceGovernorError::StateUnavailable)
    }
}

fn validate_request(request: &ResourceLeaseRequestV1) -> Result<(), LocalResourceGovernorError> {
    if request.owner_id.trim().is_empty()
        || request.owner_id.len() > MAX_IDENTITY_BYTES
        || request.owner_id.chars().any(char::is_control)
        || request.generation == 0
        || request.requested.is_zero()
        || request.duration.is_zero()
    {
        return Err(LocalResourceGovernorError::InvalidRequest(
            "owner, generation, units, and duration must be bounded and non-zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_reason(reason_code: &str) -> Result<(), LocalResourceGovernorError> {
    if reason_code.trim().is_empty()
        || reason_code.len() > MAX_REASON_BYTES
        || reason_code.chars().any(char::is_control)
    {
        return Err(LocalResourceGovernorError::InvalidRequest(
            "reason code must be non-empty, bounded, and free of control characters".to_owned(),
        ));
    }
    Ok(())
}

fn find_lease_index(
    state: &GovernorState,
    lease_id: &str,
) -> Result<usize, LocalResourceGovernorError> {
    state
        .registry
        .leases
        .iter()
        .position(|lease| lease.lease_id == lease_id)
        .ok_or(LocalResourceGovernorError::LeaseNotFound)
}

fn validate_mutation(
    lease: &ResourceLeaseV1,
    generation: u64,
) -> Result<(), LocalResourceGovernorError> {
    if lease.generation != generation {
        return Err(LocalResourceGovernorError::GenerationMismatch);
    }
    if !lease.state.is_active() {
        return Err(LocalResourceGovernorError::LeaseNotActive);
    }
    Ok(())
}

fn restore_owner_usage_after_release(state: &mut GovernorState, lease: &ResourceLeaseV1) {
    if let Some(owner) = state.owner_usage.get_mut(lease.owner_id.as_str()) {
        *owner = owner.saturating_sub(lease.granted);
        if owner.is_zero() {
            state.owner_usage.remove(lease.owner_id.as_str());
        }
    }
}

fn active_leases_by_eviction_order(state: &GovernorState) -> Vec<ResourceLeaseV1> {
    let mut leases = state
        .registry
        .leases
        .iter()
        .filter(|lease| lease.state.is_active())
        .cloned()
        .collect::<Vec<_>>();
    leases.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.issued_at_unix_ms.cmp(&right.issued_at_unix_ms))
            .then_with(|| left.lease_id.cmp(&right.lease_id))
    });
    leases
}

fn reap_expired(
    config: &LocalResourceGovernorConfig,
    state: &mut GovernorState,
) -> Result<(), LocalResourceGovernorError> {
    let now = unix_time_ms();
    let expired = state
        .registry
        .leases
        .iter()
        .enumerate()
        .filter_map(|(index, lease)| {
            (lease.state.is_active() && lease.expires_at_unix_ms <= now).then_some(index)
        })
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return Ok(());
    }
    let previous = state.registry.clone();
    for index in expired {
        let lease = state.registry.leases[index].clone();
        state.registry.leases[index].state = ResourceLeaseState::Expired;
        state.registry.leases[index].reason_code = "resource.lease_expired".to_owned();
        state.registry.leases[index].updated_at_unix_ms = now;
        state.used = state.used.saturating_sub(lease.granted);
        restore_owner_usage_after_release(state, &lease);
    }
    state.registry.updated_at_unix_ms = now;
    if let Err(error) = write_registry(config.registry_path.as_path(), &state.registry) {
        state.registry = previous;
        rebuild_usage(state);
        return Err(error);
    }
    Ok(())
}

fn rebuild_usage(state: &mut GovernorState) {
    state.used = ResourceUnitsV1::default();
    state.owner_usage.clear();
    for lease in state.registry.leases.iter().filter(|lease| lease.state.is_active()) {
        state.used = state.used.checked_add(lease.granted).unwrap_or(ResourceUnitsV1 {
            processes: u64::MAX,
            memory_bytes: u64::MAX,
            file_descriptors: u64::MAX,
            sockets: u64::MAX,
            spool_bytes: u64::MAX,
            concurrency: u64::MAX,
        });
        let owner = state.owner_usage.entry(lease.owner_id.clone()).or_default();
        *owner = owner.checked_add(lease.granted).unwrap_or(ResourceUnitsV1 {
            processes: u64::MAX,
            memory_bytes: u64::MAX,
            file_descriptors: u64::MAX,
            sockets: u64::MAX,
            spool_bytes: u64::MAX,
            concurrency: u64::MAX,
        });
    }
}

fn trim_terminal_records(registry: &mut ResourceRegistryV2, target_len: usize) {
    if registry.leases.len() <= target_len {
        return;
    }
    let mut active =
        registry.leases.iter().filter(|lease| lease.state.is_active()).cloned().collect::<Vec<_>>();
    let mut terminal = registry
        .leases
        .iter()
        .filter(|lease| !lease.state.is_active())
        .cloned()
        .collect::<Vec<_>>();
    terminal.sort_by(|left, right| {
        right
            .updated_at_unix_ms
            .cmp(&left.updated_at_unix_ms)
            .then_with(|| right.lease_id.cmp(&left.lease_id))
    });
    terminal.truncate(target_len.saturating_sub(active.len()));
    active.extend(terminal);
    registry.leases = active;
}

fn read_registry(path: &Path) -> Result<ResourceRegistryV2, LocalResourceGovernorError> {
    let bytes = fs::read(path)
        .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    let registry = serde_json::from_slice::<ResourceRegistryV2>(bytes.as_slice())
        .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    if registry.schema_version != RESOURCE_REGISTRY_SCHEMA_VERSION {
        return Err(LocalResourceGovernorError::Persistence(
            "unsupported resource registry schema".to_owned(),
        ));
    }
    Ok(registry)
}

fn write_registry(
    path: &Path,
    registry: &ResourceRegistryV2,
) -> Result<(), LocalResourceGovernorError> {
    let payload = serde_json::to_vec_pretty(registry)
        .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    atomic_replace(path, payload.as_slice())
}

fn atomic_replace(path: &Path, payload: &[u8]) -> Result<(), LocalResourceGovernorError> {
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);
    fs::write(temporary_path.as_path(), payload)
        .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    harden_file(temporary_path.as_path())?;
    if let Err(rename_error) = fs::rename(temporary_path.as_path(), path) {
        if !path.is_file() {
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(LocalResourceGovernorError::Persistence(rename_error.to_string()));
        }
        // Preserve a rollback copy because Windows rename cannot replace an
        // existing open registry in one operation.
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, rollback_path.as_path())
            .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
        if let Err(install_error) = fs::rename(temporary_path.as_path(), path) {
            let _ = fs::rename(rollback_path.as_path(), path);
            let _ = fs::remove_file(temporary_path.as_path());
            return Err(LocalResourceGovernorError::Persistence(install_error.to_string()));
        }
        let _ = fs::remove_file(rollback_path);
    }
    harden_file(path)
}

fn create_private_dir(path: &Path) -> Result<(), LocalResourceGovernorError> {
    fs::create_dir_all(path)
        .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    }
    Ok(())
}

fn harden_file(path: &Path) -> Result<(), LocalResourceGovernorError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| LocalResourceGovernorError::Persistence(error.to_string()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn bounded_reason(value: &str) -> String {
    value.chars().take(MAX_REASON_BYTES).collect()
}

fn unix_time_ms() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(value: u64) -> ResourceUnitsV1 {
        ResourceUnitsV1 {
            processes: value,
            memory_bytes: value * 1_024,
            file_descriptors: value * 2,
            sockets: value,
            spool_bytes: value * 4_096,
            concurrency: value,
        }
    }

    fn config(path: &Path) -> LocalResourceGovernorConfig {
        LocalResourceGovernorConfig {
            registry_path: path.join("governor").join("leases.json"),
            global_limit: units(10),
            per_owner_limit: units(6),
            max_records: 32,
        }
    }

    fn request(
        owner_id: &str,
        generation: u64,
        priority: ResourcePriority,
        requested: ResourceUnitsV1,
    ) -> ResourceLeaseRequestV1 {
        ResourceLeaseRequestV1 {
            owner_id: owner_id.to_owned(),
            generation,
            service: ResourceServiceKind::Process,
            priority,
            requested,
            duration: Duration::from_secs(30),
        }
    }

    #[test]
    fn admission_is_atomic_across_global_and_owner_limits() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        governor
            .acquire(request("owner-a", 1, ResourcePriority::Foreground, units(6)))
            .expect("first lease");
        let owner_error = governor
            .acquire(request("owner-a", 2, ResourcePriority::Foreground, units(1)))
            .expect_err("owner limit");
        assert!(matches!(owner_error, LocalResourceGovernorError::OwnerCapacityExhausted));
        governor
            .acquire(request("owner-b", 1, ResourcePriority::Foreground, units(4)))
            .expect("second owner");
        let global_error = governor
            .acquire(request("owner-c", 1, ResourcePriority::Foreground, units(1)))
            .expect_err("global limit");
        assert!(matches!(global_error, LocalResourceGovernorError::GlobalCapacityExhausted));
        assert_eq!(governor.snapshot().used, units(10));
    }

    #[test]
    fn stale_generation_cannot_release_replacement_lease() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        let lease = governor
            .acquire(request("owner-a", 7, ResourcePriority::Foreground, units(1)))
            .expect("lease");
        let error = governor.release(lease.lease_id.as_str(), 6).expect_err("stale release");
        assert!(matches!(error, LocalResourceGovernorError::GenerationMismatch));
        assert_eq!(governor.snapshot().active_leases, 1);
        governor.release(lease.lease_id.as_str(), 7).expect("release");
        assert_eq!(governor.snapshot().active_leases, 0);
    }

    #[test]
    fn pressure_plan_preserves_interactive_pty_until_last() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        let mut background = request("fanout", 1, ResourcePriority::BackgroundFanout, units(2));
        background.service = ResourceServiceKind::Process;
        governor.acquire(background).expect("background");
        let mut idle = request("lsp", 1, ResourcePriority::IdleService, units(2));
        idle.service = ResourceServiceKind::Lsp;
        governor.acquire(idle).expect("idle lsp");
        let mut interactive = request("terminal", 1, ResourcePriority::Interactive, units(2));
        interactive.service = ResourceServiceKind::Pty;
        governor.acquire(interactive).expect("interactive pty");
        let plan = governor.plan_pressure_relief(units(8));
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].priority, ResourcePriority::BackgroundFanout);
        assert_eq!(plan[1].priority, ResourcePriority::IdleService);
        assert!(plan.iter().all(|lease| lease.service != ResourceServiceKind::Pty));
    }

    #[test]
    fn pressure_snapshot_exposes_background_first_memory_fd_and_spool_relief() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        let mut background = request("fanout", 1, ResourcePriority::BackgroundFanout, units(3));
        background.service = ResourceServiceKind::Process;
        governor.acquire(background).expect("background lease");
        let mut idle = request("lsp", 1, ResourcePriority::IdleService, units(3));
        idle.service = ResourceServiceKind::Lsp;
        governor.acquire(idle).expect("idle lease");
        let snapshot = governor.pressure_snapshot(units(5)).expect("pressure snapshot");
        assert!(!snapshot.capacity_available);
        assert_eq!(snapshot.reason_code, "resource.pressure_detected");
        assert_eq!(snapshot.eviction_plan[0].priority, ResourcePriority::BackgroundFanout);
        assert!(snapshot.eviction_plan[0].released.memory_bytes > 0);
        assert!(snapshot.eviction_plan[0].released.file_descriptors > 0);
        assert!(snapshot.eviction_plan[0].released.spool_bytes > 0);
    }

    #[test]
    fn pressure_action_evidence_is_bounded_and_survives_restart() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        let mut idle = request("lsp-owner", 7, ResourcePriority::IdleService, units(6));
        idle.service = ResourceServiceKind::Lsp;
        governor.acquire(idle).expect("idle lease");
        let pressure = governor.pressure_snapshot(units(5)).expect("pressure snapshot");
        let decision = pressure.eviction_plan[0].clone();
        governor
            .record_pressure_action(
                decision.clone(),
                ResourcePressureActionStateV1::Applied,
                "resource.lsp_eviction_applied",
            )
            .expect("record action");
        let (last_pressure, actions) = governor.pressure_evidence().expect("pressure evidence");
        assert_eq!(last_pressure.expect("last pressure"), pressure);
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].decision, decision);
        drop(governor);

        let reopened = LocalResourceGovernor::open(config(temp.path())).expect("reopen governor");
        let (last_pressure, actions) =
            reopened.pressure_evidence().expect("reopened pressure evidence");
        assert!(last_pressure.is_some());
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].state, ResourcePressureActionStateV1::Applied);
    }

    #[test]
    fn restart_expires_accounting_without_claiming_live_authority() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        governor
            .acquire(request("owner-a", 1, ResourcePriority::Foreground, units(2)))
            .expect("lease");
        drop(governor);
        let reopened = LocalResourceGovernor::open(config(temp.path())).expect("reopen governor");
        let snapshot = reopened.snapshot();
        assert_eq!(snapshot.active_leases, 0);
        assert!(snapshot.used.is_zero());
        let registry =
            read_registry(config(temp.path()).registry_path.as_path()).expect("read registry");
        assert_eq!(registry.leases[0].state, ResourceLeaseState::Expired);
        assert_eq!(registry.leases[0].reason_code, "resource.restart_requires_reacquire");
    }

    #[test]
    fn expiry_releases_every_resource_dimension() {
        let temp = tempfile::tempdir().expect("temp dir");
        let governor = LocalResourceGovernor::open(config(temp.path())).expect("open governor");
        let mut short = request("owner-a", 1, ResourcePriority::Foreground, units(2));
        short.duration = Duration::from_millis(1);
        governor.acquire(short).expect("lease");
        std::thread::sleep(Duration::from_millis(5));
        governor
            .acquire(request("owner-b", 1, ResourcePriority::Foreground, units(1)))
            .expect("reap and acquire");
        assert_eq!(governor.snapshot().used, units(1));
    }

    #[test]
    fn registry_rejects_unknown_versions_and_fields_without_rewrite() {
        let mutators: [fn(&mut serde_json::Value); 2] = [
            |value| value["schema_version"] = serde_json::json!(999),
            |value| value["unknown_registry_field"] = serde_json::json!(true),
        ];
        for mutate in mutators {
            let temp = tempfile::tempdir().expect("temp dir");
            let registry_path = config(temp.path()).registry_path;
            drop(
                LocalResourceGovernor::open(config(temp.path())).expect("create resource registry"),
            );
            let mut value: serde_json::Value = serde_json::from_slice(
                fs::read(registry_path.as_path()).expect("read registry").as_slice(),
            )
            .expect("decode registry");
            mutate(&mut value);
            let bytes = serde_json::to_vec_pretty(&value).expect("encode invalid registry");
            fs::write(registry_path.as_path(), bytes.as_slice()).expect("write invalid registry");

            let error = match LocalResourceGovernor::open(config(temp.path())) {
                Ok(_) => panic!("invalid registry was accepted"),
                Err(error) => error,
            };
            assert!(matches!(error, LocalResourceGovernorError::Persistence(_)));
            assert_eq!(fs::read(registry_path.as_path()).expect("read unchanged registry"), bytes);
        }
    }
}
