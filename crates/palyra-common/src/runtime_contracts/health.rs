//! Shared health, quarantine, probing, and circuit-breaker contracts.
//!
//! Health degradation may reduce availability but never widens authority or
//! bypasses policy, approval, sandbox, or side-effect gates.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{RuntimeGeneration, RuntimeInstanceId, RuntimeLeaseId};

/// Schema version for [`RuntimeComponentHealthV1`].
pub const RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION: u32 = 1;
/// Schema version for [`HealthProbeLeaseV1`].
pub const HEALTH_PROBE_LEASE_SCHEMA_VERSION: u32 = 1;
/// Schema version for [`HealthProbeResult`].
pub const HEALTH_PROBE_RESULT_SCHEMA_VERSION: u32 = 1;
/// Schema version for [`HealthProbeSettlementV1`].
pub const HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION: u32 = 1;
/// Schema version for [`QuarantineClearRequest`].
pub const QUARANTINE_CLEAR_REQUEST_SCHEMA_VERSION: u32 = 1;

const MAX_RUNTIME_HEALTH_REASON_CODE_BYTES: usize = 128;

runtime_contract_enum! {
    /// Canonical managed-runtime health states.
    pub enum RuntimeHealthState {
        Healthy => "healthy",
        Degraded => "degraded",
        Cooldown => "cooldown",
        Quarantined => "quarantined",
        Disabled => "disabled",
        Probing => "probing"
    }
}

runtime_contract_enum! {
    /// Authority class used to ensure fallbacks never gain privileges.
    pub enum RuntimeAuthorityClass {
        ObserveOnly => "observe_only",
        ReadOnly => "read_only",
        ScopedMutation => "scoped_mutation",
        PrivilegedMutation => "privileged_mutation"
    }
}

impl RuntimeAuthorityClass {
    const fn rank(self) -> u8 {
        match self {
            Self::ObserveOnly => 0,
            Self::ReadOnly => 1,
            Self::ScopedMutation => 2,
            Self::PrivilegedMutation => 3,
        }
    }

    /// Returns whether `fallback` has no more authority than `self`.
    #[must_use]
    pub const fn permits_fallback(self, fallback: Self) -> bool {
        fallback.rank() <= self.rank()
    }
}

runtime_contract_enum! {
    /// Result of one non-mutating health probe.
    pub enum HealthProbeDisposition {
        Passed => "passed",
        Failed => "failed",
        Inconclusive => "inconclusive",
        DeniedMutatingProbe => "denied_mutating_probe"
    }
}

runtime_contract_enum! {
    /// Ordinary serving admission derived from durable component health.
    pub enum RuntimeOrdinaryAdmissionDecision {
        Allowed => "allowed",
        CooldownBlocked => "cooldown_blocked",
        ProbeRequired => "probe_required",
        Quarantined => "quarantined",
        Disabled => "disabled",
        ProbeInProgress => "probe_in_progress"
    }
}

runtime_contract_enum! {
    /// Probe-only admission derived from durable component health and exact lease authority.
    pub enum RuntimeProbeAdmissionDecision {
        AuthorizedNonMutating => "authorized_non_mutating",
        LeaseRequired => "lease_required",
        LeaseInactive => "lease_inactive",
        LeaseMismatch => "lease_mismatch",
        HealthNotProbing => "health_not_probing",
        Quarantined => "quarantined",
        Disabled => "disabled"
    }
}

/// Deterministic circuit-breaker policy for one component class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerPolicy {
    /// Failures required to open the breaker.
    pub strike_threshold: u32,
    /// Cooldown interval before a probe is allowed.
    pub cooldown_ms: u64,
    /// Maximum number of concurrent probes.
    pub max_probe_concurrency: u32,
    /// Whether security quarantine may clear by time alone; must be false.
    pub security_quarantine_auto_clear: bool,
}

impl CircuitBreakerPolicy {
    /// Validates bounded deterministic breaker policy.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidPolicy`] for empty/unbounded probes or
    /// automatic security-quarantine clearing.
    pub fn validate(&self) -> Result<(), RuntimeHealthError> {
        if self.strike_threshold == 0
            || self.cooldown_ms == 0
            || self.max_probe_concurrency != 1
            || self.security_quarantine_auto_clear
        {
            return Err(RuntimeHealthError::InvalidPolicy);
        }
        Ok(())
    }
}

/// Durable health projection for one managed component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeComponentHealthV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Component runtime identity.
    pub component_id: RuntimeInstanceId,
    /// Generation whose evidence produced this state.
    pub generation: RuntimeGeneration,
    /// Current state.
    pub state: RuntimeHealthState,
    /// Component authority class.
    pub authority_class: RuntimeAuthorityClass,
    /// Consecutive classified failures.
    pub strike_count: u32,
    /// Stable latest reason code.
    pub reason_code: String,
    /// First failure timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_failure_at_unix_ms: Option<i64>,
    /// Most recent failure timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_at_unix_ms: Option<i64>,
    /// Cooldown expiry; required in cooldown state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<i64>,
    /// Optional fallback component.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_component_id: Option<RuntimeInstanceId>,
    /// Fallback authority class for admission checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_authority_class: Option<RuntimeAuthorityClass>,
    /// Whether the state was created for a security risk.
    pub security_quarantine: bool,
    /// Circuit-breaker policy snapshot.
    pub policy: CircuitBreakerPolicy,
    /// Last update timestamp.
    pub updated_at_unix_ms: i64,
}

/// Durable single-flight authority for one bounded, non-mutating health probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthProbeLeaseV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Host-issued lease identity.
    pub lease_id: RuntimeLeaseId,
    /// Component that may be probed.
    pub component_id: RuntimeInstanceId,
    /// Exact component generation the probe may affect.
    pub expected_generation: RuntimeGeneration,
    /// Authority-class snapshot binding the lease to the current component contract.
    pub authority_class: RuntimeAuthorityClass,
    /// Lease issue timestamp.
    pub issued_at_unix_ms: i64,
    /// Lease expiry timestamp.
    pub expires_at_unix_ms: i64,
    /// Explicit non-mutating posture. This field never grants mutation authority.
    pub non_mutating: bool,
}

impl HealthProbeLeaseV1 {
    /// Validates schema, timing, and non-mutating posture.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidProbeLease`] for malformed or mutating leases.
    pub fn validate(&self) -> Result<(), RuntimeHealthError> {
        if self.schema_version != HEALTH_PROBE_LEASE_SCHEMA_VERSION
            || self.issued_at_unix_ms < 0
            || self.expires_at_unix_ms <= self.issued_at_unix_ms
            || !self.non_mutating
        {
            return Err(RuntimeHealthError::InvalidProbeLease);
        }
        Ok(())
    }

    /// Returns whether this lease is active and exactly bound to `health`.
    #[must_use]
    pub fn is_active_for(&self, health: &RuntimeComponentHealthV1, now_unix_ms: i64) -> bool {
        self.validate().is_ok()
            && self.component_id == health.component_id
            && self.expected_generation == health.generation
            && self.authority_class == health.authority_class
            && self.issued_at_unix_ms <= now_unix_ms
            && now_unix_ms < self.expires_at_unix_ms
    }
}

impl RuntimeComponentHealthV1 {
    /// Validates state, timestamp, and fallback authority invariants.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError`] for malformed state or authority escalation.
    pub fn validate(&self) -> Result<(), RuntimeHealthError> {
        self.policy.validate()?;
        if self.schema_version != RUNTIME_COMPONENT_HEALTH_SCHEMA_VERSION
            || !is_bounded_reason_code(self.reason_code.as_str())
            || self.updated_at_unix_ms < 0
            || self.first_failure_at_unix_ms.is_some_and(|value| value < 0)
            || self.last_failure_at_unix_ms.is_some_and(|value| value < 0)
            || self.expires_at_unix_ms.is_some_and(|value| value < 0)
            || (self.state == RuntimeHealthState::Cooldown && self.expires_at_unix_ms.is_none())
            || (self.state != RuntimeHealthState::Cooldown && self.expires_at_unix_ms.is_some())
            || self
                .first_failure_at_unix_ms
                .is_some_and(|first| self.last_failure_at_unix_ms.is_some_and(|last| first > last))
            || self.last_failure_at_unix_ms.is_some_and(|last| last > self.updated_at_unix_ms)
            || self.fallback_component_id.is_some() != self.fallback_authority_class.is_some()
        {
            return Err(RuntimeHealthError::InvalidRecord);
        }
        if let Some(fallback) = self.fallback_authority_class {
            if !self.authority_class.permits_fallback(fallback) {
                return Err(RuntimeHealthError::FallbackAuthorityEscalation);
            }
        }
        Ok(())
    }

    /// Validates a state transition before durable persistence.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::ProbeRequired`] for direct quarantine recovery,
    /// or [`RuntimeHealthError::InvalidTransition`] for unsupported state changes.
    pub fn can_transition_to(&self, next: RuntimeHealthState) -> Result<(), RuntimeHealthError> {
        if self.state == next {
            return Ok(());
        }
        if self.state == RuntimeHealthState::Quarantined
            && (next == RuntimeHealthState::Healthy
                || (self.security_quarantine && next == RuntimeHealthState::Probing))
        {
            return Err(RuntimeHealthError::ProbeRequired);
        }
        let allowed = matches!(
            (self.state, next),
            (RuntimeHealthState::Healthy, RuntimeHealthState::Degraded)
                | (RuntimeHealthState::Healthy, RuntimeHealthState::Cooldown)
                | (RuntimeHealthState::Healthy, RuntimeHealthState::Disabled)
                | (RuntimeHealthState::Degraded, RuntimeHealthState::Cooldown)
                | (RuntimeHealthState::Degraded, RuntimeHealthState::Quarantined)
                | (RuntimeHealthState::Degraded, RuntimeHealthState::Healthy)
                | (RuntimeHealthState::Cooldown, RuntimeHealthState::Probing)
                | (RuntimeHealthState::Cooldown, RuntimeHealthState::Quarantined)
                | (RuntimeHealthState::Probing, RuntimeHealthState::Healthy)
                | (RuntimeHealthState::Probing, RuntimeHealthState::Degraded)
                | (RuntimeHealthState::Probing, RuntimeHealthState::Quarantined)
                | (RuntimeHealthState::Quarantined, RuntimeHealthState::Probing)
                | (_, RuntimeHealthState::Disabled)
        );
        if allowed {
            Ok(())
        } else {
            Err(RuntimeHealthError::InvalidTransition)
        }
    }

    /// Derives fail-closed ordinary serving admission without consulting probe authority.
    #[must_use]
    pub fn ordinary_admission_decision(
        &self,
        now_unix_ms: i64,
    ) -> RuntimeOrdinaryAdmissionDecision {
        if self.security_quarantine {
            return RuntimeOrdinaryAdmissionDecision::Quarantined;
        }
        match self.state {
            RuntimeHealthState::Healthy | RuntimeHealthState::Degraded => {
                RuntimeOrdinaryAdmissionDecision::Allowed
            }
            RuntimeHealthState::Cooldown => {
                if self.expires_at_unix_ms.is_some_and(|expiry| now_unix_ms >= expiry) {
                    RuntimeOrdinaryAdmissionDecision::ProbeRequired
                } else {
                    RuntimeOrdinaryAdmissionDecision::CooldownBlocked
                }
            }
            RuntimeHealthState::Quarantined => RuntimeOrdinaryAdmissionDecision::Quarantined,
            RuntimeHealthState::Disabled => RuntimeOrdinaryAdmissionDecision::Disabled,
            RuntimeHealthState::Probing => RuntimeOrdinaryAdmissionDecision::ProbeInProgress,
        }
    }

    /// Derives probe-only admission from durable health and exact lease authority.
    #[must_use]
    pub fn probe_admission_decision(
        &self,
        now_unix_ms: i64,
        probe_lease: Option<&HealthProbeLeaseV1>,
    ) -> RuntimeProbeAdmissionDecision {
        if self.state == RuntimeHealthState::Disabled {
            return RuntimeProbeAdmissionDecision::Disabled;
        }
        if self.state != RuntimeHealthState::Probing {
            return if self.state == RuntimeHealthState::Quarantined || self.security_quarantine {
                RuntimeProbeAdmissionDecision::Quarantined
            } else {
                RuntimeProbeAdmissionDecision::HealthNotProbing
            };
        }
        let Some(lease) = probe_lease else {
            return RuntimeProbeAdmissionDecision::LeaseRequired;
        };
        if lease.validate().is_err()
            || lease.component_id != self.component_id
            || lease.expected_generation != self.generation
            || lease.authority_class != self.authority_class
        {
            return RuntimeProbeAdmissionDecision::LeaseMismatch;
        }
        if lease.issued_at_unix_ms > now_unix_ms || now_unix_ms >= lease.expires_at_unix_ms {
            return RuntimeProbeAdmissionDecision::LeaseInactive;
        }
        RuntimeProbeAdmissionDecision::AuthorizedNonMutating
    }
}

/// Result of an authority-bounded, non-mutating probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthProbeResult {
    /// Contract schema version.
    pub schema_version: u32,
    /// Component that was probed.
    pub component_id: RuntimeInstanceId,
    /// Probe disposition.
    pub disposition: HealthProbeDisposition,
    /// Stable reason code.
    pub reason_code: String,
    /// Whether the probe attempted mutation; must be false to pass.
    pub mutation_attempted: bool,
    /// Completion timestamp.
    pub completed_at_unix_ms: i64,
}

impl HealthProbeResult {
    /// Validates the independently versioned probe result.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidProbeResult`] for malformed timestamps,
    /// reason codes, or disposition/mutation combinations.
    pub fn validate(&self) -> Result<(), RuntimeHealthError> {
        let disposition_matches_mutation = match self.disposition {
            HealthProbeDisposition::Passed
            | HealthProbeDisposition::Failed
            | HealthProbeDisposition::Inconclusive => !self.mutation_attempted,
            HealthProbeDisposition::DeniedMutatingProbe => self.mutation_attempted,
        };
        if self.schema_version != HEALTH_PROBE_RESULT_SCHEMA_VERSION
            || !is_bounded_reason_code(self.reason_code.as_str())
            || self.completed_at_unix_ms < 0
            || !disposition_matches_mutation
        {
            return Err(RuntimeHealthError::InvalidProbeResult);
        }
        Ok(())
    }

    /// Returns whether this result can clear a non-security quarantine.
    #[must_use]
    pub fn can_clear_quarantine(&self, security_quarantine: bool) -> bool {
        !security_quarantine
            && self.validate().is_ok()
            && self.disposition == HealthProbeDisposition::Passed
    }
}

/// Exact durable settlement request for one health-probe lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthProbeSettlementV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Exact host-issued lease being settled.
    pub lease_id: RuntimeLeaseId,
    /// Exact component generation the result may affect.
    pub expected_generation: RuntimeGeneration,
    /// Result produced by the designated non-mutating probe.
    pub result: HealthProbeResult,
}

impl HealthProbeSettlementV1 {
    /// Returns the canonical settlement for this lease.
    ///
    /// A non-mutating result completed at or after expiry is preserved with its
    /// actual completion timestamp but cannot be a late success or definitive
    /// failure. Mutation-denial evidence retains its stronger security posture.
    #[must_use]
    pub fn normalized_for_lease(&self, lease: &HealthProbeLeaseV1) -> Self {
        let mut normalized = self.clone();
        if !normalized.result.mutation_attempted
            && normalized.result.completed_at_unix_ms >= lease.expires_at_unix_ms
        {
            normalized.result.disposition = HealthProbeDisposition::Inconclusive;
            normalized.result.reason_code =
                "runtime.health.probe_completed_after_expiry".to_owned();
        }
        normalized
    }

    /// Validates exact lease, component, generation, authority, and timing binding.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidProbeSettlement`] when the settlement
    /// is stale, mismatched, malformed, or not bound to a probing component.
    pub fn validate_for(
        &self,
        lease: &HealthProbeLeaseV1,
        health: &RuntimeComponentHealthV1,
    ) -> Result<(), RuntimeHealthError> {
        let normalized = self.normalized_for_lease(lease);
        normalized.result.validate()?;
        if normalized.schema_version != HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION
            || normalized.lease_id != lease.lease_id
            || normalized.expected_generation != lease.expected_generation
            || normalized.result.component_id != lease.component_id
            || health.state != RuntimeHealthState::Probing
            || lease.component_id != health.component_id
            || lease.expected_generation != health.generation
            || lease.authority_class != health.authority_class
            || normalized.result.completed_at_unix_ms < lease.issued_at_unix_ms
        {
            return Err(RuntimeHealthError::InvalidProbeSettlement);
        }
        lease.validate()?;
        Ok(())
    }

    /// Returns the fail-closed durable state and security posture for this result.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidProbeSettlement`] when exact settlement
    /// validation fails.
    pub fn resulting_posture(
        &self,
        lease: &HealthProbeLeaseV1,
        health: &RuntimeComponentHealthV1,
    ) -> Result<(RuntimeHealthState, bool), RuntimeHealthError> {
        self.validate_for(lease, health)?;
        Ok(match self.normalized_for_lease(lease).result.disposition {
            HealthProbeDisposition::Passed => (RuntimeHealthState::Healthy, false),
            HealthProbeDisposition::Failed | HealthProbeDisposition::Inconclusive => {
                (RuntimeHealthState::Quarantined, health.security_quarantine)
            }
            HealthProbeDisposition::DeniedMutatingProbe => (RuntimeHealthState::Quarantined, true),
        })
    }
}

/// Operator request to clear quarantine after independent authorization review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuarantineClearRequest {
    /// Contract schema version.
    pub schema_version: u32,
    /// Component being cleared.
    pub component_id: RuntimeInstanceId,
    /// Exact generation whose quarantine evidence was reviewed.
    pub expected_generation: RuntimeGeneration,
    /// Principal hash or stable operator identity.
    pub actor_id: String,
    /// Stable reason code.
    pub reason_code: String,
    /// Digest of the independent policy or authorization decision.
    pub authorization_evidence_sha256: String,
    /// Optional matching bounded probe lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_lease: Option<HealthProbeLeaseV1>,
    /// Optional successful probe evidence digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_evidence_sha256: Option<String>,
}

impl QuarantineClearRequest {
    /// Validates bounded actor/reason data, authorization evidence, and probe binding.
    ///
    /// # Errors
    /// Returns [`RuntimeHealthError::InvalidQuarantineClearRequest`] when evidence is
    /// missing, malformed, or not bound to the requested component generation.
    pub fn validate(&self) -> Result<(), RuntimeHealthError> {
        let paired_probe_evidence =
            self.probe_lease.is_some() == self.probe_evidence_sha256.is_some();
        if self.schema_version != QUARANTINE_CLEAR_REQUEST_SCHEMA_VERSION
            || self.actor_id.trim().is_empty()
            || self.actor_id.len() > 128
            || self.reason_code.trim().is_empty()
            || self.reason_code.len() > 128
            || !is_sha256(self.authorization_evidence_sha256.as_str())
            || self.probe_evidence_sha256.as_deref().is_some_and(|digest| !is_sha256(digest))
            || !paired_probe_evidence
        {
            return Err(RuntimeHealthError::InvalidQuarantineClearRequest);
        }
        if let Some(lease) = self.probe_lease.as_ref() {
            lease.validate()?;
            if lease.component_id != self.component_id
                || lease.expected_generation != self.expected_generation
            {
                return Err(RuntimeHealthError::InvalidQuarantineClearRequest);
            }
        }
        Ok(())
    }
}

/// Shared health validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeHealthError {
    /// Circuit-breaker policy is malformed.
    #[error("runtime health policy is invalid")]
    InvalidPolicy,
    /// Health projection is malformed.
    #[error("runtime health record is invalid")]
    InvalidRecord,
    /// Fallback would increase authority.
    #[error("runtime health fallback would increase authority")]
    FallbackAuthorityEscalation,
    /// Quarantine requires a probe or authorized operator action.
    #[error("runtime quarantine requires probe or operator action")]
    ProbeRequired,
    /// Health transition is not part of the state machine.
    #[error("runtime health transition is invalid")]
    InvalidTransition,
    /// Probe lease is malformed or could authorize mutation.
    #[error("runtime health probe lease is invalid")]
    InvalidProbeLease,
    /// Probe result is malformed or contradicts its mutation posture.
    #[error("runtime health probe result is invalid")]
    InvalidProbeResult,
    /// Probe settlement is stale, late, or not bound to exact durable authority.
    #[error("runtime health probe settlement is invalid")]
    InvalidProbeSettlement,
    /// Operator quarantine-clear evidence is malformed or mismatched.
    #[error("runtime quarantine clear request is invalid")]
    InvalidQuarantineClearRequest,
}

fn is_bounded_reason_code(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_RUNTIME_HEALTH_REASON_CODE_BYTES
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn health(state: RuntimeHealthState) -> RuntimeComponentHealthV1 {
        RuntimeComponentHealthV1 {
            schema_version: 1,
            component_id: RuntimeInstanceId::parse("provider_route_01").expect("component id"),
            generation: RuntimeGeneration::new(1).expect("generation"),
            state,
            authority_class: RuntimeAuthorityClass::ScopedMutation,
            strike_count: 3,
            reason_code: "runtime.health.test".to_owned(),
            first_failure_at_unix_ms: Some(1),
            last_failure_at_unix_ms: Some(2),
            expires_at_unix_ms: (state == RuntimeHealthState::Cooldown).then_some(100),
            fallback_component_id: None,
            fallback_authority_class: None,
            security_quarantine: false,
            policy: CircuitBreakerPolicy {
                strike_threshold: 3,
                cooldown_ms: 1_000,
                max_probe_concurrency: 1,
                security_quarantine_auto_clear: false,
            },
            updated_at_unix_ms: 2,
        }
    }

    #[test]
    fn threshold_failure_can_move_healthy_directly_to_cooldown() {
        assert_eq!(
            health(RuntimeHealthState::Healthy).can_transition_to(RuntimeHealthState::Cooldown),
            Ok(())
        );
    }

    #[test]
    fn quarantine_cannot_jump_directly_to_healthy() {
        assert_eq!(
            health(RuntimeHealthState::Quarantined).can_transition_to(RuntimeHealthState::Healthy),
            Err(RuntimeHealthError::ProbeRequired)
        );
    }

    #[test]
    fn fallback_cannot_increase_authority() {
        let mut record = health(RuntimeHealthState::Degraded);
        record.authority_class = RuntimeAuthorityClass::ReadOnly;
        record.fallback_component_id = Some(
            RuntimeInstanceId::parse("provider_route_fallback").expect("fallback component id"),
        );
        record.fallback_authority_class = Some(RuntimeAuthorityClass::ScopedMutation);
        assert_eq!(record.validate(), Err(RuntimeHealthError::FallbackAuthorityEscalation));
    }

    fn probe_lease(record: &RuntimeComponentHealthV1) -> HealthProbeLeaseV1 {
        HealthProbeLeaseV1 {
            schema_version: 1,
            lease_id: RuntimeLeaseId::parse("probe_lease_01").expect("lease id"),
            component_id: record.component_id.clone(),
            expected_generation: record.generation,
            authority_class: record.authority_class,
            issued_at_unix_ms: 10,
            expires_at_unix_ms: 20,
            non_mutating: true,
        }
    }

    #[test]
    fn probing_requires_exact_active_non_mutating_lease() {
        let record = health(RuntimeHealthState::Probing);
        assert_eq!(
            record.ordinary_admission_decision(15),
            RuntimeOrdinaryAdmissionDecision::ProbeInProgress
        );
        assert_eq!(
            record.probe_admission_decision(15, None),
            RuntimeProbeAdmissionDecision::LeaseRequired
        );
        let mut lease = probe_lease(&record);
        assert_eq!(
            record.probe_admission_decision(15, Some(&lease)),
            RuntimeProbeAdmissionDecision::AuthorizedNonMutating
        );
        lease.non_mutating = false;
        assert_eq!(lease.validate(), Err(RuntimeHealthError::InvalidProbeLease));
        assert_eq!(
            record.probe_admission_decision(15, Some(&lease)),
            RuntimeProbeAdmissionDecision::LeaseMismatch
        );
    }

    #[test]
    fn cooldown_boundary_requires_probe_and_security_quarantine_stays_blocked() {
        let mut record = health(RuntimeHealthState::Cooldown);
        assert_eq!(
            record.ordinary_admission_decision(99),
            RuntimeOrdinaryAdmissionDecision::CooldownBlocked
        );
        assert_eq!(
            record.ordinary_admission_decision(100),
            RuntimeOrdinaryAdmissionDecision::ProbeRequired
        );
        record.security_quarantine = true;
        assert_eq!(
            record.ordinary_admission_decision(100),
            RuntimeOrdinaryAdmissionDecision::Quarantined
        );
    }

    #[test]
    fn security_quarantine_cannot_auto_enter_probe_or_clear_from_probe_result() {
        let mut record = health(RuntimeHealthState::Quarantined);
        record.security_quarantine = true;
        assert_eq!(
            record.can_transition_to(RuntimeHealthState::Probing),
            Err(RuntimeHealthError::ProbeRequired)
        );
        let result = HealthProbeResult {
            schema_version: 1,
            component_id: record.component_id,
            disposition: HealthProbeDisposition::Passed,
            reason_code: "runtime.health.probe_passed".to_owned(),
            mutation_attempted: false,
            completed_at_unix_ms: 20,
        };
        assert!(!result.can_clear_quarantine(true));
        assert!(result.can_clear_quarantine(false));
    }

    #[test]
    fn probe_settlement_requires_exact_in_window_authority() {
        let record = health(RuntimeHealthState::Probing);
        let lease = probe_lease(&record);
        let mut settlement = HealthProbeSettlementV1 {
            schema_version: HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
            lease_id: lease.lease_id.clone(),
            expected_generation: record.generation,
            result: HealthProbeResult {
                schema_version: HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                component_id: record.component_id.clone(),
                disposition: HealthProbeDisposition::Passed,
                reason_code: "runtime.health.probe_passed".to_owned(),
                mutation_attempted: false,
                completed_at_unix_ms: 15,
            },
        };
        assert_eq!(settlement.validate_for(&lease, &record), Ok(()));
        assert_eq!(
            settlement.resulting_posture(&lease, &record),
            Ok((RuntimeHealthState::Healthy, false))
        );

        settlement.result.completed_at_unix_ms = lease.expires_at_unix_ms;
        assert_eq!(settlement.validate_for(&lease, &record), Ok(()));
        let normalized = settlement.normalized_for_lease(&lease);
        assert_eq!(normalized.result.disposition, HealthProbeDisposition::Inconclusive);
        assert_eq!(normalized.result.reason_code, "runtime.health.probe_completed_after_expiry");
        assert_eq!(normalized.result.completed_at_unix_ms, lease.expires_at_unix_ms);
        assert_eq!(
            settlement.resulting_posture(&lease, &record),
            Ok((RuntimeHealthState::Quarantined, false))
        );
    }

    #[test]
    fn mutating_probe_is_security_quarantined() {
        let record = health(RuntimeHealthState::Probing);
        let lease = probe_lease(&record);
        let settlement = HealthProbeSettlementV1 {
            schema_version: HEALTH_PROBE_SETTLEMENT_SCHEMA_VERSION,
            lease_id: lease.lease_id.clone(),
            expected_generation: record.generation,
            result: HealthProbeResult {
                schema_version: HEALTH_PROBE_RESULT_SCHEMA_VERSION,
                component_id: record.component_id.clone(),
                disposition: HealthProbeDisposition::DeniedMutatingProbe,
                reason_code: "runtime.health.probe_mutation_denied".to_owned(),
                mutation_attempted: true,
                completed_at_unix_ms: 15,
            },
        };
        assert_eq!(
            settlement.resulting_posture(&lease, &record),
            Ok((RuntimeHealthState::Quarantined, true))
        );
    }

    #[test]
    fn quarantine_clear_requires_authorization_and_paired_probe_evidence() {
        let record = health(RuntimeHealthState::Quarantined);
        let mut request = QuarantineClearRequest {
            schema_version: 1,
            component_id: record.component_id.clone(),
            expected_generation: record.generation,
            actor_id: "operator_sha256".to_owned(),
            reason_code: "runtime.health.operator_clear".to_owned(),
            authorization_evidence_sha256: "a".repeat(64),
            probe_lease: None,
            probe_evidence_sha256: None,
        };
        assert_eq!(request.validate(), Ok(()));
        request.probe_evidence_sha256 = Some("b".repeat(64));
        assert_eq!(request.validate(), Err(RuntimeHealthError::InvalidQuarantineClearRequest));
        request.probe_lease = Some(probe_lease(&record));
        assert_eq!(request.validate(), Ok(()));
        request.authorization_evidence_sha256 = "invalid".to_owned();
        assert_eq!(request.validate(), Err(RuntimeHealthError::InvalidQuarantineClearRequest));
    }
}
