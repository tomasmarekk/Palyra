//! Runtime write generations and stale-callback disposition contracts.
//!
//! Generations are host-issued monotonic values. A superseded generation may
//! contribute redacted forensic evidence but can never mutate current state.

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{RuntimeLeaseId, RuntimeRunId, RuntimeSessionId};

/// Schema version for generation lease and transition records.
pub const RUNTIME_GENERATION_SCHEMA_VERSION: u32 = 1;

/// Positive host-issued write generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RuntimeGeneration(u64);

impl<'de> Deserialize<'de> for RuntimeGeneration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl RuntimeGeneration {
    /// Creates a non-zero runtime generation.
    ///
    /// # Errors
    /// Returns [`RuntimeGenerationError::Zero`] when `value` is zero.
    pub const fn new(value: u64) -> Result<Self, RuntimeGenerationError> {
        if value == 0 {
            Err(RuntimeGenerationError::Zero)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the generation number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next generation without wrapping.
    ///
    /// # Errors
    /// Returns [`RuntimeGenerationError::Exhausted`] at `u64::MAX`.
    pub const fn next(self) -> Result<Self, RuntimeGenerationError> {
        match self.0.checked_add(1) {
            Some(value) => Ok(Self(value)),
            None => Err(RuntimeGenerationError::Exhausted),
        }
    }
}

impl fmt::Display for RuntimeGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Generation construction and transition error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeGenerationError {
    /// Generation zero is reserved for missing legacy data.
    #[error("runtime generation must be greater than zero")]
    Zero,
    /// No later generation can be represented.
    #[error("runtime generation space is exhausted")]
    Exhausted,
    /// The observed generation did not match the active generation.
    #[error("stale runtime generation: expected {expected}, observed {observed}")]
    Stale {
        /// Active host generation.
        expected: RuntimeGeneration,
        /// Generation carried by the callback.
        observed: RuntimeGeneration,
    },
    /// A transition skipped or repeated a generation.
    #[error("generation transition must advance exactly once")]
    InvalidTransition,
}

runtime_contract_enum! {
    /// Independent session write lanes, each with at most one active generation.
    pub enum RuntimeGenerationLane {
        Run => "run",
        Harness => "harness",
        Provider => "provider",
        Tool => "tool",
        Plugin => "plugin",
        Worker => "worker",
        Process => "process",
        Mcp => "mcp",
        Delivery => "delivery"
    }
}

runtime_contract_enum! {
    /// Reason an active generation was replaced.
    pub enum RuntimeGenerationTransitionKind {
        Activated => "activated",
        SteerSuperseded => "steer_superseded",
        ModelSwitchSuperseded => "model_switch_superseded",
        CrashContinuation => "crash_continuation",
        RecoverySuperseded => "recovery_superseded",
        Cancelled => "cancelled",
        Released => "released"
    }
}

runtime_contract_enum! {
    /// Forensic handling of a stale external event.
    pub enum StaleEventDisposition {
        PersistedDiagnostic => "persisted_diagnostic",
        DroppedBeforeParse => "dropped_before_parse",
        Rejected => "rejected"
    }
}

runtime_contract_enum! {
    /// Result of checking a callback generation against the active lease.
    pub enum GenerationCheckDisposition {
        Current => "current",
        Stale => "stale",
        MissingActiveGeneration => "missing_active_generation"
    }
}

/// Active generation lease for one session lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationLeaseV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Lease identity.
    pub lease_id: RuntimeLeaseId,
    /// Owning session.
    pub session_id: RuntimeSessionId,
    /// Owning run, when the lane is run-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<RuntimeRunId>,
    /// Independent write lane.
    pub lane: RuntimeGenerationLane,
    /// Current generation.
    pub generation: RuntimeGeneration,
    /// Owner label, never a raw secret or payload.
    pub owner: String,
    /// Acquisition timestamp.
    pub acquired_at_unix_ms: i64,
    /// Lease expiry timestamp.
    pub expires_at_unix_ms: i64,
}

impl GenerationLeaseV1 {
    /// Validates lease timing and owner fields.
    ///
    /// # Errors
    /// Returns [`RuntimeGenerationError::InvalidTransition`] for malformed timing or owner data.
    pub fn validate(&self) -> Result<(), RuntimeGenerationError> {
        if self.schema_version != RUNTIME_GENERATION_SCHEMA_VERSION
            || self.owner.trim().is_empty()
            || self.acquired_at_unix_ms < 0
            || self.expires_at_unix_ms <= self.acquired_at_unix_ms
        {
            return Err(RuntimeGenerationError::InvalidTransition);
        }
        Ok(())
    }

    /// Checks whether a callback belongs to this active generation.
    #[must_use]
    pub fn check(&self, observed: RuntimeGeneration) -> GenerationCheckOutcome {
        let disposition = if observed == self.generation {
            GenerationCheckDisposition::Current
        } else {
            GenerationCheckDisposition::Stale
        };
        GenerationCheckOutcome {
            schema_version: 1,
            expected: Some(self.generation),
            observed,
            disposition,
            reason_code: if disposition == GenerationCheckDisposition::Current {
                "runtime.generation.current"
            } else {
                "runtime.generation.stale_suppressed"
            }
            .to_owned(),
        }
    }
}

/// Atomic transition between generation owners.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationTransitionV1 {
    /// Contract schema version.
    pub schema_version: u32,
    /// Owning session.
    pub session_id: RuntimeSessionId,
    /// Lane being transitioned.
    pub lane: RuntimeGenerationLane,
    /// Previously active generation, absent on first activation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_generation: Option<RuntimeGeneration>,
    /// Newly active generation, absent on release/cancel closure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_generation: Option<RuntimeGeneration>,
    /// Transition reason.
    pub kind: RuntimeGenerationTransitionKind,
    /// Stable reason code.
    pub reason_code: String,
    /// Transition timestamp.
    pub occurred_at_unix_ms: i64,
}

impl GenerationTransitionV1 {
    /// Validates monotonic activation and explicit closure transitions.
    ///
    /// # Errors
    /// Returns [`RuntimeGenerationError::InvalidTransition`] for skipped, repeated, or
    /// structurally invalid transitions.
    pub fn validate(&self) -> Result<(), RuntimeGenerationError> {
        if self.schema_version != RUNTIME_GENERATION_SCHEMA_VERSION
            || self.reason_code.trim().is_empty()
            || self.occurred_at_unix_ms < 0
        {
            return Err(RuntimeGenerationError::InvalidTransition);
        }
        match (self.from_generation, self.to_generation, self.kind) {
            (None, Some(_), RuntimeGenerationTransitionKind::Activated) => Ok(()),
            (Some(from), Some(to), kind)
                if !matches!(
                    kind,
                    RuntimeGenerationTransitionKind::Activated
                        | RuntimeGenerationTransitionKind::Cancelled
                        | RuntimeGenerationTransitionKind::Released
                ) && from.next()? == to =>
            {
                Ok(())
            }
            (Some(_), None, RuntimeGenerationTransitionKind::Cancelled)
            | (Some(_), None, RuntimeGenerationTransitionKind::Released) => Ok(()),
            _ => Err(RuntimeGenerationError::InvalidTransition),
        }
    }
}

/// Result of one expected-generation check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationCheckOutcome {
    /// Projection schema version.
    pub schema_version: u32,
    /// Active generation, absent when no lane is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<RuntimeGeneration>,
    /// Generation carried by the callback.
    pub observed: RuntimeGeneration,
    /// Check result.
    pub disposition: GenerationCheckDisposition,
    /// Stable reason code.
    pub reason_code: String,
}

impl GenerationCheckOutcome {
    /// Returns whether the callback may mutate state.
    #[must_use]
    pub const fn permits_mutation(&self) -> bool {
        matches!(self.disposition, GenerationCheckDisposition::Current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_is_positive_and_monotonic() {
        assert_eq!(RuntimeGeneration::new(0), Err(RuntimeGenerationError::Zero));
        assert!(serde_json::from_str::<RuntimeGeneration>("0").is_err());
        let first = RuntimeGeneration::new(1).expect("generation should validate");
        assert_eq!(first.next().expect("next generation").get(), 2);
    }

    #[test]
    fn transition_requires_single_step_activation() {
        let transition = GenerationTransitionV1 {
            schema_version: 1,
            session_id: RuntimeSessionId::parse("session_01").expect("session id"),
            lane: RuntimeGenerationLane::Run,
            from_generation: Some(RuntimeGeneration::new(1).expect("generation")),
            to_generation: Some(RuntimeGeneration::new(3).expect("generation")),
            kind: RuntimeGenerationTransitionKind::CrashContinuation,
            reason_code: "runtime.generation.crash_continuation".to_owned(),
            occurred_at_unix_ms: 42,
        };
        assert_eq!(transition.validate(), Err(RuntimeGenerationError::InvalidTransition));
    }

    #[test]
    fn stale_generation_has_no_mutation_authority() {
        let lease = GenerationLeaseV1 {
            schema_version: 1,
            lease_id: RuntimeLeaseId::parse("lease_01").expect("lease id"),
            session_id: RuntimeSessionId::parse("session_01").expect("session id"),
            run_id: Some(RuntimeRunId::parse("run_01").expect("run id")),
            lane: RuntimeGenerationLane::Provider,
            generation: RuntimeGeneration::new(2).expect("generation"),
            owner: "provider_attempt".to_owned(),
            acquired_at_unix_ms: 10,
            expires_at_unix_ms: 20,
        };
        assert!(!lease.check(RuntimeGeneration::new(1).expect("generation")).permits_mutation());
    }
}
