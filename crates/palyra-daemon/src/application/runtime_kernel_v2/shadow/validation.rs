//! Validation and digest helpers for bounded shadow comparison artifacts.
//!
//! The parent module owns the observe-only contracts; this module keeps their
//! fail-closed validation rules and stable digest construction together.

use std::fmt;

use palyra_common::runtime_contracts::RuntimeErrorPhase;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    RuntimeDifferentialClassification, RuntimeDifferentialOutcome, RuntimeDifferentialReportV1,
    MAX_SHADOW_PHASE_PLAN_ENTRIES, MAX_SHADOW_SEMANTIC_LABEL_BYTES,
    RUNTIME_DIFFERENTIAL_SCHEMA_VERSION,
};

impl RuntimeDifferentialReportV1 {
    /// Returns the aggregate low-cardinality classification.
    #[must_use]
    pub(crate) const fn classification(&self) -> RuntimeDifferentialClassification {
        self.classification
    }

    /// Returns the stable diagnostics and metadata-trace reason code.
    #[must_use]
    pub(crate) fn reason_code(&self) -> &str {
        self.reason_code.as_str()
    }

    /// Returns the context-safety comparison outcome.
    #[must_use]
    pub(crate) const fn context_safety(&self) -> RuntimeDifferentialOutcome {
        self.context_safety
    }

    /// Returns whether this report must block promotion.
    #[must_use]
    pub(crate) const fn blocks_promotion(&self) -> bool {
        matches!(self.classification, RuntimeDifferentialClassification::InvariantViolation)
    }

    /// Validates fixed dimension semantics and aggregate classification.
    ///
    /// # Errors
    /// Returns [`RuntimeDifferentialError::InvalidReport`] when a serialized
    /// report uses an impossible outcome for a dimension or disagrees with its
    /// derived classification.
    pub(crate) fn validate(&self) -> Result<(), RuntimeDifferentialError> {
        if self.schema_version != RUNTIME_DIFFERENTIAL_SCHEMA_VERSION
            || self.reason_code != self.classification.as_reason_code()
            || !matches!(
                self.runtime_selection,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::RiskyDifference
            )
            || !matches!(
                self.context_segments,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::RiskyDifference
            )
            || !matches!(
                self.context_safety,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::InvariantViolation
            )
            || !matches!(
                self.token_budget,
                RuntimeDifferentialOutcome::Match
                    | RuntimeDifferentialOutcome::BenignDifference
                    | RuntimeDifferentialOutcome::RiskyDifference
            )
            || !matches!(
                self.tool_catalog,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::RiskyDifference
            )
            || !matches!(
                self.policy_input,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::InvariantViolation
            )
            || !matches!(
                self.phase_plan,
                RuntimeDifferentialOutcome::Match | RuntimeDifferentialOutcome::InvariantViolation
            )
            || self.classification != classify(self.outcomes())
        {
            return Err(RuntimeDifferentialError::InvalidReport);
        }
        Ok(())
    }

    const fn outcomes(&self) -> [RuntimeDifferentialOutcome; 7] {
        [
            self.runtime_selection,
            self.context_segments,
            self.context_safety,
            self.token_budget,
            self.tool_catalog,
            self.policy_input,
            self.phase_plan,
        ]
    }
}

pub(super) fn exact_or_risky(authoritative: &str, candidate: &str) -> RuntimeDifferentialOutcome {
    if authoritative == candidate {
        RuntimeDifferentialOutcome::Match
    } else {
        RuntimeDifferentialOutcome::RiskyDifference
    }
}

pub(super) fn classify(
    outcomes: [RuntimeDifferentialOutcome; 7],
) -> RuntimeDifferentialClassification {
    let mut index = 0;
    let mut classification = RuntimeDifferentialClassification::Expected;
    while index < outcomes.len() {
        let observed = match outcomes[index] {
            RuntimeDifferentialOutcome::Match => RuntimeDifferentialClassification::Expected,
            RuntimeDifferentialOutcome::BenignDifference => {
                RuntimeDifferentialClassification::Benign
            }
            RuntimeDifferentialOutcome::RiskyDifference => RuntimeDifferentialClassification::Risky,
            RuntimeDifferentialOutcome::InvariantViolation => {
                RuntimeDifferentialClassification::InvariantViolation
            }
        };
        if observed > classification {
            classification = observed;
        }
        index += 1;
    }
    classification
}

pub(super) fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub(super) fn is_bounded_shadow_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SHADOW_SEMANTIC_LABEL_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(super) fn digest_shadow_semantics(
    domain: &[u8],
    value: &impl Serialize,
) -> Result<String, RuntimeDifferentialError> {
    let encoded = serde_json::to_vec(value).map_err(|_| RuntimeDifferentialError::InvalidPlan)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(encoded);
    Ok(hex::encode(hasher.finalize()))
}

pub(super) fn deserialize_phase_plan<'de, D>(
    deserializer: D,
) -> Result<Vec<RuntimeErrorPhase>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct BoundedPhasePlanVisitor;

    impl<'de> serde::de::Visitor<'de> for BoundedPhasePlanVisitor {
        type Value = Vec<RuntimeErrorPhase>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a runtime phase plan with at most {MAX_SHADOW_PHASE_PLAN_ENTRIES} entries"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            if sequence.size_hint().is_some_and(|hint| hint > MAX_SHADOW_PHASE_PLAN_ENTRIES) {
                return Err(serde::de::Error::custom("runtime shadow phase plan is too large"));
            }
            let mut phases = Vec::with_capacity(
                sequence.size_hint().unwrap_or_default().min(MAX_SHADOW_PHASE_PLAN_ENTRIES),
            );
            while let Some(phase) = sequence.next_element()? {
                if phases.len() == MAX_SHADOW_PHASE_PLAN_ENTRIES {
                    return Err(serde::de::Error::custom("runtime shadow phase plan is too large"));
                }
                phases.push(phase);
            }
            Ok(phases)
        }
    }

    deserializer.deserialize_seq(BoundedPhasePlanVisitor)
}

/// Fail-closed shadow plan and report validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum RuntimeDifferentialError {
    /// A plan exceeded fixed bounds or contained a non-digest input.
    #[error("runtime shadow plan is invalid")]
    InvalidPlan,
    /// Capability and plan generations did not match.
    #[error("runtime shadow comparison generation mismatch")]
    GenerationMismatch,
    /// A serialized report contradicted its fixed dimension semantics.
    #[error("runtime differential report is invalid")]
    InvalidReport,
}
