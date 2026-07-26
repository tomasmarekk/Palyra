//! Host-owned context compaction plans, quality gates, and read-only tools.
//!
//! The context engine may propose work, but this module retains the only
//! compaction lease and validates protected segments, catalog epoch, and
//! realized token savings before the host reports a successful projection.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
};

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::application::tool_registry::ModelVisibleToolCatalogSnapshot;

pub(crate) const CONTEXT_COMPACTION_PLAN_SCHEMA_VERSION: u32 = 2;
pub(crate) const CONTEXT_INSPECT_TOOL_NAME: &str = "palyra.context.inspect";
const CONTEXT_TOOL_SCHEMA_VERSION: u32 = 1;

/// Authority permitted to request compaction; the host remains the only writer.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextCompactionOwner {
    Host,
    Engine,
}

/// Segment classes that every accepted plan must preserve.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextProtectedSegment {
    SystemInstructions,
    SafetyInstructions,
    UnresolvedApproval,
    SideEffectFence,
    ActiveObjective,
    ToolCallResultPair,
    CitationProvenance,
}

/// Durable host-validated plan for one compaction generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextCompactionPlanV2 {
    pub(crate) schema_version: u32,
    pub(crate) plan_id: String,
    pub(crate) owner: ContextCompactionOwner,
    pub(crate) generation: u64,
    pub(crate) context_projection_epoch: u64,
    pub(crate) catalog_hash: String,
    pub(crate) estimated_input_tokens: u64,
    pub(crate) expected_savings_tokens: u64,
    pub(crate) minimum_savings_tokens: u64,
    pub(crate) protected_segments: Vec<ContextProtectedSegment>,
    pub(crate) fallback_engine_id: String,
    pub(crate) fallback_reason_code: String,
}

impl ContextCompactionPlanV2 {
    #[must_use]
    pub(crate) fn host(
        generation: u64,
        context_projection_epoch: u64,
        catalog_hash: String,
        estimated_input_tokens: u64,
    ) -> Self {
        let minimum_savings_tokens = estimated_input_tokens.saturating_div(20).max(1);
        Self {
            schema_version: CONTEXT_COMPACTION_PLAN_SCHEMA_VERSION,
            plan_id: Ulid::new().to_string(),
            owner: ContextCompactionOwner::Host,
            generation,
            context_projection_epoch,
            catalog_hash,
            estimated_input_tokens,
            expected_savings_tokens: estimated_input_tokens.saturating_div(3).max(1),
            minimum_savings_tokens,
            protected_segments: vec![
                ContextProtectedSegment::SystemInstructions,
                ContextProtectedSegment::SafetyInstructions,
                ContextProtectedSegment::UnresolvedApproval,
                ContextProtectedSegment::SideEffectFence,
                ContextProtectedSegment::ActiveObjective,
                ContextProtectedSegment::ToolCallResultPair,
                ContextProtectedSegment::CitationProvenance,
            ],
            fallback_engine_id: "default_context_engine".to_owned(),
            fallback_reason_code: "context.compaction.safe_builtin_fallback".to_owned(),
        }
    }

    /// Validates the closed plan before any host write starts.
    ///
    /// # Errors
    /// Returns a stable reason code when identity, epoch, savings, or protected
    /// segment requirements are incomplete.
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != CONTEXT_COMPACTION_PLAN_SCHEMA_VERSION {
            return Err("context.compaction.plan_schema_unsupported");
        }
        if Ulid::from_string(self.plan_id.as_str()).is_err()
            || self.generation == 0
            || self.context_projection_epoch == 0
            || self.catalog_hash.len() != 64
        {
            return Err("context.compaction.plan_identity_invalid");
        }
        if self.estimated_input_tokens == 0
            || self.expected_savings_tokens == 0
            || self.minimum_savings_tokens == 0
            || self.expected_savings_tokens < self.minimum_savings_tokens
        {
            return Err("context.compaction.plan_savings_invalid");
        }
        let protected = self.protected_segments.iter().copied().collect::<BTreeSet<_>>();
        let mandatory = [
            ContextProtectedSegment::SystemInstructions,
            ContextProtectedSegment::SafetyInstructions,
            ContextProtectedSegment::UnresolvedApproval,
            ContextProtectedSegment::SideEffectFence,
            ContextProtectedSegment::ActiveObjective,
            ContextProtectedSegment::ToolCallResultPair,
            ContextProtectedSegment::CitationProvenance,
        ];
        if mandatory.iter().any(|segment| !protected.contains(segment)) {
            return Err("context.compaction.protected_segment_missing");
        }
        if self.fallback_engine_id != "default_context_engine"
            || self.fallback_reason_code.trim().is_empty()
        {
            return Err("context.compaction.fallback_invalid");
        }
        Ok(())
    }
}

/// Result of the post-compaction savings and preservation gate.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextCompactionQualityOutcome {
    pub(crate) accepted: bool,
    pub(crate) reason_code: String,
    pub(crate) realized_savings_tokens: u64,
    pub(crate) protected_segments_preserved: bool,
}

/// Applies the deterministic post-write quality gate to bounded token counts.
#[must_use]
pub(crate) fn evaluate_compaction_quality(
    plan: &ContextCompactionPlanV2,
    actual_input_tokens: u64,
    actual_output_tokens: u64,
    preserved_segments: &[ContextProtectedSegment],
) -> ContextCompactionQualityOutcome {
    let realized_savings_tokens = actual_input_tokens.saturating_sub(actual_output_tokens);
    let expected = plan.protected_segments.iter().copied().collect::<BTreeSet<_>>();
    let observed = preserved_segments.iter().copied().collect::<BTreeSet<_>>();
    let protected_segments_preserved = expected.is_subset(&observed);
    let (accepted, reason_code) = if !protected_segments_preserved {
        (false, "context.compaction.protected_segment_lost")
    } else if actual_output_tokens >= actual_input_tokens
        || realized_savings_tokens < plan.minimum_savings_tokens
    {
        (false, "context.compaction.insufficient_savings")
    } else {
        (true, "context.compaction.quality_gate_passed")
    };
    ContextCompactionQualityOutcome {
        accepted,
        reason_code: reason_code.to_owned(),
        realized_savings_tokens,
        protected_segments_preserved,
    }
}

/// Read-only context tool bound to one exact model-visible catalog snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextToolDescriptor {
    pub(crate) schema_version: u32,
    pub(crate) name: String,
    pub(crate) catalog_hash: String,
    pub(crate) provider_schema_hash: String,
    pub(crate) read_only: bool,
}

impl ContextToolDescriptor {
    #[must_use]
    pub(crate) fn inspect_from_catalog(catalog: &ModelVisibleToolCatalogSnapshot) -> Option<Self> {
        catalog
            .tools
            .iter()
            .chain(catalog.indexed_tools.iter())
            .find(|tool| tool.name == CONTEXT_INSPECT_TOOL_NAME)
            .map(|tool| Self {
                schema_version: CONTEXT_TOOL_SCHEMA_VERSION,
                name: tool.name.clone(),
                catalog_hash: catalog.catalog_hash.clone(),
                provider_schema_hash: tool.provider_schema_hash.clone(),
                read_only: true,
            })
    }
}

/// Host decision after checking context-tool identity and catalog epoch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextToolOutcome {
    pub(crate) accepted: bool,
    pub(crate) handled: bool,
    pub(crate) reason_code: String,
}

#[must_use]
pub(crate) fn gate_context_tool_call(
    descriptor: &ContextToolDescriptor,
    observed_catalog_hash: &str,
    tool_name: &str,
    mutating: bool,
) -> ContextToolOutcome {
    let (accepted, handled, reason_code) = if descriptor.catalog_hash != observed_catalog_hash {
        (false, false, "context.tool.catalog_epoch_mismatch")
    } else if descriptor.name != tool_name {
        (false, false, "context.tool.not_in_engine_schema")
    } else if mutating || !descriptor.read_only {
        (false, false, "context.tool.mutating_call_denied")
    } else {
        (true, true, "context.tool.host_gate_passed")
    };
    ContextToolOutcome { accepted, handled, reason_code: reason_code.to_owned() }
}

/// Process-local claim registry preventing concurrent compaction of one session.
///
/// A daemon crash releases all claims along with in-flight host work. Durable
/// started/terminal evidence remains in the journal, while a restarted daemon
/// issues a new monotonic process generation.
pub(crate) struct ContextCompactionOwnerRegistry {
    claims: Mutex<BTreeMap<String, String>>,
    next_generation: AtomicU64,
}

impl ContextCompactionOwnerRegistry {
    fn new() -> Self {
        Self { claims: Mutex::new(BTreeMap::new()), next_generation: AtomicU64::new(1) }
    }

    #[must_use]
    pub(crate) fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::Relaxed)
    }

    /// Acquires the sole session claim until the returned guard is dropped.
    ///
    /// # Errors
    /// Returns a stable reason code when the plan is invalid, the registry is
    /// unavailable, or another plan already owns the session.
    pub(crate) fn acquire(
        &'static self,
        session_id: &str,
        plan: &ContextCompactionPlanV2,
    ) -> Result<ContextCompactionLease, &'static str> {
        plan.validate()?;
        let mut claims =
            self.claims.lock().map_err(|_| "context.compaction.owner_registry_unavailable")?;
        if claims.contains_key(session_id) {
            return Err("context.compaction.owner_conflict");
        }
        claims.insert(session_id.to_owned(), plan.plan_id.clone());
        Ok(ContextCompactionLease {
            registry: self,
            session_id: session_id.to_owned(),
            plan_id: plan.plan_id.clone(),
        })
    }
}

pub(crate) struct ContextCompactionLease {
    registry: &'static ContextCompactionOwnerRegistry,
    session_id: String,
    plan_id: String,
}

impl Drop for ContextCompactionLease {
    fn drop(&mut self) {
        if let Ok(mut claims) = self.registry.claims.lock() {
            if claims.get(self.session_id.as_str()) == Some(&self.plan_id) {
                claims.remove(self.session_id.as_str());
            }
        }
    }
}

#[must_use]
pub(crate) fn context_compaction_owner_registry() -> &'static ContextCompactionOwnerRegistry {
    static REGISTRY: OnceLock<ContextCompactionOwnerRegistry> = OnceLock::new();
    REGISTRY.get_or_init(ContextCompactionOwnerRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_compaction_quality, gate_context_tool_call, ContextCompactionOwnerRegistry,
        ContextCompactionPlanV2, ContextProtectedSegment, ContextToolDescriptor,
        CONTEXT_INSPECT_TOOL_NAME, CONTEXT_TOOL_SCHEMA_VERSION,
    };

    fn plan(registry: &ContextCompactionOwnerRegistry) -> ContextCompactionPlanV2 {
        ContextCompactionPlanV2::host(registry.next_generation(), 1, "a".repeat(64), 1_000)
    }

    #[test]
    fn double_compaction_request_is_rejected_until_owner_drops() {
        let registry = Box::leak(Box::new(ContextCompactionOwnerRegistry::new()));
        let first = plan(registry);
        let first_lease = registry.acquire("session-1", &first).expect("first owner acquires");
        let second = plan(registry);

        assert_eq!(
            registry.acquire("session-1", &second).err(),
            Some("context.compaction.owner_conflict")
        );
        drop(first_lease);
        assert!(registry.acquire("session-1", &second).is_ok());
    }

    #[test]
    fn protected_segment_gate_rejects_missing_active_objective() {
        let registry = ContextCompactionOwnerRegistry::new();
        let plan = plan(&registry);
        let preserved = plan
            .protected_segments
            .iter()
            .copied()
            .filter(|segment| *segment != ContextProtectedSegment::ActiveObjective)
            .collect::<Vec<_>>();

        let outcome = evaluate_compaction_quality(&plan, 1_000, 600, preserved.as_slice());

        assert!(!outcome.accepted);
        assert_eq!(outcome.reason_code, "context.compaction.protected_segment_lost");
    }

    #[test]
    fn insufficient_savings_fails_closed() {
        let registry = ContextCompactionOwnerRegistry::new();
        let plan = plan(&registry);

        let outcome =
            evaluate_compaction_quality(&plan, 1_000, 990, plan.protected_segments.as_slice());

        assert!(!outcome.accepted);
        assert_eq!(outcome.reason_code, "context.compaction.insufficient_savings");
    }

    #[test]
    fn engine_crash_releases_owner_for_safe_fallback() {
        let registry = Box::leak(Box::new(ContextCompactionOwnerRegistry::new()));
        let engine_plan = plan(registry);
        let lease = registry.acquire("session-crash", &engine_plan).expect("engine owner acquires");
        drop(lease);

        let fallback = plan(registry);

        assert!(registry.acquire("session-crash", &fallback).is_ok());
        assert!(fallback.generation > engine_plan.generation);
    }

    #[test]
    fn context_tool_rejects_stale_catalog_epoch_and_mutation() {
        let descriptor = ContextToolDescriptor {
            schema_version: CONTEXT_TOOL_SCHEMA_VERSION,
            name: CONTEXT_INSPECT_TOOL_NAME.to_owned(),
            catalog_hash: "a".repeat(64),
            provider_schema_hash: "b".repeat(64),
            read_only: true,
        };

        let stale = gate_context_tool_call(
            &descriptor,
            "c".repeat(64).as_str(),
            CONTEXT_INSPECT_TOOL_NAME,
            false,
        );
        assert!(!stale.accepted);
        assert_eq!(stale.reason_code, "context.tool.catalog_epoch_mismatch");

        let mutating = gate_context_tool_call(
            &descriptor,
            descriptor.catalog_hash.as_str(),
            CONTEXT_INSPECT_TOOL_NAME,
            true,
        );
        assert!(!mutating.accepted);
        assert_eq!(mutating.reason_code, "context.tool.mutating_call_denied");
    }
}
