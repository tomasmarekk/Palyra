//! Observe-only capability tokens and bounded RuntimeKernelV2 differential reports.
//!
//! Shadow code receives sanitized plan snapshots and comparison capabilities,
//! never provider, tool, approval, delivery, journal, or secret-resolution authority.

use palyra_common::runtime_contracts::{RuntimeErrorPhase, RuntimeGeneration};
use serde::{Deserialize, Serialize};

mod observer;
mod validation;

#[cfg(test)]
pub(crate) use observer::{ShadowAuthorityDenied, ShadowForbiddenService};
pub(crate) use observer::{
    ShadowDifferentialObserver, ShadowEnrollment, ShadowObservationResult, ShadowObserverError,
    ShadowSamplingPolicyV1,
};
pub(crate) use validation::RuntimeDifferentialError;
use validation::{
    classify, deserialize_phase_plan, digest_shadow_semantics, exact_or_risky,
    is_bounded_shadow_label, is_lower_sha256,
};

/// Schema version for [`ShadowPlanSnapshotV1`] and [`RuntimeDifferentialReportV1`].
pub(crate) const RUNTIME_DIFFERENTIAL_SCHEMA_VERSION: u32 = 1;
/// Maximum number of low-cardinality phases retained in one expected plan.
pub(crate) const MAX_SHADOW_PHASE_PLAN_ENTRIES: usize = 32;
/// Maximum provider-input token budget accepted by the comparison boundary.
pub(crate) const MAX_SHADOW_TOKEN_BUDGET: u32 = 4_000_000;
/// Token-budget drift considered bounded and operationally benign.
pub(crate) const BENIGN_TOKEN_BUDGET_DELTA: u32 = 256;
const MAX_SHADOW_CONTEXT_SEGMENTS: usize = 256;
const MAX_SHADOW_SEMANTIC_LABEL_BYTES: usize = 256;
const SHADOW_SELECTION_DIGEST_DOMAIN: &[u8] = b"palyra.runtime_kernel_v2.shadow.selection.v1\0";
const SHADOW_CONTEXT_SEGMENTS_DIGEST_DOMAIN: &[u8] =
    b"palyra.runtime_kernel_v2.shadow.context_segments.v1\0";
const SHADOW_CONTEXT_SAFETY_DIGEST_DOMAIN: &[u8] =
    b"palyra.runtime_kernel_v2.shadow.context_safety.v1\0";
const SHADOW_TOOL_CATALOG_DIGEST_DOMAIN: &[u8] =
    b"palyra.runtime_kernel_v2.shadow.tool_catalog.v1\0";
const SHADOW_POLICY_INPUT_DIGEST_DOMAIN: &[u8] =
    b"palyra.runtime_kernel_v2.shadow.policy_input.v1\0";

#[cfg(test)]
mod sealed {
    pub trait ObserveOnly {}
}

/// Marker implemented only by capabilities that cannot perform side effects.
#[cfg(test)]
pub(crate) trait ObserveOnlyShadowCapability: sealed::ObserveOnly {}

/// Capability to compare already-sanitized runtime-selection digests.
#[derive(Debug)]
pub(crate) struct ShadowSelectionComparisonCapability {
    generation: RuntimeGeneration,
}

#[cfg(test)]
impl sealed::ObserveOnly for ShadowSelectionComparisonCapability {}
#[cfg(test)]
impl ObserveOnlyShadowCapability for ShadowSelectionComparisonCapability {}

/// Capability to compare already-sanitized context, budget, catalog, policy, and phase plans.
#[derive(Debug)]
pub(crate) struct ShadowPlanComparisonCapability {
    generation: RuntimeGeneration,
}

#[cfg(test)]
impl sealed::ObserveOnly for ShadowPlanComparisonCapability {}
#[cfg(test)]
impl ObserveOnlyShadowCapability for ShadowPlanComparisonCapability {}

/// Host-issued authority for one observe-only shadow generation.
///
/// The token deliberately exposes only the two sealed comparison capabilities.
/// Side-effect services cannot require or be derived from either capability.
#[derive(Debug)]
pub(crate) struct ShadowAuthorityToken {
    selection: ShadowSelectionComparisonCapability,
    plan: ShadowPlanComparisonCapability,
}

impl ShadowAuthorityToken {
    /// Creates observe-only capabilities for one active runtime generation.
    #[must_use]
    pub(crate) const fn new(generation: RuntimeGeneration) -> Self {
        Self {
            selection: ShadowSelectionComparisonCapability { generation },
            plan: ShadowPlanComparisonCapability { generation },
        }
    }

    /// Returns authority to compare sanitized selection digests.
    #[must_use]
    pub(crate) const fn selection(&self) -> &ShadowSelectionComparisonCapability {
        &self.selection
    }

    /// Returns authority to compare sanitized plan inputs.
    #[must_use]
    pub(crate) const fn plan(&self) -> &ShadowPlanComparisonCapability {
        &self.plan
    }

    /// Rejects any attempt to acquire a service that could affect the authoritative run.
    ///
    /// The returned error is metadata-only. The token never stores or exposes a
    /// provider, tool, approval, or delivery service handle.
    #[cfg(test)]
    pub(crate) const fn request_side_effect(
        &self,
        service: ShadowForbiddenService,
    ) -> Result<(), ShadowAuthorityDenied> {
        Err(ShadowAuthorityDenied::new(service))
    }
}

/// Hash-only plan inputs admitted to shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ShadowPlanDigestsV1 {
    runtime_selection_sha256: String,
    context_segments_sha256: String,
    context_safety_sha256: String,
    tool_catalog_sha256: String,
    policy_input_sha256: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct ShadowPlanDigestsWire {
    runtime_selection_sha256: String,
    context_segments_sha256: String,
    context_safety_sha256: String,
    tool_catalog_sha256: String,
    policy_input_sha256: String,
}

impl<'de> Deserialize<'de> for ShadowPlanDigestsV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ShadowPlanDigestsWire::deserialize(deserializer)?;
        let digests = Self {
            runtime_selection_sha256: wire.runtime_selection_sha256,
            context_segments_sha256: wire.context_segments_sha256,
            context_safety_sha256: wire.context_safety_sha256,
            tool_catalog_sha256: wire.tool_catalog_sha256,
            policy_input_sha256: wire.policy_input_sha256,
        };
        digests.validate().map_err(serde::de::Error::custom)?;
        Ok(digests)
    }
}

impl ShadowPlanDigestsV1 {
    /// Creates and validates the complete allowed digest set.
    ///
    /// # Errors
    /// Returns [`RuntimeDifferentialError::InvalidPlan`] when any value is not
    /// a lowercase SHA-256 digest.
    pub(crate) fn new(
        runtime_selection_sha256: String,
        context_segments_sha256: String,
        context_safety_sha256: String,
        tool_catalog_sha256: String,
        policy_input_sha256: String,
    ) -> Result<Self, RuntimeDifferentialError> {
        let digests = Self {
            runtime_selection_sha256,
            context_segments_sha256,
            context_safety_sha256,
            tool_catalog_sha256,
            policy_input_sha256,
        };
        digests.validate()?;
        Ok(digests)
    }

    fn validate(&self) -> Result<(), RuntimeDifferentialError> {
        if [
            self.runtime_selection_sha256.as_str(),
            self.context_segments_sha256.as_str(),
            self.context_safety_sha256.as_str(),
            self.tool_catalog_sha256.as_str(),
            self.policy_input_sha256.as_str(),
        ]
        .into_iter()
        .all(is_lower_sha256)
        {
            Ok(())
        } else {
            Err(RuntimeDifferentialError::InvalidPlan)
        }
    }
}

/// Bounded, side-effect-free plan snapshot consumed by shadow comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct ShadowPlanSnapshotV1 {
    schema_version: u32,
    generation: RuntimeGeneration,
    digests: ShadowPlanDigestsV1,
    token_budget: u32,
    expected_phase_plan: Vec<RuntimeErrorPhase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct ShadowPlanSnapshotWire {
    schema_version: u32,
    generation: RuntimeGeneration,
    digests: ShadowPlanDigestsV1,
    token_budget: u32,
    #[serde(deserialize_with = "deserialize_phase_plan")]
    expected_phase_plan: Vec<RuntimeErrorPhase>,
}

impl<'de> Deserialize<'de> for ShadowPlanSnapshotV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ShadowPlanSnapshotWire::deserialize(deserializer)?;
        let snapshot = Self {
            schema_version: wire.schema_version,
            generation: wire.generation,
            digests: wire.digests,
            token_budget: wire.token_budget,
            expected_phase_plan: wire.expected_phase_plan,
        };
        snapshot.validate().map_err(serde::de::Error::custom)?;
        Ok(snapshot)
    }
}

impl ShadowPlanSnapshotV1 {
    /// Creates a sanitized shadow plan snapshot.
    ///
    /// # Errors
    /// Returns [`RuntimeDifferentialError::InvalidPlan`] for an unsupported
    /// schema, invalid digest, zero or excessive token budget, or an empty or
    /// oversized phase plan.
    pub(crate) fn new(
        generation: RuntimeGeneration,
        digests: ShadowPlanDigestsV1,
        token_budget: u32,
        expected_phase_plan: Vec<RuntimeErrorPhase>,
    ) -> Result<Self, RuntimeDifferentialError> {
        let snapshot = Self {
            schema_version: RUNTIME_DIFFERENTIAL_SCHEMA_VERSION,
            generation,
            digests,
            token_budget,
            expected_phase_plan,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Validates the complete bounded shadow input contract.
    ///
    /// # Errors
    /// Returns [`RuntimeDifferentialError::InvalidPlan`] when any field falls
    /// outside the hash-only, budget, or phase-plan bounds.
    pub(crate) fn validate(&self) -> Result<(), RuntimeDifferentialError> {
        if self.schema_version != RUNTIME_DIFFERENTIAL_SCHEMA_VERSION
            || self.token_budget == 0
            || self.token_budget > MAX_SHADOW_TOKEN_BUDGET
            || self.expected_phase_plan.is_empty()
            || self.expected_phase_plan.len() > MAX_SHADOW_PHASE_PLAN_ENTRIES
        {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        self.digests.validate()
    }

    /// Returns the generation to which this immutable plan belongs.
    #[must_use]
    pub(crate) const fn generation(&self) -> RuntimeGeneration {
        self.generation
    }
}

/// Normalized runtime-selection values with no provider credential material.
#[derive(Debug, Serialize)]
pub(crate) struct ShadowSelectionSemanticV1 {
    provider_id: String,
    model_id: String,
    credential_id: String,
    health_state: String,
}

impl ShadowSelectionSemanticV1 {
    /// Creates one bounded selection projection.
    pub(crate) fn new(
        provider_id: String,
        model_id: String,
        credential_id: String,
        health_state: String,
    ) -> Result<Self, RuntimeDifferentialError> {
        if [&provider_id, &model_id, &credential_id, &health_state]
            .into_iter()
            .all(|value| is_bounded_shadow_label(value))
        {
            Ok(Self { provider_id, model_id, credential_id, health_state })
        } else {
            Err(RuntimeDifferentialError::InvalidPlan)
        }
    }
}

/// Hash-only metadata for one ordered context segment.
#[derive(Debug, Serialize)]
pub(crate) struct ShadowContextSegmentSemanticV1 {
    kind: String,
    content_sha256: String,
    byte_len: u64,
    trust_label: String,
    cache_hint: String,
    invalidation_reason: Option<String>,
}

impl ShadowContextSegmentSemanticV1 {
    /// Creates one normalized context-segment projection.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        kind: String,
        content_sha256: String,
        byte_len: u64,
        trust_label: String,
        cache_hint: String,
        invalidation_reason: Option<String>,
    ) -> Result<Self, RuntimeDifferentialError> {
        if !is_bounded_shadow_label(&kind)
            || !is_lower_sha256(&content_sha256)
            || !is_bounded_shadow_label(&trust_label)
            || !is_bounded_shadow_label(&cache_hint)
            || invalidation_reason.as_deref().is_some_and(|value| !is_bounded_shadow_label(value))
        {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self { kind, content_sha256, byte_len, trust_label, cache_hint, invalidation_reason })
    }
}

/// Bounded context trust summary used by the V2 planner.
#[derive(Debug, Serialize)]
pub(crate) struct ShadowInstructionTrustSemanticV1 {
    selected_blocks: u32,
    untrusted_blocks: u32,
    mixed_trust: bool,
    highest_safety_action: String,
    prompt_injection_finding_count: u32,
}

impl ShadowInstructionTrustSemanticV1 {
    /// Creates one normalized trust projection.
    pub(crate) fn new(
        selected_blocks: u32,
        untrusted_blocks: u32,
        mixed_trust: bool,
        highest_safety_action: String,
        prompt_injection_finding_count: u32,
    ) -> Result<Self, RuntimeDifferentialError> {
        if untrusted_blocks > selected_blocks || !is_bounded_shadow_label(&highest_safety_action) {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self {
            selected_blocks,
            untrusted_blocks,
            mixed_trust,
            highest_safety_action,
            prompt_injection_finding_count,
        })
    }
}

/// Sanitized model-visible catalog descriptor.
#[derive(Debug, Serialize)]
pub(crate) struct ShadowToolCatalogSemanticV1 {
    catalog_sha256: String,
    exposure_mode: String,
    exposed_tool_count: u32,
}

impl ShadowToolCatalogSemanticV1 {
    /// Creates one normalized catalog projection.
    pub(crate) fn new(
        catalog_sha256: String,
        exposure_mode: String,
        exposed_tool_count: u32,
    ) -> Result<Self, RuntimeDifferentialError> {
        if !is_lower_sha256(&catalog_sha256) || !is_bounded_shadow_label(&exposure_mode) {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self { catalog_sha256, exposure_mode, exposed_tool_count })
    }
}

/// Sanitized policy posture used for V2 planning.
#[derive(Debug, Serialize)]
pub(crate) struct ShadowPolicySemanticV1 {
    json_mode: bool,
    allow_sensitive_tools: bool,
    remaining_tool_budget: u32,
    budget_profile: Option<String>,
    max_output_tokens: Option<u32>,
}

impl ShadowPolicySemanticV1 {
    /// Creates one normalized policy projection.
    pub(crate) fn new(
        json_mode: bool,
        allow_sensitive_tools: bool,
        remaining_tool_budget: u32,
        budget_profile: Option<String>,
        max_output_tokens: Option<u32>,
    ) -> Result<Self, RuntimeDifferentialError> {
        if budget_profile.as_deref().is_some_and(|value| !is_bounded_shadow_label(value)) {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self {
            json_mode,
            allow_sensitive_tools,
            remaining_tool_budget,
            budget_profile,
            max_output_tokens,
        })
    }
}

/// Raw-content-free inputs captured before either runtime prepares context.
///
/// This is the only context and policy input accepted by the V2 shadow
/// planner. In particular, it cannot carry a legacy `ProviderRequest`, a
/// prepared legacy prompt, or a legacy catalog projection.
#[derive(Debug)]
pub(crate) struct ShadowV2PreContextInputV1 {
    current_turn_sha256: String,
    current_turn_byte_len: u64,
    vision_input_count: u32,
    estimated_input_tokens: u64,
    json_mode_requested: bool,
    allow_sensitive_tools: bool,
    remaining_tool_budget: u32,
}

impl ShadowV2PreContextInputV1 {
    /// Captures the bounded common input from which V2 plans its shadow turn.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        current_turn_sha256: String,
        current_turn_byte_len: u64,
        vision_input_count: u32,
        estimated_input_tokens: u64,
        json_mode_requested: bool,
        allow_sensitive_tools: bool,
        remaining_tool_budget: u32,
    ) -> Result<Self, RuntimeDifferentialError> {
        if !is_lower_sha256(&current_turn_sha256)
            || estimated_input_tokens == 0
            || estimated_input_tokens > u64::from(MAX_SHADOW_TOKEN_BUDGET)
        {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self {
            current_turn_sha256,
            current_turn_byte_len,
            vision_input_count,
            estimated_input_tokens,
            json_mode_requested,
            allow_sensitive_tools,
            remaining_tool_budget,
        })
    }

    fn plan_context_segments(
        &self,
    ) -> Result<Vec<ShadowContextSegmentSemanticV1>, RuntimeDifferentialError> {
        let mut segments = vec![ShadowContextSegmentSemanticV1::new(
            "current_turn".to_owned(),
            self.current_turn_sha256.clone(),
            self.current_turn_byte_len,
            "untrusted".to_owned(),
            "volatile".to_owned(),
            None,
        )?];
        if self.vision_input_count > 0 {
            segments.push(ShadowContextSegmentSemanticV1::new(
                "vision_inputs".to_owned(),
                digest_shadow_semantics(
                    SHADOW_CONTEXT_SEGMENTS_DIGEST_DOMAIN,
                    &self.vision_input_count,
                )?,
                u64::from(self.vision_input_count),
                "untrusted".to_owned(),
                "sensitive".to_owned(),
                None,
            )?);
        }
        Ok(segments)
    }

    fn plan_policy(&self) -> Result<ShadowPolicySemanticV1, RuntimeDifferentialError> {
        self.plan_policy_with_host_limits(None, None)
    }

    /// Projects the legacy-observed host limits over the common requested posture.
    pub(crate) fn plan_policy_with_host_limits(
        &self,
        budget_profile: Option<String>,
        max_output_tokens: Option<u32>,
    ) -> Result<ShadowPolicySemanticV1, RuntimeDifferentialError> {
        ShadowPolicySemanticV1::new(
            self.json_mode_requested,
            self.allow_sensitive_tools,
            self.remaining_tool_budget,
            budget_profile,
            max_output_tokens,
        )
    }
}

/// One non-cloneable set of normalized semantics for plan projection.
#[derive(Debug)]
pub(crate) struct ShadowPlanSemanticInputsV1 {
    selection: ShadowSelectionSemanticV1,
    context_segments: Vec<ShadowContextSegmentSemanticV1>,
    instruction_sha256: Option<String>,
    instruction_trust: Option<ShadowInstructionTrustSemanticV1>,
    estimated_input_tokens: u64,
    catalog: ShadowToolCatalogSemanticV1,
    policy: ShadowPolicySemanticV1,
}

impl ShadowPlanSemanticInputsV1 {
    /// Creates a bounded, raw-content-free plan projection input.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        selection: ShadowSelectionSemanticV1,
        context_segments: Vec<ShadowContextSegmentSemanticV1>,
        instruction_sha256: Option<String>,
        instruction_trust: Option<ShadowInstructionTrustSemanticV1>,
        estimated_input_tokens: u64,
        catalog: ShadowToolCatalogSemanticV1,
        policy: ShadowPolicySemanticV1,
    ) -> Result<Self, RuntimeDifferentialError> {
        if context_segments.len() > MAX_SHADOW_CONTEXT_SEGMENTS
            || instruction_sha256.as_deref().is_some_and(|value| !is_lower_sha256(value))
        {
            return Err(RuntimeDifferentialError::InvalidPlan);
        }
        Ok(Self {
            selection,
            context_segments,
            instruction_sha256,
            instruction_trust,
            estimated_input_tokens,
            catalog,
            policy,
        })
    }

    /// Projects separately observed legacy semantics into an authoritative snapshot.
    pub(crate) fn into_authoritative_snapshot(
        self,
        generation: RuntimeGeneration,
        expected_phase_plan: Vec<RuntimeErrorPhase>,
    ) -> Result<ShadowPlanSnapshotV1, RuntimeDifferentialError> {
        let (digests, token_budget, _) = self.project()?;
        ShadowPlanSnapshotV1::new(generation, digests, token_budget, expected_phase_plan)
    }

    fn project(&self) -> Result<(ShadowPlanDigestsV1, u32, bool), RuntimeDifferentialError> {
        #[derive(Serialize)]
        struct ContextSafetyProjection<'a> {
            instruction_sha256: Option<&'a str>,
            instruction_trust: Option<&'a ShadowInstructionTrustSemanticV1>,
            segment_trust_labels: Vec<&'a str>,
        }

        let digests = ShadowPlanDigestsV1::new(
            digest_shadow_semantics(SHADOW_SELECTION_DIGEST_DOMAIN, &self.selection)?,
            digest_shadow_semantics(SHADOW_CONTEXT_SEGMENTS_DIGEST_DOMAIN, &self.context_segments)?,
            digest_shadow_semantics(
                SHADOW_CONTEXT_SAFETY_DIGEST_DOMAIN,
                &ContextSafetyProjection {
                    instruction_sha256: self.instruction_sha256.as_deref(),
                    instruction_trust: self.instruction_trust.as_ref(),
                    segment_trust_labels: self
                        .context_segments
                        .iter()
                        .map(|segment| segment.trust_label.as_str())
                        .collect(),
                },
            )?,
            digest_shadow_semantics(SHADOW_TOOL_CATALOG_DIGEST_DOMAIN, &self.catalog)?,
            digest_shadow_semantics(SHADOW_POLICY_INPUT_DIGEST_DOMAIN, &self.policy)?,
        )?;
        let token_budget = u32::try_from(self.estimated_input_tokens.max(1))
            .map_err(|_| RuntimeDifferentialError::InvalidPlan)?;
        Ok((digests, token_budget, self.catalog.exposed_tool_count > 0))
    }
}

/// Sanitized immutable inputs from which the shadow module derives a V2 plan.
#[derive(Debug)]
pub(crate) struct ShadowCandidatePlanInputsV1 {
    generation: RuntimeGeneration,
    source: ShadowCandidatePlanSourceV1,
}

#[derive(Debug)]
enum ShadowCandidatePlanSourceV1 {
    Independent {
        selection: ShadowSelectionSemanticV1,
        pre_context: ShadowV2PreContextInputV1,
        catalog: ShadowToolCatalogSemanticV1,
    },
    #[cfg(test)]
    TestSemantics(ShadowPlanSemanticInputsV1),
}

impl ShadowCandidatePlanInputsV1 {
    /// Seals independent V2 planning inputs without accepting legacy projections.
    #[must_use]
    pub(crate) const fn from_pre_context(
        generation: RuntimeGeneration,
        selection: ShadowSelectionSemanticV1,
        pre_context: ShadowV2PreContextInputV1,
        catalog: ShadowToolCatalogSemanticV1,
    ) -> Self {
        Self {
            generation,
            source: ShadowCandidatePlanSourceV1::Independent { selection, pre_context, catalog },
        }
    }

    /// Retains the historical semantic-fixture constructor for unit tests only.
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn new(
        generation: RuntimeGeneration,
        semantics: ShadowPlanSemanticInputsV1,
    ) -> Self {
        Self { generation, source: ShadowCandidatePlanSourceV1::TestSemantics(semantics) }
    }
}

/// Consumed, data-only planner for one sanitized V2 shadow candidate.
///
/// This concrete type deliberately stores only validated immutable inputs. It
/// cannot capture runtime services, secret resolvers, the journal, arbitrary
/// caller code, or a caller-assembled candidate plan. Consuming it proves that
/// one enrollment can plan at most once.
#[derive(Debug)]
pub(crate) struct ShadowCandidatePlannerV1 {
    inputs: ShadowCandidatePlanInputsV1,
}

impl ShadowCandidatePlannerV1 {
    /// Seals sanitized V2 projection inputs behind the one-shot planner.
    #[must_use]
    pub(crate) const fn new(inputs: ShadowCandidatePlanInputsV1) -> Self {
        Self { inputs }
    }

    fn plan(
        self,
        authority: &ShadowAuthorityToken,
    ) -> Result<ShadowPlanSnapshotV1, RuntimeDifferentialError> {
        if self.inputs.generation != authority.selection.generation {
            return Err(RuntimeDifferentialError::GenerationMismatch);
        }
        let semantics = match self.inputs.source {
            ShadowCandidatePlanSourceV1::Independent { selection, pre_context, catalog } => {
                ShadowPlanSemanticInputsV1::new(
                    selection,
                    pre_context.plan_context_segments()?,
                    None,
                    None,
                    pre_context.estimated_input_tokens,
                    catalog,
                    pre_context.plan_policy()?,
                )?
            }
            #[cfg(test)]
            ShadowCandidatePlanSourceV1::TestSemantics(semantics) => semantics,
        };
        let (digests, token_budget, tool_capable) = semantics.project()?;
        let expected_phase_plan = super::phases::canonical_v2_expected_phase_plan(tool_capable);
        ShadowPlanSnapshotV1::new(
            self.inputs.generation,
            digests,
            token_budget,
            expected_phase_plan,
        )
    }
}

/// Sanitized authoritative and candidate plans for one production shadow comparison.
///
/// Construction validates both plans and pins them to the same generation
/// before the host can schedule an observation.
#[derive(Debug)]
pub(crate) struct ShadowComparisonPlansV1 {
    authoritative: ShadowPlanSnapshotV1,
    candidate: ShadowCandidatePlannerV1,
}

impl ShadowComparisonPlansV1 {
    /// Creates one generation-bound, data-only comparison input.
    ///
    /// # Errors
    /// Returns [`RuntimeDifferentialError`] when either plan is invalid or the
    /// authoritative and candidate generations differ.
    pub(crate) fn new(
        authoritative: ShadowPlanSnapshotV1,
        candidate: ShadowCandidatePlannerV1,
    ) -> Result<Self, RuntimeDifferentialError> {
        authoritative.validate()?;
        if authoritative.generation != candidate.inputs.generation {
            return Err(RuntimeDifferentialError::GenerationMismatch);
        }
        Ok(Self { authoritative, candidate })
    }

    pub(crate) fn into_parts(self) -> (ShadowPlanSnapshotV1, ShadowCandidatePlannerV1) {
        (self.authoritative, self.candidate)
    }
}

/// Evaluates production comparison inputs without widening the one-shot planner API.
#[cfg(test)]
pub(crate) fn compare_shadow_comparison_plans_for_test(
    comparison: ShadowComparisonPlansV1,
) -> Result<RuntimeDifferentialReportV1, RuntimeDifferentialError> {
    let (authoritative, candidate) = comparison.into_parts();
    let authority = ShadowAuthorityToken::new(authoritative.generation());
    let candidate = candidate.plan(&authority)?;
    compare_shadow_plans(authority.selection(), authority.plan(), &authoritative, &candidate)
}

/// Outcome for one fixed differential dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeDifferentialOutcome {
    /// Inputs matched exactly.
    Match,
    /// Difference stays within a fixed, non-safety operational tolerance.
    BenignDifference,
    /// Difference can change runtime behavior and blocks unattended promotion.
    RiskyDifference,
    /// Difference violates a safety or authority invariant.
    InvariantViolation,
}

/// Overall low-cardinality shadow classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RuntimeDifferentialClassification {
    /// No compared dimension diverged.
    Expected,
    /// Only bounded operational differences were present.
    Benign,
    /// At least one behavioral difference requires review.
    Risky,
    /// At least one safety or authority invariant diverged.
    InvariantViolation,
}

impl RuntimeDifferentialClassification {
    const fn as_reason_code(self) -> &'static str {
        match self {
            Self::Expected => "runtime.shadow.differential_expected",
            Self::Benign => "runtime.shadow.differential_benign",
            Self::Risky => "runtime.shadow.differential_risky",
            Self::InvariantViolation => "runtime.shadow.differential_invariant_violation",
        }
    }
}

/// Fixed-size, identity-free comparison of authoritative and V2 shadow plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct RuntimeDifferentialReportV1 {
    schema_version: u32,
    generation: RuntimeGeneration,
    classification: RuntimeDifferentialClassification,
    reason_code: String,
    runtime_selection: RuntimeDifferentialOutcome,
    context_segments: RuntimeDifferentialOutcome,
    context_safety: RuntimeDifferentialOutcome,
    token_budget: RuntimeDifferentialOutcome,
    tool_catalog: RuntimeDifferentialOutcome,
    policy_input: RuntimeDifferentialOutcome,
    phase_plan: RuntimeDifferentialOutcome,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RuntimeDifferentialReportWire {
    schema_version: u32,
    generation: RuntimeGeneration,
    classification: RuntimeDifferentialClassification,
    reason_code: String,
    runtime_selection: RuntimeDifferentialOutcome,
    context_segments: RuntimeDifferentialOutcome,
    context_safety: RuntimeDifferentialOutcome,
    token_budget: RuntimeDifferentialOutcome,
    tool_catalog: RuntimeDifferentialOutcome,
    policy_input: RuntimeDifferentialOutcome,
    phase_plan: RuntimeDifferentialOutcome,
}

impl<'de> Deserialize<'de> for RuntimeDifferentialReportV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeDifferentialReportWire::deserialize(deserializer)?;
        let report = Self {
            schema_version: wire.schema_version,
            generation: wire.generation,
            classification: wire.classification,
            reason_code: wire.reason_code,
            runtime_selection: wire.runtime_selection,
            context_segments: wire.context_segments,
            context_safety: wire.context_safety,
            token_budget: wire.token_budget,
            tool_catalog: wire.tool_catalog,
            policy_input: wire.policy_input,
            phase_plan: wire.phase_plan,
        };
        report.validate().map_err(serde::de::Error::custom)?;
        Ok(report)
    }
}

/// Compares two sanitized plans without invoking either runtime.
///
/// Selection and plan capabilities are separate so the type surface makes
/// authority review explicit. Both originate from one [`ShadowAuthorityToken`]
/// and must be pinned to the compared generation.
///
/// # Errors
/// Returns [`RuntimeDifferentialError`] when either plan is invalid or the
/// capabilities and plans do not all name one runtime generation.
pub(crate) fn compare_shadow_plans(
    selection_capability: &ShadowSelectionComparisonCapability,
    plan_capability: &ShadowPlanComparisonCapability,
    authoritative: &ShadowPlanSnapshotV1,
    candidate: &ShadowPlanSnapshotV1,
) -> Result<RuntimeDifferentialReportV1, RuntimeDifferentialError> {
    authoritative.validate()?;
    candidate.validate()?;
    let generation = selection_capability.generation;
    if plan_capability.generation != generation
        || authoritative.generation != generation
        || candidate.generation != generation
    {
        return Err(RuntimeDifferentialError::GenerationMismatch);
    }

    let runtime_selection = exact_or_risky(
        authoritative.digests.runtime_selection_sha256.as_str(),
        candidate.digests.runtime_selection_sha256.as_str(),
    );
    let context_segments = exact_or_risky(
        authoritative.digests.context_segments_sha256.as_str(),
        candidate.digests.context_segments_sha256.as_str(),
    );
    let context_safety =
        if authoritative.digests.context_safety_sha256 == candidate.digests.context_safety_sha256 {
            RuntimeDifferentialOutcome::Match
        } else {
            RuntimeDifferentialOutcome::InvariantViolation
        };
    let token_budget = if authoritative.token_budget == candidate.token_budget {
        RuntimeDifferentialOutcome::Match
    } else if authoritative.token_budget.abs_diff(candidate.token_budget)
        <= BENIGN_TOKEN_BUDGET_DELTA
    {
        RuntimeDifferentialOutcome::BenignDifference
    } else {
        RuntimeDifferentialOutcome::RiskyDifference
    };
    let tool_catalog = exact_or_risky(
        authoritative.digests.tool_catalog_sha256.as_str(),
        candidate.digests.tool_catalog_sha256.as_str(),
    );
    let policy_input =
        if authoritative.digests.policy_input_sha256 == candidate.digests.policy_input_sha256 {
            RuntimeDifferentialOutcome::Match
        } else {
            RuntimeDifferentialOutcome::InvariantViolation
        };
    let phase_plan = if authoritative.expected_phase_plan == candidate.expected_phase_plan {
        RuntimeDifferentialOutcome::Match
    } else {
        RuntimeDifferentialOutcome::InvariantViolation
    };
    let outcomes = [
        runtime_selection,
        context_segments,
        context_safety,
        token_budget,
        tool_catalog,
        policy_input,
        phase_plan,
    ];
    let classification = classify(outcomes);
    let report = RuntimeDifferentialReportV1 {
        schema_version: RUNTIME_DIFFERENTIAL_SCHEMA_VERSION,
        generation,
        classification,
        reason_code: classification.as_reason_code().to_owned(),
        runtime_selection,
        context_segments,
        context_safety,
        token_budget,
        tool_catalog,
        policy_input,
        phase_plan,
    };
    report.validate()?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn generation(value: u64) -> RuntimeGeneration {
        RuntimeGeneration::new(value).expect("test generation is non-zero")
    }

    fn digests(seed: char) -> ShadowPlanDigestsV1 {
        ShadowPlanDigestsV1::new(
            seed.to_string().repeat(64),
            seed.to_string().repeat(64),
            seed.to_string().repeat(64),
            seed.to_string().repeat(64),
            seed.to_string().repeat(64),
        )
        .expect("test digests should validate")
    }

    fn plan(seed: char, token_budget: u32) -> ShadowPlanSnapshotV1 {
        ShadowPlanSnapshotV1::new(
            generation(7),
            digests(seed),
            token_budget,
            vec![
                RuntimeErrorPhase::Admission,
                RuntimeErrorPhase::RuntimeSelection,
                RuntimeErrorPhase::ContextAssembly,
                RuntimeErrorPhase::ProviderCall,
                RuntimeErrorPhase::Verification,
                RuntimeErrorPhase::Finalization,
                RuntimeErrorPhase::DeliveryIntent,
            ],
        )
        .expect("test plan should validate")
    }

    fn semantic_inputs(
        selection: &str,
        context: char,
        catalog: char,
        json_mode: bool,
        token_budget: u64,
        exposed_tool_count: u32,
    ) -> ShadowPlanSemanticInputsV1 {
        ShadowPlanSemanticInputsV1::new(
            ShadowSelectionSemanticV1::new(
                format!("provider-{selection}"),
                format!("model-{selection}"),
                format!("credential-{selection}"),
                "healthy".to_owned(),
            )
            .expect("test selection should validate"),
            vec![ShadowContextSegmentSemanticV1::new(
                "current_turn".to_owned(),
                context.to_string().repeat(64),
                42,
                "untrusted".to_owned(),
                "volatile".to_owned(),
                None,
            )
            .expect("test context segment should validate")],
            None,
            None,
            token_budget,
            ShadowToolCatalogSemanticV1::new(
                catalog.to_string().repeat(64),
                "direct".to_owned(),
                exposed_tool_count,
            )
            .expect("test catalog should validate"),
            ShadowPolicySemanticV1::new(json_mode, false, 4, None, None)
                .expect("test policy should validate"),
        )
        .expect("test semantic inputs should validate")
    }

    fn candidate_inputs(
        selection: &str,
        context: char,
        catalog: char,
        json_mode: bool,
        token_budget: u64,
        exposed_tool_count: u32,
    ) -> ShadowCandidatePlanInputsV1 {
        ShadowCandidatePlanInputsV1::from_pre_context(
            generation(7),
            ShadowSelectionSemanticV1::new(
                format!("provider-{selection}"),
                format!("model-{selection}"),
                format!("credential-{selection}"),
                "healthy".to_owned(),
            )
            .expect("test selection should validate"),
            ShadowV2PreContextInputV1::new(
                context.to_string().repeat(64),
                42,
                0,
                token_budget,
                json_mode,
                false,
                4,
            )
            .expect("test pre-context input should validate"),
            ShadowToolCatalogSemanticV1::new(
                catalog.to_string().repeat(64),
                "direct".to_owned(),
                exposed_tool_count,
            )
            .expect("test catalog should validate"),
        )
    }

    fn canonical_phases(tool_capable: bool) -> Vec<RuntimeErrorPhase> {
        let mut phases = vec![
            RuntimeErrorPhase::Admission,
            RuntimeErrorPhase::RuntimeSelection,
            RuntimeErrorPhase::ContextAssembly,
            RuntimeErrorPhase::ProviderCall,
        ];
        if tool_capable {
            phases.extend([
                RuntimeErrorPhase::ToolGate,
                RuntimeErrorPhase::Approval,
                RuntimeErrorPhase::ToolExecution,
                RuntimeErrorPhase::ResultProjection,
                RuntimeErrorPhase::Compaction,
            ]);
        }
        phases.extend([
            RuntimeErrorPhase::Verification,
            RuntimeErrorPhase::Finalization,
            RuntimeErrorPhase::DeliveryIntent,
        ]);
        phases
    }

    fn candidate_plan(inputs: ShadowCandidatePlanInputsV1) -> ShadowPlanSnapshotV1 {
        ShadowCandidatePlannerV1::new(inputs)
            .plan(&ShadowAuthorityToken::new(generation(7)))
            .expect("test candidate should plan")
    }

    fn compare(
        authoritative: &ShadowPlanSnapshotV1,
        candidate: &ShadowPlanSnapshotV1,
    ) -> RuntimeDifferentialReportV1 {
        let authority = ShadowAuthorityToken::new(generation(7));
        compare_shadow_plans(authority.selection(), authority.plan(), authoritative, candidate)
            .expect("comparison should validate")
    }

    fn assert_observe_only<C: ObserveOnlyShadowCapability>(_capability: &C) {}

    #[test]
    fn shadow_authority_exposes_only_sealed_observe_only_capabilities() {
        let authority = ShadowAuthorityToken::new(generation(7));

        assert_observe_only(authority.selection());
        assert_observe_only(authority.plan());
        assert_eq!(authority.selection().generation, authority.plan().generation);
    }

    #[test]
    fn identical_plans_have_expected_low_cardinality_outcome() {
        let authoritative = plan('a', 8_192);
        let report = compare(&authoritative, &authoritative);

        assert_eq!(report.classification(), RuntimeDifferentialClassification::Expected);
        assert_eq!(report.reason_code(), "runtime.shadow.differential_expected");
    }

    #[test]
    fn bounded_token_budget_difference_is_benign() {
        let authoritative = plan('a', 8_192);
        let candidate = plan('a', 8_192 + BENIGN_TOKEN_BUDGET_DELTA);
        let report = compare(&authoritative, &candidate);

        assert_eq!(report.classification(), RuntimeDifferentialClassification::Benign);
        assert_eq!(report.token_budget, RuntimeDifferentialOutcome::BenignDifference);
    }

    #[test]
    fn context_safety_divergence_is_an_invariant_violation() {
        let authoritative = plan('a', 8_192);
        let mut candidate = authoritative.clone();
        candidate.digests.context_safety_sha256 = "b".repeat(64);
        let report = compare(&authoritative, &candidate);

        assert_eq!(report.classification(), RuntimeDifferentialClassification::InvariantViolation);
        assert_eq!(report.context_safety(), RuntimeDifferentialOutcome::InvariantViolation);
    }

    #[test]
    fn selection_and_catalog_differences_are_risky_without_payload_diffs() {
        let authoritative = plan('a', 8_192);
        let mut candidate = authoritative.clone();
        candidate.digests.runtime_selection_sha256 = "b".repeat(64);
        candidate.digests.tool_catalog_sha256 = "c".repeat(64);
        let report = compare(&authoritative, &candidate);

        assert_eq!(report.classification(), RuntimeDifferentialClassification::Risky);
        assert_eq!(report.runtime_selection, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(report.tool_catalog, RuntimeDifferentialOutcome::RiskyDifference);
    }

    #[test]
    fn sealed_planner_independently_exposes_routing_context_catalog_and_phase_differences() {
        let authoritative = semantic_inputs("a", 'a', 'a', false, 8_192, 0)
            .into_authoritative_snapshot(generation(7), canonical_phases(false))
            .expect("authoritative semantics should project");

        let routing_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("b", 'a', 'a', false, 8_192, 0)),
        );
        assert_eq!(routing_report.runtime_selection, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(routing_report.classification(), RuntimeDifferentialClassification::Risky);

        let context_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'b', 'a', false, 8_192, 0)),
        );
        assert_eq!(context_report.context_segments, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(context_report.classification(), RuntimeDifferentialClassification::Risky);

        let catalog_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'a', 'b', false, 8_192, 0)),
        );
        assert_eq!(catalog_report.tool_catalog, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(catalog_report.classification(), RuntimeDifferentialClassification::Risky);

        let phase_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'a', 'a', false, 8_192, 1)),
        );
        assert_eq!(phase_report.phase_plan, RuntimeDifferentialOutcome::InvariantViolation);
        assert_eq!(
            phase_report.classification(),
            RuntimeDifferentialClassification::InvariantViolation
        );
    }

    #[test]
    fn v2_planner_mutations_never_rewrite_the_observed_legacy_projection() {
        let authoritative = semantic_inputs("a", 'a', 'a', false, 8_192, 0)
            .into_authoritative_snapshot(generation(7), canonical_phases(false))
            .expect("authoritative semantics should project");
        let legacy_before = authoritative.clone();

        let context_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'b', 'a', false, 8_192, 0)),
        );
        assert_eq!(context_report.context_segments, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(authoritative, legacy_before);

        let catalog_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'a', 'b', false, 8_192, 0)),
        );
        assert_eq!(catalog_report.tool_catalog, RuntimeDifferentialOutcome::RiskyDifference);
        assert_eq!(authoritative, legacy_before);

        let policy_report = compare(
            &authoritative,
            &candidate_plan(candidate_inputs("a", 'a', 'a', true, 8_192, 0)),
        );
        assert_eq!(policy_report.policy_input, RuntimeDifferentialOutcome::InvariantViolation);
        assert_eq!(authoritative, legacy_before);
    }

    #[test]
    fn plan_validation_bounds_budget_phases_and_digest_vocabulary() {
        assert!(ShadowPlanSnapshotV1::new(
            generation(7),
            digests('a'),
            MAX_SHADOW_TOKEN_BUDGET + 1,
            vec![RuntimeErrorPhase::RuntimeSelection],
        )
        .is_err());
        assert!(ShadowPlanSnapshotV1::new(generation(7), digests('a'), 1, Vec::new()).is_err());
        assert!(ShadowPlanSnapshotV1::new(
            generation(7),
            digests('a'),
            1,
            vec![RuntimeErrorPhase::ProviderCall; MAX_SHADOW_PHASE_PLAN_ENTRIES + 1],
        )
        .is_err());
        assert!(ShadowPlanDigestsV1::new(
            "A".repeat(64),
            "b".repeat(64),
            "c".repeat(64),
            "d".repeat(64),
            "e".repeat(64),
        )
        .is_err());
    }

    #[test]
    fn comparison_rejects_cross_generation_capabilities_or_plans() {
        let authority = ShadowAuthorityToken::new(generation(7));
        let authoritative = plan('a', 8_192);
        let mut candidate = authoritative.clone();
        candidate.generation = generation(8);

        assert_eq!(
            compare_shadow_plans(
                authority.selection(),
                authority.plan(),
                &authoritative,
                &candidate,
            ),
            Err(RuntimeDifferentialError::GenerationMismatch)
        );
    }

    #[test]
    fn report_serialization_is_fixed_size_snake_case_and_hash_free() {
        let authoritative = plan('a', 8_192);
        let report = compare(&authoritative, &authoritative);
        let encoded = serde_json::to_string(&report).expect("report should serialize");

        assert_eq!(
            encoded,
            concat!(
                r#"{"schema_version":1,"generation":7,"classification":"expected","#,
                r#""reason_code":"runtime.shadow.differential_expected","#,
                r#""runtime_selection":"match","context_segments":"match","#,
                r#""context_safety":"match","token_budget":"match","#,
                r#""tool_catalog":"match","policy_input":"match","phase_plan":"match"}"#
            )
        );
        assert!(!encoded.contains(&"a".repeat(64)));
        let decoded: RuntimeDifferentialReportV1 =
            serde_json::from_str(encoded.as_str()).expect("report should validate");
        assert_eq!(decoded, report);
    }

    #[test]
    fn report_deserialization_rejects_forged_classification_and_unknown_fields() {
        let forged = json!({
            "schema_version": 1,
            "generation": 7,
            "classification": "expected",
            "reason_code": "runtime.shadow.differential_expected",
            "runtime_selection": "match",
            "context_segments": "match",
            "context_safety": "invariant_violation",
            "token_budget": "match",
            "tool_catalog": "match",
            "policy_input": "match",
            "phase_plan": "match"
        });
        assert!(serde_json::from_value::<RuntimeDifferentialReportV1>(forged).is_err());

        let unknown = json!({
            "schema_version": 1,
            "generation": 7,
            "classification": "expected",
            "reason_code": "runtime.shadow.differential_expected",
            "runtime_selection": "match",
            "context_segments": "match",
            "context_safety": "match",
            "token_budget": "match",
            "tool_catalog": "match",
            "policy_input": "match",
            "phase_plan": "match",
            "raw_prompt_diff": "forbidden"
        });
        assert!(serde_json::from_value::<RuntimeDifferentialReportV1>(unknown).is_err());
    }

    #[test]
    fn shadow_plan_deserialization_validates_all_bounds() {
        let oversized = json!({
            "schema_version": 1,
            "generation": 7,
            "digests": {
                "runtime_selection_sha256": "a".repeat(64),
                "context_segments_sha256": "b".repeat(64),
                "context_safety_sha256": "c".repeat(64),
                "tool_catalog_sha256": "d".repeat(64),
                "policy_input_sha256": "e".repeat(64)
            },
            "token_budget": MAX_SHADOW_TOKEN_BUDGET + 1,
            "expected_phase_plan": ["provider_call"]
        });

        assert!(serde_json::from_value::<ShadowPlanSnapshotV1>(oversized).is_err());
    }
}
