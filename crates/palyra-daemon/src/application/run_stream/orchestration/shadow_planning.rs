//! Side-effect-free comparison planning for the retained shadow profile.
//!
//! This compatibility adapter projects the authoritative legacy plan and the
//! V2 candidate into one redacted semantic contract. It never executes either
//! plan or acquires runtime authority.

use super::*;

pub(super) struct RunStreamShadowComparisonInput<'a> {
    pub(super) routing: &'a RoutingDecision,
    pub(super) gateway: &'a GatewayProviderSelectionSnapshot,
    pub(super) legacy: LegacyShadowPlanObservation<'a>,
    pub(super) v2_pre_context: ShadowV2PreContextInputV1,
    pub(super) v2_catalog: &'a ModelVisibleToolCatalogSnapshot,
}

#[derive(Clone, Copy)]
pub(super) struct LegacyShadowPlanObservation<'a> {
    pub(super) request: &'a ProviderRequest,
    pub(super) instruction_trust_summary:
        Option<&'a crate::application::instruction_compiler::InstructionTrustSummary>,
    pub(super) catalog: &'a ModelVisibleToolCatalogSnapshot,
}

pub(super) fn run_stream_shadow_comparison_plans(
    generation: palyra_common::runtime_contracts::RuntimeGeneration,
    input: RunStreamShadowComparisonInput<'_>,
) -> Result<ShadowComparisonPlansV1, RuntimeDifferentialError> {
    let authoritative_semantics = legacy_shadow_plan_semantics(
        ShadowSelectionSemanticV1::new(
            input.routing.provider_id.clone(),
            input.routing.actual_model_id.clone(),
            input.routing.credential_id.clone(),
            input.routing.health_state.clone(),
        )?,
        input.legacy,
        &input.v2_pre_context,
    )?;
    let candidate = v2_shadow_candidate_plan(
        generation,
        input.gateway,
        input.v2_pre_context,
        input.v2_catalog,
    )?;
    let mut authoritative_phase_plan = vec![
        RuntimeErrorPhase::Admission,
        RuntimeErrorPhase::RuntimeSelection,
        RuntimeErrorPhase::ContextAssembly,
        RuntimeErrorPhase::ProviderCall,
    ];
    if input.legacy.catalog.exposed_tool_count > 0 {
        authoritative_phase_plan.extend([
            RuntimeErrorPhase::ToolGate,
            RuntimeErrorPhase::Approval,
            RuntimeErrorPhase::ToolExecution,
            RuntimeErrorPhase::ResultProjection,
            RuntimeErrorPhase::Compaction,
        ]);
    }
    authoritative_phase_plan.extend([
        RuntimeErrorPhase::Verification,
        RuntimeErrorPhase::Finalization,
        RuntimeErrorPhase::DeliveryIntent,
    ]);
    let authoritative = authoritative_semantics
        .into_authoritative_snapshot(generation, authoritative_phase_plan)?;
    let candidate = ShadowCandidatePlannerV1::new(candidate);
    ShadowComparisonPlansV1::new(authoritative, candidate)
}

pub(super) fn selected_v2_shadow_route_semantics(
    route_selection: &ProviderRouteSelectionTrace,
) -> Result<ShadowSelectionSemanticV1, RuntimeDifferentialError> {
    let mut selected_routes = route_selection
        .candidates
        .iter()
        .filter(|candidate| candidate.role == "chat" && candidate.selected);
    let selected_route = selected_routes.next().ok_or(RuntimeDifferentialError::InvalidPlan)?;
    if selected_routes.next().is_some() {
        return Err(RuntimeDifferentialError::InvalidPlan);
    }
    ShadowSelectionSemanticV1::new(
        selected_route.provider_id.clone(),
        selected_route.model_id.clone(),
        selected_route.credential_id.clone(),
        selected_route.health_state.clone(),
    )
}

fn legacy_shadow_plan_semantics(
    selection: ShadowSelectionSemanticV1,
    input: LegacyShadowPlanObservation<'_>,
    pre_context: &ShadowV2PreContextInputV1,
) -> Result<ShadowPlanSemanticInputsV1, RuntimeDifferentialError> {
    let context_segments = input
        .request
        .prompt_segments
        .iter()
        .map(|segment| {
            ShadowContextSegmentSemanticV1::new(
                shadow_prompt_segment_kind(segment.kind).to_owned(),
                segment.content_hash.clone(),
                u64::try_from(segment.byte_len)
                    .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
                segment.trust_label.clone(),
                shadow_prompt_cache_hint(segment.cache_hint).to_owned(),
                segment.invalidation_reason.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let instruction_trust = input
        .instruction_trust_summary
        .map(|summary| {
            ShadowInstructionTrustSemanticV1::new(
                u32::try_from(summary.selected_blocks)
                    .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
                u32::try_from(summary.untrusted_blocks)
                    .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
                summary.mixed_trust,
                summary.highest_safety_action.as_str().to_owned(),
                u32::try_from(summary.prompt_injection_finding_count)
                    .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
            )
        })
        .transpose()?;
    let catalog_semantics = ShadowToolCatalogSemanticV1::new(
        input.catalog.catalog_hash.clone(),
        input.catalog.exposure_mode.as_str().to_owned(),
        u32::try_from(input.catalog.exposed_tool_count)
            .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
    )?;
    let max_output_tokens = input
        .request
        .max_output_tokens
        .map(u32::try_from)
        .transpose()
        .map_err(|_| RuntimeDifferentialError::InvalidPlan)?;
    let policy = pre_context
        .plan_policy_with_host_limits(input.request.budget_profile.clone(), max_output_tokens)?;
    ShadowPlanSemanticInputsV1::new(
        selection,
        context_segments,
        input.request.instruction_hash.clone(),
        instruction_trust,
        runtime_kernel_provider_request_input_tokens(input.request),
        catalog_semantics,
        policy,
    )
}

fn v2_shadow_candidate_plan(
    generation: palyra_common::runtime_contracts::RuntimeGeneration,
    gateway: &GatewayProviderSelectionSnapshot,
    pre_context: ShadowV2PreContextInputV1,
    catalog: &ModelVisibleToolCatalogSnapshot,
) -> Result<ShadowCandidatePlanInputsV1, RuntimeDifferentialError> {
    let route_selection = &gateway.status.route_selection;
    let (selected_route, selected_provider_kind) = selected_v2_shadow_catalog_binding(gateway)?;
    if !gateway.health_authority_by_provider.contains_key(selected_route.provider_id.as_str())
        || !gateway
            .embedded_harness_descriptors
            .iter()
            .any(|descriptor| descriptor.embedded_default)
        || !gateway.context_engine_registry.engines.iter().any(|descriptor| {
            descriptor.engine_id == gateway.context_engine_registry.selected_engine_id
        })
    {
        return Err(RuntimeDifferentialError::InvalidPlan);
    }
    if !shadow_catalog_matches_selected_v2_route(
        catalog.provider_kind.as_str(),
        catalog.provider_model_id.as_deref(),
        selected_provider_kind,
        selected_route.model_id.as_str(),
    ) {
        return Err(RuntimeDifferentialError::InvalidPlan);
    }

    let sealed_catalog = SealedToolCatalogSelectionV1::from_registry_snapshot(
        catalog,
        gateway.configuration_epoch.get(),
    )
    .map_err(|_| RuntimeDifferentialError::InvalidPlan)?;
    let catalog_semantics = ShadowToolCatalogSemanticV1::new(
        sealed_catalog.catalog_hash().as_str().to_owned(),
        catalog.exposure_mode.as_str().to_owned(),
        u32::try_from(catalog.exposed_tool_count)
            .map_err(|_| RuntimeDifferentialError::InvalidPlan)?,
    )?;
    Ok(ShadowCandidatePlanInputsV1::from_pre_context(
        generation,
        selected_v2_shadow_route_semantics(route_selection)?,
        pre_context,
        catalog_semantics,
    ))
}

pub(super) fn selected_v2_shadow_catalog_binding(
    gateway: &GatewayProviderSelectionSnapshot,
) -> Result<(&ProviderRouteCandidateTrace, &str), RuntimeDifferentialError> {
    let mut selected_routes = gateway
        .status
        .route_selection
        .candidates
        .iter()
        .filter(|candidate| candidate.role == "chat" && candidate.selected);
    let selected_route = selected_routes.next().ok_or(RuntimeDifferentialError::InvalidPlan)?;
    if selected_routes.next().is_some() || selected_route.capability_state != "eligible" {
        return Err(RuntimeDifferentialError::InvalidPlan);
    }

    let mut selected_providers = gateway
        .status
        .registry
        .providers
        .iter()
        .filter(|provider| provider.provider_id == selected_route.provider_id);
    let selected_provider =
        selected_providers.next().ok_or(RuntimeDifferentialError::InvalidPlan)?;
    if selected_providers.next().is_some() || !selected_provider.enabled {
        return Err(RuntimeDifferentialError::InvalidPlan);
    }
    Ok((selected_route, selected_provider.kind.as_str()))
}

pub(super) fn shadow_catalog_matches_selected_v2_route(
    catalog_provider_kind: &str,
    catalog_model_id: Option<&str>,
    selected_provider_kind: &str,
    selected_model_id: &str,
) -> bool {
    catalog_provider_kind == selected_provider_kind && catalog_model_id == Some(selected_model_id)
}

const fn shadow_prompt_segment_kind(kind: ProviderPromptSegmentKind) -> &'static str {
    match kind {
        ProviderPromptSegmentKind::System => "system",
        ProviderPromptSegmentKind::Tool => "tool",
        ProviderPromptSegmentKind::Policy => "policy",
        ProviderPromptSegmentKind::Project => "project",
        ProviderPromptSegmentKind::Memory => "memory",
        ProviderPromptSegmentKind::Session => "session",
        ProviderPromptSegmentKind::Tail => "tail",
        ProviderPromptSegmentKind::CurrentTurn => "current_turn",
    }
}

const fn shadow_prompt_cache_hint(hint: ProviderPromptCacheHint) -> &'static str {
    match hint {
        ProviderPromptCacheHint::LongLived => "long_lived",
        ProviderPromptCacheHint::ShortLived => "short_lived",
        ProviderPromptCacheHint::Volatile => "volatile",
        ProviderPromptCacheHint::Sensitive => "sensitive",
        ProviderPromptCacheHint::Disabled => "disabled",
    }
}
