//! Monotonic, bounded recovery planning for provider context pressure.
//!
//! The controller owns ordering and replay evidence only. Host orchestration
//! executes the selected mutation, reports actual token deltas, and retains
//! full tool evidence outside the provider prompt.

use std::collections::{BTreeMap, BTreeSet};

use palyra_model_providers::provider_request_has_vision;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use ulid::Ulid;

use crate::{
    application::tool_registry::ModelVisibleToolCatalogSnapshot,
    model_provider::{
        ProviderMessage, ProviderMessageContentPart, ProviderMessageRole, ProviderRequest,
        ProviderStatusSnapshot,
    },
    orchestrator::estimate_token_count,
};

pub(crate) const CONTEXT_RECOVERY_SCHEMA_VERSION: u16 = 1;
pub(crate) const CONTEXT_RECOVERY_EVENT: &str = "context.recovery.plan";
const DEFAULT_PROVIDER_CONTEXT_LIMIT_TOKENS: u64 = 8_192;
const DEFAULT_RECOVERY_STEP_BUDGET: u8 = 5;
const DEFAULT_MINIMUM_PROGRESS_TOKENS: u64 = 128;
const TOOL_OUTPUT_SUMMARY_CHARS: usize = 384;
const RECENT_TOOL_OUTPUTS_TO_KEEP: usize = 2;

/// Aggregate token category with a closed protection policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TokenBreakdownCategory {
    SystemInstructions,
    SafetyPolicy,
    CurrentTurn,
    SessionHistory,
    ToolResults,
    ToolSchemas,
    MemoryContext,
    ProjectContext,
    Attachments,
}

impl TokenBreakdownCategory {
    #[must_use]
    const fn protected(self) -> bool {
        matches!(self, Self::SystemInstructions | Self::SafetyPolicy | Self::CurrentTurn)
    }
}

/// One redacted token aggregate; no prompt or tool content is retained.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenBreakdownItem {
    pub(crate) category: TokenBreakdownCategory,
    pub(crate) estimated_tokens: u64,
    pub(crate) protected: bool,
    pub(crate) artifact_backed: bool,
}

/// Complete prompt estimate split into protected and evictable aggregates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenBreakdown {
    pub(crate) items: Vec<TokenBreakdownItem>,
}

impl TokenBreakdown {
    #[must_use]
    pub(crate) fn total_tokens(&self) -> u64 {
        self.items.iter().map(|item| item.estimated_tokens).fold(0_u64, u64::saturating_add)
    }

    fn protected_categories(&self) -> BTreeSet<TokenBreakdownCategory> {
        self.items.iter().filter(|item| item.protected).map(|item| item.category).collect()
    }

    fn validate(&self) -> Result<(), &'static str> {
        let mut categories = BTreeSet::new();
        for item in &self.items {
            if item.protected != item.category.protected() || !categories.insert(item.category) {
                return Err("context.recovery.token_breakdown_invalid");
            }
        }
        Ok(())
    }
}

/// Larger-window route admitted by both provider policy and cost budget.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRouteFallback {
    pub(crate) provider_id: String,
    pub(crate) model_id: String,
    pub(crate) context_window_tokens: u64,
    pub(crate) policy_allowed: bool,
    pub(crate) cost_allowed: bool,
    pub(crate) tool_catalog_hash: String,
}

/// Closed recovery budget for one generation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRecoveryBudget {
    pub(crate) max_steps: u8,
    pub(crate) minimum_progress_tokens: u64,
}

impl Default for ContextRecoveryBudget {
    fn default() -> Self {
        Self {
            max_steps: DEFAULT_RECOVERY_STEP_BUDGET,
            minimum_progress_tokens: DEFAULT_MINIMUM_PROGRESS_TOKENS,
        }
    }
}

/// Immutable controller input for one context projection generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRecoveryInput {
    pub(crate) provider_limit_tokens: u64,
    pub(crate) reserved_output_tokens: u64,
    pub(crate) schema_cost_tokens: u64,
    pub(crate) breakdown: TokenBreakdown,
    pub(crate) provider_overflow_observed: bool,
    pub(crate) prior_generation: u64,
    pub(crate) recovery_budget: ContextRecoveryBudget,
    pub(crate) route_fallback: Option<ContextRouteFallback>,
}

impl ContextRecoveryInput {
    pub(crate) fn required_tokens(&self) -> u64 {
        self.breakdown
            .total_tokens()
            .saturating_add(self.schema_cost_tokens)
            .saturating_add(self.reserved_output_tokens)
    }

    fn validate(&self) -> Result<(), &'static str> {
        self.breakdown.validate()?;
        if self.provider_limit_tokens == 0
            || self.recovery_budget.max_steps == 0
            || self.recovery_budget.max_steps > DEFAULT_RECOVERY_STEP_BUDGET
            || self.recovery_budget.minimum_progress_tokens == 0
        {
            return Err("context.recovery.input_invalid");
        }
        if self.route_fallback.as_ref().is_some_and(|route| {
            route.provider_id.trim().is_empty()
                || route.model_id.trim().is_empty()
                || route.context_window_tokens <= self.provider_limit_tokens
                || route.tool_catalog_hash.len() != 64
        }) {
            return Err("context.recovery.route_fallback_invalid");
        }
        Ok(())
    }
}

/// Ordered recovery action; host authority never changes across variants.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContextRecoveryAction {
    Compact,
    TruncateOldToolTails,
    ReduceOptionalContext,
    RouteLargerWindow,
    FailDeterministic,
}

/// One replayable request for host mutation or terminal failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRecoveryStep {
    pub(crate) step_id: String,
    pub(crate) generation: u64,
    pub(crate) ordinal: u8,
    pub(crate) action: ContextRecoveryAction,
    pub(crate) before_tokens: u64,
    pub(crate) provider_limit_tokens: u64,
    pub(crate) reason_code: String,
}

/// Actual host-observed outcome for one recovery step.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRecoveryStepOutcome {
    pub(crate) step_id: String,
    pub(crate) action: ContextRecoveryAction,
    pub(crate) accepted: bool,
    pub(crate) terminal: bool,
    pub(crate) before_tokens: u64,
    pub(crate) after_tokens: u64,
    pub(crate) token_delta: u64,
    pub(crate) removed_categories: Vec<TokenBreakdownCategory>,
    pub(crate) evidence_retained: bool,
    pub(crate) route_fallback: Option<ContextRouteFallback>,
    pub(crate) host_reason_code: Option<String>,
    pub(crate) reason_code: String,
}

/// Durable aggregate describing one bounded recovery generation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ContextRecoveryPlan {
    pub(crate) schema_version: u16,
    pub(crate) event_type: String,
    pub(crate) plan_id: String,
    pub(crate) generation: u64,
    pub(crate) input_sha256: String,
    pub(crate) initial_tokens: u64,
    pub(crate) estimated_initial_tokens: u64,
    pub(crate) initial_provider_limit_tokens: u64,
    pub(crate) reserved_output_tokens: u64,
    pub(crate) schema_cost_tokens: u64,
    pub(crate) token_breakdown: TokenBreakdown,
    pub(crate) provider_overflow_observed: bool,
    pub(crate) protected_categories: Vec<TokenBreakdownCategory>,
    pub(crate) steps: Vec<ContextRecoveryStep>,
    pub(crate) outcomes: Vec<ContextRecoveryStepOutcome>,
    pub(crate) terminal_reason_code: Option<String>,
}

impl ContextRecoveryPlan {
    #[must_use]
    pub(crate) fn tape_payload(&self) -> Value {
        json!({
            "schema_version": self.schema_version,
            "event": self.event_type,
            "redaction_level": "aggregate_token_counts_and_hashes",
            "plan_id": self.plan_id,
            "generation": self.generation,
            "input_sha256": self.input_sha256,
            "initial_tokens": self.initial_tokens,
            "estimated_initial_tokens": self.estimated_initial_tokens,
            "initial_provider_limit_tokens": self.initial_provider_limit_tokens,
            "reserved_output_tokens": self.reserved_output_tokens,
            "schema_cost_tokens": self.schema_cost_tokens,
            "token_breakdown": self.token_breakdown,
            "provider_overflow_observed": self.provider_overflow_observed,
            "protected_categories": self.protected_categories,
            "steps": self.steps,
            "outcomes": self.outcomes,
            "terminal_reason_code": self.terminal_reason_code,
        })
    }
}

/// Side-effect-free state machine that emits every action at most once.
pub(crate) struct ContextRecoveryController {
    input: ContextRecoveryInput,
    current_tokens: u64,
    current_limit_tokens: u64,
    action_cursor: usize,
    pending_step_id: Option<String>,
    plan: ContextRecoveryPlan,
}

impl ContextRecoveryController {
    /// Creates a controller for one immutable input generation.
    ///
    /// # Errors
    /// Rejects malformed budgets, duplicated/incorrect category protection,
    /// and invalid fallback routes.
    pub(crate) fn new(input: ContextRecoveryInput) -> Result<Self, &'static str> {
        input.validate()?;
        let estimated_initial_tokens = input.required_tokens();
        let initial_tokens = if input.provider_overflow_observed {
            estimated_initial_tokens.max(input.provider_limit_tokens.saturating_add(1))
        } else {
            estimated_initial_tokens
        };
        let generation = input.prior_generation.saturating_add(1).max(1);
        let input_sha256 =
            crate::sha256_hex(serde_json::to_vec(&input).unwrap_or_default().as_slice());
        let protected_categories =
            input.breakdown.protected_categories().into_iter().collect::<Vec<_>>();
        let reserved_output_tokens = input.reserved_output_tokens;
        let schema_cost_tokens = input.schema_cost_tokens;
        let token_breakdown = input.breakdown.clone();
        let provider_overflow_observed = input.provider_overflow_observed;
        Ok(Self {
            current_tokens: initial_tokens,
            current_limit_tokens: input.provider_limit_tokens,
            input,
            action_cursor: 0,
            pending_step_id: None,
            plan: ContextRecoveryPlan {
                schema_version: CONTEXT_RECOVERY_SCHEMA_VERSION,
                event_type: CONTEXT_RECOVERY_EVENT.to_owned(),
                plan_id: Ulid::generate().to_string(),
                generation,
                input_sha256,
                initial_tokens,
                estimated_initial_tokens,
                initial_provider_limit_tokens: 0,
                reserved_output_tokens,
                schema_cost_tokens,
                token_breakdown,
                provider_overflow_observed,
                protected_categories,
                steps: Vec::new(),
                outcomes: Vec::new(),
                terminal_reason_code: None,
            },
        }
        .with_initial_limit())
    }

    fn with_initial_limit(mut self) -> Self {
        self.plan.initial_provider_limit_tokens = self.current_limit_tokens;
        self
    }

    #[must_use]
    pub(crate) fn requires_recovery(&self) -> bool {
        self.input.provider_overflow_observed || self.current_tokens > self.current_limit_tokens
    }

    #[must_use]
    pub(crate) fn current_tokens(&self) -> u64 {
        self.current_tokens
    }

    #[must_use]
    pub(crate) fn plan(&self) -> &ContextRecoveryPlan {
        &self.plan
    }

    pub(crate) fn record_provider_success(&mut self) {
        if !self.plan.steps.is_empty() && self.pending_step_id.is_none() {
            self.plan.terminal_reason_code =
                Some("context.recovery.provider_retry_succeeded".to_owned());
        }
    }

    /// Returns the next untried action or closes the plan.
    ///
    /// # Errors
    /// Fails when the prior step is unresolved.
    pub(crate) fn next_step(&mut self) -> Result<Option<ContextRecoveryStep>, &'static str> {
        if self.pending_step_id.is_some() {
            return Err("context.recovery.step_already_pending");
        }
        if self.plan.terminal_reason_code.is_some() {
            return Ok(None);
        }
        if !self.requires_recovery() {
            self.plan.terminal_reason_code =
                Some("context.recovery.within_provider_limit".to_owned());
            return Ok(None);
        }
        let actions = [
            ContextRecoveryAction::Compact,
            ContextRecoveryAction::TruncateOldToolTails,
            ContextRecoveryAction::ReduceOptionalContext,
            ContextRecoveryAction::RouteLargerWindow,
            ContextRecoveryAction::FailDeterministic,
        ];
        let max_steps = usize::from(self.input.recovery_budget.max_steps);
        if self.action_cursor >= actions.len() || self.action_cursor >= max_steps {
            self.plan.terminal_reason_code = Some("context.recovery.budget_exhausted".to_owned());
            return Ok(None);
        }
        let mut action = actions[self.action_cursor];
        self.action_cursor += 1;
        if action == ContextRecoveryAction::RouteLargerWindow && self.input.route_fallback.is_none()
        {
            if self.action_cursor >= actions.len() || self.action_cursor >= max_steps {
                self.plan.terminal_reason_code =
                    Some("context.recovery.route_unavailable_budget_exhausted".to_owned());
                return Ok(None);
            }
            action = actions[self.action_cursor];
            self.action_cursor += 1;
        }
        let ordinal = u8::try_from(self.plan.steps.len().saturating_add(1)).unwrap_or(u8::MAX);
        let step = ContextRecoveryStep {
            step_id: Ulid::generate().to_string(),
            generation: self.plan.generation,
            ordinal,
            action,
            before_tokens: self.current_tokens,
            provider_limit_tokens: self.current_limit_tokens,
            reason_code: match action {
                ContextRecoveryAction::Compact => "context.recovery.compact_requested",
                ContextRecoveryAction::TruncateOldToolTails => {
                    "context.recovery.tool_tail_truncation_requested"
                }
                ContextRecoveryAction::ReduceOptionalContext => {
                    "context.recovery.optional_context_reduction_requested"
                }
                ContextRecoveryAction::RouteLargerWindow => {
                    "context.recovery.larger_window_route_requested"
                }
                ContextRecoveryAction::FailDeterministic => {
                    "context.recovery.deterministic_failure"
                }
            }
            .to_owned(),
        };
        self.pending_step_id = Some(step.step_id.clone());
        self.plan.steps.push(step.clone());
        Ok(Some(step))
    }

    /// Records actual host progress and advances the monotonic estimate.
    ///
    /// # Errors
    /// Rejects stale steps, protected-category removal, missing evidence for
    /// tool-tail reduction, or unauthorized route growth.
    pub(crate) fn record_outcome(
        &mut self,
        step: &ContextRecoveryStep,
        after_tokens: u64,
        removed_categories: Vec<TokenBreakdownCategory>,
        evidence_retained: bool,
    ) -> Result<ContextRecoveryStepOutcome, &'static str> {
        self.record_outcome_with_host_reason(
            step,
            after_tokens,
            removed_categories,
            evidence_retained,
            None,
        )
    }

    /// Records an outcome together with a bounded host classification.
    ///
    /// # Errors
    /// Applies the same identity, protection, evidence, and route checks as
    /// [`Self::record_outcome`].
    pub(crate) fn record_outcome_with_host_reason(
        &mut self,
        step: &ContextRecoveryStep,
        after_tokens: u64,
        removed_categories: Vec<TokenBreakdownCategory>,
        evidence_retained: bool,
        host_reason_code: Option<&str>,
    ) -> Result<ContextRecoveryStepOutcome, &'static str> {
        if self.pending_step_id.as_deref() != Some(step.step_id.as_str())
            || step.before_tokens != self.current_tokens
        {
            return Err("context.recovery.step_identity_mismatch");
        }
        let protected = self.input.breakdown.protected_categories();
        if removed_categories.iter().any(|category| protected.contains(category)) {
            return Err("context.recovery.protected_segment_removal_denied");
        }
        if removed_categories.contains(&TokenBreakdownCategory::ToolResults) && !evidence_retained {
            return Err("context.recovery.tool_evidence_missing");
        }
        let mut route_fallback = None;
        let mut accepted = false;
        let mut terminal = false;
        let reason_code = match step.action {
            ContextRecoveryAction::RouteLargerWindow => {
                let route = self
                    .input
                    .route_fallback
                    .clone()
                    .ok_or("context.recovery.route_fallback_unavailable")?;
                if !route.policy_allowed || !route.cost_allowed {
                    return Err("context.recovery.route_fallback_not_authorized");
                }
                self.current_limit_tokens = route.context_window_tokens;
                route_fallback = Some(route);
                accepted = self.current_tokens <= self.current_limit_tokens;
                terminal = accepted;
                if accepted {
                    "context.recovery.larger_window_route_applied"
                } else {
                    "context.recovery.larger_window_route_insufficient"
                }
            }
            ContextRecoveryAction::FailDeterministic => {
                terminal = true;
                "context.recovery.exhausted_fail_deterministic"
            }
            _ => {
                let token_delta = self.current_tokens.saturating_sub(after_tokens);
                if after_tokens > self.current_tokens {
                    "context.recovery.step_estimate_increased"
                } else if token_delta >= self.input.recovery_budget.minimum_progress_tokens {
                    self.current_tokens = after_tokens;
                    accepted = true;
                    terminal = self.current_tokens <= self.current_limit_tokens;
                    if terminal {
                        "context.recovery.step_reached_provider_limit"
                    } else {
                        "context.recovery.step_progressed"
                    }
                } else {
                    "context.recovery.step_no_progress"
                }
            }
        }
        .to_owned();
        let outcome = ContextRecoveryStepOutcome {
            step_id: step.step_id.clone(),
            action: step.action,
            accepted,
            terminal,
            before_tokens: step.before_tokens,
            after_tokens: if matches!(step.action, ContextRecoveryAction::RouteLargerWindow) {
                self.current_tokens
            } else {
                after_tokens
            },
            token_delta: step.before_tokens.saturating_sub(after_tokens),
            removed_categories,
            evidence_retained,
            route_fallback,
            host_reason_code: host_reason_code.map(ToOwned::to_owned),
            reason_code: reason_code.clone(),
        };
        self.pending_step_id = None;
        self.plan.outcomes.push(outcome.clone());
        if terminal {
            self.plan.terminal_reason_code = Some(reason_code);
        }
        Ok(outcome)
    }
}

/// Builds a controller input from the provider-visible request and live route
/// snapshot. The selected fallback must have a strictly larger context window
/// and must not raise the configured cost tier.
#[must_use]
pub(crate) fn context_recovery_input_for_request(
    request: &ProviderRequest,
    provider_snapshot: &ProviderStatusSnapshot,
    selected_provider_id: &str,
    selected_model_id: &str,
    catalog: &ModelVisibleToolCatalogSnapshot,
    provider_overflow_observed: bool,
    prior_generation: u64,
) -> ContextRecoveryInput {
    let selected_model = provider_snapshot.registry.models.iter().find(|model| {
        model.enabled
            && model.provider_id == selected_provider_id
            && model.model_id == selected_model_id
    });
    let provider_limit_tokens = provider_context_limit_tokens_for_route(
        provider_snapshot,
        selected_provider_id,
        selected_model_id,
    );
    let selected_cost_tier = selected_model
        .map(|model| model.capabilities.cost_tier.as_str())
        .unwrap_or(provider_snapshot.capabilities.cost_tier.as_str());
    let route_fallback = provider_snapshot
        .registry
        .models
        .iter()
        .filter(|model| {
            model.enabled && model.role == "chat" && model.provider_id == selected_provider_id
        })
        .filter(|model| model_supports_recovery_request(request, &model.capabilities))
        .filter_map(|model| {
            let context_window_tokens = u64::from(model.capabilities.max_context_tokens?);
            (context_window_tokens > provider_limit_tokens).then_some(ContextRouteFallback {
                provider_id: model.provider_id.clone(),
                model_id: model.model_id.clone(),
                context_window_tokens,
                policy_allowed: provider_snapshot.registry.failover_enabled,
                cost_allowed: cost_tier_rank(model.capabilities.cost_tier.as_str())
                    <= cost_tier_rank(selected_cost_tier),
                tool_catalog_hash: catalog.catalog_hash.clone(),
            })
        })
        .filter(|route| route.policy_allowed && route.cost_allowed)
        .min_by(|left, right| {
            left.context_window_tokens
                .cmp(&right.context_window_tokens)
                .then_with(|| left.model_id.cmp(&right.model_id))
        });
    ContextRecoveryInput {
        provider_limit_tokens,
        reserved_output_tokens: request.max_output_tokens.unwrap_or_default(),
        schema_cost_tokens: bytes_to_tokens(catalog.estimated_exposed_tool_bytes),
        breakdown: token_breakdown_for_request(request),
        provider_overflow_observed,
        prior_generation,
        recovery_budget: ContextRecoveryBudget::default(),
        route_fallback,
    }
}

#[must_use]
pub(crate) fn provider_context_limit_tokens_for_route(
    provider_snapshot: &ProviderStatusSnapshot,
    selected_provider_id: &str,
    selected_model_id: &str,
) -> u64 {
    provider_snapshot
        .registry
        .models
        .iter()
        .find(|model| {
            model.enabled
                && model.provider_id == selected_provider_id
                && model.model_id == selected_model_id
        })
        .and_then(|model| model.capabilities.max_context_tokens)
        .or(provider_snapshot.capabilities.max_context_tokens)
        .map(u64::from)
        .unwrap_or(DEFAULT_PROVIDER_CONTEXT_LIMIT_TOKENS)
}

#[must_use]
pub(crate) fn estimated_required_tokens_for_request(
    request: &ProviderRequest,
    catalog: &ModelVisibleToolCatalogSnapshot,
) -> u64 {
    token_breakdown_for_request(request)
        .total_tokens()
        .saturating_add(bytes_to_tokens(catalog.estimated_exposed_tool_bytes))
        .saturating_add(request.max_output_tokens.unwrap_or_default())
}

/// Preflight result after the complete bounded recovery ladder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ContextPreflightRecoveryOutcome {
    NotRequired,
    Recovered { plan: ContextRecoveryPlan },
    Exhausted { plan: ContextRecoveryPlan },
}

/// Applies context-only recovery before a provider side effect can start.
///
/// The context engine has already had its normal compaction opportunity when
/// this function runs, so the first ladder step records zero progress and then
/// advances to prompt-local reductions. Route fallback is restricted to a
/// larger model on the already-authorized provider.
///
/// # Errors
/// Returns a stable controller error when an invariant is violated.
pub(crate) fn recover_provider_request_preflight(
    request: &mut ProviderRequest,
    provider_snapshot: &ProviderStatusSnapshot,
    selected_provider_id: &str,
    selected_model_id: &str,
    catalog: &ModelVisibleToolCatalogSnapshot,
    prior_generation: u64,
) -> Result<ContextPreflightRecoveryOutcome, &'static str> {
    recover_provider_request(
        request,
        provider_snapshot,
        selected_provider_id,
        selected_model_id,
        catalog,
        false,
        prior_generation,
    )
}

/// Applies the same bounded ladder after the provider explicitly rejects the
/// estimated context size.
///
/// # Errors
/// Returns a stable controller error when an invariant is violated.
pub(crate) fn recover_provider_request_after_overflow(
    request: &mut ProviderRequest,
    provider_snapshot: &ProviderStatusSnapshot,
    selected_provider_id: &str,
    selected_model_id: &str,
    catalog: &ModelVisibleToolCatalogSnapshot,
    prior_generation: u64,
) -> Result<ContextPreflightRecoveryOutcome, &'static str> {
    recover_provider_request(
        request,
        provider_snapshot,
        selected_provider_id,
        selected_model_id,
        catalog,
        true,
        prior_generation,
    )
}

fn recover_provider_request(
    request: &mut ProviderRequest,
    provider_snapshot: &ProviderStatusSnapshot,
    selected_provider_id: &str,
    selected_model_id: &str,
    catalog: &ModelVisibleToolCatalogSnapshot,
    provider_overflow_observed: bool,
    prior_generation: u64,
) -> Result<ContextPreflightRecoveryOutcome, &'static str> {
    let input = context_recovery_input_for_request(
        request,
        provider_snapshot,
        selected_provider_id,
        selected_model_id,
        catalog,
        provider_overflow_observed,
        prior_generation,
    );
    let mut controller = ContextRecoveryController::new(input)?;
    if !controller.requires_recovery() {
        return Ok(ContextPreflightRecoveryOutcome::NotRequired);
    }

    loop {
        let Some(step) = controller.next_step()? else {
            return Ok(ContextPreflightRecoveryOutcome::Exhausted {
                plan: controller.plan().clone(),
            });
        };
        let outcome = match step.action {
            ContextRecoveryAction::Compact => {
                controller.record_outcome(&step, controller.current_tokens(), Vec::new(), true)?
            }
            ContextRecoveryAction::TruncateOldToolTails => {
                let mutation = truncate_old_tool_tails(request.messages.as_mut_slice());
                controller.record_outcome(
                    &step,
                    estimated_required_tokens_for_request(request, catalog),
                    mutation.removed_categories,
                    !mutation.evidence_refs.is_empty(),
                )?
            }
            ContextRecoveryAction::ReduceOptionalContext => {
                let mutation = reduce_optional_context(&mut request.messages);
                controller.record_outcome(
                    &step,
                    estimated_required_tokens_for_request(request, catalog),
                    mutation.removed_categories,
                    true,
                )?
            }
            ContextRecoveryAction::RouteLargerWindow => {
                let route = controller
                    .input
                    .route_fallback
                    .as_ref()
                    .ok_or("context.recovery.route_fallback_unavailable")?;
                request.model_override = Some(route.model_id.clone());
                controller.record_outcome(&step, controller.current_tokens(), Vec::new(), true)?
            }
            ContextRecoveryAction::FailDeterministic => {
                controller.record_outcome(&step, controller.current_tokens(), Vec::new(), true)?
            }
        };
        if outcome.terminal {
            let plan = controller.plan().clone();
            return Ok(if outcome.action == ContextRecoveryAction::FailDeterministic {
                ContextPreflightRecoveryOutcome::Exhausted { plan }
            } else {
                ContextPreflightRecoveryOutcome::Recovered { plan }
            });
        }
    }
}

fn model_supports_recovery_request(
    request: &ProviderRequest,
    capabilities: &crate::model_provider::ProviderCapabilitiesSnapshot,
) -> bool {
    (!request.json_mode || capabilities.json_mode)
        && (!provider_request_has_vision(request) || capabilities.vision)
}

fn cost_tier_rank(tier: &str) -> u8 {
    match tier {
        "low" => 0,
        "standard" => 1,
        "premium" => 2,
        _ => 1,
    }
}

fn bytes_to_tokens(bytes: usize) -> u64 {
    u64::try_from(bytes.saturating_add(3) / 4).unwrap_or(u64::MAX)
}

fn message_tokens(message: &ProviderMessage) -> u64 {
    let text_tokens = estimate_token_count(message.text_content().as_str());
    let image_tokens = message
        .content
        .iter()
        .filter(|part| matches!(part, ProviderMessageContentPart::Image { .. }))
        .count()
        .saturating_mul(256);
    let tool_tokens = message
        .tool_calls
        .iter()
        .map(|call| estimate_token_count(call.input_json.to_string().as_str()))
        .fold(0_u64, u64::saturating_add);
    text_tokens
        .saturating_add(u64::try_from(image_tokens).unwrap_or(u64::MAX))
        .saturating_add(tool_tokens)
}

fn token_breakdown_for_request(request: &ProviderRequest) -> TokenBreakdown {
    let messages = request.effective_messages();
    let last_user_index =
        messages.iter().rposition(|message| message.role == ProviderMessageRole::User);
    let mut totals = BTreeMap::<TokenBreakdownCategory, u64>::new();
    for (index, message) in messages.iter().enumerate() {
        let category = match message.role {
            ProviderMessageRole::System => TokenBreakdownCategory::SystemInstructions,
            ProviderMessageRole::Developer => TokenBreakdownCategory::SafetyPolicy,
            ProviderMessageRole::User if Some(index) == last_user_index => {
                TokenBreakdownCategory::CurrentTurn
            }
            ProviderMessageRole::Tool => TokenBreakdownCategory::ToolResults,
            ProviderMessageRole::User | ProviderMessageRole::Assistant => {
                TokenBreakdownCategory::SessionHistory
            }
        };
        let total = totals.entry(category).or_default();
        *total = total.saturating_add(message_tokens(message));
    }
    if !request.vision_inputs.is_empty() {
        totals.insert(
            TokenBreakdownCategory::Attachments,
            u64::try_from(request.vision_inputs.len()).unwrap_or(u64::MAX).saturating_mul(256),
        );
    }
    for segment in &request.prompt_segments {
        let category = match segment.kind {
            palyra_model_providers::ProviderPromptSegmentKind::Memory => {
                Some(TokenBreakdownCategory::MemoryContext)
            }
            palyra_model_providers::ProviderPromptSegmentKind::Project => {
                Some(TokenBreakdownCategory::ProjectContext)
            }
            _ => None,
        };
        if let Some(category) = category {
            totals.entry(category).or_insert_with(|| bytes_to_tokens(segment.byte_len));
        }
    }
    let mut items = totals
        .into_iter()
        .map(|(category, estimated_tokens)| TokenBreakdownItem {
            category,
            estimated_tokens,
            protected: category.protected(),
            artifact_backed: category == TokenBreakdownCategory::ToolResults,
        })
        .collect::<Vec<_>>();
    items.push(TokenBreakdownItem {
        category: TokenBreakdownCategory::ToolSchemas,
        estimated_tokens: 0,
        protected: false,
        artifact_backed: false,
    });
    items.sort_by_key(|item| item.category);
    TokenBreakdown { items }
}

/// Mutation result containing only aggregate savings and host evidence refs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContextPromptMutation {
    pub(crate) before_tokens: u64,
    pub(crate) after_tokens: u64,
    pub(crate) removed_categories: Vec<TokenBreakdownCategory>,
    pub(crate) evidence_refs: Vec<String>,
}

/// Replaces old large tool outputs with bounded summaries while preserving
/// tool-result IDs and keeping the newest results verbatim.
#[must_use]
pub(crate) fn truncate_old_tool_tails(messages: &mut [ProviderMessage]) -> ContextPromptMutation {
    let before_tokens = messages.iter().map(message_tokens).fold(0_u64, u64::saturating_add);
    let mut tool_indices = messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role == ProviderMessageRole::Tool)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    tool_indices.reverse();
    let mut evidence_refs = Vec::new();
    for index in tool_indices.into_iter().skip(RECENT_TOOL_OUTPUTS_TO_KEEP) {
        let text = messages[index].text_content();
        if text.chars().count() <= TOOL_OUTPUT_SUMMARY_CHARS {
            continue;
        }
        let evidence_ref = format!("tool-result-sha256:{}", crate::sha256_hex(text.as_bytes()));
        let preview = text.chars().take(TOOL_OUTPUT_SUMMARY_CHARS).collect::<String>();
        messages[index].content = vec![ProviderMessageContentPart::Text {
            text: format!(
                "{preview}\n[Older tool output truncated; full host evidence: {evidence_ref}]"
            ),
        }];
        evidence_refs.push(evidence_ref);
    }
    let after_tokens = messages.iter().map(message_tokens).fold(0_u64, u64::saturating_add);
    ContextPromptMutation {
        before_tokens,
        after_tokens,
        removed_categories: if evidence_refs.is_empty() {
            Vec::new()
        } else {
            vec![TokenBreakdownCategory::ToolResults]
        },
        evidence_refs,
    }
}

/// Drops only old text-only conversation messages. System/developer/current
/// user messages and every tool call/result pair remain untouched.
#[must_use]
pub(crate) fn reduce_optional_context(
    messages: &mut Vec<ProviderMessage>,
) -> ContextPromptMutation {
    let before_tokens = messages.iter().map(message_tokens).fold(0_u64, u64::saturating_add);
    let last_user_index =
        messages.iter().rposition(|message| message.role == ProviderMessageRole::User);
    let mut removed_hashes = Vec::new();
    let retained = messages
        .drain(..)
        .enumerate()
        .filter_map(|(index, message)| {
            let protected = matches!(
                message.role,
                ProviderMessageRole::System
                    | ProviderMessageRole::Developer
                    | ProviderMessageRole::Tool
            ) || !message.tool_calls.is_empty()
                || Some(index) == last_user_index;
            if protected {
                Some(message)
            } else {
                removed_hashes.push(format!(
                    "prompt-segment-sha256:{}",
                    crate::sha256_hex(message.text_content().as_bytes())
                ));
                None
            }
        })
        .collect::<Vec<_>>();
    *messages = retained;
    let after_tokens = messages.iter().map(message_tokens).fold(0_u64, u64::saturating_add);
    ContextPromptMutation {
        before_tokens,
        after_tokens,
        removed_categories: if removed_hashes.is_empty() {
            Vec::new()
        } else {
            vec![TokenBreakdownCategory::SessionHistory]
        },
        evidence_refs: removed_hashes,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        model_supports_recovery_request, reduce_optional_context, truncate_old_tool_tails,
        ContextRecoveryAction, ContextRecoveryBudget, ContextRecoveryController,
        ContextRecoveryInput, ContextRouteFallback, TokenBreakdown, TokenBreakdownCategory,
        TokenBreakdownItem,
    };
    use crate::model_provider::{
        ProviderCapabilitiesSnapshot, ProviderImageInput, ProviderMessage,
        ProviderMessageContentPart, ProviderMessageRole, ProviderRequest,
    };
    use serde_json::json;

    fn item(category: TokenBreakdownCategory, estimated_tokens: u64) -> TokenBreakdownItem {
        TokenBreakdownItem {
            category,
            estimated_tokens,
            protected: category.protected(),
            artifact_backed: category == TokenBreakdownCategory::ToolResults,
        }
    }

    fn input(route: Option<ContextRouteFallback>) -> ContextRecoveryInput {
        ContextRecoveryInput {
            provider_limit_tokens: 4_000,
            reserved_output_tokens: 800,
            schema_cost_tokens: 200,
            breakdown: TokenBreakdown {
                items: vec![
                    item(TokenBreakdownCategory::SystemInstructions, 700),
                    item(TokenBreakdownCategory::SafetyPolicy, 300),
                    item(TokenBreakdownCategory::CurrentTurn, 500),
                    item(TokenBreakdownCategory::SessionHistory, 2_000),
                    item(TokenBreakdownCategory::ToolResults, 1_500),
                ],
            },
            provider_overflow_observed: true,
            prior_generation: 7,
            recovery_budget: ContextRecoveryBudget::default(),
            route_fallback: route,
        }
    }

    fn next(controller: &mut ContextRecoveryController) -> super::ContextRecoveryStep {
        controller
            .next_step()
            .expect("step selection should succeed")
            .expect("recovery should still be required")
    }

    fn text_message(role: ProviderMessageRole, text: &str) -> ProviderMessage {
        ProviderMessage {
            role,
            content: vec![ProviderMessageContentPart::Text { text: text.to_owned() }],
            name: None,
            tool_call_id: None,
            tool_calls: Vec::new(),
        }
    }

    #[test]
    fn overflow_before_tool_call_uses_monotonic_ladder() {
        let mut controller =
            ContextRecoveryController::new(input(None)).expect("input should validate");
        assert!(controller.requires_recovery());
        let compact = next(&mut controller);
        assert_eq!(compact.action, ContextRecoveryAction::Compact);
        let compact_outcome = controller
            .record_outcome(&compact, compact.before_tokens - 600, Vec::new(), true)
            .expect("compaction should record");
        assert!(compact_outcome.accepted);
        assert!(compact_outcome.after_tokens < compact_outcome.before_tokens);

        let truncate = next(&mut controller);
        assert_eq!(truncate.action, ContextRecoveryAction::TruncateOldToolTails);
        let outcome = controller
            .record_outcome(
                &truncate,
                truncate.before_tokens - 1_500,
                vec![TokenBreakdownCategory::ToolResults],
                true,
            )
            .expect("tool-tail reduction should record");
        assert!(outcome.terminal);
        assert_eq!(
            controller.plan().terminal_reason_code.as_deref(),
            Some("context.recovery.step_reached_provider_limit")
        );
    }

    #[test]
    fn compaction_without_savings_advances_and_never_repeats() {
        let mut controller =
            ContextRecoveryController::new(input(None)).expect("input should validate");
        let compact = next(&mut controller);
        let no_progress = controller
            .record_outcome_with_host_reason(
                &compact,
                compact.before_tokens,
                Vec::new(),
                true,
                Some("runtime.compaction.preflight_insufficient_savings"),
            )
            .expect("zero progress is a recorded outcome");
        assert!(!no_progress.accepted);
        assert_eq!(no_progress.reason_code, "context.recovery.step_no_progress");
        assert_eq!(
            no_progress.host_reason_code.as_deref(),
            Some("runtime.compaction.preflight_insufficient_savings")
        );
        assert_eq!(next(&mut controller).action, ContextRecoveryAction::TruncateOldToolTails);
    }

    #[test]
    fn repeated_zero_progress_skips_unavailable_route_and_fails_once() {
        let mut controller =
            ContextRecoveryController::new(input(None)).expect("input should validate");
        for action in [
            ContextRecoveryAction::Compact,
            ContextRecoveryAction::TruncateOldToolTails,
            ContextRecoveryAction::ReduceOptionalContext,
        ] {
            let step = next(&mut controller);
            assert_eq!(step.action, action);
            let outcome = controller
                .record_outcome(&step, step.before_tokens, Vec::new(), true)
                .expect("zero progress should advance the monotonic ladder");
            assert!(!outcome.accepted);
            assert!(!outcome.terminal);
        }

        let failure = next(&mut controller);
        assert_eq!(failure.action, ContextRecoveryAction::FailDeterministic);
        let outcome = controller
            .record_outcome(&failure, failure.before_tokens, Vec::new(), true)
            .expect("terminal failure should be recorded");
        assert!(outcome.terminal);
        assert_eq!(
            controller.plan().terminal_reason_code.as_deref(),
            Some("context.recovery.exhausted_fail_deterministic")
        );
        assert!(controller
            .next_step()
            .expect("terminal controller should not emit another action")
            .is_none());
    }

    #[test]
    fn route_fallback_requires_larger_authorized_same_cost_window() {
        let route = ContextRouteFallback {
            provider_id: "provider-b".to_owned(),
            model_id: "large-window".to_owned(),
            context_window_tokens: 16_000,
            policy_allowed: true,
            cost_allowed: true,
            tool_catalog_hash: "a".repeat(64),
        };
        let mut controller =
            ContextRecoveryController::new(input(Some(route))).expect("input should validate");
        for action in [
            ContextRecoveryAction::Compact,
            ContextRecoveryAction::TruncateOldToolTails,
            ContextRecoveryAction::ReduceOptionalContext,
        ] {
            let step = next(&mut controller);
            assert_eq!(step.action, action);
            controller
                .record_outcome(&step, step.before_tokens, Vec::new(), true)
                .expect("zero progress should advance");
        }
        let route = next(&mut controller);
        assert_eq!(route.action, ContextRecoveryAction::RouteLargerWindow);
        let routed = controller
            .record_outcome(&route, route.before_tokens, Vec::new(), true)
            .expect("authorized larger route should apply");
        assert!(routed.accepted);
        assert!(routed.terminal);
        assert_eq!(
            routed.route_fallback.as_ref().map(|route| route.model_id.as_str()),
            Some("large-window")
        );
    }

    #[test]
    fn recovery_fallback_requires_json_and_vision_capabilities() {
        let json_request =
            ProviderRequest::from_input_text("json".to_owned(), true, Vec::new(), None);
        let vision_request = ProviderRequest::from_input_text(
            "vision".to_owned(),
            false,
            vec![ProviderImageInput {
                mime_type: "image/png".to_owned(),
                bytes_base64: "AA==".to_owned(),
                file_name: None,
                width_px: None,
                height_px: None,
                artifact_id: None,
            }],
            None,
        );
        let mut capabilities = ProviderCapabilitiesSnapshot {
            streaming_tokens: true,
            tool_calls: true,
            json_mode: false,
            vision: false,
            audio_transcribe: false,
            embeddings: false,
            reasoning: false,
            reasoning_efforts: Vec::new(),
            service_tier: false,
            service_tiers: Vec::new(),
            max_context_tokens: Some(16_000),
            cost_tier: "standard".to_owned(),
            latency_tier: "standard".to_owned(),
            recommended_use_cases: Vec::new(),
            known_limitations: Vec::new(),
            operator_override: false,
            metadata_source: "test".to_owned(),
        };

        assert!(!model_supports_recovery_request(&json_request, &capabilities));
        assert!(!model_supports_recovery_request(&vision_request, &capabilities));
        capabilities.json_mode = true;
        capabilities.vision = true;
        assert!(model_supports_recovery_request(&json_request, &capabilities));
        assert!(model_supports_recovery_request(&vision_request, &capabilities));
    }

    #[test]
    fn protected_removal_fails_and_estimate_growth_advances_without_retrying() {
        let mut controller =
            ContextRecoveryController::new(input(None)).expect("input should validate");
        let compact = next(&mut controller);
        assert_eq!(
            controller
                .record_outcome(
                    &compact,
                    compact.before_tokens - 200,
                    vec![TokenBreakdownCategory::CurrentTurn],
                    true,
                )
                .expect_err("protected current turn must not be removed"),
            "context.recovery.protected_segment_removal_denied"
        );

        let outcome = controller
            .record_outcome(&compact, compact.before_tokens + 1, Vec::new(), true)
            .expect("estimate growth should record as a failed step");
        assert!(!outcome.accepted);
        assert_eq!(outcome.reason_code, "context.recovery.step_estimate_increased");
        assert_eq!(next(&mut controller).action, ContextRecoveryAction::TruncateOldToolTails);
    }

    #[test]
    fn large_old_tool_results_become_bounded_host_evidence_refs() {
        let mut messages = vec![
            ProviderMessage::user_text("start"),
            ProviderMessage::tool_result("tool-1", "x ".repeat(4_000)),
            ProviderMessage::tool_result("tool-2", "y ".repeat(4_000)),
            ProviderMessage::tool_result("tool-3", "recent"),
            ProviderMessage::tool_result("tool-4", "latest"),
        ];
        let tool_ids = messages
            .iter()
            .filter(|message| message.role == ProviderMessageRole::Tool)
            .map(|message| message.tool_call_id.clone())
            .collect::<Vec<_>>();

        let mutation = truncate_old_tool_tails(messages.as_mut_slice());

        assert!(mutation.after_tokens < mutation.before_tokens);
        assert_eq!(mutation.evidence_refs.len(), 2);
        assert!(mutation
            .evidence_refs
            .iter()
            .all(|reference| reference.starts_with("tool-result-sha256:")));
        assert_eq!(
            messages
                .iter()
                .filter(|message| message.role == ProviderMessageRole::Tool)
                .map(|message| message.tool_call_id.clone())
                .collect::<Vec<_>>(),
            tool_ids
        );
    }

    #[test]
    fn optional_reduction_preserves_instructions_current_turn_and_tool_pairs() {
        let mut messages = vec![
            text_message(ProviderMessageRole::System, "system"),
            ProviderMessage::user_text("old user"),
            text_message(ProviderMessageRole::Assistant, "old answer"),
            text_message(ProviderMessageRole::Assistant, ""),
            ProviderMessage::tool_result("tool-1", "result"),
            ProviderMessage::user_text("current user"),
        ];
        messages[3].tool_calls.push(crate::model_provider::ProviderMessageToolCall {
            proposal_id: "tool-1".to_owned(),
            tool_name: "palyra.echo".to_owned(),
            input_json: json!({"text":"hello"}),
        });

        let mutation = reduce_optional_context(&mut messages);

        assert!(mutation.after_tokens < mutation.before_tokens);
        assert!(messages.iter().any(|message| message.role == ProviderMessageRole::System));
        assert!(messages.iter().any(|message| message.text_content() == "current user"));
        assert!(messages.iter().any(|message| message.role == ProviderMessageRole::Tool));
        assert!(messages.iter().any(|message| !message.tool_calls.is_empty()));
    }
}
