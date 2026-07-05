//! Advisor fanout planning and usage governance.
//!
//! Advisors are deliberately non-authoritative: they receive redacted context,
//! cannot call tools, and produce review notes for the acting run to consider.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::json;

pub(crate) const ADVISOR_FANOUT_SCHEMA_VERSION: u64 = 1;
const ADVISOR_REDACTION_LEVEL: &str = "redacted_metadata";
const ADVISOR_AUTHORITY: &str = "advisory_only";

/// Built-in advisor presets understood by the runtime planner.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdvisorPreset {
    CheapReview,
    CodeArchitect,
    SecurityReview,
}

impl AdvisorPreset {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CheapReview => "cheap_review",
            Self::CodeArchitect => "code_architect",
            Self::SecurityReview => "security_review",
        }
    }

    #[must_use]
    const fn default_budget_tokens(self) -> u64 {
        match self {
            Self::CheapReview => 1_000,
            Self::CodeArchitect => 2_500,
            Self::SecurityReview => 2_000,
        }
    }

    #[must_use]
    const fn security_required_failure_blocks(self) -> bool {
        matches!(self, Self::SecurityReview)
    }
}

/// One requested advisor invocation before budget and feature gates apply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorFanoutRequest {
    pub run_id: String,
    pub feature_enabled: bool,
    pub security_review_required: bool,
    pub redacted_context_available: bool,
    pub requested_presets: Vec<AdvisorPreset>,
    pub max_advisors: usize,
    pub token_budget_remaining: u64,
    pub cost_budget_microusd_remaining: u64,
}

/// Advisor selected for a read-only fanout task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorInvocationPlan {
    pub preset: AdvisorPreset,
    pub authority: String,
    pub tool_access: bool,
    pub context_redaction_level: String,
    pub budget_tokens: u64,
    pub max_cost_microusd: u64,
    pub failure_blocks_parent: bool,
}

/// Stable reason for skipping one requested advisor.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AdvisorSkipReason {
    #[serde(rename = "advisor_fanout.feature_disabled")]
    FeatureDisabled,
    #[serde(rename = "advisor_fanout.redacted_context_missing")]
    RedactedContextMissing,
    #[serde(rename = "advisor_fanout.max_advisors_exceeded")]
    MaxAdvisorsExceeded,
    #[serde(rename = "advisor_fanout.token_budget_exhausted")]
    TokenBudgetExhausted,
    #[serde(rename = "advisor_fanout.cost_budget_exhausted")]
    CostBudgetExhausted,
}

impl AdvisorSkipReason {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "advisor_fanout.feature_disabled",
            Self::RedactedContextMissing => "advisor_fanout.redacted_context_missing",
            Self::MaxAdvisorsExceeded => "advisor_fanout.max_advisors_exceeded",
            Self::TokenBudgetExhausted => "advisor_fanout.token_budget_exhausted",
            Self::CostBudgetExhausted => "advisor_fanout.cost_budget_exhausted",
        }
    }
}

/// One advisor skipped during planning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SkippedAdvisor {
    pub preset: AdvisorPreset,
    pub reason: AdvisorSkipReason,
}

/// Resulting fanout plan and metadata-only trace payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorFanoutPlan {
    pub schema_version: u64,
    pub enabled: bool,
    pub run_id_hash: String,
    pub selected: Vec<AdvisorInvocationPlan>,
    pub skipped: Vec<SkippedAdvisor>,
    pub advisor_count: usize,
    pub reserved_tokens: u64,
    pub reserved_cost_microusd: u64,
    pub acting_run_authority: String,
    pub trace_json: String,
}

/// Usage ledger that keeps acting-run and advisor spend attributable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorUsageLedger {
    pub schema_version: u64,
    pub run_id_hash: String,
    pub acting_input_tokens: u64,
    pub acting_output_tokens: u64,
    pub advisor_input_tokens: u64,
    pub advisor_output_tokens: u64,
    pub advisor_cost_microusd: u64,
    pub failed_advisors: u64,
    pub budget_policy: String,
}

#[must_use]
pub(crate) fn plan_advisor_fanout(request: &AdvisorFanoutRequest) -> AdvisorFanoutPlan {
    let mut selected = Vec::new();
    let mut skipped = Vec::new();
    let mut reserved_tokens = 0u64;
    let mut reserved_cost = 0u64;

    let mut requested = request.requested_presets.clone();
    if request.security_review_required && !requested.contains(&AdvisorPreset::SecurityReview) {
        requested.push(AdvisorPreset::SecurityReview);
    }

    for preset in requested {
        if !request.feature_enabled {
            skipped.push(SkippedAdvisor { preset, reason: AdvisorSkipReason::FeatureDisabled });
            continue;
        }
        if !request.redacted_context_available {
            skipped
                .push(SkippedAdvisor { preset, reason: AdvisorSkipReason::RedactedContextMissing });
            continue;
        }
        if selected.len() >= request.max_advisors {
            skipped.push(SkippedAdvisor { preset, reason: AdvisorSkipReason::MaxAdvisorsExceeded });
            continue;
        }

        let budget_tokens = preset.default_budget_tokens();
        let projected_tokens = reserved_tokens.saturating_add(budget_tokens);
        if projected_tokens > request.token_budget_remaining {
            skipped
                .push(SkippedAdvisor { preset, reason: AdvisorSkipReason::TokenBudgetExhausted });
            continue;
        }

        let max_cost_microusd = budget_tokens.saturating_mul(2);
        let projected_cost = reserved_cost.saturating_add(max_cost_microusd);
        if projected_cost > request.cost_budget_microusd_remaining {
            skipped.push(SkippedAdvisor { preset, reason: AdvisorSkipReason::CostBudgetExhausted });
            continue;
        }

        reserved_tokens = projected_tokens;
        reserved_cost = projected_cost;
        selected.push(AdvisorInvocationPlan {
            preset,
            authority: ADVISOR_AUTHORITY.to_owned(),
            tool_access: false,
            context_redaction_level: ADVISOR_REDACTION_LEVEL.to_owned(),
            budget_tokens,
            max_cost_microusd,
            failure_blocks_parent: request.security_review_required
                && preset.security_required_failure_blocks(),
        });
    }

    let run_id_hash = crate::sha256_hex(request.run_id.as_bytes());
    let trace = json!({
        "schema_version": ADVISOR_FANOUT_SCHEMA_VERSION,
        "event_type": "advisor_fanout.plan",
        "run_id_hash": run_id_hash,
        "selected": selected.iter().map(|plan| plan.preset.as_str()).collect::<Vec<_>>(),
        "skipped": skipped.iter().map(|entry| json!({
            "preset": entry.preset.as_str(),
            "reason": entry.reason.as_str(),
        })).collect::<Vec<_>>(),
        "authority": ADVISOR_AUTHORITY,
        "tool_access": false,
        "redaction_level": ADVISOR_REDACTION_LEVEL,
        "reserved_tokens": reserved_tokens,
        "reserved_cost_microusd": reserved_cost,
    });

    AdvisorFanoutPlan {
        schema_version: ADVISOR_FANOUT_SCHEMA_VERSION,
        enabled: request.feature_enabled,
        run_id_hash,
        advisor_count: selected.len(),
        selected,
        skipped,
        reserved_tokens,
        reserved_cost_microusd: reserved_cost,
        acting_run_authority: "authoritative".to_owned(),
        trace_json: trace.to_string(),
    }
}

#[must_use]
pub(crate) fn advisor_usage_ledger(
    run_id: &str,
    acting_input_tokens: u64,
    acting_output_tokens: u64,
    advisor_input_tokens: u64,
    advisor_output_tokens: u64,
    advisor_cost_microusd: u64,
    failed_advisors: u64,
) -> AdvisorUsageLedger {
    AdvisorUsageLedger {
        schema_version: ADVISOR_FANOUT_SCHEMA_VERSION,
        run_id_hash: crate::sha256_hex(run_id.as_bytes()),
        acting_input_tokens,
        acting_output_tokens,
        advisor_input_tokens,
        advisor_output_tokens,
        advisor_cost_microusd,
        failed_advisors,
        budget_policy: "acting_and_advisor_usage_accounted_separately".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> AdvisorFanoutRequest {
        AdvisorFanoutRequest {
            run_id: "run-1".to_owned(),
            feature_enabled: true,
            security_review_required: false,
            redacted_context_available: true,
            requested_presets: vec![AdvisorPreset::CheapReview, AdvisorPreset::CodeArchitect],
            max_advisors: 2,
            token_budget_remaining: 10_000,
            cost_budget_microusd_remaining: 50_000,
        }
    }

    #[test]
    fn advisor_plan_is_read_only_and_non_authoritative() {
        let plan = plan_advisor_fanout(&request());

        assert_eq!(plan.advisor_count, 2);
        assert!(plan.selected.iter().all(|advisor| !advisor.tool_access));
        assert!(plan.selected.iter().all(|advisor| advisor.authority == ADVISOR_AUTHORITY));
        assert!(plan.trace_json.contains("advisor_fanout.plan"));
        assert!(!plan.trace_json.contains("run-1"));
    }

    #[test]
    fn security_required_adds_blocking_security_review() {
        let mut request = request();
        request.security_review_required = true;
        request.requested_presets = vec![AdvisorPreset::CheapReview];

        let plan = plan_advisor_fanout(&request);

        let security = plan
            .selected
            .iter()
            .find(|advisor| advisor.preset == AdvisorPreset::SecurityReview)
            .expect("security review should be selected");
        assert!(security.failure_blocks_parent);
    }

    #[test]
    fn budget_skips_do_not_cancel_parent_plan() {
        let mut request = request();
        request.token_budget_remaining = 1_000;

        let plan = plan_advisor_fanout(&request);

        assert_eq!(plan.selected.len(), 1);
        assert_eq!(plan.skipped[0].reason, AdvisorSkipReason::TokenBudgetExhausted);
        assert_eq!(plan.acting_run_authority, "authoritative");
    }

    #[test]
    fn usage_ledger_separates_acting_and_advisor_usage() {
        let ledger = advisor_usage_ledger("run-1", 10, 20, 30, 40, 50, 1);

        assert_eq!(ledger.acting_input_tokens, 10);
        assert_eq!(ledger.advisor_output_tokens, 40);
        assert_eq!(ledger.failed_advisors, 1);
        assert_eq!(ledger.budget_policy, "acting_and_advisor_usage_accounted_separately");
    }
}
