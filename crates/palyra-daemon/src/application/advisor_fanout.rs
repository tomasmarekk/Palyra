//! Advisor fanout planning, aggregation, and usage governance.
//!
//! Advisors are deliberately non-authoritative: they receive redacted context,
//! cannot call tools, and produce evidence segments for the acting run to consider.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const ADVISOR_FANOUT_SCHEMA_VERSION: u64 = 1;
const ADVISOR_REDACTION_LEVEL: &str = "redacted_metadata";
const ADVISOR_AUTHORITY: &str = "advisory_only";
const ADVISOR_MAX_TIMEOUT_MS: u64 = 45_000;
const ADVISOR_EVIDENCE_TEXT_LIMIT: usize = 1_200;

/// Built-in advisor presets understood by the runtime planner.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdvisorPreset {
    #[serde(alias = "cheap_review")]
    CodeReview,
    SecurityReview,
    #[serde(alias = "code_architect")]
    ArchitectureReview,
    TestPlan,
    MigrationRisk,
}

impl AdvisorPreset {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CodeReview => "code_review",
            Self::SecurityReview => "security_review",
            Self::ArchitectureReview => "architecture_review",
            Self::TestPlan => "test_plan",
            Self::MigrationRisk => "migration_risk",
        }
    }

    #[must_use]
    const fn default_budget_tokens(self) -> u64 {
        match self {
            Self::CodeReview => 1_400,
            Self::SecurityReview => 2_000,
            Self::ArchitectureReview => 2_500,
            Self::TestPlan => 1_200,
            Self::MigrationRisk => 1_600,
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
    pub timeout_ms: u64,
    pub recursion_depth: u8,
    pub allow_degraded_failure: bool,
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
    pub timeout_ms: u64,
    pub output_contract: String,
    pub non_authoritative: bool,
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
    #[serde(rename = "advisor_fanout.recursion_denied")]
    RecursionDenied,
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
            Self::RecursionDenied => "advisor_fanout.recursion_denied",
        }
    }
}

/// One advisor skipped during planning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct SkippedAdvisor {
    pub preset: AdvisorPreset,
    pub reason: AdvisorSkipReason,
}

/// One redacted advisor lifecycle event for journal or progress streams.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorTraceEvent {
    pub event_type: String,
    pub preset: Option<AdvisorPreset>,
    pub status: String,
    pub reason_code: String,
    pub run_id_hash: String,
    pub redaction_level: String,
    pub non_authoritative: bool,
}

/// One bounded evidence segment produced by an advisor result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorEvidenceSegment {
    pub segment_id: String,
    pub advisor_id: String,
    pub preset: AdvisorPreset,
    pub claim: String,
    pub summary: String,
    pub severity: String,
    pub evidence_refs: Vec<String>,
    pub conflicts_with: Vec<String>,
    pub safety_warning: bool,
    pub authority: String,
    pub tool_access: bool,
    pub non_authoritative: bool,
    pub redaction_level: String,
}

/// Raw advisor finding before aggregation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorFindingInput {
    pub advisor_id: String,
    pub preset: AdvisorPreset,
    pub claim: String,
    pub summary: String,
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub conflicts_with: Vec<String>,
    #[serde(default)]
    pub safety_warning: bool,
}

/// Budget and policy envelope for advisor aggregation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorAggregationRequest {
    pub run_id: String,
    pub findings: Vec<AdvisorFindingInput>,
    pub advisor_reserved_tokens: u64,
    pub aggregator_budget_tokens: u64,
    pub token_budget_remaining: u64,
    pub fail_open_with_raw_summaries: bool,
}

/// Deduplicated claim with all supporting advisors and evidence refs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorAggregatedClaim {
    pub claim: String,
    pub advisor_ids: Vec<String>,
    pub presets: Vec<AdvisorPreset>,
    pub evidence_refs: Vec<String>,
    pub safety_warning: bool,
}

/// Non-authoritative aggregation summary available to the main agent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorAggregationSummary {
    pub schema_version: u64,
    pub run_id_hash: String,
    pub non_authoritative: bool,
    pub status: String,
    pub budget_exhausted: bool,
    pub advisor_reserved_tokens: u64,
    pub aggregator_budget_tokens: u64,
    pub total_reserved_tokens: u64,
    pub agreements: Vec<AdvisorAggregatedClaim>,
    pub conflicts: Vec<Value>,
    pub unique_safety_warnings: Vec<AdvisorEvidenceSegment>,
    pub raw_segments: Vec<AdvisorEvidenceSegment>,
    pub trace_events: Vec<AdvisorTraceEvent>,
    pub reason_code: String,
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
    pub degraded_failure_allowed: bool,
    pub lifecycle_events: Vec<AdvisorTraceEvent>,
    pub trace_json: String,
}

/// Usage ledger that keeps acting-run, advisor, and aggregator spend attributable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AdvisorUsageLedger {
    pub schema_version: u64,
    pub run_id_hash: String,
    pub acting_input_tokens: u64,
    pub acting_output_tokens: u64,
    pub advisor_input_tokens: u64,
    pub advisor_output_tokens: u64,
    pub aggregator_input_tokens: u64,
    pub aggregator_output_tokens: u64,
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
        if request.recursion_depth > 0 {
            skipped.push(SkippedAdvisor { preset, reason: AdvisorSkipReason::RecursionDenied });
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
            timeout_ms: request.timeout_ms.clamp(1, ADVISOR_MAX_TIMEOUT_MS),
            output_contract: "non_authoritative_evidence_segment".to_owned(),
            non_authoritative: true,
            failure_blocks_parent: request.security_review_required
                && preset.security_required_failure_blocks(),
        });
    }

    let run_id_hash = crate::sha256_hex(request.run_id.as_bytes());
    let lifecycle_events = advisor_plan_trace_events(run_id_hash.as_str(), &selected, &skipped);
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
        "non_authoritative": true,
        "degraded_failure_allowed": request.allow_degraded_failure,
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
        degraded_failure_allowed: request.allow_degraded_failure,
        lifecycle_events,
        trace_json: trace.to_string(),
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
pub(crate) fn advisor_usage_ledger(
    run_id: &str,
    acting_input_tokens: u64,
    acting_output_tokens: u64,
    advisor_input_tokens: u64,
    advisor_output_tokens: u64,
    aggregator_input_tokens: u64,
    aggregator_output_tokens: u64,
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
        aggregator_input_tokens,
        aggregator_output_tokens,
        advisor_cost_microusd,
        failed_advisors,
        budget_policy: "acting_advisor_and_aggregator_usage_accounted_separately".to_owned(),
    }
}

#[must_use]
pub(crate) fn build_advisor_evidence_segment(
    run_id: &str,
    finding: AdvisorFindingInput,
) -> AdvisorEvidenceSegment {
    let claim = bounded_advisor_text(finding.claim.as_str(), ADVISOR_EVIDENCE_TEXT_LIMIT);
    let advisor_id = normalize_identifier(finding.advisor_id.as_str(), "advisor");
    let preset = finding.preset;
    AdvisorEvidenceSegment {
        segment_id: format!(
            "advseg_{}",
            crate::sha256_hex(
                format!("{run_id}:{advisor_id}:{}:{claim}", preset.as_str()).as_bytes()
            )
            .chars()
            .take(16)
            .collect::<String>()
        ),
        advisor_id,
        preset,
        claim,
        summary: bounded_advisor_text(finding.summary.as_str(), ADVISOR_EVIDENCE_TEXT_LIMIT),
        severity: normalize_optional_label(finding.severity.as_str(), "info"),
        evidence_refs: normalize_string_set(finding.evidence_refs, 32, 256),
        conflicts_with: normalize_string_set(finding.conflicts_with, 16, 256),
        safety_warning: finding.safety_warning,
        authority: ADVISOR_AUTHORITY.to_owned(),
        tool_access: false,
        non_authoritative: true,
        redaction_level: ADVISOR_REDACTION_LEVEL.to_owned(),
    }
}

#[must_use]
pub(crate) fn aggregate_advisor_findings(
    request: AdvisorAggregationRequest,
) -> AdvisorAggregationSummary {
    let run_id_hash = crate::sha256_hex(request.run_id.as_bytes());
    let raw_segments = request
        .findings
        .into_iter()
        .map(|finding| build_advisor_evidence_segment(request.run_id.as_str(), finding))
        .collect::<Vec<_>>();
    let total_reserved_tokens =
        request.advisor_reserved_tokens.saturating_add(request.aggregator_budget_tokens);
    let budget_exhausted = total_reserved_tokens > request.token_budget_remaining;
    if budget_exhausted {
        let preserved_segments =
            if request.fail_open_with_raw_summaries { raw_segments } else { Vec::new() };
        return degraded_advisor_aggregation(
            run_id_hash,
            preserved_segments,
            request.advisor_reserved_tokens,
            request.aggregator_budget_tokens,
            total_reserved_tokens,
            "advisor_aggregator_budget_exhausted",
        );
    }

    let mut claims = BTreeMap::<String, AdvisorAggregatedClaim>::new();
    for segment in &raw_segments {
        let key = normalize_claim_key(segment.claim.as_str());
        claims
            .entry(key)
            .and_modify(|claim| {
                push_unique(&mut claim.advisor_ids, segment.advisor_id.clone(), 16);
                push_unique(&mut claim.presets, segment.preset, 8);
                claim.evidence_refs =
                    merge_string_sets(claim.evidence_refs.clone(), segment.evidence_refs.clone());
                claim.safety_warning |= segment.safety_warning;
            })
            .or_insert_with(|| AdvisorAggregatedClaim {
                claim: segment.claim.clone(),
                advisor_ids: vec![segment.advisor_id.clone()],
                presets: vec![segment.preset],
                evidence_refs: segment.evidence_refs.clone(),
                safety_warning: segment.safety_warning,
            });
    }
    let agreements = claims.into_values().collect::<Vec<_>>();
    let conflicts = advisor_conflicts(raw_segments.as_slice());
    let unique_safety_warnings =
        raw_segments.iter().filter(|segment| segment.safety_warning).cloned().collect::<Vec<_>>();
    AdvisorAggregationSummary {
        schema_version: ADVISOR_FANOUT_SCHEMA_VERSION,
        run_id_hash: run_id_hash.clone(),
        non_authoritative: true,
        status: "succeeded".to_owned(),
        budget_exhausted: false,
        advisor_reserved_tokens: request.advisor_reserved_tokens,
        aggregator_budget_tokens: request.aggregator_budget_tokens,
        total_reserved_tokens,
        agreements,
        conflicts,
        unique_safety_warnings,
        raw_segments,
        trace_events: advisor_aggregation_trace_events(run_id_hash.as_str(), "completed"),
        reason_code: "advisor_aggregation_completed".to_owned(),
    }
}

#[must_use]
pub(crate) fn degraded_advisor_aggregation(
    run_id_hash: String,
    raw_segments: Vec<AdvisorEvidenceSegment>,
    advisor_reserved_tokens: u64,
    aggregator_budget_tokens: u64,
    total_reserved_tokens: u64,
    reason_code: &'static str,
) -> AdvisorAggregationSummary {
    AdvisorAggregationSummary {
        schema_version: ADVISOR_FANOUT_SCHEMA_VERSION,
        run_id_hash: run_id_hash.clone(),
        non_authoritative: true,
        status: "degraded".to_owned(),
        budget_exhausted: reason_code == "advisor_aggregator_budget_exhausted",
        advisor_reserved_tokens,
        aggregator_budget_tokens,
        total_reserved_tokens,
        agreements: Vec::new(),
        conflicts: Vec::new(),
        unique_safety_warnings: raw_segments
            .iter()
            .filter(|segment| segment.safety_warning)
            .cloned()
            .collect(),
        raw_segments,
        trace_events: advisor_aggregation_trace_events(run_id_hash.as_str(), "failed"),
        reason_code: reason_code.to_owned(),
    }
}

fn advisor_plan_trace_events(
    run_id_hash: &str,
    selected: &[AdvisorInvocationPlan],
    skipped: &[SkippedAdvisor],
) -> Vec<AdvisorTraceEvent> {
    let mut events = selected
        .iter()
        .map(|plan| AdvisorTraceEvent {
            event_type: "advisor.started".to_owned(),
            preset: Some(plan.preset),
            status: "started".to_owned(),
            reason_code: "advisor_fanout_selected".to_owned(),
            run_id_hash: run_id_hash.to_owned(),
            redaction_level: ADVISOR_REDACTION_LEVEL.to_owned(),
            non_authoritative: true,
        })
        .collect::<Vec<_>>();
    events.extend(skipped.iter().map(|entry| AdvisorTraceEvent {
        event_type: "advisor.skipped".to_owned(),
        preset: Some(entry.preset),
        status: "skipped".to_owned(),
        reason_code: entry.reason.as_str().to_owned(),
        run_id_hash: run_id_hash.to_owned(),
        redaction_level: ADVISOR_REDACTION_LEVEL.to_owned(),
        non_authoritative: true,
    }));
    events
}

fn advisor_aggregation_trace_events(
    run_id_hash: &str,
    terminal_status: &str,
) -> Vec<AdvisorTraceEvent> {
    [
        ("advisor.aggregation.started", "started", "advisor_aggregation_started"),
        (
            "advisor.aggregation.completed",
            terminal_status,
            if terminal_status == "completed" {
                "advisor_aggregation_completed"
            } else {
                "advisor_aggregation_failed_degraded"
            },
        ),
    ]
    .into_iter()
    .map(|(event_type, status, reason_code)| AdvisorTraceEvent {
        event_type: event_type.to_owned(),
        preset: None,
        status: status.to_owned(),
        reason_code: reason_code.to_owned(),
        run_id_hash: run_id_hash.to_owned(),
        redaction_level: ADVISOR_REDACTION_LEVEL.to_owned(),
        non_authoritative: true,
    })
    .collect()
}

fn advisor_conflicts(segments: &[AdvisorEvidenceSegment]) -> Vec<Value> {
    let by_key = segments
        .iter()
        .map(|segment| (normalize_claim_key(segment.claim.as_str()), segment))
        .collect::<BTreeMap<_, _>>();
    let mut conflicts = Vec::new();
    let mut seen_pairs = BTreeSet::new();
    for segment in segments {
        for target in &segment.conflicts_with {
            let target_key = normalize_claim_key(target.as_str());
            let Some(other) = by_key.get(&target_key) else {
                continue;
            };
            let pair_key = ordered_pair_key(segment.segment_id.as_str(), other.segment_id.as_str());
            if !seen_pairs.insert(pair_key) {
                continue;
            }
            conflicts.push(json!({
                "claim": segment.claim,
                "advisor_id": segment.advisor_id,
                "conflicts_with": other.claim,
                "conflicting_advisor_id": other.advisor_id,
                "non_authoritative": true,
            }));
        }
    }
    conflicts
}

fn ordered_pair_key(left: &str, right: &str) -> String {
    if left <= right {
        format!("{left}:{right}")
    } else {
        format!("{right}:{left}")
    }
}

fn normalize_claim_key(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ").to_ascii_lowercase()
}

fn bounded_advisor_text(value: &str, limit: usize) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "unspecified".to_owned();
    }
    trimmed.chars().take(limit).collect()
}

fn normalize_optional_label(value: &str, default: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return default.to_owned();
    }
    trimmed
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        .take(48)
        .collect::<String>()
        .to_ascii_lowercase()
}

fn normalize_identifier(value: &str, default_prefix: &str) -> String {
    let label = normalize_optional_label(value, "");
    if label.is_empty() {
        format!("{default_prefix}_unknown")
    } else {
        label
    }
}

fn normalize_string_set(values: Vec<String>, limit: usize, text_limit: usize) -> Vec<String> {
    let mut output = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let bounded = trimmed.chars().take(text_limit).collect::<String>();
        push_unique(&mut output, bounded, limit);
    }
    output
}

fn merge_string_sets(left: Vec<String>, right: Vec<String>) -> Vec<String> {
    let mut output = left;
    for value in right {
        push_unique(&mut output, value, 64);
    }
    output
}

fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T, limit: usize) {
    if values.len() >= limit || values.iter().any(|existing| existing == &value) {
        return;
    }
    values.push(value);
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
            requested_presets: vec![AdvisorPreset::CodeReview, AdvisorPreset::ArchitectureReview],
            max_advisors: 2,
            token_budget_remaining: 10_000,
            cost_budget_microusd_remaining: 50_000,
            timeout_ms: 30_000,
            recursion_depth: 0,
            allow_degraded_failure: true,
        }
    }

    #[test]
    fn advisor_plan_is_read_only_and_non_authoritative() {
        let plan = plan_advisor_fanout(&request());

        assert_eq!(plan.advisor_count, 2);
        assert!(plan.selected.iter().all(|advisor| !advisor.tool_access));
        assert!(plan.selected.iter().all(|advisor| advisor.authority == ADVISOR_AUTHORITY));
        assert!(plan.selected.iter().all(|advisor| advisor.non_authoritative));
        assert!(plan.lifecycle_events.iter().any(|event| event.event_type == "advisor.started"));
        assert!(plan.trace_json.contains("advisor_fanout.plan"));
        assert!(!plan.trace_json.contains("run-1"));
    }

    #[test]
    fn security_required_adds_blocking_security_review() {
        let mut request = request();
        request.security_review_required = true;
        request.requested_presets = vec![AdvisorPreset::CodeReview];

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
        request.token_budget_remaining = 1_400;

        let plan = plan_advisor_fanout(&request);

        assert_eq!(plan.selected.len(), 1);
        assert_eq!(plan.skipped[0].reason, AdvisorSkipReason::TokenBudgetExhausted);
        assert_eq!(plan.acting_run_authority, "authoritative");
    }

    #[test]
    fn usage_ledger_separates_acting_advisor_and_aggregator_usage() {
        let ledger = advisor_usage_ledger("run-1", 10, 20, 30, 40, 5, 6, 50, 1);

        assert_eq!(ledger.acting_input_tokens, 10);
        assert_eq!(ledger.advisor_output_tokens, 40);
        assert_eq!(ledger.aggregator_input_tokens, 5);
        assert_eq!(ledger.aggregator_output_tokens, 6);
        assert_eq!(ledger.failed_advisors, 1);
        assert_eq!(
            ledger.budget_policy,
            "acting_advisor_and_aggregator_usage_accounted_separately"
        );
    }

    #[test]
    fn recursion_guard_skips_every_advisor_without_parent_failure() {
        let mut request = request();
        request.recursion_depth = 1;

        let plan = plan_advisor_fanout(&request);

        assert!(plan.selected.is_empty());
        assert!(plan
            .skipped
            .iter()
            .all(|entry| entry.reason == AdvisorSkipReason::RecursionDenied));
        assert_eq!(plan.acting_run_authority, "authoritative");
    }

    #[test]
    fn aggregation_dedups_claims_and_keeps_unique_safety_warnings() {
        let summary = aggregate_advisor_findings(AdvisorAggregationRequest {
            run_id: "run-1".to_owned(),
            findings: vec![
                AdvisorFindingInput {
                    advisor_id: "code".to_owned(),
                    preset: AdvisorPreset::CodeReview,
                    claim: "Cache invalidation can panic".to_owned(),
                    summary: "Indexing call unwraps an empty result.".to_owned(),
                    severity: "medium".to_owned(),
                    evidence_refs: vec!["tool:rg".to_owned()],
                    conflicts_with: Vec::new(),
                    safety_warning: false,
                },
                AdvisorFindingInput {
                    advisor_id: "security".to_owned(),
                    preset: AdvisorPreset::SecurityReview,
                    claim: "cache invalidation can panic".to_owned(),
                    summary: "Same panic also exposes a DoS risk.".to_owned(),
                    severity: "high".to_owned(),
                    evidence_refs: vec!["test:panic".to_owned()],
                    conflicts_with: Vec::new(),
                    safety_warning: true,
                },
                AdvisorFindingInput {
                    advisor_id: "migration".to_owned(),
                    preset: AdvisorPreset::MigrationRisk,
                    claim: "Migration needs rollback".to_owned(),
                    summary: "Rollback fixture is missing.".to_owned(),
                    severity: "high".to_owned(),
                    evidence_refs: vec!["fixture:rollback".to_owned()],
                    conflicts_with: Vec::new(),
                    safety_warning: true,
                },
            ],
            advisor_reserved_tokens: 3_000,
            aggregator_budget_tokens: 500,
            token_budget_remaining: 4_000,
            fail_open_with_raw_summaries: true,
        });

        assert_eq!(summary.status, "succeeded");
        assert_eq!(summary.agreements.len(), 2);
        assert_eq!(summary.agreements[0].advisor_ids.len(), 2);
        assert_eq!(summary.unique_safety_warnings.len(), 2);
        assert!(summary.non_authoritative);
    }

    #[test]
    fn aggregation_budget_failure_preserves_raw_segments_degraded() {
        let summary = aggregate_advisor_findings(AdvisorAggregationRequest {
            run_id: "run-1".to_owned(),
            findings: vec![AdvisorFindingInput {
                advisor_id: "security".to_owned(),
                preset: AdvisorPreset::SecurityReview,
                claim: "Secret could leak".to_owned(),
                summary: "Output includes a token field.".to_owned(),
                severity: "high".to_owned(),
                evidence_refs: vec!["trace:redacted".to_owned()],
                conflicts_with: Vec::new(),
                safety_warning: true,
            }],
            advisor_reserved_tokens: 3_000,
            aggregator_budget_tokens: 2_000,
            token_budget_remaining: 4_000,
            fail_open_with_raw_summaries: true,
        });

        assert_eq!(summary.status, "degraded");
        assert!(summary.budget_exhausted);
        assert_eq!(summary.raw_segments.len(), 1);
        assert_eq!(summary.unique_safety_warnings.len(), 1);
        assert!(summary
            .trace_events
            .iter()
            .any(|event| event.reason_code == "advisor_aggregation_failed_degraded"));
    }
}
