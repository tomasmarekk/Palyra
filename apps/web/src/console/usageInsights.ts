import type {
  OperatorCompactionInsight,
  OperatorCronInsight,
  OperatorInsightDrillDown,
  OperatorInsightsEnvelope,
  OperatorInsightsPrivacyPolicy,
  OperatorInsightsRetentionPolicy,
  OperatorInsightsSamplingPolicy,
  OperatorInsightsSummary,
  OperatorMemoryLearningInsight,
  OperatorOperationsOverviewInsight,
  OperatorPluginInsight,
  OperatorProviderHealthInsight,
  OperatorRecallInsight,
  OperatorReloadInsight,
  OperatorRoutineInsight,
  OperatorSafetyBoundaryInsight,
  OperatorSecurityInsight,
  UsageInsightsEnvelope,
} from "../consoleApi";

const EMPTY_DRILL_DOWN: OperatorInsightDrillDown = {
  label: "Open details",
  section: "usage",
  api_path: "/console/v1/usage/insights",
  console_path: "/control/usage",
};

const EMPTY_SUMMARY: OperatorInsightsSummary = {
  state: "unknown",
  severity: "unknown",
  hotspot_count: 0,
  blocking_hotspots: 0,
  warning_hotspots: 0,
  recommendation: "Refresh insights to load the current operator posture.",
};

const EMPTY_RETENTION: OperatorInsightsRetentionPolicy = {
  source_of_truth: "journal",
  aggregation_mode: "unknown",
  derived_metrics_persisted: false,
  support_bundle_embeds_latest_snapshot: false,
  window_start_at_unix_ms: 0,
  window_end_at_unix_ms: 0,
};

const EMPTY_SAMPLING: OperatorInsightsSamplingPolicy = {
  run_sample_limit: 0,
  tape_event_limit_per_run: 0,
  cron_run_limit: 0,
  plugin_limit: 0,
  observed_runs: 0,
  sampled_runs: 0,
  observed_cron_runs: 0,
  sampled_cron_runs: 0,
  observed_plugins: 0,
  sampled_plugins: 0,
  notes: [],
};

const EMPTY_PRIVACY: OperatorInsightsPrivacyPolicy = {
  redaction_mode: "unknown",
  raw_queries_included: false,
  raw_error_messages_included: false,
  raw_config_values_included: false,
  secret_like_values_redacted: true,
};

const EMPTY_PROVIDER_HEALTH: OperatorProviderHealthInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Provider health is unavailable.",
  provider_kind: "unknown",
  error_rate_bps: 0,
  avg_latency_ms: 0,
  circuit_open: false,
  auth_state: "unknown",
  refresh_failures: 0,
  response_cache_enabled: false,
  response_cache_entries: 0,
  response_cache_hit_rate_bps: 0,
  recommended_action: "Refresh diagnostics to load provider health.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_OPERATIONS: OperatorOperationsOverviewInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Operations overview is unavailable.",
  stuck_runs: 0,
  provider_cooldowns: 0,
  queue_backlog: 0,
  routine_failures: 0,
  plugin_errors: 0,
  worker_orphaned: 0,
  worker_failed_closed: 0,
  recommended_action: "Refresh diagnostics to load operations overview.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_SECURITY: OperatorSecurityInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Security insights are unavailable.",
  approval_denies: 0,
  policy_denies: 0,
  redaction_events: 0,
  sandbox_violations: 0,
  skill_execution_denies: 0,
  sampled_denied_tool_decisions: 0,
  recommended_action: "Refresh diagnostics to load security insights.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_RECALL: OperatorRecallInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Recall aggregates are unavailable.",
  explicit_recall_events: 0,
  explicit_recall_zero_hit_events: 0,
  explicit_recall_zero_hit_rate_bps: 0,
  auto_inject_events: 0,
  auto_inject_zero_hit_events: 0,
  auto_inject_avg_hits: 0,
  samples: [],
  recommended_action: "Refresh usage to load recall aggregates.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_COMPACTION: OperatorCompactionInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Compaction aggregates are unavailable.",
  preview_events: 0,
  created_events: 0,
  dry_run_events: 0,
  avg_token_delta: 0,
  avg_reduction_bps: 0,
  samples: [],
  recommended_action: "Refresh usage to load compaction aggregates.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_SAFETY: OperatorSafetyBoundaryInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Safety-boundary aggregates are unavailable.",
  inspected_tool_decisions: 0,
  denied_tool_decisions: 0,
  policy_enforced_denies: 0,
  approval_required_decisions: 0,
  deny_rate_bps: 0,
  samples: [],
  recommended_action: "Refresh diagnostics to load safety-boundary aggregates.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_PLUGINS: OperatorPluginInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Plugin operability is unavailable.",
  total_bindings: 0,
  ready_bindings: 0,
  unhealthy_bindings: 0,
  typed_contract_failures: 0,
  config_failures: 0,
  discovery_failures: 0,
  samples: [],
  recommended_action: "Refresh diagnostics to load plugin operability.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_CRON: OperatorCronInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Cron delivery aggregates are unavailable.",
  total_runs: 0,
  failed_runs: 0,
  success_rate_bps: 0,
  total_tool_denies: 0,
  samples: [],
  recommended_action: "Refresh diagnostics to load cron aggregates.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_ROUTINES: OperatorRoutineInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Routine delivery insights are unavailable.",
  total_runs: 0,
  failed_runs: 0,
  skipped_runs: 0,
  policy_denies: 0,
  success_rate_bps: 0,
  recommended_action: "Refresh diagnostics to load routine delivery insights.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_MEMORY_LEARNING: OperatorMemoryLearningInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Memory learning insights are unavailable.",
  total_candidates: 0,
  proposed_candidates: 0,
  needs_review_candidates: 0,
  approved_candidates: 0,
  rejected_candidates: 0,
  deployed_candidates: 0,
  rolled_back_candidates: 0,
  auto_applied_candidates: 0,
  memory_rejections: 0,
  injection_conflicts: 0,
  rollback_events: 0,
  recommended_action: "Refresh memory learning to load candidate lifecycle insights.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_RELOAD: OperatorReloadInsight = {
  state: "unknown",
  severity: "unknown",
  summary: "Reload health is unavailable.",
  blocking_refs: 0,
  warning_refs: 0,
  hotspots: [],
  recommended_action: "Refresh diagnostics to load reload health.",
  drill_down: EMPTY_DRILL_DOWN,
};

const EMPTY_OPERATOR_INSIGHTS: OperatorInsightsEnvelope = {
  generated_at_unix_ms: 0,
  summary: EMPTY_SUMMARY,
  hotspots: [],
  retention: EMPTY_RETENTION,
  sampling: EMPTY_SAMPLING,
  privacy: EMPTY_PRIVACY,
  operations: EMPTY_OPERATIONS,
  provider_health: EMPTY_PROVIDER_HEALTH,
  security: EMPTY_SECURITY,
  recall: EMPTY_RECALL,
  compaction: EMPTY_COMPACTION,
  safety_boundary: EMPTY_SAFETY,
  plugins: EMPTY_PLUGINS,
  cron: EMPTY_CRON,
  routines: EMPTY_ROUTINES,
  memory_learning: EMPTY_MEMORY_LEARNING,
  reload: EMPTY_RELOAD,
};

export function normalizeUsageInsightsEnvelope(
  value: UsageInsightsEnvelope,
): UsageInsightsEnvelope {
  return {
    ...value,
    timeline: Array.isArray(value.timeline) ? value.timeline : [],
    routing: {
      ...value.routing,
      recent_decisions: Array.isArray(value.routing?.recent_decisions)
        ? value.routing.recent_decisions
        : [],
    },
    budgets: {
      ...value.budgets,
      policies: Array.isArray(value.budgets?.policies) ? value.budgets.policies : [],
      evaluations: Array.isArray(value.budgets?.evaluations) ? value.budgets.evaluations : [],
    },
    alerts: Array.isArray(value.alerts) ? value.alerts : [],
    model_mix: Array.isArray(value.model_mix) ? value.model_mix : [],
    scope_mix: Array.isArray(value.scope_mix) ? value.scope_mix : [],
    tool_mix: Array.isArray(value.tool_mix) ? value.tool_mix : [],
    operator: normalizeOperatorInsightsEnvelope(value.operator),
  };
}

export function normalizeOperatorInsightsEnvelope(
  value: OperatorInsightsEnvelope | null | undefined,
): OperatorInsightsEnvelope {
  return {
    ...EMPTY_OPERATOR_INSIGHTS,
    ...value,
    summary: {
      ...EMPTY_SUMMARY,
      ...value?.summary,
    },
    hotspots: Array.isArray(value?.hotspots) ? value.hotspots : [],
    retention: {
      ...EMPTY_RETENTION,
      ...value?.retention,
    },
    sampling: {
      ...EMPTY_SAMPLING,
      ...value?.sampling,
      notes: Array.isArray(value?.sampling?.notes) ? value.sampling.notes : [],
    },
    privacy: {
      ...EMPTY_PRIVACY,
      ...value?.privacy,
    },
    operations: normalizeOperations(value?.operations),
    provider_health: normalizeProviderHealth(value?.provider_health),
    security: normalizeSecurity(value?.security),
    recall: normalizeRecall(value?.recall),
    compaction: normalizeCompaction(value?.compaction),
    safety_boundary: normalizeSafety(value?.safety_boundary),
    plugins: normalizePlugins(value?.plugins),
    cron: normalizeCron(value?.cron),
    routines: normalizeRoutines(value?.routines),
    memory_learning: normalizeMemoryLearning(value?.memory_learning),
    reload: normalizeReload(value?.reload),
  };
}

function normalizeOperations(
  value: OperatorOperationsOverviewInsight | null | undefined,
): OperatorOperationsOverviewInsight {
  return {
    ...EMPTY_OPERATIONS,
    ...value,
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeProviderHealth(
  value: OperatorProviderHealthInsight | null | undefined,
): OperatorProviderHealthInsight {
  return {
    ...EMPTY_PROVIDER_HEALTH,
    ...value,
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeSecurity(
  value: OperatorSecurityInsight | null | undefined,
): OperatorSecurityInsight {
  return {
    ...EMPTY_SECURITY,
    ...value,
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeRecall(value: OperatorRecallInsight | null | undefined): OperatorRecallInsight {
  return {
    ...EMPTY_RECALL,
    ...value,
    samples: Array.isArray(value?.samples) ? value.samples : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeCompaction(
  value: OperatorCompactionInsight | null | undefined,
): OperatorCompactionInsight {
  return {
    ...EMPTY_COMPACTION,
    ...value,
    samples: Array.isArray(value?.samples) ? value.samples : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeSafety(
  value: OperatorSafetyBoundaryInsight | null | undefined,
): OperatorSafetyBoundaryInsight {
  return {
    ...EMPTY_SAFETY,
    ...value,
    samples: Array.isArray(value?.samples) ? value.samples : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizePlugins(value: OperatorPluginInsight | null | undefined): OperatorPluginInsight {
  return {
    ...EMPTY_PLUGINS,
    ...value,
    samples: Array.isArray(value?.samples) ? value.samples : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeCron(value: OperatorCronInsight | null | undefined): OperatorCronInsight {
  return {
    ...EMPTY_CRON,
    ...value,
    samples: Array.isArray(value?.samples) ? value.samples : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeRoutines(
  value: OperatorRoutineInsight | null | undefined,
): OperatorRoutineInsight {
  return {
    ...EMPTY_ROUTINES,
    ...value,
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeMemoryLearning(
  value: OperatorMemoryLearningInsight | null | undefined,
): OperatorMemoryLearningInsight {
  return {
    ...EMPTY_MEMORY_LEARNING,
    ...value,
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeReload(value: OperatorReloadInsight | null | undefined): OperatorReloadInsight {
  return {
    ...EMPTY_RELOAD,
    ...value,
    hotspots: Array.isArray(value?.hotspots) ? value.hotspots : [],
    drill_down: normalizeDrillDown(value?.drill_down),
  };
}

function normalizeDrillDown(
  value: OperatorInsightDrillDown | null | undefined,
): OperatorInsightDrillDown {
  return {
    ...EMPTY_DRILL_DOWN,
    ...value,
  };
}
