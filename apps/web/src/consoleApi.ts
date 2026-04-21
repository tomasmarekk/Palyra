import {
  addChannelMessageReaction as addChannelMessageReactionRequest,
  deleteChannelMessage as deleteChannelMessageRequest,
  discardChannelDeadLetter as discardChannelDeadLetterRequest,
  drainChannelQueue as drainChannelQueueRequest,
  editChannelMessage as editChannelMessageRequest,
  getChannelRouterRules as getChannelRouterRulesRequest,
  getChannelRouterWarnings as getChannelRouterWarningsRequest,
  getChannelStatus as getChannelStatusRequest,
  listChannelLogs as listChannelLogsRequest,
  listChannelRouterPairings as listChannelRouterPairingsRequest,
  listChannels as listChannelsRequest,
  mintChannelRouterPairingCode as mintChannelRouterPairingCodeRequest,
  pauseChannelQueue as pauseChannelQueueRequest,
  previewChannelRoute as previewChannelRouteRequest,
  readChannelMessages as readChannelMessagesRequest,
  removeChannelMessageReaction as removeChannelMessageReactionRequest,
  replayChannelDeadLetter as replayChannelDeadLetterRequest,
  resumeChannelQueue as resumeChannelQueueRequest,
  searchChannelMessages as searchChannelMessagesRequest,
  sendChannelTestMessage as sendChannelTestMessageRequest,
  setChannelEnabled as setChannelEnabledRequest,
} from "./consoleApi/channels/core";
import {
  applyDiscordOnboarding as applyDiscordOnboardingRequest,
  probeDiscordOnboarding as probeDiscordOnboardingRequest,
  refreshChannelHealth as refreshChannelHealthRequest,
  sendChannelDiscordTestSend as sendChannelDiscordTestSendRequest,
} from "./consoleApi/channels/discord";
import {
  normalizeAuxiliaryTaskKind,
  normalizeAuxiliaryTaskState,
  normalizeQueueMode,
  normalizeQueuedInputState,
  type AuxiliaryTaskKind,
  type AuxiliaryTaskState,
  type QueueMode,
  type QueuedInputState,
} from "./console/runtimeContracts";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export type ChannelStatusEnvelope = {
  connector: JsonValue;
  runtime?: JsonValue;
  operations?: JsonValue;
  health_refresh?: JsonValue;
  action?: JsonValue;
};

export interface ChatSessionRecord {
  session_id: string;
  session_key: string;
  session_label?: string;
  principal: string;
  device_id: string;
  channel?: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  last_run_id?: string;
}

export interface SessionCatalogFamilyRelative {
  session_id: string;
  title: string;
  branch_state: string;
  relation: string;
}

export interface SessionCatalogFamilyRecord {
  root_title: string;
  sequence: number;
  family_size: number;
  parent_session_id?: string;
  parent_title?: string;
  relatives: SessionCatalogFamilyRelative[];
}

export interface SessionCatalogArtifactRecord {
  artifact_id: string;
  kind: string;
  label: string;
}

export interface SessionProjectContextFocusRecord {
  path: string;
  reason: string;
}

export interface SessionProjectContextEntryRecord {
  entry_id: string;
  order: number;
  path: string;
  source_kind: string;
  source_label: string;
  precedence_label: string;
  depth: number;
  root: boolean;
  active: boolean;
  disabled: boolean;
  approved: boolean;
  status: string;
  content_hash: string;
  loaded_at_unix_ms: number;
  modified_at_unix_ms?: number;
  estimated_tokens: number;
  discovery_reasons: string[];
  warnings: string[];
  preview_text: string;
}

export interface SessionProjectContextRecord {
  generated_at_unix_ms: number;
  active_entries: number;
  blocked_entries: number;
  approval_required_entries: number;
  disabled_entries: number;
  active_estimated_tokens: number;
  warnings: string[];
  focus_paths: SessionProjectContextFocusRecord[];
  entries: SessionProjectContextEntryRecord[];
}

export interface SessionCatalogRecapRecord {
  touched_files: string[];
  active_context_files: string[];
  project_context?: SessionProjectContextRecord;
  recent_artifacts: SessionCatalogArtifactRecord[];
  ctas: string[];
}

export interface SessionCatalogQuickControlRecord {
  value?: string;
  display_value: string;
  source: string;
  inherited_value?: string;
  override_active: boolean;
}

export interface SessionCatalogToggleControlRecord {
  value: boolean;
  source: string;
  inherited_value: boolean;
  override_active: boolean;
}

export interface SessionCatalogQuickControlsRecord {
  agent: SessionCatalogQuickControlRecord;
  model: SessionCatalogQuickControlRecord;
  thinking: SessionCatalogToggleControlRecord;
  trace: SessionCatalogToggleControlRecord;
  verbose: SessionCatalogToggleControlRecord;
  reset_to_default_available: boolean;
}

export interface SessionCatalogRecord extends ChatSessionRecord {
  title: string;
  title_source: string;
  title_generation_state: string;
  manual_title_locked: boolean;
  auto_title_updated_at_unix_ms?: number;
  manual_title_updated_at_unix_ms?: number;
  preview?: string;
  preview_state: string;
  last_intent?: string;
  last_intent_state: string;
  last_summary?: string;
  last_summary_state: string;
  branch_state: string;
  parent_session_id?: string;
  branch_origin_run_id?: string;
  last_run_state?: string;
  last_run_started_at_unix_ms?: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  archived: boolean;
  archived_at_unix_ms?: number;
  pending_approvals: number;
  has_context_files: boolean;
  last_context_file?: string;
  agent_id?: string;
  model_profile?: string;
  artifact_count: number;
  family: SessionCatalogFamilyRecord;
  recap: SessionCatalogRecapRecord;
  quick_controls: SessionCatalogQuickControlsRecord;
}

export interface SessionCatalogSummary {
  active_sessions: number;
  archived_sessions: number;
  sessions_with_pending_approvals: number;
  sessions_with_active_runs: number;
  sessions_with_context_files: number;
}

export interface SessionCatalogListEnvelope {
  contract: ContractDescriptor;
  sessions: SessionCatalogRecord[];
  summary: SessionCatalogSummary;
  query: {
    limit: number;
    cursor: number;
    q?: string;
    include_archived: boolean;
    archived?: boolean;
    sort: string;
    title_source?: string;
    has_pending_approvals?: boolean;
    branch_state?: string;
    has_context_files?: boolean;
    agent_id?: string;
    model_profile?: string;
    title_state?: string;
  };
  page: PageInfo;
}

export interface SessionCatalogDetailEnvelope {
  contract: ContractDescriptor;
  session: SessionCatalogRecord;
}

export interface SessionCanvasListEnvelope {
  contract: ContractDescriptor;
  session: SessionCatalogRecord;
  canvases: SessionCanvasSummary[];
}

export interface SessionCanvasDetailEnvelope {
  contract: ContractDescriptor;
  session: SessionCatalogRecord;
  canvas: SessionCanvasSummary;
  runtime?: SessionCanvasRuntimeDescriptor | null;
  runtime_error?: string | null;
  state: JsonValue;
  revisions: SessionCanvasRevisionRecord[];
}

export interface SessionCanvasRestoreEnvelope extends SessionCanvasDetailEnvelope {
  restored_from_state_version: number;
  previous_state_version: number;
}

export interface ProjectContextRiskFinding {
  finding_id: string;
  action: "allow" | "warning" | "approval_required" | "blocked";
  title: string;
  detail: string;
  rule_id?: string;
  matched_text?: string;
}

export interface ProjectContextRiskScan {
  recommended_action: "allow" | "warning" | "approval_required" | "blocked";
  score: number;
  findings: ProjectContextRiskFinding[];
}

export interface ProjectContextFocusPath {
  path: string;
  reason: string;
}

export interface ProjectContextStackEntry {
  entry_id: string;
  order: number;
  path: string;
  directory: string;
  source_kind: string;
  source_label: string;
  precedence_label: string;
  depth: number;
  root: boolean;
  active: boolean;
  disabled: boolean;
  approved: boolean;
  status: string;
  estimated_tokens: number;
  content_hash: string;
  loaded_at_unix_ms: number;
  modified_at_unix_ms?: number;
  byte_size: number;
  line_count: number;
  discovery_reasons: string[];
  warnings: string[];
  risk: ProjectContextRiskScan;
  preview_text: string;
  resolved_text: string;
}

export interface ProjectContextPreviewEnvelope {
  generated_at_unix_ms: number;
  active_estimated_tokens: number;
  active_entries: number;
  blocked_entries: number;
  approval_required_entries: number;
  disabled_entries: number;
  warnings: string[];
  focus_paths: ProjectContextFocusPath[];
  entries: ProjectContextStackEntry[];
}

export interface ProjectContextScaffoldOutcome {
  path: string;
  content_hash: string;
  preview_text: string;
  created_at_unix_ms: number;
  overwritten: boolean;
}

export interface SessionProjectContextEnvelope {
  contract: ContractDescriptor;
  session: SessionCatalogRecord;
  preview: ProjectContextPreviewEnvelope;
  action: string;
  scaffold?: ProjectContextScaffoldOutcome;
}

export interface UsageQueryEcho {
  start_at_unix_ms: number;
  end_at_unix_ms: number;
  bucket: string;
  bucket_width_ms: number;
  include_archived: boolean;
}

export interface UsagePaginationQueryEcho extends UsageQueryEcho {
  limit: number;
  cursor: number;
}

export interface UsageSessionDetailQueryEcho extends UsageQueryEcho {
  run_limit: number;
}

export interface UsageTotals {
  runs: number;
  session_count: number;
  active_runs: number;
  completed_runs: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  average_latency_ms?: number;
  latest_started_at_unix_ms?: number;
  estimated_cost_usd?: number | null;
}

export interface UsageTimelineBucket {
  bucket_start_unix_ms: number;
  bucket_end_unix_ms: number;
  runs: number;
  session_count: number;
  active_runs: number;
  completed_runs: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  average_latency_ms?: number;
  estimated_cost_usd?: number | null;
}

export interface UsageSummaryEnvelope {
  contract: ContractDescriptor;
  query: UsageQueryEcho;
  totals: UsageTotals;
  timeline: UsageTimelineBucket[];
  cost_tracking_available: boolean;
}

export interface UsageSessionRecord extends ChatSessionRecord {
  runs: number;
  active_runs: number;
  completed_runs: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  average_latency_ms?: number;
  latest_started_at_unix_ms?: number;
  archived: boolean;
  archived_at_unix_ms?: number;
  estimated_cost_usd?: number | null;
}

export interface UsageSessionsEnvelope {
  contract: ContractDescriptor;
  query: UsagePaginationQueryEcho;
  sessions: UsageSessionRecord[];
  page: PageInfo;
  cost_tracking_available: boolean;
}

export interface UsageSessionDetailEnvelope {
  contract: ContractDescriptor;
  query: UsageSessionDetailQueryEcho;
  session: UsageSessionRecord;
  totals: UsageTotals;
  timeline: UsageTimelineBucket[];
  runs: ChatRunStatusRecord[];
  cost_tracking_available: boolean;
}

export interface UsageAgentRecord {
  agent_id: string;
  display_name: string;
  binding_source: string;
  default_model_profile?: string;
  session_count: number;
  runs: number;
  active_runs: number;
  completed_runs: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  average_latency_ms?: number;
  latest_started_at_unix_ms?: number;
  estimated_cost_usd?: number | null;
}

export interface UsageAgentsEnvelope {
  contract: ContractDescriptor;
  query: UsagePaginationQueryEcho;
  agents: UsageAgentRecord[];
  page: PageInfo;
  cost_tracking_available: boolean;
}

export interface UsageModelRecord {
  model_id: string;
  display_name: string;
  model_source: string;
  agent_count: number;
  session_count: number;
  runs: number;
  active_runs: number;
  completed_runs: number;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  average_latency_ms?: number;
  latest_started_at_unix_ms?: number;
  estimated_cost_usd?: number | null;
}

export interface UsageModelsEnvelope {
  contract: ContractDescriptor;
  query: UsagePaginationQueryEcho;
  models: UsageModelRecord[];
  page: PageInfo;
  cost_tracking_available: boolean;
}

export interface UsageInsightsPricingSummary {
  known_entries: number;
  estimated_models: number;
  estimate_only: boolean;
}

export interface UsageInsightsHealthSummary {
  provider_state: string;
  provider_kind: string;
  error_rate_bps: number;
  circuit_open: boolean;
  cooldown_ms: number;
  avg_latency_ms: number;
  recent_routing_overrides: number;
}

export interface UsageBudgetEvaluation {
  policy_id: string;
  scope_kind: string;
  scope_id: string;
  metric_kind: string;
  interval_kind: string;
  action: string;
  status: string;
  consumed_value?: number;
  projected_value?: number;
  soft_limit_value?: number;
  hard_limit_value?: number;
  message: string;
}

export interface UsageBudgetPolicyRecord {
  policy_id: string;
  scope_kind: string;
  scope_id: string;
  metric_kind: string;
  interval_kind: string;
  soft_limit_value?: number;
  hard_limit_value?: number;
  action: string;
  routing_mode_override?: string;
  enabled: boolean;
  created_by_principal: string;
  updated_by_principal: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface UsageRoutingDecisionRecord {
  decision_id: string;
  run_id: string;
  session_id: string;
  principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  scope_id: string;
  mode: string;
  default_model_id: string;
  recommended_model_id: string;
  actual_model_id: string;
  provider_id: string;
  provider_kind: string;
  complexity_score: number;
  health_state: string;
  explanation_json: string;
  estimated_cost_lower_usd?: number;
  estimated_cost_upper_usd?: number;
  budget_outcome?: string;
  created_at_unix_ms: number;
}

export interface UsageAlertRecord {
  alert_id: string;
  alert_kind: string;
  severity: string;
  scope_kind: string;
  scope_id: string;
  summary: string;
  reason: string;
  recommended_action: string;
  source: string;
  dedupe_key: string;
  payload_json: string;
  first_observed_at_unix_ms: number;
  last_observed_at_unix_ms: number;
  occurrence_count: number;
  acknowledged_at_unix_ms?: number;
  resolved_at_unix_ms?: number;
}

export interface UsageInsightsRoutingSummary {
  default_mode: string;
  suggest_runs: number;
  dry_run_runs: number;
  enforced_runs: number;
  overrides: number;
  recent_decisions: UsageRoutingDecisionRecord[];
}

export interface UsageInsightsBudgetsEnvelope {
  policies: UsageBudgetPolicyRecord[];
  evaluations: UsageBudgetEvaluation[];
}

export interface OperatorInsightsRetentionPolicy {
  source_of_truth: string;
  aggregation_mode: string;
  derived_metrics_persisted: boolean;
  support_bundle_embeds_latest_snapshot: boolean;
  window_start_at_unix_ms: number;
  window_end_at_unix_ms: number;
}

export interface OperatorInsightsSamplingPolicy {
  run_sample_limit: number;
  tape_event_limit_per_run: number;
  cron_run_limit: number;
  plugin_limit: number;
  observed_runs: number;
  sampled_runs: number;
  observed_cron_runs: number;
  sampled_cron_runs: number;
  observed_plugins: number;
  sampled_plugins: number;
  notes: string[];
}

export interface OperatorInsightsPrivacyPolicy {
  redaction_mode: string;
  raw_queries_included: boolean;
  raw_error_messages_included: boolean;
  raw_config_values_included: boolean;
  secret_like_values_redacted: boolean;
}

export interface OperatorInsightDrillDown {
  label: string;
  section: string;
  api_path: string;
  console_path: string;
}

export interface OperatorInsightHotspot {
  hotspot_id: string;
  subsystem: string;
  state: string;
  severity: string;
  summary: string;
  detail: string;
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorInsightsSummary {
  state: string;
  severity: string;
  hotspot_count: number;
  blocking_hotspots: number;
  warning_hotspots: number;
  recommendation: string;
}

export interface OperatorProviderHealthInsight {
  state: string;
  severity: string;
  summary: string;
  provider_kind: string;
  error_rate_bps: number;
  avg_latency_ms: number;
  circuit_open: boolean;
  auth_state: string;
  refresh_failures: number;
  response_cache_enabled: boolean;
  response_cache_entries: number;
  response_cache_hit_rate_bps: number;
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorRecallSample {
  run_id: string;
  session_id?: string;
  kind: string;
  query_preview: string;
  total_hits: number;
  memory_hits: number;
  workspace_hits: number;
  transcript_hits: number;
  checkpoint_hits: number;
  compaction_hits: number;
}

export interface OperatorRecallInsight {
  state: string;
  severity: string;
  summary: string;
  explicit_recall_events: number;
  explicit_recall_zero_hit_events: number;
  explicit_recall_zero_hit_rate_bps: number;
  auto_inject_events: number;
  auto_inject_zero_hit_events: number;
  auto_inject_avg_hits: number;
  samples: OperatorRecallSample[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorCompactionSample {
  run_id: string;
  session_id?: string;
  trigger: string;
  token_delta: number;
  estimated_input_tokens: number;
  estimated_output_tokens: number;
  artifact_id?: string;
}

export interface OperatorCompactionInsight {
  state: string;
  severity: string;
  summary: string;
  preview_events: number;
  created_events: number;
  dry_run_events: number;
  avg_token_delta: number;
  avg_reduction_bps: number;
  samples: OperatorCompactionSample[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorSafetySample {
  run_id: string;
  tool_name: string;
  reason: string;
  approval_required: boolean;
}

export interface OperatorSafetyBoundaryInsight {
  state: string;
  severity: string;
  summary: string;
  inspected_tool_decisions: number;
  denied_tool_decisions: number;
  policy_enforced_denies: number;
  approval_required_decisions: number;
  deny_rate_bps: number;
  samples: OperatorSafetySample[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorPluginSample {
  plugin_id: string;
  discovery_state?: string;
  config_state?: string;
  contracts_mode?: string;
  reasons: string[];
}

export interface OperatorPluginInsight {
  state: string;
  severity: string;
  summary: string;
  total_bindings: number;
  ready_bindings: number;
  unhealthy_bindings: number;
  typed_contract_failures: number;
  config_failures: number;
  discovery_failures: number;
  samples: OperatorPluginSample[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorCronRunSample {
  run_id: string;
  job_id: string;
  status: string;
  error_kind?: string;
  tool_denies: number;
}

export interface OperatorCronInsight {
  state: string;
  severity: string;
  summary: string;
  total_runs: number;
  failed_runs: number;
  success_rate_bps: number;
  total_tool_denies: number;
  samples: OperatorCronRunSample[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorReloadHotspot {
  ref_id: string;
  config_path: string;
  state: string;
  severity: string;
  reload_mode: string;
  advice?: string;
}

export interface OperatorReloadInsight {
  state: string;
  severity: string;
  summary: string;
  blocking_refs: number;
  warning_refs: number;
  hotspots: OperatorReloadHotspot[];
  recommended_action: string;
  drill_down: OperatorInsightDrillDown;
}

export interface OperatorInsightsEnvelope {
  generated_at_unix_ms: number;
  summary: OperatorInsightsSummary;
  hotspots: OperatorInsightHotspot[];
  retention: OperatorInsightsRetentionPolicy;
  sampling: OperatorInsightsSamplingPolicy;
  privacy: OperatorInsightsPrivacyPolicy;
  provider_health: OperatorProviderHealthInsight;
  recall: OperatorRecallInsight;
  compaction: OperatorCompactionInsight;
  safety_boundary: OperatorSafetyBoundaryInsight;
  plugins: OperatorPluginInsight;
  cron: OperatorCronInsight;
  reload: OperatorReloadInsight;
}

export interface UsageInsightsEnvelope {
  contract: ContractDescriptor;
  query: UsageQueryEcho;
  totals: UsageTotals;
  timeline: UsageTimelineBucket[];
  pricing: UsageInsightsPricingSummary;
  health: UsageInsightsHealthSummary;
  routing: UsageInsightsRoutingSummary;
  budgets: UsageInsightsBudgetsEnvelope;
  alerts: UsageAlertRecord[];
  model_mix: Array<{
    model_id: string;
    provider_kind: string;
    runs: number;
    total_tokens: number;
    estimated_cost_usd?: number;
    source: string;
  }>;
  scope_mix: Array<{
    scope: string;
    runs: number;
    total_tokens: number;
    estimated_cost_usd?: number;
  }>;
  tool_mix: Array<{
    tool_name: string;
    proposals: number;
  }>;
  operator: OperatorInsightsEnvelope;
  cost_tracking_available: boolean;
}

export interface UsageBudgetOverrideRequestEnvelope {
  contract: ContractDescriptor;
  operator_principal: string;
  policy: UsageBudgetPolicyRecord;
  approval: {
    approval_id: string;
    subject_id: string;
    request_summary: string;
    decision?: string;
    decision_scope?: string;
  };
}

export interface LogQueryEcho {
  limit: number;
  direction: string;
  cursor?: string;
  source?: string;
  severity?: string;
  contains?: string;
  start_at_unix_ms?: number;
  end_at_unix_ms?: number;
}

export interface LogRecord {
  cursor: string;
  source: string;
  source_kind: string;
  severity: string;
  message: string;
  timestamp_unix_ms: number;
  session_id?: string;
  run_id?: string;
  device_id?: string;
  connector_id?: string;
  event_name?: string;
  structured_payload?: JsonValue;
}

export interface LogListEnvelope {
  contract: ContractDescriptor;
  query: LogQueryEcho;
  records: LogRecord[];
  page: PageInfo;
  newest_cursor?: string;
  available_sources: string[];
}

export interface ChatRunStatusRecord {
  run_id: string;
  session_id: string;
  state: string;
  cancel_requested: boolean;
  cancel_reason?: string;
  principal: string;
  device_id: string;
  channel?: string;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  created_at_unix_ms: number;
  started_at_unix_ms: number;
  completed_at_unix_ms?: number;
  updated_at_unix_ms: number;
  last_error?: string;
  origin_kind: string;
  origin_run_id?: string;
  parent_run_id?: string;
  triggered_by_principal?: string;
  parameter_delta_json?: string;
  delegation?: ChatDelegationSnapshot;
  merge_result?: ChatDelegationMergeResult;
  tape_events: number;
}

export interface ChatDelegationMergeContract {
  strategy: string;
  approval_required: boolean;
}

export interface ChatDelegationRuntimeLimits {
  max_concurrent_children: number;
  max_children_per_parent: number;
  max_parallel_groups: number;
  child_budget_override?: number;
  child_timeout_ms: number;
}

export interface ChatDelegationSnapshot {
  profile_id: string;
  display_name: string;
  description?: string;
  template_id?: string;
  role: string;
  execution_mode: "serial" | "parallel";
  group_id: string;
  model_profile: string;
  tool_allowlist: string[];
  skill_allowlist: string[];
  memory_scope: string;
  budget_tokens: number;
  max_attempts: number;
  merge_contract: ChatDelegationMergeContract;
  runtime_limits: ChatDelegationRuntimeLimits;
  agent_id?: string;
}

export interface ChatDelegationMergeProvenanceRecord {
  child_run_id: string;
  kind: string;
  label: string;
  excerpt: string;
  tool_name?: string;
  requires_approval: boolean;
}

export interface ChatDelegationMergeUsageSummary {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  started_at_unix_ms?: number;
  completed_at_unix_ms?: number;
  duration_ms?: number;
}

export interface ChatDelegationMergeApprovalSummary {
  approval_required: boolean;
  approval_events: number;
  approval_pending: boolean;
  approval_denied: boolean;
}

export interface ChatDelegationMergeArtifactReference {
  artifact_id: string;
  artifact_kind: string;
  label: string;
}

export interface ChatDelegationToolTraceSummary {
  child_run_id: string;
  proposal_id?: string;
  tool_name: string;
  status: string;
  excerpt: string;
  requires_approval: boolean;
}

export interface ChatDelegationMergeResult {
  status: string;
  strategy: string;
  summary_text: string;
  warnings: string[];
  failure_category?: string;
  approval_required: boolean;
  approval_summary: ChatDelegationMergeApprovalSummary;
  usage_summary: ChatDelegationMergeUsageSummary;
  artifact_references?: ChatDelegationMergeArtifactReference[];
  tool_trace_summary?: ChatDelegationToolTraceSummary[];
  provenance: ChatDelegationMergeProvenanceRecord[];
  merged_at_unix_ms?: number;
}

export interface ChatDelegationProfileDefinition {
  profile_id: string;
  display_name: string;
  description: string;
  role: string;
  model_profile: string;
  tool_allowlist: string[];
  skill_allowlist: string[];
  memory_scope: string;
  budget_tokens: number;
  max_attempts: number;
  execution_mode: "serial" | "parallel";
  merge_contract: ChatDelegationMergeContract;
  runtime_limits: ChatDelegationRuntimeLimits;
}

export interface ChatDelegationTemplateDefinition {
  template_id: string;
  display_name: string;
  description: string;
  primary_profile_id: string;
  recommended_profiles: string[];
  execution_mode: "serial" | "parallel";
  merge_strategy: string;
  runtime_limits?: ChatDelegationRuntimeLimits;
  examples: string[];
}

export interface ChatDelegationCatalog {
  profiles: ChatDelegationProfileDefinition[];
  templates: ChatDelegationTemplateDefinition[];
}

export interface ChatRunLineage {
  focus_run_id: string;
  root_run_id: string;
  runs: ChatRunStatusRecord[];
}

export interface ChatRunTapeRecord {
  seq: number;
  event_type: string;
  payload_json: string;
}

export interface ChatRunTapeSnapshot {
  run_id: string;
  requested_after_seq?: number;
  limit: number;
  max_response_bytes: number;
  returned_bytes: number;
  next_after_seq?: number;
  events: ChatRunTapeRecord[];
}

export interface ChatTranscriptRecord {
  session_id: string;
  run_id: string;
  seq: number;
  event_type: string;
  payload_json: string;
  created_at_unix_ms: number;
  origin_kind: string;
  origin_run_id?: string;
}

export interface CanvasTranscriptReference {
  source_run_id?: string;
  source_tape_seq?: number;
  source_event_type?: string;
  origin_kind?: string;
  origin_run_id?: string;
  last_referenced_at_unix_ms?: number;
}

export interface SessionCanvasSummary {
  canvas_id: string;
  session_id: string;
  state_version: number;
  state_schema_version: number;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  expires_at_unix_ms: number;
  closed: boolean;
  close_reason?: string;
  runtime_status: string;
  reference: CanvasTranscriptReference;
}

export interface SessionCanvasRuntimeDescriptor {
  canvas_id: string;
  frame_url: string;
  runtime_url: string;
  auth_token: string;
  expires_at_unix_ms: number;
}

export interface SessionCanvasRevisionRecord {
  seq: number;
  canvas_id: string;
  state_version: number;
  base_state_version: number;
  state_schema_version: number;
  patch_json: string;
  resulting_state_json: string;
  closed: boolean;
  close_reason?: string;
  actor_principal: string;
  actor_device_id: string;
  applied_at_unix_ms: number;
}

export interface ChatQueuedInputRecord {
  queued_input_id: string;
  run_id: string;
  session_id: string;
  state: QueuedInputState;
  queue_mode: QueueMode;
  priority_lane: string;
  coalescing_group?: string;
  overflow_summary_ref?: string;
  safe_boundary_flags_json: string;
  decision_reason: string;
  text: string;
  accepted_at_unix_ms?: number;
  coalesced_at_unix_ms?: number;
  forwarded_at_unix_ms?: number;
  terminal_at_unix_ms?: number;
  policy_snapshot_json: string;
  explain_json: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  origin_run_id?: string;
}

export interface ChatQueueControlRecord {
  session_id: string;
  paused: boolean;
  pause_reason?: string;
  updated_at_unix_ms: number;
}

export interface ChatQueuePolicySnapshot {
  session_id: string;
  control: ChatQueueControlRecord;
  policy: JsonValue;
  safe_boundary: JsonValue;
  active_run_id?: string;
  queued_inputs: ChatQueuedInputRecord[];
  metrics: {
    pending_depth: number;
    terminal_count: number;
    total_count: number;
  };
  decision_preview: JsonValue;
  contract: ContractDescriptor;
}

export interface ChatQueueActionEnvelope {
  action: string;
  control?: ChatQueueControlRecord;
  queue: ChatQueuePolicySnapshot;
  queued_input?: ChatQueuedInputRecord;
  queued_input_id?: string;
  drained_count?: number;
  merged_count?: number;
  contract: ContractDescriptor;
}

export interface ChatPinRecord {
  pin_id: string;
  session_id: string;
  run_id: string;
  tape_seq: number;
  title: string;
  note?: string;
  created_at_unix_ms: number;
}

export interface ChatCompactionPreview {
  eligible: boolean;
  strategy: string;
  compressor_version: string;
  trigger_reason: string;
  trigger_policy?: string;
  estimated_input_tokens: number;
  estimated_output_tokens: number;
  token_delta: number;
  source_event_count: number;
  protected_event_count: number;
  condensed_event_count: number;
  omitted_event_count: number;
  summary_text: string;
  summary_preview: string;
  source_records: JsonValue;
  summary: JsonValue;
}

export interface ChatCompactionArtifactRecord {
  artifact_id: string;
  session_id: string;
  run_id?: string;
  mode: string;
  strategy: string;
  compressor_version: string;
  trigger_reason: string;
  trigger_policy?: string;
  trigger_inputs_json?: string;
  summary_text: string;
  summary_preview: string;
  source_event_count: number;
  protected_event_count: number;
  condensed_event_count: number;
  omitted_event_count: number;
  estimated_input_tokens: number;
  estimated_output_tokens: number;
  source_records_json: string;
  summary_json: string;
  created_by_principal: string;
  created_at_unix_ms: number;
}

export interface ChatCheckpointRecord {
  checkpoint_id: string;
  session_id: string;
  run_id?: string;
  name: string;
  tags_json: string;
  note?: string;
  branch_state: string;
  parent_session_id?: string;
  referenced_compaction_ids_json: string;
  workspace_paths_json: string;
  created_by_principal: string;
  created_at_unix_ms: number;
  restore_count: number;
  last_restored_at_unix_ms?: number;
}

export interface WorkspaceCheckpointSummary {
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  source_kind: string;
  source_label: string;
  checkpoint_stage: string;
  mutation_id?: string;
  paired_checkpoint_id?: string;
  tool_name?: string;
  proposal_id?: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  summary_text: string;
  diff_summary: JsonValue;
  compare_summary: JsonValue;
  risk_level: string;
  review_posture: string;
  created_at_unix_ms: number;
  restore_count: number;
  last_restored_at_unix_ms?: number;
  latest_restore_report_id?: string;
}

export interface WorkspaceArtifactVersion {
  artifact_id: string;
  checkpoint_id: string;
  checkpoint_created_at_unix_ms: number;
  change_kind: string;
  moved_from_path?: string;
  content_type: string;
  is_text: boolean;
  size_bytes?: number;
  content_sha256?: string;
  deleted: boolean;
}

export interface WorkspaceArtifactRecord {
  artifact_id: string;
  path: string;
  display_path: string;
  workspace_root_index: number;
  latest_checkpoint_id: string;
  latest_checkpoint_created_at_unix_ms: number;
  latest_checkpoint_label: string;
  source_kind: string;
  source_label: string;
  tool_name?: string;
  proposal_id?: string;
  device_id: string;
  channel?: string;
  change_kind: string;
  moved_from_path?: string;
  content_type: string;
  preview_kind: string;
  is_text: boolean;
  preview_text?: string;
  size_bytes?: number;
  content_sha256?: string;
  deleted: boolean;
  version_count: number;
  versions: WorkspaceArtifactVersion[];
}

export interface WorkspaceArtifactDetail {
  artifact: WorkspaceArtifactRecord;
  checkpoint: WorkspaceCheckpointSummary;
  content_available: boolean;
  content_truncated: boolean;
  text_content?: string;
  content_base64?: string;
}

export interface WorkspaceDiffSide {
  artifact_id: string;
  checkpoint_id: string;
  change_kind: string;
  content_type: string;
  size_bytes?: number;
  content_sha256?: string;
  deleted: boolean;
}

export interface WorkspaceDiffFileRecord {
  path: string;
  display_path: string;
  workspace_root_index: number;
  left?: WorkspaceDiffSide;
  right?: WorkspaceDiffSide;
  diff_kind: string;
  diff_text?: string;
}

export interface WorkspaceAnchorSummary {
  kind: string;
  id: string;
  label: string;
  session_id: string;
  run_id: string;
  created_at_unix_ms: number;
}

export interface WorkspaceDiffResponse {
  left_anchor: WorkspaceAnchorSummary;
  right_anchor: WorkspaceAnchorSummary;
  files_changed: number;
  files: WorkspaceDiffFileRecord[];
}

export interface WorkspaceRestoreFailure {
  path: string;
  display_path: string;
  workspace_root_index: number;
  error: string;
}

export interface WorkspaceCheckpointFileRecord {
  artifact_id: string;
  checkpoint_id: string;
  path: string;
  workspace_root_index: number;
  moved_from_path?: string;
  change_kind: string;
  before_content_sha256?: string;
  before_size_bytes?: number;
  after_content_sha256?: string;
  after_size_bytes?: number;
  blob_sha256?: string;
  content_type: string;
  is_text: boolean;
  preview_text?: string;
  search_text?: string;
  created_at_unix_ms: number;
}

export interface WorkspaceCheckpointRecord {
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  source_kind: string;
  source_label: string;
  checkpoint_stage: string;
  mutation_id?: string;
  paired_checkpoint_id?: string;
  tool_name?: string;
  proposal_id?: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  summary_text: string;
  diff_summary_json: string;
  compare_summary_json: string;
  risk_level: string;
  review_posture: string;
  created_at_unix_ms: number;
  restore_count: number;
  last_restored_at_unix_ms?: number;
  latest_restore_report_id?: string;
}

export interface WorkspaceRestoreReportRecord {
  report_id: string;
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  target_path?: string;
  restored_paths_json: string;
  failed_paths_json: string;
  reconciliation_summary: string;
  reconciliation_prompt: string;
  branched_session_id?: string;
  result_state: string;
  created_at_unix_ms: number;
}

export interface WorkspaceRestoreReportSummary {
  report_id: string;
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  target_path?: string;
  reconciliation_summary: string;
  reconciliation_prompt: string;
  branched_session_id?: string;
  result_state: string;
  created_at_unix_ms: number;
}

export interface WorkspaceRestoreReportDetail {
  report: WorkspaceRestoreReportSummary;
  checkpoint: WorkspaceCheckpointSummary;
  restored_paths: string[];
  failed_paths: WorkspaceRestoreFailure[];
}

export interface WorkspaceRestoreActivitySummary {
  checkpoint_count: number;
  preflight_checkpoint_count: number;
  post_change_checkpoint_count: number;
  paired_checkpoint_count: number;
  missing_checkpoint_pair_count: number;
  high_risk_mutation_count: number;
  review_required_mutation_count: number;
  checkpoint_restore_total: number;
  restore_report_count: number;
  succeeded_restore_count: number;
  partial_failure_restore_count: number;
  failed_restore_count: number;
  restore_success_rate_bps: number;
  missing_checkpoint_pair_rate_bps: number;
  high_risk_mutation_rate_bps: number;
}

export interface WorkspaceActivitySnapshot {
  summary: WorkspaceRestoreActivitySummary;
  recent_checkpoints: WorkspaceCheckpointSummary[];
  recent_restore_reports: WorkspaceRestoreReportSummary[];
}

export interface RunWorkspaceArtifactsResponse {
  artifacts: WorkspaceArtifactRecord[];
  workspace_checkpoints: WorkspaceCheckpointSummary[];
  background_tasks: ChatBackgroundTaskRecord[];
  compactions: ChatCompactionArtifactRecord[];
  session_checkpoints: ChatCheckpointRecord[];
}

export interface WorkspaceRestoreOutcome {
  scope_kind: string;
  target_path?: string;
  target_workspace_root_index?: number;
  restored_paths: string[];
  failed_paths: WorkspaceRestoreFailure[];
  affects_context_stack: boolean;
  report: WorkspaceRestoreReportRecord;
}

export interface ChatRunWorkspaceEnvelope {
  run: ChatRunStatusRecord;
  workspace: RunWorkspaceArtifactsResponse;
  contract: ContractDescriptor;
}

export interface WorkspaceArtifactDetailEnvelope {
  run: ChatRunStatusRecord;
  detail: WorkspaceArtifactDetail;
  contract: ContractDescriptor;
}

export interface WorkspaceCompareEnvelope {
  diff: WorkspaceDiffResponse;
  contract: ContractDescriptor;
}

export interface WorkspaceCheckpointDetailEnvelope {
  session: SessionCatalogRecord;
  checkpoint: WorkspaceCheckpointRecord;
  files: WorkspaceCheckpointFileRecord[];
  restore_reports: WorkspaceRestoreReportRecord[];
  contract: ContractDescriptor;
}

export interface WorkspaceRestoreReportEnvelope {
  session: SessionCatalogRecord;
  detail: WorkspaceRestoreReportDetail;
  contract: ContractDescriptor;
}

export interface WorkspaceRestoreResponseEnvelope {
  session: SessionCatalogRecord;
  source_session: SessionCatalogRecord;
  checkpoint: WorkspaceCheckpointRecord;
  restore: WorkspaceRestoreOutcome;
  project_context_refresh?: JsonValue | null;
  project_context_refresh_error?: string | null;
  project_context_copy_error?: string | null;
  suggested_session_label?: string;
  action: string;
  contract: ContractDescriptor;
}

export interface ChatBackgroundTaskRecord {
  task_id: string;
  task_kind: AuxiliaryTaskKind;
  session_id: string;
  parent_run_id?: string;
  target_run_id?: string;
  queued_input_id?: string;
  owner_principal: string;
  device_id: string;
  channel?: string;
  state: AuxiliaryTaskState;
  priority: number;
  attempt_count: number;
  max_attempts: number;
  budget_tokens: number;
  delegation?: ChatDelegationSnapshot;
  not_before_unix_ms?: number;
  expires_at_unix_ms?: number;
  notification_target_json?: string;
  input_text?: string;
  payload_json?: string;
  last_error?: string;
  result_json?: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  started_at_unix_ms?: number;
  completed_at_unix_ms?: number;
}

type RawChatQueuedInputRecord = Omit<ChatQueuedInputRecord, "state"> & {
  state: string;
};

type RawChatQueuePolicySnapshot = Omit<ChatQueuePolicySnapshot, "queued_inputs"> & {
  queued_inputs: RawChatQueuedInputRecord[];
};

type RawChatQueueActionEnvelope = Omit<ChatQueueActionEnvelope, "queue" | "queued_input"> & {
  queue: RawChatQueuePolicySnapshot;
  queued_input?: RawChatQueuedInputRecord;
};

type RawChatBackgroundTaskRecord = Omit<ChatBackgroundTaskRecord, "task_kind" | "state"> & {
  task_kind: string;
  state: string;
};

export interface LearningCandidateRecord {
  candidate_id: string;
  candidate_kind: string;
  session_id: string;
  run_id?: string;
  owner_principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  scope_id: string;
  status: string;
  auto_applied: boolean;
  confidence: number;
  risk_level: string;
  title: string;
  summary: string;
  target_path?: string;
  dedupe_key: string;
  content_json: string;
  provenance_json: string;
  source_task_id?: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  reviewed_at_unix_ms?: number;
  reviewed_by_principal?: string;
  last_action_summary?: string;
  last_action_payload_json?: string;
}

export interface LearningCandidateHistoryRecord {
  history_id: string;
  candidate_id: string;
  status: string;
  reviewed_by_principal: string;
  action_summary?: string;
  action_payload_json?: string;
  created_at_unix_ms: number;
}

export interface LearningPreferenceRecord {
  preference_id: string;
  owner_principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  scope_id: string;
  key: string;
  value: string;
  source_kind: string;
  status: string;
  confidence: number;
  candidate_id?: string;
  provenance_json: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface ChatAttachmentRecord {
  artifact_id: string;
  attachment_id: string;
  filename: string;
  declared_content_type: string;
  content_hash: string;
  size_bytes: number;
  width_px?: number;
  height_px?: number;
  kind: "image" | "file" | "audio" | "video";
  budget_tokens: number;
}

export interface MediaDerivedStatsSnapshot {
  total: number;
  pending: number;
  succeeded: number;
  failed: number;
  quarantined: number;
  purged: number;
  recompute_required: number;
  orphaned: number;
}

export interface MediaDerivedArtifactRecord {
  derived_artifact_id: string;
  source_artifact_id: string;
  attachment_id?: string;
  session_id?: string;
  principal?: string;
  device_id?: string;
  channel?: string;
  filename: string;
  declared_content_type: string;
  kind: string;
  state: string;
  parser_name: string;
  parser_version: string;
  source_content_hash: string;
  content_hash?: string;
  content_text?: string;
  summary_text?: string;
  language?: string;
  duration_ms?: number;
  processing_ms?: number;
  warnings: Array<{ code: string; message: string }>;
  anchors: Array<{
    kind: string;
    label: string;
    locator?: string;
    start_char: number;
    end_char: number;
  }>;
  failure_reason?: string;
  quarantine_reason?: string;
  workspace_document_id?: string;
  memory_item_id?: string;
  background_task_id?: string;
  recompute_required: boolean;
  orphaned: boolean;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  purged_at_unix_ms?: number;
}

export interface WorkspaceDocumentRecord {
  document_id: string;
  principal: string;
  channel?: string;
  agent_id?: string;
  latest_session_id?: string;
  path: string;
  parent_path?: string;
  title: string;
  kind: string;
  document_class: string;
  state: string;
  prompt_binding: string;
  risk_state: string;
  risk_reasons: string[];
  pinned: boolean;
  manual_override: boolean;
  template_id?: string;
  template_version?: number;
  source_memory_id?: string;
  latest_version: number;
  content_text: string;
  content_hash: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  deleted_at_unix_ms?: number;
  last_recalled_at_unix_ms?: number;
}

export interface WorkspaceDocumentVersionRecord {
  version_ulid: string;
  document_id: string;
  version: number;
  event_type: string;
  path: string;
  title?: string;
  kind: string;
  document_class: string;
  prompt_binding: string;
  risk_state: string;
  risk_reasons: string[];
  content_text: string;
  content_hash: string;
  created_at_unix_ms: number;
  source_session_id?: string;
}

export interface WorkspaceSearchHit {
  reason: string;
  snippet: string;
  score: number;
  version: number;
  chunk_index: number;
  chunk_count: number;
  document: WorkspaceDocumentRecord;
}

export interface RetrievalBranchDiagnostics {
  source_kind: string;
  query_embedding_cache_hit: boolean;
  lexical_latency_ms: number;
  vector_latency_ms: number;
  fusion_latency_ms: number;
  total_latency_ms: number;
  latency_budget_ms: number;
  latency_budget_exceeded: boolean;
  candidate_count: number;
  lexical_candidate_count: number;
  vector_candidate_count: number;
  fused_hit_count: number;
  degraded_reason?: string;
  coverage_gap?: string;
}

export interface WorkspaceBootstrapOutcome {
  ran_at_unix_ms: number;
  created_paths: string[];
  updated_paths: string[];
  skipped_paths: string[];
}

export interface RecallPlanSource {
  source_kind: string;
  decision: string;
  reason: string;
  requested_top_k: number;
  query: string;
}

export interface RecallPlan {
  original_query: string;
  expanded_queries: string[];
  session_scoped: boolean;
  budget: {
    prompt_budget_tokens: number;
    candidate_limit: number;
  };
  sources: RecallPlanSource[];
}

export interface RecallScoreBreakdown {
  lexical_score: number;
  vector_score: number;
  recency_score: number;
  source_quality_score: number;
  final_score: number;
}

export interface RecallCandidate {
  candidate_id: string;
  source_kind: string;
  source_ref: string;
  title: string;
  snippet: string;
  created_at_unix_ms: number;
  rationale: string;
  score: RecallScoreBreakdown;
}

export interface StructuredRecallFact {
  statement: string;
  evidence_ids: string[];
}

export interface StructuredRecallEvidence {
  evidence_id: string;
  source_kind: string;
  source_ref: string;
  title: string;
  snippet: string;
  rationale: string;
  score: RecallScoreBreakdown;
}

export interface StructuredRecallOutput {
  facts: StructuredRecallFact[];
  evidence: StructuredRecallEvidence[];
  why_relevant_now: string;
  suggested_next_step: string;
  confidence?: number;
}

export interface RecallPreviewEnvelope {
  query: string;
  memory_hits: JsonValue[];
  workspace_hits: WorkspaceSearchHit[];
  transcript_hits?: JsonValue[];
  checkpoint_hits?: JsonValue[];
  compaction_hits?: JsonValue[];
  top_candidates?: RecallCandidate[];
  structured_output?: StructuredRecallOutput;
  plan?: RecallPlan;
  diagnostics?: RetrievalBranchDiagnostics[];
  parameter_delta: JsonValue;
  prompt_preview: string;
  artifact?: RecallArtifactRecord;
  contract: ContractDescriptor;
}

export interface RecallArtifactRecord {
  artifact_id: string;
  artifact_kind: string;
  principal: string;
  device_id: string;
  channel?: string;
  session_id?: string;
  query: string;
  summary: string;
  payload: JsonValue;
  diagnostics: JsonValue;
  provenance: JsonValue;
  created_by_principal: string;
  created_at_unix_ms: number;
}

export interface ContextReferenceProvenance {
  kind: string;
  location: string;
  note: string;
}

export interface ContextReferenceResolvedRecord {
  reference_id: string;
  kind: "file" | "folder" | "diff" | "staged" | "url" | "memory";
  raw_text: string;
  target?: string;
  display_target: string;
  start_offset: number;
  end_offset: number;
  estimated_tokens: number;
  warnings: string[];
  provenance: ContextReferenceProvenance[];
  preview_text: string;
  resolved_text: string;
}

export interface ContextReferenceParseError {
  raw_text: string;
  message: string;
  start_offset: number;
  end_offset: number;
}

export interface ContextReferencePreviewEnvelope {
  clean_prompt: string;
  references: ContextReferenceResolvedRecord[];
  total_estimated_tokens: number;
  warnings: string[];
  errors: ContextReferenceParseError[];
  contract: ContractDescriptor;
}

export interface UnifiedSearchEnvelope {
  query: string;
  groups: {
    sessions: JsonValue[];
    workspace: WorkspaceSearchHit[];
    memory: JsonValue[];
  };
  counts: {
    sessions: number;
    workspace: number;
    memory: number;
  };
  contract: ContractDescriptor;
}

export interface SessionSearchEvent {
  session_id: string;
  run_id: string;
  seq: number;
  event_type: string;
  created_at_unix_ms: number;
  origin_kind: string;
  origin_run_id?: string;
  parent_run_id?: string;
  text: string;
  is_match: boolean;
}

export interface SessionSearchWindow {
  window_id: string;
  session_id: string;
  run_id: string;
  match_seq: number;
  match_event_type: string;
  match_created_at_unix_ms: number;
  score: number;
  snippet: string;
  before: SessionSearchEvent[];
  matched: SessionSearchEvent;
  after: SessionSearchEvent[];
  provenance: JsonValue;
}

export interface SessionSearchGroup {
  session: JsonValue;
  best_score: number;
  match_count: number;
  lineage: JsonValue;
  windows: SessionSearchWindow[];
}

export interface SessionSearchEnvelope {
  capability: "palyra.recall.session_search";
  query: string;
  groups: SessionSearchGroup[];
  diagnostics: RetrievalBranchDiagnostics;
  artifact?: RecallArtifactRecord;
  contract: ContractDescriptor;
}

export interface ChatStreamMetaLine {
  type: "meta";
  run_id: string;
  session_id: string;
}

export interface ChatStreamEventEnvelope {
  run_id: string;
  event_type: string;
  [key: string]: JsonValue;
}

export interface ChatStreamEventLine {
  type: "event";
  event: ChatStreamEventEnvelope;
}

export interface ChatStreamErrorLine {
  type: "error";
  run_id?: string;
  error: string;
}

export interface ChatStreamCompleteLine {
  type: "complete";
  run_id: string;
  status: string;
}

export type ChatStreamLine =
  | ChatStreamMetaLine
  | ChatStreamEventLine
  | ChatStreamErrorLine
  | ChatStreamCompleteLine;

export interface ConsoleProfileContext {
  name: string;
  label: string;
  environment: string;
  color: string;
  risk_level: string;
  strict_mode: boolean;
  mode: string;
}

export interface ConsoleSession {
  principal: string;
  device_id: string;
  channel?: string;
  profile?: ConsoleProfileContext;
  csrf_token: string;
  issued_at_unix_ms: number;
  expires_at_unix_ms: number;
}

export interface ConsoleDiagnosticsSnapshot {
  generated_at_unix_ms: number;
  model_provider: JsonValue;
  rate_limits: JsonValue;
  auth_profiles: JsonValue;
  browserd: JsonValue;
  feature_rollouts?: JsonValue;
  runtime_controls?: JsonValue;
  execution_backends?: JsonValue;
  observability?: JsonValue;
  memory?: JsonValue;
  media?: JsonValue;
}

export interface ContractDescriptor {
  contract_version: string;
}

export interface MobileReleaseScope {
  approvals_inbox: boolean;
  polling_notifications: boolean;
  recent_sessions: boolean;
  safe_url_open: boolean;
  voice_note: boolean;
}

export interface MobileNotificationPolicy {
  delivery_mode: string;
  quiet_hours_supported: boolean;
  grouping_supported: boolean;
  priority_supported: boolean;
  default_poll_interval_ms: number;
  max_alerts_per_poll: number;
}

export interface MobilePairingPolicy {
  auth_flow: string;
  trust_model: string;
  revoke_supported: boolean;
  recovery_supported: boolean;
  offline_state_visible: boolean;
}

export interface MobileHandoffPolicy {
  contract: string;
  safe_url_open_requires_mediation: boolean;
  heavy_surface_handoff_supported: boolean;
  browser_automation_exposed: boolean;
}

export interface MobileLocalStoreContract {
  approvals_cache_key: string;
  sessions_cache_key: string;
  inbox_cache_key: string;
  outbox_queue_key: string;
  revoke_marker_key: string;
}

export interface MobileRolloutStatus {
  mobile_companion_enabled: boolean;
  approvals_enabled: boolean;
  notifications_enabled: boolean;
  recent_sessions_enabled: boolean;
  safe_url_open_enabled: boolean;
  voice_notes_enabled: boolean;
}

export interface MobileBootstrapEnvelope {
  contract: ContractDescriptor;
  release_scope: MobileReleaseScope;
  notifications: MobileNotificationPolicy;
  pairing: MobilePairingPolicy;
  handoff: MobileHandoffPolicy;
  store: MobileLocalStoreContract;
  rollout: MobileRolloutStatus;
  locales: string[];
  default_locale: string;
}

export interface MobileApprovalInboxSummary {
  pending: number;
  ready_on_device: number;
  handoff_recommended: number;
}

export interface MobileApprovalsEnvelope {
  contract: ContractDescriptor;
  approvals: JsonValue[];
  summary: MobileApprovalInboxSummary;
  page: PageInfo;
}

export interface MobileApprovalExplainability {
  evaluation_summary: string;
  policy_explanation: string;
  recommended_surface: string;
  web_handoff_path?: string;
}

export interface MobileApprovalDetailEnvelope {
  contract: ContractDescriptor;
  approval: JsonValue;
  explainability: MobileApprovalExplainability;
}

export interface MobileHandoffTarget {
  path: string;
  intent?: string;
  requires_full_console: boolean;
}

export interface MobileSessionRecap {
  title: string;
  preview?: string;
  last_summary?: string;
  last_intent?: string;
  last_run_state?: string;
  pending_approvals: number;
  handoff_recommended: boolean;
}

export interface MobileSessionSummary {
  session: JsonValue;
  recap: MobileSessionRecap;
  handoff: MobileHandoffTarget;
}

export interface MobileSessionsEnvelope {
  contract: ContractDescriptor;
  sessions: MobileSessionSummary[];
  page: PageInfo;
}

export interface MobileSessionDetailEnvelope {
  contract: ContractDescriptor;
  session: JsonValue;
  recap: MobileSessionRecap;
  actions: string[];
}

export type MobileInboxItemKind = "approval" | "run_update" | "support";
export type MobileInboxPriority = "critical" | "high" | "medium" | "low";

export interface MobileInboxItem {
  alert_id: string;
  kind: MobileInboxItemKind;
  priority: MobileInboxPriority;
  group_key: string;
  title: string;
  body: string;
  session_id?: string;
  run_id?: string;
  approval_id?: string;
  task_id?: string;
  created_at_unix_ms: number;
  handoff?: MobileHandoffTarget;
}

export interface MobileInboxSummary {
  pending_approvals: number;
  active_tasks: number;
  completed_tasks: number;
  failed_tasks: number;
}

export interface MobileInboxEnvelope {
  contract: ContractDescriptor;
  delivery_mode: string;
  quiet_hours_respected: boolean;
  summary: MobileInboxSummary;
  alerts: MobileInboxItem[];
}

export interface MobileSafeUrlOpenEnvelope {
  contract: ContractDescriptor;
  action: string;
  target: string;
  normalized_url?: string;
  handoff_url?: string;
  reason?: string;
}

export interface MobileVoiceNoteEnvelope {
  contract: ContractDescriptor;
  session: JsonValue;
  task: JsonValue;
  queued_for_existing_session: boolean;
}

export type ToolPostureScopeKind = "global" | "workspace" | "agent" | "session";
export type ToolPostureState = "always_allow" | "ask_each_time" | "disabled";
export type ToolPostureRecommendationAction = "accepted" | "dismissed" | "deferred";

export interface ToolPostureScopeRef {
  kind: ToolPostureScopeKind;
  scope_id: string;
  label: string;
}

export interface ToolPostureChainEntry extends ToolPostureScopeRef {
  state?: ToolPostureState;
  source?: string;
}

export interface EffectiveToolPosture {
  effective_state: ToolPostureState;
  default_state: ToolPostureState;
  approval_mode: string;
  source_scope_kind: ToolPostureScopeKind;
  source_scope_id: string;
  source_scope_label: string;
  chain: ToolPostureChainEntry[];
  lock_reason?: string;
  editable: boolean;
}

export interface ToolFrictionMetrics {
  requested_14d: number;
  approved_14d: number;
  denied_14d: number;
  pending_14d: number;
  unique_sessions_14d: number;
}

export interface ToolPosturePresetAssignment {
  tool_name: string;
  state: ToolPostureState;
}

export interface ToolPosturePresetDefinition {
  preset_id: string;
  label: string;
  description: string;
  assignments: ToolPosturePresetAssignment[];
}

export interface ToolPostureAuditEventRecord {
  audit_id: string;
  scope_kind: ToolPostureScopeKind;
  scope_id: string;
  tool_name?: string;
  actor_principal: string;
  action: string;
  previous_state?: ToolPostureState;
  new_state?: ToolPostureState;
  source: string;
  reason?: string;
  recommendation_id?: string;
  preset_id?: string;
  created_at_unix_ms: number;
}

export interface ToolPostureRecommendation {
  recommendation_id: string;
  tool_name: string;
  scope_kind: ToolPostureScopeKind;
  scope_id: string;
  current_state: ToolPostureState;
  recommended_state: ToolPostureState;
  reason: string;
  approvals_14d: number;
  action?: ToolPostureRecommendationAction;
}

export interface ToolPermissionRecord {
  tool_name: string;
  title: string;
  description: string;
  category: string;
  risk_level: string;
  effective_posture: EffectiveToolPosture;
  friction: ToolFrictionMetrics;
  recent_approvals: JsonValue[];
  last_change?: ToolPostureAuditEventRecord;
  recommendation?: ToolPostureRecommendation;
}

export interface ToolPermissionsScopeEnvelope {
  active: ToolPostureScopeRef;
  workspace?: ToolPostureScopeRef;
  agent?: ToolPostureScopeRef;
  chain: ToolPostureScopeRef[];
}

export interface ToolPermissionsSummary {
  total_tools: number;
  locked_tools: number;
  high_friction_tools: number;
  approval_requests_14d: number;
  pending_approvals_14d: number;
}

export interface ToolPermissionsEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  scope: ToolPermissionsScopeEnvelope;
  summary: ToolPermissionsSummary;
  categories: string[];
  presets: ToolPosturePresetDefinition[];
  tools: ToolPermissionRecord[];
}

export interface ToolPermissionDetailEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  scope: ToolPermissionsScopeEnvelope;
  tool: ToolPermissionRecord;
  change_history: ToolPostureAuditEventRecord[];
}

export interface ToolPermissionPresetDiffEntry {
  tool_name: string;
  title: string;
  current_state: ToolPostureState;
  proposed_state: ToolPostureState;
  changed: boolean;
  locked: boolean;
  lock_reason?: string;
}

export interface ToolPermissionPresetPreviewEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  scope: ToolPermissionsScopeEnvelope;
  preset: ToolPosturePresetDefinition;
  preview: ToolPermissionPresetDiffEntry[];
}

export interface ToolPermissionMutationEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  override_record?: JsonValue;
  recommendation_action?: JsonValue;
  detail: ToolPermissionDetailEnvelope;
}

export interface ToolPermissionScopeResetEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  scope: ToolPermissionsScopeEnvelope;
  removed: JsonValue[];
}

export interface AccessFeatureFlagRecord {
  key: string;
  label: string;
  description: string;
  enabled: boolean;
  stage: string;
  depends_on: string[];
  updated_at_unix_ms: number;
  updated_by_principal: string;
}

export interface AccessApiTokenView {
  token_id: string;
  label: string;
  token_prefix: string;
  scopes: string[];
  principal: string;
  workspace_id?: string;
  role: string;
  rate_limit_per_minute: number;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  expires_at_unix_ms?: number;
  revoked_at_unix_ms?: number;
  last_used_at_unix_ms?: number;
  rotated_from_token_id?: string;
  status: string;
}

export interface AccessTeamRecord {
  team_id: string;
  slug: string;
  display_name: string;
  created_by_principal: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AccessWorkspaceRecord {
  workspace_id: string;
  team_id: string;
  slug: string;
  display_name: string;
  runtime_principal: string;
  runtime_device_id: string;
  created_by_principal: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AccessMembershipView {
  membership_id: string;
  workspace_id: string;
  workspace_name: string;
  team_id: string;
  team_name: string;
  principal: string;
  role: string;
  permissions: string[];
  created_by_principal: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AccessInvitationRecord {
  invitation_id: string;
  workspace_id: string;
  invited_identity: string;
  role: string;
  issued_by_principal: string;
  created_at_unix_ms: number;
  expires_at_unix_ms: number;
  accepted_by_principal?: string;
  accepted_at_unix_ms?: number;
}

export interface AccessShareRecord {
  share_id: string;
  resource_kind: string;
  resource_id: string;
  workspace_id: string;
  access_level: string;
  created_by_principal: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AccessTelemetrySummary {
  feature_key: string;
  total_events: number;
  success_events: number;
  error_events: number;
  latest_at_unix_ms?: number;
}

export interface AccessMigrationCheck {
  key: string;
  state: string;
  detail: string;
  remediation: string;
}

export interface AccessMigrationStatus {
  registry_path: string;
  version: number;
  backfill_required: boolean;
  blocking_issues: number;
  warning_issues: number;
  last_backfill_at_unix_ms?: number;
  checks: AccessMigrationCheck[];
}

export interface AccessBackfillReport {
  dry_run: boolean;
  changed_records: number;
  feature_flags_added: number;
  teams_repaired: number;
  workspaces_repaired: number;
  api_tokens_repaired: number;
  memberships_repaired: number;
  telemetry_trimmed: number;
  notes: string[];
}

export interface AccessRolloutPackageStatus {
  feature_key: string;
  label: string;
  enabled: boolean;
  stage: string;
  depends_on: string[];
  dependency_blockers: string[];
  safe_mode_when_disabled: boolean;
  kill_switch_command: string;
}

export interface AccessRolloutStatus {
  staged_rollout_enabled: boolean;
  external_api_safe_mode: boolean;
  team_mode_safe_mode: boolean;
  telemetry_events_retained: number;
  packages: AccessRolloutPackageStatus[];
  operator_notes: string[];
}

export interface AccessRegistrySnapshot {
  version: number;
  feature_flags: AccessFeatureFlagRecord[];
  api_tokens: AccessApiTokenView[];
  teams: AccessTeamRecord[];
  workspaces: AccessWorkspaceRecord[];
  memberships: AccessMembershipView[];
  invitations: AccessInvitationRecord[];
  shares: AccessShareRecord[];
  telemetry: AccessTelemetrySummary[];
  migration: AccessMigrationStatus;
  rollout: AccessRolloutStatus;
}

export interface PageInfo {
  limit: number;
  returned: number;
  has_more: boolean;
  next_cursor?: string;
}

export type ErrorCategory =
  | "auth"
  | "validation"
  | "policy"
  | "not_found"
  | "conflict"
  | "dependency"
  | "availability"
  | "internal";

export interface ValidationIssue {
  field: string;
  code: string;
  message: string;
}

export interface ErrorEnvelope {
  error?: string;
  code?: string;
  category?: ErrorCategory;
  retryable?: boolean;
  redacted?: boolean;
  validation_errors?: ValidationIssue[];
}

export interface CapabilityEntry {
  id: string;
  domain: string;
  dashboard_section: string;
  title: string;
  owner: string;
  surfaces: string[];
  execution_mode: string;
  dashboard_exposure?: "direct_action" | "cli_handoff" | "internal_only";
  cli_handoff_commands: string[];
  mutation_classes: string[];
  test_refs: string[];
  contract_paths: string[];
  notes?: string;
}

export interface CapabilityMigrationNote {
  id: string;
  message: string;
}

export interface CapabilityCatalog {
  contract: ContractDescriptor;
  version: string;
  generated_at_unix_ms: number;
  capabilities: CapabilityEntry[];
  migration_notes: CapabilityMigrationNote[];
}

export interface DeploymentPostureSummary {
  contract: ContractDescriptor;
  mode: string;
  bind_profile: string;
  bind_addresses: {
    admin: string;
    grpc: string;
    quic: string;
  };
  tls: {
    gateway_enabled: boolean;
  };
  admin_auth_required: boolean;
  dangerous_remote_bind_ack: {
    config: boolean;
    env: boolean;
    env_name: string;
  };
  remote_bind_detected: boolean;
  last_remote_admin_access_attempt?: {
    observed_at_unix_ms: number;
    remote_ip_fingerprint: string;
    method: string;
    path: string;
    status_code: number;
    outcome: string;
  };
  warnings: string[];
}

export type OnboardingFlow = "quick_start" | "advanced_setup";
export type OnboardingPostureState =
  | "not_started"
  | "in_progress"
  | "blocked"
  | "ready"
  | "complete";
export type OnboardingStepStatus = "todo" | "in_progress" | "blocked" | "done" | "skipped";
export type OnboardingActionKind =
  | "open_console_path"
  | "run_cli_command"
  | "open_desktop_section"
  | "read_docs";

export interface OnboardingStepAction {
  label: string;
  kind: OnboardingActionKind;
  surface: string;
  target: string;
}

export interface OnboardingBlockedReason {
  code: string;
  detail: string;
  repair_hint: string;
}

export interface OnboardingStepView {
  step_id: string;
  title: string;
  summary: string;
  status: OnboardingStepStatus;
  optional?: boolean;
  verification_state?: string;
  blocked?: OnboardingBlockedReason;
  action?: OnboardingStepAction;
}

export interface OnboardingStepCounts {
  todo: number;
  in_progress: number;
  blocked: number;
  done: number;
  skipped: number;
}

export interface OnboardingPostureEnvelope {
  contract: ContractDescriptor;
  flow: OnboardingFlow;
  flow_variant: string;
  status: OnboardingPostureState;
  config_path: string;
  resume_supported: boolean;
  ready_for_first_success: boolean;
  recommended_step_id?: string;
  first_success_hint?: string;
  counts: OnboardingStepCounts;
  available_flows: OnboardingFlow[];
  steps: OnboardingStepView[];
}

export interface AuthProfileProvider {
  kind: string;
  custom_name?: string;
}

export interface AuthProfileScope {
  kind: string;
  agent_id?: string;
}

export type AuthCredentialView =
  | {
      type: "api_key";
      api_key_vault_ref: string;
    }
  | {
      type: "oauth";
      access_token_vault_ref: string;
      refresh_token_vault_ref: string;
      token_endpoint: string;
      client_id?: string;
      client_secret_vault_ref?: string;
      scopes: string[];
      expires_at_unix_ms?: number;
      refresh_state: JsonValue;
    };

export interface AuthProfileView {
  profile_id: string;
  provider: AuthProfileProvider;
  profile_name: string;
  scope: AuthProfileScope;
  credential: AuthCredentialView;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface AuthProfileListEnvelope {
  contract: ContractDescriptor;
  profiles: AuthProfileView[];
  page: PageInfo;
}

export interface AuthProfileEnvelope {
  contract: ContractDescriptor;
  profile: AuthProfileView;
}

export interface AuthProfileDeleteEnvelope {
  contract: ContractDescriptor;
  profile_id: string;
  deleted: boolean;
}

export interface AuthHealthEnvelope {
  contract: ContractDescriptor;
  summary: AuthHealthSummary;
  expiry_distribution: JsonValue;
  profiles: AuthHealthProfile[];
  refresh_metrics: JsonValue;
}

export interface AuthHealthSummary {
  total: number;
  ok: number;
  expiring: number;
  expired: number;
  missing: number;
  static_count: number;
}

export interface AuthHealthProfile {
  profile_id: string;
  provider: string;
  profile_name: string;
  scope: string;
  credential_type: string;
  state: string;
  reason: string;
  expires_at_unix_ms?: number;
}

export interface ProviderAuthStateEnvelope {
  contract: ContractDescriptor;
  provider: string;
  oauth_supported: boolean;
  bootstrap_supported: boolean;
  callback_supported: boolean;
  reconnect_supported: boolean;
  revoke_supported: boolean;
  default_selection_supported: boolean;
  default_profile_id?: string;
  available_profile_ids: string[];
  state: string;
  note?: string;
}

export interface ProviderAuthActionEnvelope {
  contract: ContractDescriptor;
  provider: string;
  action: string;
  state: string;
  message: string;
  profile_id?: string;
}

export interface ProviderAuthActionRequest {
  profile_id?: string;
}

export interface ProviderApiKeyUpsertRequest {
  profile_id?: string;
  profile_name: string;
  scope: AuthProfileScope;
  api_key: string;
  set_default?: boolean;
}

export interface ProviderProbeRequest {
  provider_id?: string;
  timeout_ms?: number;
}

export interface ProviderProbeResult {
  provider_id: string;
  kind: string;
  enabled: boolean;
  endpoint_base_url?: string;
  credential_source: string;
  state: string;
  message: string;
  checked_at_unix_ms: number;
  cache_status: string;
  discovery_source: string;
  discovered_model_ids: string[];
  configured_model_ids: string[];
  latency_ms?: number;
}

export interface ProviderProbeEnvelope {
  contract: ContractDescriptor;
  mode: string;
  provider_filter?: string;
  timeout_ms: number;
  provider_count: number;
  providers: ProviderProbeResult[];
}

export interface OpenAiOAuthBootstrapRequest {
  profile_id?: string;
  profile_name?: string;
  scope?: AuthProfileScope;
  client_id?: string;
  client_secret?: string;
  scopes?: string[];
  set_default?: boolean;
}

export interface OpenAiOAuthBootstrapEnvelope {
  contract: ContractDescriptor;
  provider: string;
  attempt_id: string;
  authorization_url: string;
  expires_at_unix_ms: number;
  profile_id?: string;
  message: string;
}

export interface OpenAiOAuthCallbackStateEnvelope {
  contract: ContractDescriptor;
  provider: string;
  attempt_id: string;
  state: string;
  message: string;
  profile_id?: string;
  completed_at_unix_ms?: number;
  expires_at_unix_ms?: number;
}

export interface ConfigBackupRecord {
  index: number;
  path: string;
  exists: boolean;
}

export interface ConfigDocumentSnapshot {
  contract: ContractDescriptor;
  source_path: string;
  config_version: number;
  migrated_from_version?: number;
  redacted: boolean;
  document_toml: string;
  backups: ConfigBackupRecord[];
}

export interface ConfigValidationEnvelope {
  contract: ContractDescriptor;
  source_path: string;
  valid: boolean;
  config_version: number;
  migrated_from_version?: number;
}

export interface ConfigMutationEnvelope {
  contract: ContractDescriptor;
  operation: string;
  source_path: string;
  backups_retained: number;
  config_version: number;
  migrated_from_version?: number;
  changed_key?: string;
}

export interface SecretMetadata {
  scope: string;
  key: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  value_bytes: number;
}

export interface SecretMetadataList {
  contract: ContractDescriptor;
  scope: string;
  secrets: SecretMetadata[];
  page: PageInfo;
}

export interface SecretMetadataEnvelope {
  contract: ContractDescriptor;
  secret: SecretMetadata;
}

export interface SecretRevealEnvelope {
  contract: ContractDescriptor;
  scope: string;
  key: string;
  value_bytes: number;
  value_base64: string;
  value_utf8?: string;
}

export interface ConfiguredSecretSourceView {
  kind: string;
  fingerprint: string;
  required: boolean;
  refresh_policy: string;
  snapshot_policy: string;
  description: string;
  display_name?: string;
  redaction_label?: string;
  max_bytes?: number;
  exec_timeout_ms?: number;
  trusted_dir_count?: number;
  inherited_env_count?: number;
  allow_symlinks?: boolean;
}

export interface ConfiguredSecretRecord {
  secret_id: string;
  component: string;
  config_path: string;
  status: string;
  resolution_scope: string;
  reload_action: string;
  snapshot_generation: number;
  source: ConfiguredSecretSourceView;
  affected_components: string[];
  last_resolved_at_unix_ms?: number;
  last_error_kind?: string;
  last_error?: string;
  value_bytes?: number;
}

export interface ConfiguredSecretListEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  snapshot_generation: number;
  secrets: ConfiguredSecretRecord[];
  page: PageInfo;
}

export interface ConfiguredSecretEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  snapshot_generation: number;
  secret: ConfiguredSecretRecord;
}

export interface ConfigReloadPlanSummary {
  hot_safe: number;
  restart_required: number;
  blocked_while_runs_active: number;
  manual_review: number;
}

export interface ConfigReloadPlanStep {
  component: string;
  config_path: string;
  category: string;
  reason: string;
}

export interface ConfigReloadPlanEnvelope {
  contract: ContractDescriptor;
  plan_id: string;
  source_path: string;
  generated_at_unix_ms: number;
  active_runs: number;
  requires_restart: boolean;
  hot_safe_applicable: boolean;
  summary: ConfigReloadPlanSummary;
  steps: ConfigReloadPlanStep[];
}

export interface ConfigReloadApplyEnvelope {
  contract: ContractDescriptor;
  outcome: string;
  message: string;
  plan: ConfigReloadPlanEnvelope;
  applied_steps: ConfigReloadPlanStep[];
  skipped_steps: ConfigReloadPlanStep[];
}

export interface PairingCodeRecord {
  code: string;
  channel: string;
  issued_by: string;
  created_at_unix_ms: number;
  expires_at_unix_ms: number;
}

export interface PairingPendingRecord {
  channel: string;
  sender_identity: string;
  code: string;
  requested_at_unix_ms: number;
  expires_at_unix_ms: number;
  approval_id?: string;
}

export interface PairingGrantRecord {
  channel: string;
  sender_identity: string;
  approved_at_unix_ms: number;
  expires_at_unix_ms?: number;
  approval_id?: string;
}

export interface PairingChannelSnapshot {
  channel: string;
  pending: PairingPendingRecord[];
  paired: PairingGrantRecord[];
  active_codes: PairingCodeRecord[];
}

export interface PairingSummaryEnvelope {
  contract: ContractDescriptor;
  channels: PairingChannelSnapshot[];
}

export type InventoryPresenceState = "ok" | "stale" | "degraded" | "offline";
export type InventoryTrustState =
  | "trusted"
  | "pending"
  | "revoked"
  | "removed"
  | "legacy"
  | "unknown";
export type NodePairingRequestState =
  | "pending_approval"
  | "approved"
  | "rejected"
  | "completed"
  | "expired";
export type NodePairingMethod = "pin" | "qr";

export interface NodePairingCodeView {
  code: string;
  method: NodePairingMethod;
  issued_by: string;
  created_at_unix_ms: number;
  expires_at_unix_ms: number;
}

export interface NodeCapabilityView {
  name: string;
  available: boolean;
  execution_mode: string;
}

export interface NodeCapabilityRequestView {
  request_id: string;
  device_id: string;
  capability: string;
  state: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  dispatched_at_unix_ms?: number;
  completed_at_unix_ms?: number;
  max_payload_bytes: number;
  input_summary?: string;
  output_summary?: string;
  error?: string;
}

export interface InventoryCapabilitySummary {
  total: number;
  available: number;
  unavailable: number;
}

export interface InventoryActionAvailability {
  can_rotate: boolean;
  can_revoke: boolean;
  can_remove: boolean;
  can_invoke: boolean;
}

export interface NodePairingRequestView {
  request_id: string;
  session_id: string;
  device_id: string;
  client_kind: string;
  method: NodePairingMethod;
  code_issued_by: string;
  requested_at_unix_ms: number;
  expires_at_unix_ms: number;
  approval_id: string;
  state: NodePairingRequestState;
  decision_reason?: string;
  decision_scope_ttl_ms?: number;
  identity_fingerprint: string;
  transcript_hash_hex: string;
  cert_expires_at_unix_ms?: number;
}

export interface NodePairingListEnvelope {
  contract: ContractDescriptor;
  codes: NodePairingCodeView[];
  requests: NodePairingRequestView[];
  page: PageInfo;
}

export interface NodePairingRequestEnvelope {
  contract: ContractDescriptor;
  request: NodePairingRequestView;
}

export interface NodePairingCodeEnvelope {
  contract: ContractDescriptor;
  code: NodePairingCodeView;
}

export interface InventoryDeviceRecord {
  device_id: string;
  client_kind: string;
  device_status: string;
  trust_state: InventoryTrustState;
  presence_state: InventoryPresenceState;
  paired_at_unix_ms: number;
  updated_at_unix_ms: number;
  registered_at_unix_ms?: number;
  last_seen_at_unix_ms?: number;
  heartbeat_age_ms?: number;
  latest_session_id?: string;
  pending_pairings: number;
  issued_by: string;
  approval_id: string;
  identity_fingerprint: string;
  transcript_hash_hex: string;
  current_certificate_fingerprint?: string;
  certificate_fingerprint_history: string[];
  platform?: string;
  capabilities: NodeCapabilityView[];
  capability_summary: InventoryCapabilitySummary;
  last_event_name?: string;
  last_event_at_unix_ms?: number;
  current_certificate_expires_at_unix_ms?: number;
  revoked_reason?: string;
  warnings: string[];
  actions: InventoryActionAvailability;
}

export interface InventoryInstanceRecord {
  instance_id: string;
  label: string;
  kind: string;
  presence_state: InventoryPresenceState;
  observed_at_unix_ms: number;
  state_label: string;
  detail?: string;
  capability_summary: InventoryCapabilitySummary;
}

export interface InventorySummary {
  devices: number;
  trusted_devices: number;
  pending_pairings: number;
  ok_devices: number;
  stale_devices: number;
  degraded_devices: number;
  offline_devices: number;
  ok_instances: number;
  stale_instances: number;
  degraded_instances: number;
  offline_instances: number;
}

export interface InventoryListEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  summary: InventorySummary;
  devices: InventoryDeviceRecord[];
  pending_pairings: NodePairingRequestView[];
  instances: InventoryInstanceRecord[];
  page: PageInfo;
}

export interface InventoryDeviceDetailEnvelope {
  contract: ContractDescriptor;
  generated_at_unix_ms: number;
  device: InventoryDeviceRecord;
  pairings: NodePairingRequestView[];
  capability_requests: NodeCapabilityRequestView[];
  workspace_activity?: InventoryWorkspaceActivity;
}

export interface InventoryWorkspaceRestoreSummary {
  checkpoint_count: number;
  preflight_checkpoint_count: number;
  post_change_checkpoint_count: number;
  paired_checkpoint_count: number;
  missing_checkpoint_pair_count: number;
  high_risk_mutation_count: number;
  review_required_mutation_count: number;
  checkpoint_restore_total: number;
  restore_report_count: number;
  succeeded_restore_count: number;
  partial_failure_restore_count: number;
  failed_restore_count: number;
  restore_success_rate_bps: number;
  missing_checkpoint_pair_rate_bps: number;
  high_risk_mutation_rate_bps: number;
}

export interface InventoryWorkspaceCheckpointRecord {
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  source_kind: string;
  source_label: string;
  checkpoint_stage: string;
  mutation_id?: string;
  paired_checkpoint_id?: string;
  tool_name?: string;
  proposal_id?: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  summary_text: string;
  compare_summary?: JsonValue;
  risk_level: string;
  review_posture: string;
  created_at_unix_ms: number;
  restore_count: number;
  last_restored_at_unix_ms?: number;
  latest_restore_report_id?: string;
}

export interface InventoryWorkspaceRestoreReportRecord {
  report_id: string;
  checkpoint_id: string;
  session_id: string;
  run_id: string;
  actor_principal: string;
  device_id: string;
  channel?: string;
  scope_kind: string;
  target_path?: string;
  reconciliation_summary: string;
  branched_session_id?: string;
  result_state: string;
  created_at_unix_ms: number;
}

export interface InventoryWorkspaceActivity {
  summary: InventoryWorkspaceRestoreSummary;
  recent_checkpoints: InventoryWorkspaceCheckpointRecord[];
  recent_restore_reports: InventoryWorkspaceRestoreReportRecord[];
}

export interface DeviceEnvelope {
  contract: ContractDescriptor;
  device: {
    device_id: string;
    client_kind: string;
    status: string;
    paired_at_unix_ms: number;
    updated_at_unix_ms: number;
    issued_by: string;
    approval_id: string;
    identity_fingerprint: string;
    transcript_hash_hex: string;
    current_certificate_fingerprint?: string;
    current_certificate_expires_at_unix_ms?: number;
    revoked_reason?: string;
    revoked_at_unix_ms?: number;
    removed_at_unix_ms?: number;
  };
}

export interface NodeInvokeEnvelope {
  contract: ContractDescriptor;
  device_id: string;
  capability: string;
  success: boolean;
  output_json?: JsonValue;
  error: string;
}

export interface SupportBundleJob {
  job_id: string;
  state: "queued" | "running" | "succeeded" | "failed";
  requested_at_unix_ms: number;
  started_at_unix_ms?: number;
  completed_at_unix_ms?: number;
  output_path?: string;
  command_output: string;
  error?: string;
}

export interface SupportBundleJobEnvelope {
  contract: ContractDescriptor;
  job: SupportBundleJob;
}

export interface SupportBundleJobListEnvelope {
  contract: ContractDescriptor;
  jobs: SupportBundleJob[];
  page: PageInfo;
}

export interface DoctorRecoveryJob {
  job_id: string;
  state: "queued" | "running" | "succeeded" | "failed";
  requested_at_unix_ms: number;
  started_at_unix_ms?: number;
  completed_at_unix_ms?: number;
  command: string[];
  report?: JsonValue;
  command_output: string;
  error?: string;
}

export interface DoctorRecoveryJobEnvelope {
  contract: ContractDescriptor;
  job: DoctorRecoveryJob;
}

export interface DoctorRecoveryJobListEnvelope {
  contract: ContractDescriptor;
  jobs: DoctorRecoveryJob[];
  page: PageInfo;
}

export interface AgentRecord {
  agent_id: string;
  display_name: string;
  agent_dir: string;
  workspace_roots: string[];
  default_model_profile: string;
  execution_backend_preference: string;
  default_tool_allowlist: string[];
  default_skill_allowlist: string[];
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface ExecutionBackendInventoryRecord {
  backend_id: string;
  label: string;
  state: string;
  selectable: boolean;
  selected_by_default: boolean;
  description: string;
  operator_summary: string;
  executor_label?: string;
  rollout_flag?: string;
  rollout_enabled: boolean;
  capabilities: string[];
  tradeoffs: string[];
  active_node_count: number;
  total_node_count: number;
}

export interface AgentListEnvelope {
  contract: ContractDescriptor;
  agents: AgentRecord[];
  execution_backends: ExecutionBackendInventoryRecord[];
  default_agent_id?: string;
  page: PageInfo;
}

export interface AgentEnvelope {
  contract: ContractDescriptor;
  agent: AgentRecord;
  is_default: boolean;
  execution_backends: ExecutionBackendInventoryRecord[];
  resolved_execution_backend: string;
  execution_backend_fallback_used: boolean;
  execution_backend_reason: string;
}

export interface AgentCreateRequest {
  agent_id: string;
  display_name: string;
  agent_dir?: string;
  workspace_roots?: string[];
  default_model_profile?: string;
  execution_backend_preference?: string;
  default_tool_allowlist?: string[];
  default_skill_allowlist?: string[];
  set_default?: boolean;
  allow_absolute_paths?: boolean;
}

export interface AgentCreateEnvelope {
  contract: ContractDescriptor;
  agent: AgentRecord;
  default_changed: boolean;
  execution_backends: ExecutionBackendInventoryRecord[];
  resolved_execution_backend: string;
  execution_backend_fallback_used: boolean;
  execution_backend_reason: string;
  default_agent_id?: string;
}

export interface AgentSetDefaultEnvelope {
  contract: ContractDescriptor;
  default_agent_id: string;
  previous_default_agent_id?: string;
}

export interface SkillBuilderCapabilityRequest {
  http_hosts?: string[];
  secrets?: string[];
  storage_prefixes?: string[];
  channels?: string[];
}

export interface SkillBuilderCandidateRecord {
  candidate_id: string;
  skill_id: string;
  version: string;
  publisher: string;
  name: string;
  source_kind: string;
  source_ref: string;
  summary: string;
  status: string;
  rollout_flag: string;
  rollout_enabled: boolean;
  scaffold_root: string;
  manifest_path: string;
  capability_declaration_path: string;
  provenance_path: string;
  test_harness_path: string;
  capability_profile: {
    http_hosts: string[];
    secrets: string[];
    storage_prefixes: string[];
    channels: string[];
  };
  generated_at_unix_ms: number;
  updated_at_unix_ms: number;
}

export interface SkillBuilderCandidatesEnvelope {
  rollout_flag: string;
  rollout_enabled: boolean;
  count: number;
  entries: SkillBuilderCandidateRecord[];
}

export interface SkillBuilderCreateRequest {
  learning_candidate_id?: string;
  prompt?: string;
  skill_id?: string;
  version?: string;
  publisher?: string;
  name?: string;
  tool_id?: string;
  tool_name?: string;
  tool_description?: string;
  review_notes?: string;
  capabilities?: SkillBuilderCapabilityRequest;
}

export interface SkillBuilderCreateEnvelope {
  rollout_flag: string;
  rollout_enabled: boolean;
  candidate?: SkillBuilderCandidateRecord;
  skill: JsonValue;
}

export class ControlPlaneApiError extends Error {
  readonly status: number;
  readonly code?: string;
  readonly category?: ErrorCategory;
  readonly retryable: boolean;
  readonly redacted: boolean;
  readonly validationErrors: ValidationIssue[];

  constructor(
    message: string,
    options: {
      status: number;
      code?: string;
      category?: ErrorCategory;
      retryable?: boolean;
      redacted?: boolean;
      validationErrors?: ValidationIssue[];
      cause?: unknown;
    },
  ) {
    super(message, { cause: options.cause });
    this.name = "ControlPlaneApiError";
    this.status = options.status;
    this.code = options.code;
    this.category = options.category;
    this.retryable = options.retryable ?? false;
    this.redacted = options.redacted ?? false;
    this.validationErrors = options.validationErrors ?? [];
  }
}

interface RequestOptions {
  csrf?: boolean;
  timeoutMs?: number;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
const DEFAULT_SAFE_READ_RETRIES = 1;

function buildPathWithQuery(path: string, params?: URLSearchParams): string {
  if (params === undefined || params.size === 0) {
    return path;
  }
  return `${path}?${params.toString()}`;
}

function normalizeChatQueuedInputRecord(record: RawChatQueuedInputRecord): ChatQueuedInputRecord {
  return {
    ...record,
    state: normalizeQueuedInputState(record.state),
    queue_mode: normalizeQueueMode(record.queue_mode),
    priority_lane: record.priority_lane ?? "normal",
    safe_boundary_flags_json: record.safe_boundary_flags_json ?? "{}",
    decision_reason: record.decision_reason ?? "legacy_followup",
    policy_snapshot_json: record.policy_snapshot_json ?? "{}",
    explain_json: record.explain_json ?? "{}",
  };
}

function normalizeChatQueuePolicySnapshot(
  snapshot: RawChatQueuePolicySnapshot,
): ChatQueuePolicySnapshot {
  return {
    ...snapshot,
    queued_inputs: snapshot.queued_inputs.map(normalizeChatQueuedInputRecord),
  };
}

function normalizeChatQueueActionEnvelope(
  envelope: RawChatQueueActionEnvelope,
): ChatQueueActionEnvelope {
  return {
    ...envelope,
    queue: normalizeChatQueuePolicySnapshot(envelope.queue),
    queued_input:
      envelope.queued_input !== undefined
        ? normalizeChatQueuedInputRecord(envelope.queued_input)
        : undefined,
  };
}

function normalizeChatBackgroundTaskRecord(
  record: RawChatBackgroundTaskRecord,
): ChatBackgroundTaskRecord {
  return {
    ...record,
    task_kind: normalizeAuxiliaryTaskKind(record.task_kind),
    state: normalizeAuxiliaryTaskState(record.state),
  };
}

function invokeFetch(
  fetcher: typeof fetch,
  input: RequestInfo | URL,
  init: RequestInit,
): Promise<Response> {
  return Reflect.apply(
    fetcher as unknown as (input: RequestInfo | URL, init: RequestInit) => Promise<Response>,
    globalThis,
    [input, init],
  );
}

export class ConsoleApiClient {
  private csrfToken: string | null = null;

  constructor(
    private readonly basePath = "",
    private readonly fetcher: typeof fetch = fetch,
  ) {}

  resolvePath(path: string): string {
    return `${this.basePath}${path}`;
  }

  async getSession(): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>("/console/v1/auth/session");
    this.csrfToken = session.csrf_token;
    return session;
  }

  async login(payload: {
    admin_token: string;
    principal: string;
    device_id: string;
    channel?: string;
  }): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>(
      "/console/v1/auth/login",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: false },
    );
    this.csrfToken = session.csrf_token;
    return session;
  }

  async consumeDesktopHandoff(token: string): Promise<ConsoleSession> {
    const session = await this.request<ConsoleSession>(
      "/console/v1/auth/browser-handoff/session",
      {
        method: "POST",
        body: JSON.stringify({ token }),
      },
      { csrf: false },
    );
    this.csrfToken = session.csrf_token;
    return session;
  }

  async logout(): Promise<void> {
    await this.request<{ signed_out: boolean }>(
      "/console/v1/auth/logout",
      { method: "POST" },
      { csrf: true },
    );
    this.csrfToken = null;
  }

  async getDiagnostics(): Promise<ConsoleDiagnosticsSnapshot> {
    return this.request("/console/v1/diagnostics");
  }

  async getCapabilityCatalog(): Promise<CapabilityCatalog> {
    return this.request("/console/v1/control-plane/capabilities");
  }

  async getDeploymentPosture(): Promise<DeploymentPostureSummary> {
    return this.request("/console/v1/deployment/posture");
  }

  async getOnboardingPosture(params?: URLSearchParams): Promise<OnboardingPostureEnvelope> {
    const query = params?.toString();
    const path =
      query !== undefined && query.length > 0
        ? `/console/v1/onboarding/posture?${query}`
        : "/console/v1/onboarding/posture";
    return this.request(path);
  }

  async listAuthProfiles(params?: URLSearchParams): Promise<AuthProfileListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/auth/profiles", params));
  }

  async getAuthProfile(profileId: string): Promise<AuthProfileEnvelope> {
    return this.request(`/console/v1/auth/profiles/${encodeURIComponent(profileId)}`);
  }

  async upsertAuthProfile(profile: AuthProfileView): Promise<AuthProfileEnvelope> {
    return this.request(
      "/console/v1/auth/profiles",
      {
        method: "POST",
        body: JSON.stringify(profile),
      },
      { csrf: true },
    );
  }

  async deleteAuthProfile(profileId: string): Promise<AuthProfileDeleteEnvelope> {
    return this.request(
      `/console/v1/auth/profiles/${encodeURIComponent(profileId)}/delete`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async getAuthHealth(params?: URLSearchParams): Promise<AuthHealthEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/auth/health", params));
  }

  async connectOpenAiApiKey(
    payload: ProviderApiKeyUpsertRequest,
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/api-key",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getOpenAiProviderState(): Promise<ProviderAuthStateEnvelope> {
    return this.request("/console/v1/auth/providers/openai");
  }

  async connectAnthropicApiKey(
    payload: ProviderApiKeyUpsertRequest,
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/anthropic/api-key",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getAnthropicProviderState(): Promise<ProviderAuthStateEnvelope> {
    return this.request("/console/v1/auth/providers/anthropic");
  }

  async startOpenAiProviderBootstrap(
    payload: OpenAiOAuthBootstrapRequest = {},
  ): Promise<OpenAiOAuthBootstrapEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/bootstrap",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getOpenAiProviderCallbackState(
    attemptId: string,
  ): Promise<OpenAiOAuthCallbackStateEnvelope> {
    const params = new URLSearchParams();
    params.set("attempt_id", attemptId);
    return this.request(
      buildPathWithQuery("/console/v1/auth/providers/openai/callback-state", params),
    );
  }

  async reconnectOpenAiProvider(
    payload: ProviderAuthActionRequest = {},
  ): Promise<OpenAiOAuthBootstrapEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/reconnect",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async refreshOpenAiProvider(
    payload: ProviderAuthActionRequest = {},
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/refresh",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async revokeOpenAiProvider(
    payload: ProviderAuthActionRequest = {},
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/revoke",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async setOpenAiDefaultProfile(
    payload: ProviderAuthActionRequest = {},
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/openai/default-profile",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async revokeAnthropicProvider(
    payload: ProviderAuthActionRequest = {},
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/anthropic/revoke",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async setAnthropicDefaultProfile(
    payload: ProviderAuthActionRequest = {},
  ): Promise<ProviderAuthActionEnvelope> {
    return this.request(
      "/console/v1/auth/providers/anthropic/default-profile",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async testModelProviderConnection(
    payload: ProviderProbeRequest = {},
  ): Promise<ProviderProbeEnvelope> {
    return this.request(
      "/console/v1/models/test-connection",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async discoverModelProviderModels(
    payload: ProviderProbeRequest = {},
  ): Promise<ProviderProbeEnvelope> {
    return this.request(
      "/console/v1/models/discover",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getAccessSnapshot(): Promise<{
    contract: ContractDescriptor;
    snapshot: AccessRegistrySnapshot;
  }> {
    return this.request("/console/v1/access");
  }

  async runAccessBackfill(payload: { dry_run?: boolean }): Promise<{
    contract: ContractDescriptor;
    backfill: AccessBackfillReport;
    snapshot: AccessRegistrySnapshot;
  }> {
    return this.request(
      "/console/v1/access/backfill",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async setAccessFeatureFlag(payload: {
    feature_key: string;
    enabled: boolean;
    stage?: string;
  }): Promise<{ contract: ContractDescriptor; feature_flag: AccessFeatureFlagRecord }> {
    return this.request(
      `/console/v1/access/features/${encodeURIComponent(payload.feature_key)}`,
      {
        method: "POST",
        body: JSON.stringify({
          enabled: payload.enabled,
          stage: payload.stage,
        }),
      },
      { csrf: true },
    );
  }

  async createAccessApiToken(payload: {
    label: string;
    principal: string;
    workspace_id?: string;
    role: string;
    scopes: string[];
    expires_at_unix_ms?: number;
    rate_limit_per_minute?: number;
  }): Promise<{
    contract: ContractDescriptor;
    created: { token: string; token_record: AccessApiTokenView };
  }> {
    return this.request(
      "/console/v1/access/api-tokens",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async rotateAccessApiToken(tokenId: string): Promise<{
    contract: ContractDescriptor;
    rotated: { token: string; token_record: AccessApiTokenView };
  }> {
    return this.request(
      `/console/v1/access/api-tokens/${encodeURIComponent(tokenId)}/rotate`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async revokeAccessApiToken(
    tokenId: string,
  ): Promise<{ contract: ContractDescriptor; revoked: AccessApiTokenView }> {
    return this.request(
      `/console/v1/access/api-tokens/${encodeURIComponent(tokenId)}/revoke`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async createAccessWorkspace(payload: { team_name: string; workspace_name: string }): Promise<{
    contract: ContractDescriptor;
    created: {
      team: AccessTeamRecord;
      workspace: AccessWorkspaceRecord;
      membership: AccessMembershipView;
    };
  }> {
    return this.request(
      "/console/v1/access/workspaces",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async createAccessInvitation(payload: {
    workspace_id: string;
    invited_identity: string;
    role: string;
    expires_at_unix_ms: number;
  }): Promise<{
    contract: ContractDescriptor;
    created: { invitation_token: string; invitation: AccessInvitationRecord };
  }> {
    return this.request(
      "/console/v1/access/invitations",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async acceptAccessInvitation(payload: {
    invitation_token: string;
  }): Promise<{ contract: ContractDescriptor; membership: AccessMembershipView }> {
    return this.request(
      "/console/v1/access/invitations/accept",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async updateAccessMembershipRole(payload: {
    workspace_id: string;
    member_principal: string;
    role: string;
  }): Promise<{ contract: ContractDescriptor; membership: AccessMembershipView }> {
    return this.request(
      "/console/v1/access/memberships/role",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async removeAccessMembership(payload: {
    workspace_id: string;
    member_principal: string;
  }): Promise<{ contract: ContractDescriptor; removed: boolean }> {
    return this.request(
      "/console/v1/access/memberships/remove",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async upsertAccessShare(payload: {
    workspace_id: string;
    resource_kind: string;
    resource_id: string;
    access_level: string;
  }): Promise<{ contract: ContractDescriptor; share: AccessShareRecord }> {
    return this.request(
      "/console/v1/access/shares",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async inspectConfig(payload: {
    path?: string;
    show_secrets?: boolean;
    backups?: number;
  }): Promise<ConfigDocumentSnapshot> {
    return this.request(
      "/console/v1/config/inspect",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: false },
    );
  }

  async validateConfig(payload: { path?: string }): Promise<ConfigValidationEnvelope> {
    return this.request(
      "/console/v1/config/validate",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: false },
    );
  }

  async mutateConfig(payload: {
    path?: string;
    key: string;
    value?: string;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/mutate",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async migrateConfig(payload: {
    path?: string;
    show_secrets?: boolean;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/migrate",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async recoverConfig(payload: {
    path?: string;
    backup?: number;
    backups?: number;
  }): Promise<ConfigMutationEnvelope> {
    return this.request(
      "/console/v1/config/recover",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async planConfigReload(payload: { path?: string }): Promise<ConfigReloadPlanEnvelope> {
    return this.request(
      "/console/v1/config/reload/plan",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: false },
    );
  }

  async applyConfigReload(payload: {
    path?: string;
    plan_id?: string;
    dry_run?: boolean;
    force?: boolean;
  }): Promise<ConfigReloadApplyEnvelope> {
    return this.request(
      "/console/v1/config/reload/apply",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listSecrets(scope: string): Promise<SecretMetadataList> {
    return this.request(`/console/v1/secrets?scope=${encodeURIComponent(scope)}`);
  }

  async getSecretMetadata(scope: string, key: string): Promise<SecretMetadataEnvelope> {
    return this.request(
      `/console/v1/secrets/metadata?scope=${encodeURIComponent(scope)}&key=${encodeURIComponent(key)}`,
    );
  }

  async setSecret(payload: {
    scope: string;
    key: string;
    value_base64: string;
  }): Promise<SecretMetadataEnvelope> {
    return this.request(
      "/console/v1/secrets",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async revealSecret(payload: {
    scope: string;
    key: string;
    reveal: true;
  }): Promise<SecretRevealEnvelope> {
    return this.request(
      "/console/v1/secrets/reveal",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async deleteSecret(payload: { scope: string; key: string }): Promise<SecretMetadataEnvelope> {
    return this.request(
      "/console/v1/secrets/delete",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listConfiguredSecrets(): Promise<ConfiguredSecretListEnvelope> {
    return this.request("/console/v1/secrets/configured");
  }

  async getConfiguredSecret(secretId: string): Promise<ConfiguredSecretEnvelope> {
    return this.request(
      `/console/v1/secrets/configured/detail?secret_id=${encodeURIComponent(secretId)}`,
    );
  }

  async getPairingSummary(): Promise<PairingSummaryEnvelope> {
    return this.request("/console/v1/pairing");
  }

  async listNodePairingRequests(params?: {
    client_kind?: string;
    state?: NodePairingRequestState;
  }): Promise<NodePairingListEnvelope> {
    const query = new URLSearchParams();
    if (params?.client_kind !== undefined) {
      query.set("client_kind", params.client_kind);
    }
    if (params?.state !== undefined) {
      query.set("state", params.state);
    }
    return this.request(buildPathWithQuery("/console/v1/pairing/requests", query));
  }

  async mintNodePairingCode(payload: {
    method: NodePairingMethod;
    issued_by?: string;
    ttl_ms?: number;
  }): Promise<NodePairingCodeEnvelope> {
    return this.request(
      "/console/v1/pairing/requests/code",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async approveNodePairingRequest(
    requestId: string,
    payload: { reason?: string } = {},
  ): Promise<NodePairingRequestEnvelope> {
    return this.request(
      `/console/v1/pairing/requests/${encodeURIComponent(requestId)}/approve`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async rejectNodePairingRequest(
    requestId: string,
    payload: { reason?: string } = {},
  ): Promise<NodePairingRequestEnvelope> {
    return this.request(
      `/console/v1/pairing/requests/${encodeURIComponent(requestId)}/reject`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listInventory(): Promise<InventoryListEnvelope> {
    return this.request("/console/v1/inventory");
  }

  async getInventoryDevice(deviceId: string): Promise<InventoryDeviceDetailEnvelope> {
    return this.request(`/console/v1/inventory/${encodeURIComponent(deviceId)}`);
  }

  async listAgents(params?: URLSearchParams): Promise<AgentListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/agents", params));
  }

  async getAgent(agentId: string): Promise<AgentEnvelope> {
    return this.request(`/console/v1/agents/${encodeURIComponent(agentId)}`);
  }

  async createAgent(payload: AgentCreateRequest): Promise<AgentCreateEnvelope> {
    return this.request(
      "/console/v1/agents",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async setDefaultAgent(agentId: string): Promise<AgentSetDefaultEnvelope> {
    return this.request(
      `/console/v1/agents/${encodeURIComponent(agentId)}/set-default`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async mintPairingCode(payload: {
    channel: string;
    issued_by?: string;
    ttl_ms?: number;
  }): Promise<PairingSummaryEnvelope> {
    return this.request(
      "/console/v1/pairing/codes",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async rotateDevice(deviceId: string): Promise<DeviceEnvelope> {
    return this.request(
      `/console/v1/devices/${encodeURIComponent(deviceId)}/rotate`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async revokeDevice(deviceId: string, payload: { reason?: string } = {}): Promise<DeviceEnvelope> {
    return this.request(
      `/console/v1/devices/${encodeURIComponent(deviceId)}/revoke`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async removeDevice(deviceId: string, payload: { reason?: string } = {}): Promise<DeviceEnvelope> {
    return this.request(
      `/console/v1/devices/${encodeURIComponent(deviceId)}/remove`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async invokeNode(
    deviceId: string,
    payload: {
      capability: string;
      input_json?: JsonValue;
      max_payload_bytes?: number;
      timeout_ms?: number;
    },
  ): Promise<NodeInvokeEnvelope> {
    return this.request(
      `/console/v1/nodes/${encodeURIComponent(deviceId)}/invoke`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listSupportBundleJobs(params?: URLSearchParams): Promise<SupportBundleJobListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/support-bundle/jobs", params));
  }

  async createSupportBundleJob(
    payload: { retain_jobs?: number } = {},
  ): Promise<SupportBundleJobEnvelope> {
    return this.request(
      "/console/v1/support-bundle/jobs",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getSupportBundleJob(jobId: string): Promise<SupportBundleJobEnvelope> {
    return this.request(`/console/v1/support-bundle/jobs/${encodeURIComponent(jobId)}`);
  }

  async listDoctorRecoveryJobs(params?: URLSearchParams): Promise<DoctorRecoveryJobListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/doctor/jobs", params));
  }

  async createDoctorRecoveryJob(payload: {
    retain_jobs?: number;
    repair?: boolean;
    dry_run?: boolean;
    force?: boolean;
    only?: string[];
    skip?: string[];
    rollback_run?: string;
  }): Promise<DoctorRecoveryJobEnvelope> {
    return this.request(
      "/console/v1/doctor/jobs",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getDoctorRecoveryJob(jobId: string): Promise<DoctorRecoveryJobEnvelope> {
    return this.request(`/console/v1/doctor/jobs/${encodeURIComponent(jobId)}`);
  }

  async listApprovals(params?: URLSearchParams): Promise<{ approvals: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/approvals", params));
  }

  async listChatSessions(params?: URLSearchParams): Promise<{
    sessions: ChatSessionRecord[];
    next_after_session_key?: string;
  }> {
    return this.request(buildPathWithQuery("/console/v1/chat/sessions", params));
  }

  async listSessionCatalog(params?: URLSearchParams): Promise<SessionCatalogListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/sessions", params));
  }

  async getSessionCatalogEntry(sessionId: string): Promise<SessionCatalogDetailEnvelope> {
    return this.request(`/console/v1/sessions/${encodeURIComponent(sessionId)}`);
  }

  async getUsageSummary(params?: URLSearchParams): Promise<UsageSummaryEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/usage/summary", params));
  }

  async listUsageSessions(params?: URLSearchParams): Promise<UsageSessionsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/usage/sessions", params));
  }

  async getUsageSessionDetail(
    sessionId: string,
    params?: URLSearchParams,
  ): Promise<UsageSessionDetailEnvelope> {
    return this.request(
      buildPathWithQuery(`/console/v1/usage/sessions/${encodeURIComponent(sessionId)}`, params),
    );
  }

  async listUsageAgents(params?: URLSearchParams): Promise<UsageAgentsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/usage/agents", params));
  }

  async listUsageModels(params?: URLSearchParams): Promise<UsageModelsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/usage/models", params));
  }

  async getUsageInsights(params?: URLSearchParams): Promise<UsageInsightsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/usage/insights", params));
  }

  async requestUsageBudgetOverride(
    policyId: string,
    payload: { reason?: string } = {},
  ): Promise<UsageBudgetOverrideRequestEnvelope> {
    return this.request(
      `/console/v1/usage/budgets/${encodeURIComponent(policyId)}/override-request`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listLogs(params?: URLSearchParams): Promise<LogListEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/logs", params));
  }

  async resolveChatSession(payload: {
    session_id?: string;
    session_key?: string;
    session_label?: string;
    require_existing?: boolean;
    reset_session?: boolean;
  }): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      "/console/v1/chat/sessions",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async renameChatSession(
    sessionId: string,
    payload: { session_label?: string; manual_title_locked?: boolean },
  ): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/rename`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async resetChatSession(
    sessionId: string,
  ): Promise<{ session: ChatSessionRecord; created: boolean; reset_applied: boolean }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/reset`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async archiveSession(
    sessionId: string,
  ): Promise<{ session: SessionCatalogRecord; action: string; contract: ContractDescriptor }> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/archive`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async getSessionProjectContext(sessionId: string): Promise<SessionProjectContextEnvelope> {
    return this.request(`/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context`);
  }

  async refreshSessionProjectContext(sessionId: string): Promise<SessionProjectContextEnvelope> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context/refresh`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async disableSessionProjectContextEntry(
    sessionId: string,
    entryId: string,
  ): Promise<SessionProjectContextEnvelope> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context/entries/${encodeURIComponent(entryId)}/disable`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async enableSessionProjectContextEntry(
    sessionId: string,
    entryId: string,
  ): Promise<SessionProjectContextEnvelope> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context/entries/${encodeURIComponent(entryId)}/enable`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async approveSessionProjectContextEntry(
    sessionId: string,
    entryId: string,
  ): Promise<SessionProjectContextEnvelope> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context/entries/${encodeURIComponent(entryId)}/approve`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async scaffoldSessionProjectContext(
    sessionId: string,
    payload: { project_name?: string; force?: boolean } = {},
  ): Promise<SessionProjectContextEnvelope> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/project-context/scaffold`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async updateSessionQuickControls(
    sessionId: string,
    payload: {
      agent_id?: string | null;
      model_profile?: string | null;
      thinking?: boolean | null;
      trace?: boolean | null;
      verbose?: boolean | null;
      reset_to_default?: boolean;
    },
  ): Promise<{ session: SessionCatalogRecord; action: string; contract: ContractDescriptor }> {
    return this.request(
      `/console/v1/sessions/${encodeURIComponent(sessionId)}/quick-controls`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async abortSessionRun(
    runId: string,
    payload: { reason?: string } = {},
  ): Promise<{
    contract: ContractDescriptor;
    run_id: string;
    cancel_requested: boolean;
    reason: string;
  }> {
    return this.request(
      `/console/v1/sessions/runs/${encodeURIComponent(runId)}/abort`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async chatRunStatus(
    runId: string,
  ): Promise<{ run: ChatRunStatusRecord; lineage: ChatRunLineage }> {
    return this.request(`/console/v1/chat/runs/${encodeURIComponent(runId)}/status`);
  }

  async chatRunEvents(
    runId: string,
    params?: URLSearchParams,
  ): Promise<{ run: ChatRunStatusRecord; tape: ChatRunTapeSnapshot; lineage: ChatRunLineage }> {
    return this.request(
      buildPathWithQuery(`/console/v1/chat/runs/${encodeURIComponent(runId)}/events`, params),
    );
  }

  async chatRunWorkspace(
    runId: string,
    params?: { q?: string; limit?: number },
  ): Promise<ChatRunWorkspaceEnvelope> {
    const query = new URLSearchParams();
    if (params?.q?.trim()) {
      query.set("q", params.q.trim());
    }
    if (params?.limit !== undefined) {
      query.set("limit", String(params.limit));
    }
    return this.request(
      buildPathWithQuery(`/console/v1/chat/runs/${encodeURIComponent(runId)}/workspace`, query),
    );
  }

  async chatRunWorkspaceArtifact(
    runId: string,
    artifactId: string,
    params?: { include_content?: boolean },
  ): Promise<WorkspaceArtifactDetailEnvelope> {
    const query = new URLSearchParams();
    if (params?.include_content === true) {
      query.set("include_content", "true");
    }
    return this.request(
      buildPathWithQuery(
        `/console/v1/chat/runs/${encodeURIComponent(runId)}/workspace/artifacts/${encodeURIComponent(artifactId)}`,
        query,
      ),
    );
  }

  async compareWorkspace(payload: {
    left_run_id?: string;
    right_run_id?: string;
    left_checkpoint_id?: string;
    right_checkpoint_id?: string;
    limit?: number;
  }): Promise<WorkspaceCompareEnvelope> {
    return this.request(
      "/console/v1/chat/workspace/compare",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async streamChatMessage(
    sessionId: string,
    payload: {
      text: string;
      allow_sensitive_tools?: boolean;
      session_label?: string;
      origin_kind?: string;
      origin_run_id?: string;
      parameter_delta?: JsonValue;
      queued_input_id?: string;
      attachments?: Array<{ artifact_id: string }>;
    },
    options: {
      signal?: AbortSignal;
      onLine: (line: ChatStreamLine) => void;
    },
  ): Promise<void> {
    const path = `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/messages/stream`;
    const headers = new Headers();
    headers.set("Content-Type", "application/json");
    if (this.csrfToken === null) {
      throw new Error("Missing CSRF token. Please sign in again.");
    }
    headers.set("x-palyra-csrf-token", this.csrfToken);
    const response = await this.fetcher(`${this.basePath}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      credentials: "include",
      signal: options.signal,
    });
    if (!response.ok) {
      throw await buildRequestError(response);
    }
    if (response.body === null) {
      throw new Error("Chat stream response body is missing.");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) {
          break;
        }
        buffered += decoder.decode(chunk.value, { stream: true });
        buffered = flushNdjsonBuffer(buffered, options.onLine);
      }
      buffered += decoder.decode();
      flushNdjsonBuffer(buffered, options.onLine, true);
    } finally {
      reader.releaseLock();
    }
  }

  async previewChatContextReferences(
    sessionId: string,
    payload: { text: string },
  ): Promise<ContextReferencePreviewEnvelope> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/references/preview`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async previewChatProjectContext(
    sessionId: string,
    payload: { text?: string } = {},
  ): Promise<{
    preview: ProjectContextPreviewEnvelope;
    prompt_preview?: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/project-context/preview`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getDelegationCatalog(): Promise<{
    catalog: ChatDelegationCatalog;
    contract: ContractDescriptor;
  }> {
    return this.request("/console/v1/chat/delegation/catalog");
  }

  async prepareRetry(
    sessionId: string,
    payload: { parameter_delta?: JsonValue } = {},
  ): Promise<{
    session: SessionCatalogRecord;
    text: string;
    origin_kind: string;
    origin_run_id: string;
    parameter_delta?: JsonValue;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/retry`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async branchSession(
    sessionId: string,
    payload: { session_label?: string } = {},
  ): Promise<{
    session: SessionCatalogRecord;
    source_run_id: string;
    suggested_session_label?: string;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/branch`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async queueFollowUp(
    runId: string,
    payload: { text: string; queue_mode?: QueueMode },
  ): Promise<{
    queued_input: ChatQueuedInputRecord;
    decision?: JsonValue;
    policy?: JsonValue;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      queued_input: RawChatQueuedInputRecord;
      decision?: JsonValue;
      policy?: JsonValue;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/runs/${encodeURIComponent(runId)}/queue`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return {
      ...response,
      queued_input: normalizeChatQueuedInputRecord(response.queued_input),
    };
  }

  async getChatQueuePolicy(sessionId: string): Promise<ChatQueuePolicySnapshot> {
    const response = await this.request<RawChatQueuePolicySnapshot>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/policy`,
    );
    return normalizeChatQueuePolicySnapshot(response);
  }

  async pauseChatQueue(
    sessionId: string,
    payload: { reason?: string } = {},
  ): Promise<ChatQueueActionEnvelope> {
    const response = await this.request<RawChatQueueActionEnvelope>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/pause`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return normalizeChatQueueActionEnvelope(response);
  }

  async resumeChatQueue(sessionId: string): Promise<ChatQueueActionEnvelope> {
    const response = await this.request<RawChatQueueActionEnvelope>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/resume`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
    return normalizeChatQueueActionEnvelope(response);
  }

  async drainChatQueue(
    sessionId: string,
    payload: { reason?: string } = {},
  ): Promise<ChatQueueActionEnvelope> {
    const response = await this.request<RawChatQueueActionEnvelope>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/drain`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return normalizeChatQueueActionEnvelope(response);
  }

  async collectChatQueueSummary(
    sessionId: string,
    payload: { reason?: string } = {},
  ): Promise<ChatQueueActionEnvelope> {
    const response = await this.request<RawChatQueueActionEnvelope>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/collect-summary`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return normalizeChatQueueActionEnvelope(response);
  }

  async cancelChatQueuedInput(
    sessionId: string,
    queuedInputId: string,
    payload: { reason?: string } = {},
  ): Promise<ChatQueueActionEnvelope> {
    const response = await this.request<RawChatQueueActionEnvelope>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/queue/items/${encodeURIComponent(queuedInputId)}/cancel`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return normalizeChatQueueActionEnvelope(response);
  }

  async getSessionTranscript(sessionId: string): Promise<{
    session: SessionCatalogRecord;
    records: ChatTranscriptRecord[];
    attachments: ChatAttachmentRecord[];
    derived_artifacts: MediaDerivedArtifactRecord[];
    pins: ChatPinRecord[];
    compactions: ChatCompactionArtifactRecord[];
    checkpoints: ChatCheckpointRecord[];
    queued_inputs: ChatQueuedInputRecord[];
    runs: ChatRunStatusRecord[];
    background_tasks: ChatBackgroundTaskRecord[];
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      session: SessionCatalogRecord;
      records: ChatTranscriptRecord[];
      attachments: ChatAttachmentRecord[];
      derived_artifacts: MediaDerivedArtifactRecord[];
      pins: ChatPinRecord[];
      compactions: ChatCompactionArtifactRecord[];
      checkpoints: ChatCheckpointRecord[];
      queued_inputs: RawChatQueuedInputRecord[];
      runs: ChatRunStatusRecord[];
      background_tasks: RawChatBackgroundTaskRecord[];
      contract: ContractDescriptor;
    }>(`/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/transcript`);
    return {
      ...response,
      queued_inputs: response.queued_inputs.map(normalizeChatQueuedInputRecord),
      background_tasks: response.background_tasks.map(normalizeChatBackgroundTaskRecord),
    };
  }

  async listSessionCanvases(sessionId: string): Promise<SessionCanvasListEnvelope> {
    return this.request(`/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/canvases`);
  }

  async getSessionCanvas(
    sessionId: string,
    canvasId: string,
  ): Promise<SessionCanvasDetailEnvelope> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/canvases/${encodeURIComponent(canvasId)}`,
    );
  }

  async restoreSessionCanvas(
    sessionId: string,
    canvasId: string,
    payload: { state_version: number },
  ): Promise<SessionCanvasRestoreEnvelope> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/canvases/${encodeURIComponent(canvasId)}/restore`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async previewSessionCompaction(
    sessionId: string,
    payload: {
      trigger_reason?: string;
      trigger_policy?: string;
      accept_candidate_ids?: string[];
      reject_candidate_ids?: string[];
    } = {},
  ): Promise<{
    session: SessionCatalogRecord;
    preview: ChatCompactionPreview;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/compactions/preview`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async applySessionCompaction(
    sessionId: string,
    payload: {
      trigger_reason?: string;
      trigger_policy?: string;
      accept_candidate_ids?: string[];
      reject_candidate_ids?: string[];
    } = {},
  ): Promise<{
    session: SessionCatalogRecord;
    artifact: ChatCompactionArtifactRecord;
    checkpoint: ChatCheckpointRecord;
    preview: ChatCompactionPreview;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/compactions`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getSessionCompactionArtifact(artifactId: string): Promise<{
    session: SessionCatalogRecord;
    artifact: ChatCompactionArtifactRecord;
    related_checkpoints: ChatCheckpointRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(`/console/v1/chat/compactions/${encodeURIComponent(artifactId)}`);
  }

  async createSessionCheckpoint(
    sessionId: string,
    payload: { name: string; tags?: string[]; note?: string },
  ): Promise<{
    session: SessionCatalogRecord;
    checkpoint: ChatCheckpointRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/checkpoints`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getSessionCheckpoint(checkpointId: string): Promise<{
    session: SessionCatalogRecord;
    checkpoint: ChatCheckpointRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(`/console/v1/chat/checkpoints/${encodeURIComponent(checkpointId)}`);
  }

  async restoreSessionCheckpoint(
    checkpointId: string,
    payload: { session_label?: string } = {},
  ): Promise<{
    session: SessionCatalogRecord;
    checkpoint: ChatCheckpointRecord;
    suggested_session_label?: string;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/checkpoints/${encodeURIComponent(checkpointId)}/restore`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getWorkspaceCheckpoint(checkpointId: string): Promise<WorkspaceCheckpointDetailEnvelope> {
    return this.request(
      `/console/v1/chat/workspace-checkpoints/${encodeURIComponent(checkpointId)}`,
    );
  }

  async restoreWorkspaceCheckpoint(
    checkpointId: string,
    payload: {
      session_label?: string;
      scope_kind?: "workspace" | "file";
      target_path?: string;
      target_workspace_root_index?: number;
      branch_session?: boolean;
    } = {},
  ): Promise<WorkspaceRestoreResponseEnvelope> {
    return this.request(
      `/console/v1/chat/workspace-checkpoints/${encodeURIComponent(checkpointId)}/restore`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getWorkspaceRestoreReport(reportId: string): Promise<WorkspaceRestoreReportEnvelope> {
    return this.request(
      `/console/v1/chat/workspace-restore-reports/${encodeURIComponent(reportId)}`,
    );
  }

  async listBackgroundTasks(params?: {
    session_id?: string;
    include_completed?: boolean;
    limit?: number;
  }): Promise<{ tasks: ChatBackgroundTaskRecord[]; contract: ContractDescriptor }> {
    const query = new URLSearchParams();
    if (params?.session_id?.trim()) {
      query.set("session_id", params.session_id.trim());
    }
    if (params?.include_completed !== undefined) {
      query.set("include_completed", String(params.include_completed));
    }
    if (params?.limit !== undefined) {
      query.set("limit", String(params.limit));
    }
    const response = await this.request<{
      tasks: RawChatBackgroundTaskRecord[];
      contract: ContractDescriptor;
    }>(buildPathWithQuery("/console/v1/chat/background-tasks", query));
    return {
      ...response,
      tasks: response.tasks.map(normalizeChatBackgroundTaskRecord),
    };
  }

  async createBackgroundTask(
    sessionId: string,
    payload: {
      text: string;
      task_kind?: AuxiliaryTaskKind;
      priority?: number;
      max_attempts?: number;
      budget_tokens?: number;
      not_before_unix_ms?: number;
      expires_at_unix_ms?: number;
      notification_target?: JsonValue;
      parameter_delta?: JsonValue;
      delegation?: {
        profile_id?: string;
        template_id?: string;
        group_id?: string;
        execution_mode?: "serial" | "parallel";
        manifest?: {
          profile_id?: string;
          display_name?: string;
          description?: string;
          role?: string;
          model_profile?: string;
          tool_allowlist?: string[];
          skill_allowlist?: string[];
          memory_scope?: string;
          budget_tokens?: number;
          max_attempts?: number;
          merge_strategy?: string;
          approval_required?: boolean;
          max_concurrent_children?: number;
          max_children_per_parent?: number;
          max_parallel_groups?: number;
          child_budget_override?: number;
          child_timeout_ms?: number;
        };
      };
    },
  ): Promise<{
    session: SessionCatalogRecord;
    task: ChatBackgroundTaskRecord;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      session: SessionCatalogRecord;
      task: RawChatBackgroundTaskRecord;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/background-tasks`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async getBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    run?: ChatRunStatusRecord;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      task: RawChatBackgroundTaskRecord;
      run?: ChatRunStatusRecord;
      contract: ContractDescriptor;
    }>(`/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}`);
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async pauseBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      task: RawChatBackgroundTaskRecord;
      action: string;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/pause`,
      { method: "POST" },
      { csrf: true },
    );
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async resumeBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      task: RawChatBackgroundTaskRecord;
      action: string;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/resume`,
      { method: "POST" },
      { csrf: true },
    );
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async retryBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      task: RawChatBackgroundTaskRecord;
      action: string;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/retry`,
      { method: "POST" },
      { csrf: true },
    );
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async cancelBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    const response = await this.request<{
      task: RawChatBackgroundTaskRecord;
      action: string;
      contract: ContractDescriptor;
    }>(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/cancel`,
      { method: "POST" },
      { csrf: true },
    );
    return {
      ...response,
      task: normalizeChatBackgroundTaskRecord(response.task),
    };
  }

  async searchSessionTranscript(
    sessionId: string,
    query: string,
  ): Promise<{
    session: SessionCatalogRecord;
    query: string;
    matches: Array<{
      session_id: string;
      run_id: string;
      seq: number;
      event_type: string;
      created_at_unix_ms: number;
      origin_kind: string;
      origin_run_id?: string;
      snippet: string;
    }>;
    contract: ContractDescriptor;
  }> {
    return this.request(
      buildPathWithQuery(
        `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/transcript/search`,
        new URLSearchParams([["q", query]]),
      ),
    );
  }

  async exportSessionTranscript(
    sessionId: string,
    format: "json" | "markdown" = "json",
  ): Promise<{ format: string; content: JsonValue; contract: ContractDescriptor }> {
    return this.request(
      buildPathWithQuery(
        `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/export`,
        new URLSearchParams([["format", format]]),
      ),
    );
  }

  async listSessionPins(sessionId: string): Promise<{
    session: SessionCatalogRecord;
    pins: ChatPinRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(`/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/pins`);
  }

  async createSessionPin(
    sessionId: string,
    payload: { run_id: string; tape_seq: number; title: string; note?: string },
  ): Promise<{ pin: ChatPinRecord; contract: ContractDescriptor }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/pins`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async deleteSessionPin(
    sessionId: string,
    pinId: string,
  ): Promise<{ deleted: boolean; contract: ContractDescriptor }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/pins/${encodeURIComponent(pinId)}`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async uploadChatAttachment(
    sessionId: string,
    payload: {
      filename: string;
      content_type: string;
      bytes_base64: string;
    },
  ): Promise<{
    attachment: ChatAttachmentRecord;
    derived_artifacts: MediaDerivedArtifactRecord[];
    task: ChatBackgroundTaskRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/attachments`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listSessionDerivedArtifacts(
    sessionId: string,
    params?: { kind?: string; state?: string },
  ): Promise<{
    session: SessionCatalogRecord;
    derived_artifacts: MediaDerivedArtifactRecord[];
    contract: ContractDescriptor;
  }> {
    const query = new URLSearchParams();
    if (params?.kind?.trim()) {
      query.set("kind", params.kind.trim());
    }
    if (params?.state?.trim()) {
      query.set("state", params.state.trim());
    }
    return this.request(
      buildPathWithQuery(
        `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/derived-artifacts`,
        query,
      ),
    );
  }

  async listAttachmentDerivedArtifacts(artifactId: string): Promise<{
    source_artifact_id: string;
    derived_artifacts: MediaDerivedArtifactRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/attachments/${encodeURIComponent(artifactId)}/derived-artifacts`,
    );
  }

  async getDerivedArtifact(derivedArtifactId: string): Promise<{
    derived_artifact: MediaDerivedArtifactRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/derived-artifacts/${encodeURIComponent(derivedArtifactId)}`,
    );
  }

  async quarantineDerivedArtifact(
    derivedArtifactId: string,
    payload: { reason?: string } = {},
  ): Promise<{
    derived_artifact: MediaDerivedArtifactRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/derived-artifacts/${encodeURIComponent(derivedArtifactId)}/quarantine`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async releaseDerivedArtifact(derivedArtifactId: string): Promise<{
    derived_artifact: MediaDerivedArtifactRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/derived-artifacts/${encodeURIComponent(derivedArtifactId)}/release`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async recomputeDerivedArtifact(derivedArtifactId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    derived_artifact: MediaDerivedArtifactRecord;
    derived_artifacts: MediaDerivedArtifactRecord[];
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/derived-artifacts/${encodeURIComponent(derivedArtifactId)}/recompute`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async purgeDerivedArtifact(derivedArtifactId: string): Promise<{
    derived_artifact: MediaDerivedArtifactRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/derived-artifacts/${encodeURIComponent(derivedArtifactId)}/purge`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async getApproval(approvalId: string): Promise<{ approval: JsonValue }> {
    return this.request(`/console/v1/approvals/${encodeURIComponent(approvalId)}`);
  }

  async getMobileBootstrap(): Promise<MobileBootstrapEnvelope> {
    return this.request("/console/v1/mobile/bootstrap");
  }

  async getMobileInbox(): Promise<MobileInboxEnvelope> {
    return this.request("/console/v1/mobile/inbox");
  }

  async listMobileApprovals(params?: URLSearchParams): Promise<MobileApprovalsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/mobile/approvals", params));
  }

  async getMobileApproval(approvalId: string): Promise<MobileApprovalDetailEnvelope> {
    return this.request(`/console/v1/mobile/approvals/${encodeURIComponent(approvalId)}`);
  }

  async decideMobileApproval(
    approvalId: string,
    payload: {
      approved: boolean;
      reason?: string;
      decision_scope?: "once" | "session" | "timeboxed";
      decision_scope_ttl_ms?: number;
    },
  ): Promise<{ approval: JsonValue; dm_pairing?: string }> {
    return this.request(
      `/console/v1/mobile/approvals/${encodeURIComponent(approvalId)}/decision`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listMobileSessions(params?: URLSearchParams): Promise<MobileSessionsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/mobile/sessions", params));
  }

  async getMobileSession(sessionId: string): Promise<MobileSessionDetailEnvelope> {
    return this.request(`/console/v1/mobile/sessions/${encodeURIComponent(sessionId)}`);
  }

  async prepareMobileSafeUrlOpen(payload: { target: string }): Promise<MobileSafeUrlOpenEnvelope> {
    return this.request(
      "/console/v1/mobile/safe-url-open",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async createMobileVoiceNote(payload: {
    session_id?: string;
    create_session_label?: string;
    transcript_text: string;
    transcript_reviewed: boolean;
    duration_ms?: number;
    draft_id?: string;
    notification_target?: JsonValue;
  }): Promise<MobileVoiceNoteEnvelope> {
    return this.request(
      "/console/v1/mobile/voice-notes",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getToolPermissions(params?: URLSearchParams): Promise<ToolPermissionsEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/tool-permissions", params));
  }

  async getToolPermission(
    toolName: string,
    params?: URLSearchParams,
  ): Promise<ToolPermissionDetailEnvelope> {
    return this.request(
      buildPathWithQuery(`/console/v1/tool-permissions/${encodeURIComponent(toolName)}`, params),
    );
  }

  async setToolPermissionOverride(
    toolName: string,
    payload: {
      scope_kind: ToolPostureScopeKind;
      scope_id?: string;
      state: ToolPostureState;
      reason?: string;
      expires_at_unix_ms?: number;
    },
  ): Promise<ToolPermissionMutationEnvelope> {
    return this.request(
      `/console/v1/tool-permissions/${encodeURIComponent(toolName)}/override`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async resetToolPermission(
    toolName: string,
    payload: {
      scope_kind: ToolPostureScopeKind;
      scope_id?: string;
      reason?: string;
    },
  ): Promise<ToolPermissionMutationEnvelope> {
    return this.request(
      `/console/v1/tool-permissions/${encodeURIComponent(toolName)}/reset`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async resetToolPermissionScope(payload: {
    scope_kind: ToolPostureScopeKind;
    scope_id?: string;
    reason?: string;
  }): Promise<ToolPermissionScopeResetEnvelope> {
    return this.request(
      "/console/v1/tool-permissions/scopes/reset",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async previewToolPermissionPreset(payload: {
    preset_id: string;
    scope_kind: ToolPostureScopeKind;
    scope_id?: string;
  }): Promise<ToolPermissionPresetPreviewEnvelope> {
    return this.request(
      "/console/v1/tool-permissions/presets/preview",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async applyToolPermissionPreset(payload: {
    preset_id: string;
    scope_kind: ToolPostureScopeKind;
    scope_id?: string;
    reason?: string;
  }): Promise<ToolPermissionPresetPreviewEnvelope> {
    return this.request(
      "/console/v1/tool-permissions/presets/apply",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async actOnToolPermissionRecommendation(payload: {
    recommendation_id: string;
    tool_name: string;
    scope_kind: ToolPostureScopeKind;
    scope_id?: string;
    action: ToolPostureRecommendationAction;
  }): Promise<ToolPermissionMutationEnvelope> {
    return this.request(
      "/console/v1/tool-permissions/recommendations/action",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async decideApproval(
    approvalId: string,
    payload: {
      approved: boolean;
      reason?: string;
      decision_scope?: "once" | "session" | "timeboxed";
      decision_scope_ttl_ms?: number;
    },
  ): Promise<{ approval: JsonValue }> {
    return this.request(
      `/console/v1/approvals/${encodeURIComponent(approvalId)}/decision`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listCronJobs(params?: URLSearchParams): Promise<{ jobs: JsonValue[] }> {
    const response = await this.listRoutines(params);
    return { jobs: response.routines };
  }

  async createCronJob(payload: {
    name: string;
    prompt: string;
    schedule_type: "cron" | "every" | "at";
    cron_expression?: string;
    every_interval_ms?: number;
    at_timestamp_rfc3339?: string;
    enabled?: boolean;
    channel?: string;
  }): Promise<{ job: JsonValue; approval?: JsonValue }> {
    const response = await this.upsertRoutine({
      ...payload,
      trigger_kind: "schedule",
    });
    return { job: response.routine, approval: response.approval };
  }

  async setCronJobEnabled(
    jobId: string,
    enabled: boolean,
  ): Promise<{
    job: JsonValue;
    approval?: JsonValue;
  }> {
    const response = await this.setRoutineEnabled(jobId, enabled);
    return { job: response.routine, approval: response.approval };
  }

  async runCronJobNow(
    jobId: string,
  ): Promise<{ run_id?: string; status: string; message: string }> {
    return this.runRoutineNow(jobId);
  }

  async listCronRuns(jobId: string, params?: URLSearchParams): Promise<{ runs: JsonValue[] }> {
    return this.listRoutineRuns(jobId, params);
  }

  async listObjectives(params?: URLSearchParams): Promise<{ objectives: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/objectives", params));
  }

  async getObjective(objectiveId: string): Promise<{ objective: JsonValue }> {
    return this.request(`/console/v1/objectives/${encodeURIComponent(objectiveId)}`);
  }

  async upsertObjective(payload: Record<string, JsonValue | undefined>): Promise<{
    objective: JsonValue;
    linked_routine?: JsonValue;
    last_run?: JsonValue;
    health?: JsonValue;
  }> {
    return this.request(
      "/console/v1/objectives",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async lifecycleObjective(
    objectiveId: string,
    payload: {
      action: "fire" | "pause" | "resume" | "cancel" | "archive";
      reason?: string;
    },
  ): Promise<{
    objective: JsonValue;
    linked_routine?: JsonValue;
    last_run?: JsonValue;
    health?: JsonValue;
  }> {
    return this.request(
      `/console/v1/objectives/${encodeURIComponent(objectiveId)}/lifecycle`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getObjectiveSummary(objectiveId: string): Promise<{
    objective: JsonValue;
    linked_routine?: JsonValue;
    last_run?: JsonValue;
    health?: JsonValue;
    summary_markdown: string;
  }> {
    return this.request(`/console/v1/objectives/${encodeURIComponent(objectiveId)}/summary`);
  }

  async listRoutines(params?: URLSearchParams): Promise<{ routines: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/routines", params));
  }

  async getRoutine(routineId: string): Promise<{ routine: JsonValue }> {
    return this.request(`/console/v1/routines/${encodeURIComponent(routineId)}`);
  }

  async upsertRoutine(payload: Record<string, JsonValue | undefined>): Promise<{
    routine: JsonValue;
    approval?: JsonValue;
  }> {
    return this.request(
      "/console/v1/routines",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async deleteRoutine(routineId: string): Promise<{ deleted: boolean; routine_id: string }> {
    return this.request(
      `/console/v1/routines/${encodeURIComponent(routineId)}/delete`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async setRoutineEnabled(
    routineId: string,
    enabled: boolean,
  ): Promise<{
    routine: JsonValue;
    approval?: JsonValue;
  }> {
    return this.request(
      `/console/v1/routines/${encodeURIComponent(routineId)}/enabled`,
      {
        method: "POST",
        body: JSON.stringify({ enabled }),
      },
      { csrf: true },
    );
  }

  async runRoutineNow(
    routineId: string,
  ): Promise<{ run_id?: string; status: string; message: string }> {
    return this.request(
      `/console/v1/routines/${encodeURIComponent(routineId)}/run-now`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async testRunRoutine(
    routineId: string,
    payload: {
      source_run_id?: string;
      trigger_reason?: string;
      trigger_payload?: JsonValue;
    },
  ): Promise<{
    routine_id: string;
    run_id?: string;
    status: string;
    message: string;
    dispatch_mode?: string;
    delivery_preview?: JsonValue;
  }> {
    return this.request(
      `/console/v1/routines/${encodeURIComponent(routineId)}/test-run`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listRoutineRuns(
    routineId: string,
    params?: URLSearchParams,
  ): Promise<{ runs: JsonValue[] }> {
    return this.request(
      buildPathWithQuery(`/console/v1/routines/${encodeURIComponent(routineId)}/runs`, params),
    );
  }

  async dispatchRoutine(
    routineId: string,
    payload: {
      trigger_kind?: string;
      trigger_reason?: string;
      trigger_payload?: JsonValue;
      trigger_dedupe_key?: string;
    },
  ): Promise<{ routine_id: string; run_id?: string; status: string; message: string }> {
    return this.request(
      `/console/v1/routines/${encodeURIComponent(routineId)}/dispatch`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listRoutineTemplates(): Promise<{ version: number; templates: JsonValue[] }> {
    return this.request("/console/v1/routines/templates");
  }

  async previewRoutineSchedule(payload: {
    phrase: string;
    timezone?: "local" | "utc";
  }): Promise<{ preview: JsonValue }> {
    return this.request(
      "/console/v1/routines/schedule-preview",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async exportRoutine(routineId: string): Promise<{ export: JsonValue }> {
    return this.request(`/console/v1/routines/${encodeURIComponent(routineId)}/export`);
  }

  async importRoutine(payload: {
    export: JsonValue;
    routine_id?: string;
    enabled?: boolean;
  }): Promise<{ routine: JsonValue; approval?: JsonValue; imported_from: string }> {
    return this.request(
      "/console/v1/routines/import",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async dispatchHookRoutineTrigger(payload: {
    hook_id: string;
    event?: string;
    payload?: JsonValue;
    dedupe_key?: string;
  }): Promise<{ binding: JsonValue; dispatches: JsonValue[] }> {
    return this.request(
      `/console/v1/hooks/${encodeURIComponent(payload.hook_id)}/fire`,
      {
        method: "POST",
        body: JSON.stringify({
          event: payload.event,
          payload: payload.payload,
          dedupe_key: payload.dedupe_key,
        }),
      },
      { csrf: true },
    );
  }

  async dispatchWebhookRoutineTrigger(payload: {
    integration_id: string;
    event: string;
    payload?: JsonValue;
    source?: string;
    dedupe_key?: string;
  }): Promise<{ integration: JsonValue; dispatches: JsonValue[] }> {
    return this.request(
      `/console/v1/webhooks/${encodeURIComponent(payload.integration_id)}/dispatch`,
      {
        method: "POST",
        body: JSON.stringify({
          event: payload.event,
          payload: payload.payload,
          source: payload.source,
          dedupe_key: payload.dedupe_key,
        }),
      },
      { csrf: true },
    );
  }

  async emitSystemEvent(payload: { name: string; summary?: string; details?: JsonValue }): Promise<{
    status: string;
    event: string;
    details: JsonValue;
    routine_dispatches: JsonValue[];
  }> {
    return this.request(
      "/console/v1/system/events/emit",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listSystemEvents(params?: URLSearchParams): Promise<{
    hash_chain_enabled: boolean;
    total_events: number;
    returned_events: number;
    events: JsonValue[];
    page: JsonValue;
  }> {
    const path =
      params === undefined
        ? "/console/v1/system/events"
        : buildPathWithQuery("/console/v1/system/events", params);
    return this.request(path);
  }

  async getSystemInsights(): Promise<OperatorInsightsEnvelope> {
    return this.request("/console/v1/system/insights");
  }

  async listChannels(): Promise<{ connectors: JsonValue[] }> {
    return listChannelsRequest(this.request.bind(this));
  }

  async getChannelStatus(connectorId: string): Promise<ChannelStatusEnvelope> {
    return getChannelStatusRequest(this.request.bind(this), connectorId);
  }

  async setChannelEnabled(
    connectorId: string,
    enabled: boolean,
  ): Promise<{ connector: JsonValue }> {
    return setChannelEnabledRequest(this.request.bind(this), connectorId, enabled);
  }

  async listChannelLogs(
    connectorId: string,
    params?: URLSearchParams,
  ): Promise<{ events: JsonValue[]; dead_letters: JsonValue[] }> {
    return listChannelLogsRequest(this.request.bind(this), buildPathWithQuery, connectorId, params);
  }

  async sendChannelTestMessage(
    connectorId: string,
    payload: {
      text: string;
      conversation_id?: string;
      sender_id?: string;
      sender_display?: string;
      simulate_crash_once?: boolean;
      is_direct_message?: boolean;
      requested_broadcast?: boolean;
    },
  ): Promise<{ ingest: JsonValue; status: JsonValue }> {
    return sendChannelTestMessageRequest(this.request.bind(this), connectorId, payload);
  }

  async readChannelMessages(
    connectorId: string,
    payload: {
      request: {
        conversation_id: string;
        thread_id?: string;
        message_id?: string;
        before_message_id?: string;
        after_message_id?: string;
        around_message_id?: string;
        limit: number;
      };
    },
  ): Promise<{ result: JsonValue }> {
    return readChannelMessagesRequest(this.request.bind(this), connectorId, payload);
  }

  async searchChannelMessages(
    connectorId: string,
    payload: {
      request: {
        conversation_id: string;
        thread_id?: string;
        query?: string;
        author_id?: string;
        has_attachments?: boolean;
        before_message_id?: string;
        limit: number;
      };
    },
  ): Promise<{ result: JsonValue }> {
    return searchChannelMessagesRequest(this.request.bind(this), connectorId, payload);
  }

  async editChannelMessage(
    connectorId: string,
    payload: {
      request: {
        locator: {
          conversation_id: string;
          thread_id?: string;
          message_id: string;
        };
        body: string;
      };
      approval_id?: string;
    },
  ): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
    return editChannelMessageRequest(this.request.bind(this), connectorId, payload);
  }

  async deleteChannelMessage(
    connectorId: string,
    payload: {
      request: {
        locator: {
          conversation_id: string;
          thread_id?: string;
          message_id: string;
        };
        reason?: string;
      };
      approval_id?: string;
    },
  ): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
    return deleteChannelMessageRequest(this.request.bind(this), connectorId, payload);
  }

  async addChannelMessageReaction(
    connectorId: string,
    payload: {
      request: {
        locator: {
          conversation_id: string;
          thread_id?: string;
          message_id: string;
        };
        emoji: string;
      };
      approval_id?: string;
    },
  ): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
    return addChannelMessageReactionRequest(this.request.bind(this), connectorId, payload);
  }

  async removeChannelMessageReaction(
    connectorId: string,
    payload: {
      request: {
        locator: {
          conversation_id: string;
          thread_id?: string;
          message_id: string;
        };
        emoji: string;
      };
      approval_id?: string;
    },
  ): Promise<{ result?: JsonValue; approval_required?: boolean; approval?: JsonValue }> {
    return removeChannelMessageReactionRequest(this.request.bind(this), connectorId, payload);
  }

  async sendChannelDiscordTestSend(
    connectorId: string,
    payload: {
      target: string;
      text?: string;
      confirm: boolean;
      auto_reaction?: string;
      thread_id?: string;
    },
  ): Promise<{ dispatch: JsonValue; status: JsonValue; runtime?: JsonValue }> {
    return sendChannelDiscordTestSendRequest(this.request.bind(this), connectorId, payload);
  }

  async refreshChannelHealth(
    connectorId: string,
    payload: { verify_channel_id?: string },
  ): Promise<ChannelStatusEnvelope> {
    return refreshChannelHealthRequest(this.request.bind(this), connectorId, payload);
  }

  async pauseChannelQueue(connectorId: string): Promise<ChannelStatusEnvelope> {
    return pauseChannelQueueRequest(this.request.bind(this), connectorId);
  }

  async resumeChannelQueue(connectorId: string): Promise<ChannelStatusEnvelope> {
    return resumeChannelQueueRequest(this.request.bind(this), connectorId);
  }

  async drainChannelQueue(connectorId: string): Promise<ChannelStatusEnvelope> {
    return drainChannelQueueRequest(this.request.bind(this), connectorId);
  }

  async replayChannelDeadLetter(
    connectorId: string,
    deadLetterId: number,
  ): Promise<ChannelStatusEnvelope> {
    return replayChannelDeadLetterRequest(this.request.bind(this), connectorId, deadLetterId);
  }

  async discardChannelDeadLetter(
    connectorId: string,
    deadLetterId: number,
  ): Promise<ChannelStatusEnvelope> {
    return discardChannelDeadLetterRequest(this.request.bind(this), connectorId, deadLetterId);
  }

  async getChannelRouterRules(): Promise<{ config: JsonValue; config_hash: string }> {
    return getChannelRouterRulesRequest(this.request.bind(this));
  }

  async getChannelRouterWarnings(): Promise<{ warnings: JsonValue[]; config_hash: string }> {
    return getChannelRouterWarningsRequest(this.request.bind(this));
  }

  async previewChannelRoute(payload: {
    channel: string;
    text: string;
    conversation_id?: string;
    sender_identity?: string;
    sender_display?: string;
    sender_verified?: boolean;
    is_direct_message?: boolean;
    requested_broadcast?: boolean;
    adapter_message_id?: string;
    adapter_thread_id?: string;
    max_payload_bytes?: number;
  }): Promise<{ preview: JsonValue }> {
    return previewChannelRouteRequest(this.request.bind(this), payload);
  }

  async listChannelRouterPairings(
    params?: URLSearchParams,
  ): Promise<{ pairings: JsonValue[]; config_hash: string }> {
    return listChannelRouterPairingsRequest(this.request.bind(this), buildPathWithQuery, params);
  }

  async mintChannelRouterPairingCode(payload: {
    channel: string;
    issued_by?: string;
    ttl_ms?: number;
  }): Promise<{ code: JsonValue; config_hash: string }> {
    return mintChannelRouterPairingCodeRequest(this.request.bind(this), payload);
  }

  async probeDiscordOnboarding(payload: {
    account_id?: string;
    token: string;
    mode?: "local" | "remote_vps";
    inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
    allow_from?: string[];
    deny_from?: string[];
    require_mention?: boolean;
    mention_patterns?: string[];
    concurrency_limit?: number;
    broadcast_strategy?: "deny" | "mention_only" | "allow";
    confirm_open_guild_channels?: boolean;
    verify_channel_id?: string;
  }): Promise<{ [key: string]: JsonValue }> {
    return probeDiscordOnboardingRequest(this.request.bind(this), payload);
  }

  async applyDiscordOnboarding(payload: {
    account_id?: string;
    token: string;
    mode?: "local" | "remote_vps";
    inbound_scope?: "dm_only" | "allowlisted_guild_channels" | "open_guild_channels";
    allow_from?: string[];
    deny_from?: string[];
    require_mention?: boolean;
    mention_patterns?: string[];
    concurrency_limit?: number;
    broadcast_strategy?: "deny" | "mention_only" | "allow";
    confirm_open_guild_channels?: boolean;
    verify_channel_id?: string;
  }): Promise<{ [key: string]: JsonValue }> {
    return applyDiscordOnboardingRequest(this.request.bind(this), payload);
  }

  async searchMemory(params?: URLSearchParams): Promise<{
    hits: JsonValue[];
    diagnostics?: RetrievalBranchDiagnostics;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/search", params));
  }

  async getMemoryStatus(): Promise<{
    usage: JsonValue;
    retention: JsonValue;
    maintenance: JsonValue;
    workspace?: {
      roots: string[];
      curated_paths: string[];
      recent_documents: WorkspaceDocumentRecord[];
    };
    recall_artifacts?: {
      latest: RecallArtifactRecord[];
    };
  }> {
    return this.request("/console/v1/memory/status");
  }

  async listLearningCandidates(params?: URLSearchParams): Promise<{
    candidates: LearningCandidateRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/learning/candidates", params));
  }

  async getLearningCandidateHistory(candidateId: string): Promise<{
    candidate: LearningCandidateRecord;
    history: LearningCandidateHistoryRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/memory/learning/candidates/${encodeURIComponent(candidateId)}/history`,
    );
  }

  async reviewLearningCandidate(
    candidateId: string,
    payload: {
      status: string;
      action_summary?: string;
      action_payload_json?: string;
      apply_preference?: boolean;
    },
  ): Promise<{
    candidate: LearningCandidateRecord;
    applied_preference?: LearningPreferenceRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/memory/learning/candidates/${encodeURIComponent(candidateId)}/review`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async applyLearningCandidate(
    candidateId: string,
    payload?: {
      action_summary?: string;
    },
  ): Promise<{
    candidate: LearningCandidateRecord;
    apply: JsonValue;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/memory/learning/candidates/${encodeURIComponent(candidateId)}/apply`,
      {
        method: "POST",
        body: JSON.stringify(payload ?? {}),
      },
      { csrf: true },
    );
  }

  async listLearningPreferences(params?: URLSearchParams): Promise<{
    preferences: LearningPreferenceRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/preferences", params));
  }

  async listWorkspaceDocuments(params?: URLSearchParams): Promise<{
    documents: WorkspaceDocumentRecord[];
    roots: string[];
    contract: ContractDescriptor;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/workspace/documents", params));
  }

  async getWorkspaceDocument(
    params: URLSearchParams,
  ): Promise<{ document: WorkspaceDocumentRecord; contract: ContractDescriptor }> {
    return this.request(buildPathWithQuery("/console/v1/memory/workspace/document", params));
  }

  async writeWorkspaceDocument(payload: {
    document_id?: string;
    path: string;
    title?: string;
    content_text: string;
    channel?: string;
    agent_id?: string;
    session_id?: string;
    template_id?: string;
    template_version?: number;
    template_content_hash?: string;
    source_memory_id?: string;
    manual_override?: boolean;
  }): Promise<{ document: WorkspaceDocumentRecord; contract: ContractDescriptor }> {
    return this.request(
      "/console/v1/memory/workspace/document",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async moveWorkspaceDocument(payload: {
    path: string;
    next_path: string;
    channel?: string;
    agent_id?: string;
    session_id?: string;
  }): Promise<{ document: WorkspaceDocumentRecord; contract: ContractDescriptor }> {
    return this.request(
      "/console/v1/memory/workspace/document/move",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async deleteWorkspaceDocument(payload: {
    path: string;
    channel?: string;
    agent_id?: string;
    session_id?: string;
  }): Promise<{ document: WorkspaceDocumentRecord; contract: ContractDescriptor }> {
    return this.request(
      "/console/v1/memory/workspace/document/delete",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async pinWorkspaceDocument(payload: {
    path: string;
    pinned: boolean;
    channel?: string;
    agent_id?: string;
  }): Promise<{ document: WorkspaceDocumentRecord; contract: ContractDescriptor }> {
    return this.request(
      "/console/v1/memory/workspace/document/pin",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getWorkspaceDocumentVersions(params: URLSearchParams): Promise<{
    document: WorkspaceDocumentRecord;
    versions: WorkspaceDocumentVersionRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(
      buildPathWithQuery("/console/v1/memory/workspace/document/versions", params),
    );
  }

  async bootstrapWorkspace(payload: {
    channel?: string;
    agent_id?: string;
    session_id?: string;
    force_repair?: boolean;
  }): Promise<{
    bootstrap: WorkspaceBootstrapOutcome;
    roots: string[];
    contract: ContractDescriptor;
  }> {
    return this.request(
      "/console/v1/memory/workspace/bootstrap",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async searchWorkspaceDocuments(params?: URLSearchParams): Promise<{
    hits: WorkspaceSearchHit[];
    diagnostics?: RetrievalBranchDiagnostics;
    contract: ContractDescriptor;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/workspace/search", params));
  }

  async previewRecall(payload: {
    query: string;
    channel?: string;
    session_id?: string;
    agent_id?: string;
    memory_top_k?: number;
    workspace_top_k?: number;
    min_score?: number;
    workspace_prefix?: string;
    include_workspace_historical?: boolean;
    include_workspace_quarantined?: boolean;
    max_candidates?: number;
    prompt_budget_tokens?: number;
  }): Promise<RecallPreviewEnvelope> {
    return this.request(
      "/console/v1/memory/recall/preview",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listMemoryDerivedArtifacts(params: {
    workspace_document_id?: string;
    memory_item_id?: string;
    limit?: number;
  }): Promise<{
    workspace_document_id?: string;
    memory_item_id?: string;
    derived_artifacts: MediaDerivedArtifactRecord[];
    contract: ContractDescriptor;
  }> {
    const query = new URLSearchParams();
    if (params.workspace_document_id?.trim()) {
      query.set("workspace_document_id", params.workspace_document_id.trim());
    }
    if (params.memory_item_id?.trim()) {
      query.set("memory_item_id", params.memory_item_id.trim());
    }
    if (params.limit !== undefined) {
      query.set("limit", String(params.limit));
    }
    return this.request(buildPathWithQuery("/console/v1/memory/derived-artifacts", query));
  }

  async searchAll(params?: URLSearchParams): Promise<UnifiedSearchEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/memory/search-all", params));
  }

  async searchSessionHistory(params?: URLSearchParams): Promise<SessionSearchEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/memory/session-search", params));
  }

  async listRecallArtifacts(params?: URLSearchParams): Promise<{
    artifacts: RecallArtifactRecord[];
    contract: ContractDescriptor;
  }> {
    return this.request(buildPathWithQuery("/console/v1/memory/recall-artifacts", params));
  }

  async purgeMemory(payload: {
    channel?: string;
    session_id?: string;
    purge_all_principal?: boolean;
  }): Promise<{ deleted_count: number }> {
    return this.request(
      "/console/v1/memory/purge",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listSkills(params?: URLSearchParams): Promise<{ entries: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/skills", params));
  }

  async listPlugins(params?: URLSearchParams): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    plugins_root: string;
    count: number;
    entries: JsonValue[];
    page: PageInfo;
  }> {
    return this.request(buildPathWithQuery("/console/v1/plugins", params));
  }

  async getPlugin(pluginId: string): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    binding: JsonValue;
    check: JsonValue;
    installed_skill?: JsonValue | null;
  }> {
    return this.request(`/console/v1/plugins/${encodeURIComponent(pluginId)}`);
  }

  async checkPlugin(pluginId: string): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    binding: JsonValue;
    check: JsonValue;
    installed_skill?: JsonValue | null;
  }> {
    return this.request(`/console/v1/plugins/${encodeURIComponent(pluginId)}/check`);
  }

  async upsertPlugin(payload: {
    plugin_id: string;
    skill_id: string;
    skill_version?: string;
    artifact_path?: string;
    tool_id?: string;
    module_path?: string;
    entrypoint?: string;
    enabled?: boolean;
    capability_profile?: JsonValue;
    operator?: JsonValue;
    config?: JsonValue;
    clear_config?: boolean;
    allow_tofu?: boolean;
    allow_untrusted?: boolean;
  }): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    binding: JsonValue;
    check: JsonValue;
    installed_skill?: JsonValue | null;
  }> {
    return this.request(
      "/console/v1/plugins/install-or-bind",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async enablePlugin(pluginId: string): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    binding: JsonValue;
    check: JsonValue;
    installed_skill?: JsonValue | null;
  }> {
    return this.request(
      `/console/v1/plugins/${encodeURIComponent(pluginId)}/enable`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async disablePlugin(pluginId: string): Promise<{
    contract: ContractDescriptor;
    schema_version: number;
    binding: JsonValue;
    check: JsonValue;
    installed_skill?: JsonValue | null;
  }> {
    return this.request(
      `/console/v1/plugins/${encodeURIComponent(pluginId)}/disable`,
      {
        method: "POST",
        body: JSON.stringify({}),
      },
      { csrf: true },
    );
  }

  async deletePlugin(pluginId: string): Promise<{
    contract: ContractDescriptor;
    deleted: boolean;
    binding: JsonValue;
  }> {
    return this.request(
      `/console/v1/plugins/${encodeURIComponent(pluginId)}/delete`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async listSkillBuilderCandidates(
    params?: URLSearchParams,
  ): Promise<SkillBuilderCandidatesEnvelope> {
    return this.request(buildPathWithQuery("/console/v1/skills/builder/candidates", params));
  }

  async createSkillBuilderCandidate(
    payload: SkillBuilderCreateRequest,
  ): Promise<SkillBuilderCreateEnvelope> {
    return this.request(
      "/console/v1/skills/builder/candidates",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async installSkill(payload: {
    artifact_path: string;
    allow_tofu?: boolean;
    allow_untrusted?: boolean;
  }): Promise<{ record: JsonValue }> {
    return this.request(
      "/console/v1/skills/install",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async verifySkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean },
  ): Promise<{ report: JsonValue }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/verify`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async auditSkill(
    skillId: string,
    payload: { version?: string; allow_tofu?: boolean; quarantine_on_fail?: boolean },
  ): Promise<{ report: JsonValue; quarantined: boolean }> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(skillId)}/audit`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async quarantineSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/quarantine`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason,
        }),
      },
      { csrf: true },
    );
  }

  async enableSkill(payload: {
    skill_id: string;
    version: string;
    reason?: string;
  }): Promise<JsonValue> {
    return this.request(
      `/console/v1/skills/${encodeURIComponent(payload.skill_id)}/enable`,
      {
        method: "POST",
        body: JSON.stringify({
          version: payload.version,
          reason: payload.reason,
          override: true,
        }),
      },
      { csrf: true },
    );
  }

  async promoteProcedureCandidate(
    candidateId: string,
    payload: {
      skill_id?: string;
      version?: string;
      publisher?: string;
      name?: string;
      accept_candidate?: boolean;
    } = {},
  ): Promise<{ candidate: JsonValue; skill: JsonValue }> {
    return this.request(
      `/console/v1/skills/candidates/${encodeURIComponent(candidateId)}/promote`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listAuditEvents(params?: URLSearchParams): Promise<{ events: JsonValue[] }> {
    return this.request(buildPathWithQuery("/console/v1/audit/events", params));
  }

  async listBrowserProfiles(params?: URLSearchParams): Promise<{
    principal: string;
    active_profile_id?: string;
    profiles: JsonValue[];
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/profiles", params));
  }

  async createBrowserProfile(payload: {
    principal?: string;
    name: string;
    theme_color?: string;
    persistence_enabled?: boolean;
    private_profile?: boolean;
  }): Promise<{ profile: JsonValue }> {
    return this.request(
      "/console/v1/browser/profiles/create",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async renameBrowserProfile(
    profileId: string,
    payload: { principal?: string; name: string },
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/rename`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async deleteBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {},
  ): Promise<{ deleted: boolean; active_profile_id?: string }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/delete`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async activateBrowserProfile(
    profileId: string,
    payload: { principal?: string } = {},
  ): Promise<{ profile: JsonValue }> {
    return this.request(
      `/console/v1/browser/profiles/${encodeURIComponent(profileId)}/activate`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async listBrowserDownloads(params?: URLSearchParams): Promise<{
    artifacts: JsonValue[];
    truncated: boolean;
    error: string;
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/downloads", params));
  }

  async listBrowserSessions(params?: URLSearchParams): Promise<{
    principal: string;
    truncated: boolean;
    error: string;
    page: PageInfo;
    sessions: JsonValue[];
  }> {
    return this.request(buildPathWithQuery("/console/v1/browser/sessions", params));
  }

  async getBrowserSession(sessionId: string): Promise<{
    session_id: string;
    success: boolean;
    error: string;
    session: JsonValue | null;
  }> {
    return this.request(`/console/v1/browser/sessions/${encodeURIComponent(sessionId)}`);
  }

  async inspectBrowserSession(
    sessionId: string,
    params?: URLSearchParams,
  ): Promise<{
    session_id: string;
    success: boolean;
    error: string;
    session: JsonValue | null;
    cookies: JsonValue[];
    storage: JsonValue[];
    action_log: JsonValue[];
    network_log: JsonValue[];
    dom_snapshot: string;
    visible_text: string;
    page_url: string;
    cookies_truncated: boolean;
    storage_truncated: boolean;
    action_log_truncated: boolean;
    network_log_truncated: boolean;
    dom_truncated: boolean;
    visible_text_truncated: boolean;
    console_log: JsonValue[];
    console_log_truncated: boolean;
    page_diagnostics: JsonValue | null;
  }> {
    return this.request(
      buildPathWithQuery(
        `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/inspect`,
        params,
      ),
    );
  }

  async createBrowserSession(payload: {
    principal?: string;
    channel?: string;
    idle_ttl_ms?: number;
    allow_private_targets?: boolean;
    allow_downloads?: boolean;
    persistence_enabled?: boolean;
    persistence_id?: string;
    profile_id?: string;
    private_profile?: boolean;
    action_allowed_domains?: string[];
    budget?: JsonValue;
  }): Promise<{
    session_id?: string;
    channel?: string;
    created_at_unix_ms: number;
    downloads_enabled: boolean;
    persistence_enabled: boolean;
    persistence_id: string;
    state_restored: boolean;
    profile_id?: string;
    private_profile: boolean;
    effective_budget?: JsonValue;
    action_allowed_domains: string[];
  }> {
    return this.request(
      "/console/v1/browser/sessions",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async closeBrowserSession(sessionId: string): Promise<{
    session_id: string;
    closed: boolean;
    reason: string;
  }> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/close`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async navigateBrowserSession(
    sessionId: string,
    payload: {
      url: string;
      timeout_ms?: number;
      allow_redirects?: boolean;
      max_redirects?: number;
      allow_private_targets?: boolean;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/navigate`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async clickBrowserSession(
    sessionId: string,
    payload: {
      selector: string;
      max_retries?: number;
      timeout_ms?: number;
      capture_failure_screenshot?: boolean;
      max_failure_screenshot_bytes?: number;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/click`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async typeBrowserSession(
    sessionId: string,
    payload: {
      selector: string;
      text: string;
      clear_existing?: boolean;
      timeout_ms?: number;
      capture_failure_screenshot?: boolean;
      max_failure_screenshot_bytes?: number;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/type`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async pressBrowserSession(
    sessionId: string,
    payload: {
      key: string;
      timeout_ms?: number;
      capture_failure_screenshot?: boolean;
      max_failure_screenshot_bytes?: number;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/press`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async selectBrowserSession(
    sessionId: string,
    payload: {
      selector: string;
      value: string;
      timeout_ms?: number;
      capture_failure_screenshot?: boolean;
      max_failure_screenshot_bytes?: number;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/select`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async highlightBrowserSession(
    sessionId: string,
    payload: {
      selector: string;
      timeout_ms?: number;
      duration_ms?: number;
      capture_failure_screenshot?: boolean;
      max_failure_screenshot_bytes?: number;
    },
  ): Promise<JsonValue> {
    return this.request(
      `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/highlight`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getBrowserConsoleLog(
    sessionId: string,
    params?: URLSearchParams,
  ): Promise<{
    success: boolean;
    entries: JsonValue[];
    truncated: boolean;
    page_diagnostics?: JsonValue | null;
    error: string;
    page: PageInfo;
  }> {
    return this.request(
      buildPathWithQuery(
        `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/console`,
        params,
      ),
    );
  }

  async getBrowserPdf(
    sessionId: string,
    params?: URLSearchParams,
  ): Promise<{
    success: boolean;
    mime_type?: string;
    size_bytes: number;
    sha256?: string;
    artifact?: JsonValue | null;
    pdf_base64?: string;
    error: string;
  }> {
    return this.request(
      buildPathWithQuery(
        `/console/v1/browser/sessions/${encodeURIComponent(sessionId)}/pdf`,
        params,
      ),
    );
  }

  async mintBrowserRelayToken(payload: {
    session_id: string;
    extension_id: string;
    ttl_ms?: number;
  }): Promise<{
    relay_token: string;
    session_id: string;
    extension_id: string;
    issued_at_unix_ms: number;
    expires_at_unix_ms: number;
    token_ttl_ms: number;
    warning: string;
  }> {
    return this.request(
      "/console/v1/browser/relay/tokens",
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async relayBrowserAction(
    payload: {
      session_id: string;
      extension_id: string;
      action: "open_tab" | "capture_selection" | "send_page_snapshot";
      open_tab?: { url: string; activate?: boolean; timeout_ms?: number };
      capture_selection?: { selector: string; max_selection_bytes?: number };
      page_snapshot?: {
        include_dom_snapshot?: boolean;
        include_visible_text?: boolean;
        max_dom_snapshot_bytes?: number;
        max_visible_text_bytes?: number;
      };
      max_payload_bytes?: number;
    },
    relayToken?: string,
  ): Promise<{
    success: boolean;
    action: string;
    error: string;
    result: JsonValue;
  }> {
    const headers =
      relayToken !== undefined && relayToken.trim().length > 0
        ? { authorization: `Bearer ${relayToken.trim()}` }
        : undefined;
    return this.request(
      "/console/v1/browser/relay/actions",
      {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
      },
      { csrf: false },
    );
  }

  private async request<T>(
    path: string,
    init: RequestInit = {},
    options: RequestOptions = {},
  ): Promise<T> {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }

    const method = (init.method ?? "GET").toUpperCase();
    const requiresCsrf = options.csrf ?? method !== "GET";
    if (requiresCsrf) {
      if (this.csrfToken === null) {
        throw new Error("Missing CSRF token. Please sign in again.");
      }
      headers.set("x-palyra-csrf-token", this.csrfToken);
    }

    const timeoutMs = normalizeRequestTimeoutMs(options.timeoutMs);
    const maxAttempts = method === "GET" ? DEFAULT_SAFE_READ_RETRIES + 1 : 1;
    let attempt = 0;

    while (true) {
      attempt += 1;
      const requestController = new AbortController();
      let timedOut = false;
      const releaseCallerSignal = forwardAbortSignal(init.signal, requestController);
      const timeoutHandle = setTimeout(() => {
        timedOut = true;
        requestController.abort();
      }, timeoutMs);

      try {
        const response = await invokeFetch(this.fetcher, `${this.basePath}${path}`, {
          ...init,
          headers,
          credentials: "include",
          signal: requestController.signal,
        });
        return (await parseJsonResponse<T>(response)) as T;
      } catch (error) {
        if (isAbortError(error)) {
          if (timedOut) {
            throw new Error(`Request timed out after ${timeoutMs} ms.`, { cause: error });
          }
          if (init.signal?.aborted === true) {
            throw new Error("Request canceled.", { cause: error });
          }
        }
        if (!shouldRetrySafeRead(method, attempt, maxAttempts, error)) {
          throw error;
        }
      } finally {
        clearTimeout(timeoutHandle);
        releaseCallerSignal();
      }
    }
  }
}

function normalizeRequestTimeoutMs(timeoutMs: number | undefined): number {
  if (timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return DEFAULT_REQUEST_TIMEOUT_MS;
  }
  return Math.floor(timeoutMs);
}

function forwardAbortSignal(
  signal: AbortSignal | null | undefined,
  controller: AbortController,
): () => void {
  if (signal === undefined || signal === null) {
    return () => {};
  }
  if (signal.aborted) {
    controller.abort();
    return () => {};
  }
  const onAbort = () => {
    controller.abort();
  };
  signal.addEventListener("abort", onAbort, { once: true });
  return () => {
    signal.removeEventListener("abort", onAbort);
  };
}

function isAbortError(error: unknown): boolean {
  if (error instanceof Error && error.name === "AbortError") {
    return true;
  }
  if (typeof DOMException !== "undefined" && error instanceof DOMException) {
    return error.name === "AbortError";
  }
  return false;
}

function shouldRetrySafeRead(
  method: string,
  attempt: number,
  maxAttempts: number,
  error: unknown,
): boolean {
  if (method !== "GET" || attempt >= maxAttempts) {
    return false;
  }
  if (error instanceof ControlPlaneApiError) {
    return error.retryable;
  }
  return !isAbortError(error);
}

function parseErrorEnvelope(payload: JsonValue): ErrorEnvelope | null {
  if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
    const envelope = payload as ErrorEnvelope;
    if (typeof envelope.error === "string" && envelope.error.trim().length > 0) {
      return envelope;
    }
  }
  return null;
}

function buildControlPlaneApiError(payload: JsonValue, status: number): ControlPlaneApiError {
  const envelope = parseErrorEnvelope(payload);
  return new ControlPlaneApiError(
    envelope?.error?.trim().length ? envelope.error : `Request failed with HTTP ${status}.`,
    {
      status,
      code: envelope?.code,
      category: envelope?.category,
      retryable: envelope?.retryable,
      redacted: envelope?.redacted,
      validationErrors: envelope?.validation_errors,
    },
  );
}

async function buildRequestError(response: Response): Promise<ControlPlaneApiError> {
  const contentType = response.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json")
    ? ((await response.json()) as JsonValue)
    : ((await response.text()) as unknown as JsonValue);
  return buildControlPlaneApiError(payload, response.status);
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get("content-type") ?? "";
  const isJson = contentType.includes("application/json");
  const payload = isJson
    ? ((await response.json()) as JsonValue)
    : ((await response.text()) as unknown as JsonValue);

  if (!response.ok) {
    throw buildControlPlaneApiError(payload, response.status);
  }
  return payload as T;
}

function flushNdjsonBuffer(
  buffer: string,
  onLine: (line: ChatStreamLine) => void,
  flushRemainder = false,
): string {
  let remainder = buffer;
  while (true) {
    const newline = remainder.indexOf("\n");
    if (newline === -1) {
      break;
    }
    const line = remainder.slice(0, newline).trim();
    remainder = remainder.slice(newline + 1);
    if (line.length > 0) {
      onLine(parseChatStreamLine(line));
    }
  }
  if (flushRemainder) {
    const tail = remainder.trim();
    if (tail.length > 0) {
      onLine(parseChatStreamLine(tail));
    }
    return "";
  }
  return remainder;
}

function parseChatStreamLine(line: string): ChatStreamLine {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    throw new Error("Chat stream emitted malformed JSON line.");
  }
  if (!isRecord(parsed) || typeof parsed.type !== "string") {
    throw new Error("Chat stream emitted an invalid line envelope.");
  }
  if (parsed.type === "meta") {
    if (typeof parsed.run_id !== "string" || typeof parsed.session_id !== "string") {
      throw new Error("Chat stream meta line is missing run_id/session_id.");
    }
    return {
      type: "meta",
      run_id: parsed.run_id,
      session_id: parsed.session_id,
    };
  }
  if (parsed.type === "event") {
    if (!isRecord(parsed.event)) {
      throw new Error("Chat stream event line is missing event payload.");
    }
    const eventType = parsed.event.event_type;
    const runId = parsed.event.run_id;
    if (typeof eventType !== "string" || typeof runId !== "string") {
      throw new Error("Chat stream event payload is missing run_id/event_type.");
    }
    return {
      type: "event",
      event: parsed.event as ChatStreamEventEnvelope,
    };
  }
  if (parsed.type === "error") {
    if (typeof parsed.error !== "string") {
      throw new Error("Chat stream error line is missing error text.");
    }
    return {
      type: "error",
      run_id: typeof parsed.run_id === "string" ? parsed.run_id : undefined,
      error: parsed.error,
    };
  }
  if (parsed.type === "complete") {
    if (typeof parsed.run_id !== "string" || typeof parsed.status !== "string") {
      throw new Error("Chat stream complete line is missing run_id/status.");
    }
    return {
      type: "complete",
      run_id: parsed.run_id,
      status: parsed.status,
    };
  }
  throw new Error(`Unsupported chat stream line type '${parsed.type}'.`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
