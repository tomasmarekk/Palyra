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

export interface SessionCatalogRecord extends ChatSessionRecord {
  title: string;
  title_source: string;
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
}

export interface SessionCatalogSummary {
  active_sessions: number;
  archived_sessions: number;
  sessions_with_pending_approvals: number;
  sessions_with_active_runs: number;
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
  };
  page: PageInfo;
}

export interface SessionCatalogDetailEnvelope {
  contract: ContractDescriptor;
  session: SessionCatalogRecord;
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

export interface ChatDelegationMergeResult {
  status: string;
  strategy: string;
  summary_text: string;
  warnings: string[];
  approval_required: boolean;
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
}

export interface ChatDelegationTemplateDefinition {
  template_id: string;
  display_name: string;
  description: string;
  primary_profile_id: string;
  recommended_profiles: string[];
  execution_mode: "serial" | "parallel";
  merge_strategy: string;
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

export interface ChatQueuedInputRecord {
  queued_input_id: string;
  run_id: string;
  session_id: string;
  state: string;
  text: string;
  created_at_unix_ms: number;
  updated_at_unix_ms: number;
  origin_run_id?: string;
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

export interface ChatBackgroundTaskRecord {
  task_id: string;
  task_kind: string;
  session_id: string;
  parent_run_id?: string;
  target_run_id?: string;
  queued_input_id?: string;
  owner_principal: string;
  device_id: string;
  channel?: string;
  state: string;
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

export interface WorkspaceBootstrapOutcome {
  ran_at_unix_ms: number;
  created_paths: string[];
  updated_paths: string[];
  skipped_paths: string[];
}

export interface RecallPreviewEnvelope {
  query: string;
  memory_hits: JsonValue[];
  workspace_hits: WorkspaceSearchHit[];
  parameter_delta: JsonValue;
  prompt_preview: string;
  contract: ContractDescriptor;
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
  observability?: JsonValue;
  memory?: JsonValue;
  media?: JsonValue;
}

export interface ContractDescriptor {
  contract_version: string;
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
    payload: { session_label: string },
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
    payload: { text: string },
  ): Promise<{ queued_input: ChatQueuedInputRecord; contract: ContractDescriptor }> {
    return this.request(
      `/console/v1/chat/runs/${encodeURIComponent(runId)}/queue`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
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
    return this.request(`/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/transcript`);
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
    return this.request(buildPathWithQuery("/console/v1/chat/background-tasks", query));
  }

  async createBackgroundTask(
    sessionId: string,
    payload: {
      text: string;
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
        };
      };
    },
  ): Promise<{
    session: SessionCatalogRecord;
    task: ChatBackgroundTaskRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/sessions/${encodeURIComponent(sessionId)}/background-tasks`,
      {
        method: "POST",
        body: JSON.stringify(payload),
      },
      { csrf: true },
    );
  }

  async getBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    run?: ChatRunStatusRecord;
    contract: ContractDescriptor;
  }> {
    return this.request(`/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}`);
  }

  async pauseBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/pause`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async resumeBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/resume`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async retryBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/retry`,
      { method: "POST" },
      { csrf: true },
    );
  }

  async cancelBackgroundTask(taskId: string): Promise<{
    task: ChatBackgroundTaskRecord;
    action: string;
    contract: ContractDescriptor;
  }> {
    return this.request(
      `/console/v1/chat/background-tasks/${encodeURIComponent(taskId)}/cancel`,
      { method: "POST" },
      { csrf: true },
    );
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

  async searchMemory(params?: URLSearchParams): Promise<{ hits: JsonValue[] }> {
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

  async searchWorkspaceDocuments(
    params?: URLSearchParams,
  ): Promise<{ hits: WorkspaceSearchHit[]; contract: ContractDescriptor }> {
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
