import type { JsonValue } from "./consoleApi";

interface ConsoleRequestClient {
  request<T>(path: string, init?: RequestInit, options?: { csrf?: boolean }): Promise<T>;
}

export interface ConsoleFlowRecord {
  flow_id: string;
  mode: string;
  state: string;
  owner_principal: string;
  device_id: string;
  channel?: string | null;
  session_id?: string | null;
  origin_run_id?: string | null;
  objective_id?: string | null;
  routine_id?: string | null;
  webhook_id?: string | null;
  title: string;
  summary?: string | null;
  current_step_id?: string | null;
  revision: number;
  lock_owner?: string | null;
  lock_expires_at_unix_ms?: number | null;
  retry_policy?: JsonValue;
  timeout_ms?: number | null;
  metadata?: JsonValue;
  created_at_unix_ms?: number;
  updated_at_unix_ms?: number;
  completed_at_unix_ms?: number | null;
}

export interface ConsoleFlowStepRecord {
  step_id: string;
  flow_id: string;
  step_index: number;
  step_kind: string;
  adapter: string;
  state: string;
  title: string;
  input?: JsonValue;
  output?: JsonValue | null;
  lineage?: JsonValue;
  depends_on_step_ids?: JsonValue;
  attempt_count?: number;
  max_attempts?: number;
  backoff_ms?: number;
  timeout_ms?: number | null;
  not_before_unix_ms?: number | null;
  waiting_reason?: string | null;
  last_error?: string | null;
  created_at_unix_ms?: number;
  updated_at_unix_ms?: number;
  started_at_unix_ms?: number | null;
  completed_at_unix_ms?: number | null;
}

export interface ConsoleFlowEventRecord {
  event_id: string;
  flow_id: string;
  step_id?: string | null;
  event_type: string;
  actor_principal?: string;
  from_state?: string | null;
  to_state?: string | null;
  summary?: string | null;
  payload?: JsonValue;
  created_at_unix_ms?: number;
}

export interface ConsoleFlowRevisionRecord {
  revision_id: string;
  flow_id: string;
  revision: number;
  parent_revision?: number | null;
  change_kind: string;
  actor_principal?: string;
  payload?: JsonValue;
  created_at_unix_ms?: number;
}

export interface ConsoleFlowListEnvelope {
  flows: ConsoleFlowRecord[];
  summary?: JsonValue;
  adapters?: JsonValue;
  rollout?: JsonValue;
  page?: JsonValue;
}

export interface ConsoleFlowBundleEnvelope {
  flow: ConsoleFlowRecord;
  steps: ConsoleFlowStepRecord[];
  events: ConsoleFlowEventRecord[];
  revisions: ConsoleFlowRevisionRecord[];
  blockers?: ConsoleFlowStepRecord[];
  retry_history?: ConsoleFlowEventRecord[];
  lineage?: JsonValue;
  dependency_validation?: JsonValue;
  adapters?: JsonValue;
  rollout?: JsonValue;
}

export interface ConsoleFlowActionPayload {
  reason?: string;
}

export interface ConsoleFlowDependencyRepairPayload {
  expected_revision: number;
  replacements: ConsoleFlowDependencyReplacement[];
}

export interface ConsoleFlowDependencyReplacement {
  step_id: string;
  depends_on_step_ids: string[];
}

export function listFlows(
  api: ConsoleRequestClient,
  params?: URLSearchParams,
): Promise<ConsoleFlowListEnvelope> {
  return api.request(buildPathWithQuery("/console/v1/flows", params));
}

export function getFlow(
  api: ConsoleRequestClient,
  flowId: string,
): Promise<ConsoleFlowBundleEnvelope> {
  return api.request(`/console/v1/flows/${encodeURIComponent(flowId)}`);
}

export function pauseFlow(
  api: ConsoleRequestClient,
  flowId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlow(api, flowId, "pause", payload);
}

export function resumeFlow(
  api: ConsoleRequestClient,
  flowId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlow(api, flowId, "resume", payload);
}

export function cancelFlow(
  api: ConsoleRequestClient,
  flowId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlow(api, flowId, "cancel", payload);
}

export function retryFlowStep(
  api: ConsoleRequestClient,
  flowId: string,
  stepId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlowStep(api, flowId, stepId, "retry", payload);
}

export function skipFlowStep(
  api: ConsoleRequestClient,
  flowId: string,
  stepId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlowStep(api, flowId, stepId, "skip", payload);
}

export function compensateFlowStep(
  api: ConsoleRequestClient,
  flowId: string,
  stepId: string,
  payload: ConsoleFlowActionPayload = {},
): Promise<ConsoleFlowBundleEnvelope> {
  return mutateFlowStep(api, flowId, stepId, "compensate", payload);
}

export function repairFlowDependencies(
  api: ConsoleRequestClient,
  flowId: string,
  payload: ConsoleFlowDependencyRepairPayload,
): Promise<ConsoleFlowBundleEnvelope> {
  return api.request(
    `/console/v1/flows/${encodeURIComponent(flowId)}/dependencies/repair`,
    { method: "POST", body: JSON.stringify(payload) },
    { csrf: true },
  );
}

function mutateFlow(
  api: ConsoleRequestClient,
  flowId: string,
  action: "pause" | "resume" | "cancel",
  payload: ConsoleFlowActionPayload,
): Promise<ConsoleFlowBundleEnvelope> {
  return api.request(
    `/console/v1/flows/${encodeURIComponent(flowId)}/${action}`,
    { method: "POST", body: JSON.stringify(payload) },
    { csrf: true },
  );
}

function mutateFlowStep(
  api: ConsoleRequestClient,
  flowId: string,
  stepId: string,
  action: "retry" | "skip" | "compensate",
  payload: ConsoleFlowActionPayload,
): Promise<ConsoleFlowBundleEnvelope> {
  return api.request(
    `/console/v1/flows/${encodeURIComponent(flowId)}/steps/${encodeURIComponent(stepId)}/${action}`,
    { method: "POST", body: JSON.stringify(payload) },
    { csrf: true },
  );
}

function buildPathWithQuery(path: string, params?: URLSearchParams): string {
  if (params === undefined || params.size === 0) {
    return path;
  }
  return `${path}?${params.toString()}`;
}
