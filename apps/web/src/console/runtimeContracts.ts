function normalizeRuntimeToken(value?: string | null): string | undefined {
  const normalized = value?.trim().toLowerCase().replaceAll("-", "_");
  return normalized && normalized.length > 0 ? normalized : undefined;
}

export const QUEUE_MODES = ["followup", "collect", "steer", "steer_backlog", "interrupt"] as const;
export type QueueMode = (typeof QUEUE_MODES)[number];

export function normalizeQueueMode(value?: string | null): QueueMode {
  switch (normalizeRuntimeToken(value)) {
    case "follow_up":
    case "followup":
      return "followup";
    case "collect":
      return "collect";
    case "steer":
      return "steer";
    case "steer_backlog":
      return "steer_backlog";
    case "interrupt":
      return "interrupt";
    default:
      return "followup";
  }
}

export const QUEUE_DECISIONS = [
  "enqueue",
  "merge",
  "steer",
  "steer_backlog",
  "interrupt",
  "overflow",
  "defer",
] as const;
export type QueueDecision = (typeof QUEUE_DECISIONS)[number];

export function normalizeQueueDecision(value?: string | null): QueueDecision {
  switch (normalizeRuntimeToken(value)) {
    case "enqueue":
      return "enqueue";
    case "coalesce":
    case "merge":
      return "merge";
    case "steer":
      return "steer";
    case "steer_backlog":
      return "steer_backlog";
    case "interrupt":
      return "interrupt";
    case "overflow":
      return "overflow";
    case "deferred":
    case "defer":
      return "defer";
    default:
      return "enqueue";
  }
}

export const PRUNING_POLICY_CLASSES = [
  "disabled",
  "conservative",
  "balanced",
  "aggressive",
] as const;
export type PruningPolicyClass = (typeof PRUNING_POLICY_CLASSES)[number];

export function normalizePruningPolicyClass(value?: string | null): PruningPolicyClass {
  switch (normalizeRuntimeToken(value)) {
    case "off":
    case "disabled":
      return "disabled";
    case "safe":
    case "conservative":
      return "conservative";
    case "default":
    case "balanced":
      return "balanced";
    case "high_reduction":
    case "aggressive":
      return "aggressive";
    default:
      return "disabled";
  }
}

export const AUXILIARY_TASK_KINDS = [
  "background_prompt",
  "delegation_prompt",
  "attachment_derivation",
  "attachment_recompute",
  "post_run_reflection",
] as const;
export type AuxiliaryTaskKind = (typeof AUXILIARY_TASK_KINDS)[number];

export function normalizeAuxiliaryTaskKind(value?: string | null): AuxiliaryTaskKind {
  switch (normalizeRuntimeToken(value)) {
    case "background_prompt":
      return "background_prompt";
    case "delegation_prompt":
      return "delegation_prompt";
    case "attachment_derivation":
      return "attachment_derivation";
    case "attachment_recompute":
      return "attachment_recompute";
    case "reflection":
    case "post_run_reflection":
      return "post_run_reflection";
    default:
      return "background_prompt";
  }
}

export const AUXILIARY_TASK_STATES = [
  "queued",
  "running",
  "paused",
  "succeeded",
  "failed",
  "cancel_requested",
  "cancelled",
  "expired",
] as const;
export type AuxiliaryTaskState = (typeof AUXILIARY_TASK_STATES)[number];

export function normalizeAuxiliaryTaskState(value?: string | null): AuxiliaryTaskState {
  switch (normalizeRuntimeToken(value)) {
    case "pending":
    case "queued":
      return "queued";
    case "in_progress":
    case "running":
      return "running";
    case "paused":
      return "paused";
    case "complete":
    case "completed":
    case "succeeded":
      return "succeeded";
    case "failed":
      return "failed";
    case "cancel_requested":
      return "cancel_requested";
    case "canceled":
    case "cancelled":
      return "cancelled";
    case "expired":
      return "expired";
    default:
      return "queued";
  }
}

export const QUEUED_INPUT_STATES = [
  "pending",
  "forwarded",
  "delivery_failed",
  "merged",
  "steered",
  "interrupted",
  "overflowed",
] as const;
export type QueuedInputState = (typeof QUEUED_INPUT_STATES)[number];

export function normalizeQueuedInputState(value?: string | null): QueuedInputState {
  switch (normalizeRuntimeToken(value)) {
    case "queued":
    case "pending":
      return "pending";
    case "delivered":
    case "forwarded":
      return "forwarded";
    case "failed_delivery":
    case "delivery_failed":
      return "delivery_failed";
    case "merged":
      return "merged";
    case "steered":
      return "steered";
    case "interrupted":
      return "interrupted";
    case "overflow":
    case "overflowed":
      return "overflowed";
    default:
      return "pending";
  }
}

export const FLOW_STATES = [
  "pending",
  "ready",
  "running",
  "waiting_for_approval",
  "paused",
  "blocked",
  "retrying",
  "compensating",
  "timed_out",
  "succeeded",
  "failed",
  "cancel_requested",
  "cancelled",
] as const;
export type FlowState = (typeof FLOW_STATES)[number];

export function normalizeFlowState(value?: string | null): FlowState {
  switch (normalizeRuntimeToken(value)) {
    case "pending":
      return "pending";
    case "ready":
      return "ready";
    case "in_progress":
    case "running":
      return "running";
    case "approval_wait":
    case "waiting":
    case "waiting_for_approval":
      return "waiting_for_approval";
    case "paused":
      return "paused";
    case "blocked":
      return "blocked";
    case "retrying":
      return "retrying";
    case "compensating":
      return "compensating";
    case "timeout":
    case "timed_out":
      return "timed_out";
    case "completed":
    case "succeeded":
      return "succeeded";
    case "failed":
      return "failed";
    case "cancel_requested":
      return "cancel_requested";
    case "canceled":
    case "cancelled":
      return "cancelled";
    default:
      return "pending";
  }
}

export const FLOW_STEP_STATES = [
  "pending",
  "ready",
  "running",
  "waiting_for_approval",
  "paused",
  "blocked",
  "retrying",
  "skipped",
  "compensating",
  "compensated",
  "timed_out",
  "succeeded",
  "failed",
  "cancel_requested",
  "cancelled",
] as const;
export type FlowStepState = (typeof FLOW_STEP_STATES)[number];

export function normalizeFlowStepState(value?: string | null): FlowStepState {
  switch (normalizeRuntimeToken(value)) {
    case "pending":
      return "pending";
    case "ready":
      return "ready";
    case "in_progress":
    case "running":
      return "running";
    case "approval_wait":
    case "waiting":
    case "waiting_for_approval":
      return "waiting_for_approval";
    case "paused":
      return "paused";
    case "blocked":
      return "blocked";
    case "retrying":
      return "retrying";
    case "skipped":
      return "skipped";
    case "compensating":
      return "compensating";
    case "compensated":
      return "compensated";
    case "timeout":
    case "timed_out":
      return "timed_out";
    case "completed":
    case "succeeded":
      return "succeeded";
    case "failed":
      return "failed";
    case "cancel_requested":
      return "cancel_requested";
    case "canceled":
    case "cancelled":
      return "cancelled";
    default:
      return "pending";
  }
}

export const DELIVERY_POLICIES = [
  "prefer_terminal_descendant",
  "suppress_stale_parent",
  "merge_progress_updates",
  "deliver_interim_parent",
  "require_final_review",
] as const;
export type DeliveryPolicy = (typeof DELIVERY_POLICIES)[number];

export function normalizeDeliveryPolicy(value?: string | null): DeliveryPolicy {
  switch (normalizeRuntimeToken(value)) {
    case "prefer_child_terminal":
    case "prefer_terminal_descendant":
      return "prefer_terminal_descendant";
    case "suppress_stale_parent":
      return "suppress_stale_parent";
    case "coalesce_progress":
    case "merge_progress_updates":
      return "merge_progress_updates";
    case "deliver_interim_parent":
      return "deliver_interim_parent";
    case "require_final_review":
      return "require_final_review";
    default:
      return "prefer_terminal_descendant";
  }
}

export const WORKER_LIFECYCLE_STATES = [
  "registered",
  "assigned",
  "completed",
  "failed",
  "orphaned",
] as const;
export type WorkerLifecycleState = (typeof WORKER_LIFECYCLE_STATES)[number];

export function normalizeWorkerLifecycleState(value?: string | null): WorkerLifecycleState {
  switch (normalizeRuntimeToken(value)) {
    case "registered":
      return "registered";
    case "leased":
    case "assigned":
      return "assigned";
    case "succeeded":
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "orphaned":
      return "orphaned";
    default:
      return "registered";
  }
}
