import { describe, expect, it } from "vite-plus/test";

import {
  normalizeAuxiliaryTaskKind,
  normalizeAuxiliaryTaskState,
  normalizeDeliveryPolicy,
  normalizeFlowState,
  normalizeFlowStepState,
  normalizePruningPolicyClass,
  normalizeQueueDecision,
  normalizeQueueMode,
  normalizeQueuedInputState,
  normalizeWorkerLifecycleState,
} from "./runtimeContracts";

describe("runtimeContracts", () => {
  it("normalizes canonical runtime values", () => {
    expect(normalizeQueueMode("steer_backlog")).toBe("steer_backlog");
    expect(normalizeQueueDecision("merge")).toBe("merge");
    expect(normalizePruningPolicyClass("balanced")).toBe("balanced");
    expect(normalizeAuxiliaryTaskKind("post_run_reflection")).toBe("post_run_reflection");
    expect(normalizeAuxiliaryTaskState("cancel_requested")).toBe("cancel_requested");
    expect(normalizeQueuedInputState("delivery_failed")).toBe("delivery_failed");
    expect(normalizeFlowState("waiting_for_approval")).toBe("waiting_for_approval");
    expect(normalizeFlowStepState("compensated")).toBe("compensated");
    expect(normalizeDeliveryPolicy("merge_progress_updates")).toBe("merge_progress_updates");
    expect(normalizeWorkerLifecycleState("assigned")).toBe("assigned");
  });

  it("maps compat aliases onto canonical runtime values", () => {
    expect(normalizeQueueMode("follow_up")).toBe("followup");
    expect(normalizeQueueDecision("coalesce")).toBe("merge");
    expect(normalizePruningPolicyClass("off")).toBe("disabled");
    expect(normalizeAuxiliaryTaskKind("reflection")).toBe("post_run_reflection");
    expect(normalizeAuxiliaryTaskState("pending")).toBe("queued");
    expect(normalizeQueuedInputState("delivered")).toBe("forwarded");
    expect(normalizeFlowState("approval_wait")).toBe("waiting_for_approval");
    expect(normalizeFlowStepState("timeout")).toBe("timed_out");
    expect(normalizeDeliveryPolicy("prefer_child_terminal")).toBe("prefer_terminal_descendant");
    expect(normalizeWorkerLifecycleState("leased")).toBe("assigned");
  });
});
