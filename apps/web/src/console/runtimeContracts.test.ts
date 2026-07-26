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
  normalizeQueuedInputDeliveryBoundary,
  normalizeQueuedInputState,
  normalizeWorkerLifecycleState,
} from "./runtimeContracts";

describe("runtimeContracts", () => {
  it("normalizes canonical runtime values", () => {
    expect(normalizeQueueMode("steer_backlog")).toBe("steer_backlog");
    expect(normalizeQueueDecision("merge")).toBe("merge");
    expect(normalizePruningPolicyClass("balanced")).toBe("balanced");
    expect(normalizeAuxiliaryTaskKind("summary")).toBe("summary");
    expect(normalizeAuxiliaryTaskKind("recall_search")).toBe("recall_search");
    expect(normalizeAuxiliaryTaskKind("classification")).toBe("classification");
    expect(normalizeAuxiliaryTaskKind("extraction")).toBe("extraction");
    expect(normalizeAuxiliaryTaskKind("vision")).toBe("vision");
    expect(normalizeAuxiliaryTaskKind("post_run_reflection")).toBe("post_run_reflection");
    expect(normalizeAuxiliaryTaskState("cancel_requested")).toBe("cancel_requested");
    expect(normalizeQueuedInputState("delivery_failed")).toBe("delivery_failed");
    expect(normalizeQueuedInputState("claimed")).toBe("claimed");
    expect(normalizeQueuedInputState("injected")).toBe("injected");
    expect(normalizeQueuedInputState("deferred")).toBe("deferred");
    expect(normalizeQueuedInputState("superseded")).toBe("superseded");
    expect(normalizeQueuedInputState("rejected")).toBe("rejected");
    expect(normalizeQueuedInputDeliveryBoundary("cancel_then_next_turn")).toBe(
      "cancel_then_next_turn",
    );
    expect(normalizeFlowState("waiting_for_approval")).toBe("waiting_for_approval");
    expect(normalizeFlowStepState("compensated")).toBe("compensated");
    expect(normalizeDeliveryPolicy("merge_progress_updates")).toBe("merge_progress_updates");
    expect(normalizeWorkerLifecycleState("assigned")).toBe("assigned");
  });

  it("maps compat aliases onto canonical runtime values", () => {
    expect(normalizeQueueMode("follow_up")).toBe("followup");
    expect(normalizeQueueDecision("coalesce")).toBe("merge");
    expect(normalizePruningPolicyClass("off")).toBe("disabled");
    expect(normalizeAuxiliaryTaskKind("auxiliary_summary")).toBe("summary");
    expect(normalizeAuxiliaryTaskKind("auxiliary_recall")).toBe("recall_search");
    expect(normalizeAuxiliaryTaskKind("auxiliary_classification")).toBe("classification");
    expect(normalizeAuxiliaryTaskKind("auxiliary_extraction")).toBe("extraction");
    expect(normalizeAuxiliaryTaskKind("auxiliary_vision")).toBe("vision");
    expect(normalizeAuxiliaryTaskKind("reflection")).toBe("post_run_reflection");
    expect(normalizeAuxiliaryTaskState("pending")).toBe("queued");
    expect(normalizeQueuedInputState("delivered")).toBe("forwarded");
    expect(normalizeQueuedInputDeliveryBoundary(undefined, "steer")).toBe(
      "current_run_before_provider",
    );
    expect(normalizeFlowState("approval_wait")).toBe("waiting_for_approval");
    expect(normalizeFlowStepState("timeout")).toBe("timed_out");
    expect(normalizeDeliveryPolicy("prefer_child_terminal")).toBe("prefer_terminal_descendant");
    expect(normalizeWorkerLifecycleState("leased")).toBe("assigned");
  });
});
