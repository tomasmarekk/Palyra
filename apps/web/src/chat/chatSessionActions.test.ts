// Direct action tests pin chat command boundaries independently of component rendering.
// Delegation must remain fail-closed when only session-level authority is available.

import { describe, expect, it, vi } from "vite-plus/test";

import type { ChatDelegationCatalog, ConsoleApiClient } from "../consoleApi";
import { delegateWorkAction, queueFollowUpTextAction } from "./chatSessionActions";

describe("delegateWorkAction", () => {
  it("rejects a valid delegation because the console cannot create child-run authority", async () => {
    const delegationCatalog: ChatDelegationCatalog = {
      profiles: [],
      templates: [
        {
          template_id: "review_and_patch",
          display_name: "Review and patch",
          description: "Review a bounded change and apply the approved patch.",
          primary_profile_id: "reviewer",
          recommended_profiles: [],
          execution_mode: "serial",
          merge_strategy: "reviewed_patch",
          examples: [],
        },
      ],
    };
    const setError = vi.fn();

    await delegateWorkAction({
      sessionId: "session-1",
      raw: "review_and_patch Inspect the failing lint job",
      delegationCatalog,
      setError,
    });

    expect(setError).toHaveBeenCalledOnce();
    expect(setError).toHaveBeenCalledWith(
      "Delegation is available only from an active agent run; the console cannot create child-run authority.",
    );
  });
});

describe("queueFollowUpTextAction", () => {
  it("shows the exact durable queue outcome and demotion reason", async () => {
    const queueFollowUp = vi.fn().mockResolvedValue({
      queued_input: {
        queued_input_id: "queued-1",
        run_id: "run-1",
        session_id: "session-1",
        state: "deferred",
        queue_mode: "collect",
        delivery_boundary: "backlog_summary",
        expected_active_generation: 4,
        lifecycle_revision: 0,
        priority_lane: "normal",
        safe_boundary_flags_json: "{}",
        decision_reason: "interrupt_deferred_until_safe_boundary",
        text: "stop after the current delivery",
        attachments_json: "[]",
        queue_outcome_json: "{}",
        policy_snapshot_json: "{}",
        explain_json: "{}",
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
      },
      queue_outcome: {
        schema_version: 1,
        queued_input_id: "queued-1",
        lifecycle_state: "deferred",
        delivery_boundary: "backlog_summary",
        expected_active_generation: 4,
        observed_active_generation: 4,
        accepted: true,
        reason_code: "interrupt_deferred_until_safe_boundary",
      },
      contract: { contract_version: "control-plane.v1" },
    });
    const appendLocalEntry = vi.fn();
    const setNotice = vi.fn();

    await queueFollowUpTextAction({
      api: { queueFollowUp } as unknown as ConsoleApiClient,
      targetRunId: "run-1",
      text: "stop after the current delivery",
      sessionId: "session-1",
      appendLocalEntry,
      refreshSessionTranscript: vi.fn().mockResolvedValue(undefined),
      setComposerText: vi.fn(),
      setCommandBusy: vi.fn(),
      setError: vi.fn(),
      setNotice,
    });

    expect(setNotice).toHaveBeenLastCalledWith(
      "Queue outcome: deferred at backlog_summary (interrupt_deferred_until_safe_boundary).",
    );
    expect(appendLocalEntry).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "deferred",
        text: expect.stringContaining("interrupt_deferred_until_safe_boundary"),
      }),
    );
  });
});
