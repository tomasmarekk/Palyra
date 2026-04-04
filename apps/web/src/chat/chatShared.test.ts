import { describe, expect, it } from "vite-plus/test";

import {
  buildContextBudgetSummary,
  buildSessionLineageHint,
  describeBranchState,
  parseCompactCommandMode,
  parseSlashCommand,
} from "./chatShared";

describe("chatShared helpers", () => {
  it("parses slash commands with arguments", () => {
    expect(parseSlashCommand("/branch Incident follow-up")).toEqual({
      name: "branch",
      args: "Incident follow-up",
    });
    expect(parseSlashCommand("/compact apply")).toEqual({
      name: "compact",
      args: "apply",
    });
    expect(parseSlashCommand("/delegate review_and_patch Inspect the failing lint job")).toEqual({
      name: "delegate",
      args: "review_and_patch Inspect the failing lint job",
    });
    expect(parseSlashCommand("/help")).toEqual({
      name: "help",
      args: "",
    });
  });

  it("resolves compact slash subcommands with preview as the safe default", () => {
    expect(parseCompactCommandMode("")).toBe("preview");
    expect(parseCompactCommandMode("preview")).toBe("preview");
    expect(parseCompactCommandMode("apply now")).toBe("apply");
  });

  it("builds context budget warnings from draft and attachment estimates", () => {
    const summary = buildContextBudgetSummary({
      baseline_tokens: 15_200,
      draft_text: "Inspect the last failed run and summarize the root cause.",
      attachments: [
        {
          local_id: "local-1",
          artifact_id: "01ARZ3NDEKTSV4RRFFQ69G5FAA",
          attachment_id: "att-1",
          filename: "screen.png",
          declared_content_type: "image/png",
          content_hash: "sha256:abc",
          size_bytes: 2_048,
          kind: "image",
          budget_tokens: 900,
        },
      ],
    });

    expect(summary.tone).toBe("danger");
    expect(summary.warning).toMatch(/above the safe working budget/i);
  });

  it("renders branch labels and lineage hints for current branch terminology", () => {
    expect(describeBranchState("active_branch")).toBe("Active branch");
    expect(describeBranchState("branch_source")).toBe("Branch source");
    expect(
      buildSessionLineageHint({
        session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        session_key: "web",
        title: "Incident follow-up",
        title_source: "manual",
        preview_state: "computed",
        last_intent_state: "computed",
        last_summary_state: "computed",
        branch_state: "active_branch",
        parent_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA0",
        branch_origin_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1",
        principal: "admin:web-console",
        device_id: "device-1",
        created_at_unix_ms: 100,
        updated_at_unix_ms: 100,
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        archived: false,
        pending_approvals: 0,
      }),
    ).toMatch(/Active branch from .* at run/i);
  });
});
