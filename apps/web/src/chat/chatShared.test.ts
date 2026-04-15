// @vitest-environment jsdom

import { describe, expect, it } from "vite-plus/test";

import type { JsonValue } from "../consoleApi";
import {
  CHAT_SLASH_COMMANDS,
  buildContextBudgetSummary,
  buildSessionLineageHint,
  collectCanvasFrameUrls,
  describeBranchState,
  describeTitleGenerationState,
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
    expect(parseSlashCommand("/status")).toBeNull();
    expect(parseSlashCommand("/agent default")).toBeNull();
  });

  it("exposes only web-supported slash commands from the shared registry", () => {
    const commandNames = CHAT_SLASH_COMMANDS.map((command) => command.name);
    expect(commandNames).toContain("help");
    expect(commandNames).toContain("compact");
    expect(commandNames).not.toContain("status");
    expect(commandNames).not.toContain("shell");
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
        title_generation_state: "ready",
        manual_title_locked: true,
        auto_title_updated_at_unix_ms: 100,
        manual_title_updated_at_unix_ms: 100,
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
        has_context_files: false,
        artifact_count: 0,
        family: {
          root_title: "Incident follow-up",
          sequence: 2,
          family_size: 2,
          parent_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA0",
          parent_title: "Root incident",
          relatives: [],
        },
        recap: {
          touched_files: [],
          active_context_files: [],
          recent_artifacts: [],
          ctas: [],
        },
        quick_controls: {
          agent: {
            value: "default",
            display_value: "Default agent",
            source: "default",
            inherited_value: "default",
            override_active: false,
          },
          model: {
            value: "gpt-5.4",
            display_value: "gpt-5.4",
            source: "default",
            inherited_value: "gpt-5.4",
            override_active: false,
          },
          thinking: {
            value: true,
            source: "surface_default",
            inherited_value: true,
            override_active: false,
          },
          trace: {
            value: false,
            source: "surface_default",
            inherited_value: false,
            override_active: false,
          },
          verbose: {
            value: false,
            source: "surface_default",
            inherited_value: false,
            override_active: false,
          },
          reset_to_default_available: false,
        },
      }),
    ).toMatch(/Active branch from .* at run/i);
  });

  it("maps title lifecycle states into operator-facing labels", () => {
    expect(describeTitleGenerationState("ready", false)).toBe("Auto title ready");
    expect(describeTitleGenerationState("pending", false)).toBe("Auto title pending");
    expect(describeTitleGenerationState("idle", true)).toBe("Manual title");
  });
});

describe("collectCanvasFrameUrls", () => {
  it("deduplicates nested canvas urls and ignores invalid entries", () => {
    const payload: JsonValue = {
      primary: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=one",
      nested: [
        "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=one",
        {
          secondary: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB2?token=two",
          invalid: [
            "https://evil.example/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB3?token=bad",
            "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB4",
          ],
        },
      ],
    };

    expect(collectCanvasFrameUrls(payload)).toEqual([
      "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=one",
      "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB2?token=two",
    ]);
  });

  it("caps discovered canvas urls per payload", () => {
    const payload: JsonValue = Array.from({ length: 20 }, (_, index) => {
      return `/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G${String(index).padStart(5, "0")}?token=t${index}`;
    });

    expect(collectCanvasFrameUrls(payload)).toEqual(
      Array.from({ length: 8 }, (_, index) => {
        return `/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G${String(index).padStart(5, "0")}?token=t${index}`;
      }),
    );
  });

  it("stays bounded on deeply nested payloads", () => {
    let deepPayload: JsonValue = "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G9ZZ?token=deep";
    for (let index = 0; index < 20_000; index += 1) {
      deepPayload = { nested: deepPayload };
    }
    const payload: JsonValue = {
      top: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB5?token=top",
      deep: deepPayload,
    };

    expect(() => collectCanvasFrameUrls(payload)).not.toThrow();
    expect(collectCanvasFrameUrls(payload)).toEqual([
      "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB5?token=top",
    ]);
  });
});
