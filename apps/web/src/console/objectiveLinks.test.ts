import { describe, expect, it } from "vite-plus/test";

import {
  buildObjectiveChatHref,
  buildObjectiveOverviewHref,
  findObjectiveForSession,
  objectiveMatchesSession,
  objectivePrimarySessionId,
} from "./objectiveLinks";
import type { JsonObject } from "./shared";

describe("objectiveLinks", () => {
  it("matches objectives through session key and related session ids", () => {
    const objective = sampleObjective({
      workspace: {
        session_key: "SESSION-KEY-1",
        related_session_ids: ["session-123"],
      },
    });

    expect(
      objectiveMatchesSession(objective, {
        session_id: "session-123",
        session_key: "other-key",
      }),
    ).toBe(true);
    expect(
      objectiveMatchesSession(objective, {
        session_id: "different-session",
        session_key: "SESSION-KEY-1",
      }),
    ).toBe(true);
    expect(
      objectiveMatchesSession(objective, {
        session_id: "different-session",
        session_key: "other-key",
      }),
    ).toBe(false);
  });

  it("prefers active linked objectives when selecting from a session", () => {
    const pausedObjective = sampleObjective({
      objective_id: "OBJ-paused",
      state: "paused",
      updated_at_unix_ms: 200,
      workspace: { session_key: "shared-key" },
    });
    const activeObjective = sampleObjective({
      objective_id: "OBJ-active",
      state: "active",
      updated_at_unix_ms: 100,
      workspace: { session_key: "shared-key" },
    });

    const selected = findObjectiveForSession(
      [pausedObjective, activeObjective],
      { session_key: "shared-key" },
      null,
    );

    expect(selected?.objective_id).toBe("OBJ-active");
  });

  it("builds objective deep links for overview and chat surfaces", () => {
    const objective = sampleObjective({
      objective_id: "OBJ-123",
      workspace: { related_session_ids: ["session-xyz"] },
    });

    expect(buildObjectiveOverviewHref("OBJ-123")).toBe("/control/overview?objectiveId=OBJ-123");
    expect(objectivePrimarySessionId(objective, null)).toBe("session-xyz");
    expect(
      buildObjectiveChatHref({
        objective,
        fallbackSessionId: "fallback-session",
        runId: "run-321",
      }),
    ).toBe("/chat?objectiveId=OBJ-123&sessionId=session-xyz&runId=run-321");
  });
});

function sampleObjective(overrides: Partial<JsonObject> = {}): JsonObject {
  return {
    objective_id: "OBJ-1",
    state: "active",
    updated_at_unix_ms: 1,
    workspace: {
      related_session_ids: [],
      ...((overrides.workspace as JsonObject | undefined) ?? {}),
    },
    ...overrides,
  };
}
