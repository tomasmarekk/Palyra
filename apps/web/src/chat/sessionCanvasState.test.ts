// @vitest-environment jsdom

import { beforeEach, describe, expect, it } from "vite-plus/test";

import type { SessionCanvasSummary } from "../consoleApi";
import {
  buildCanvasRuntimeFrameUrl,
  buildChatCanvasHref,
  extractCanvasIdFromFrameUrl,
  readSessionCanvasPreference,
  rememberSessionCanvasPreference,
  resolvePreferredSessionCanvasId,
  togglePinnedSessionCanvasPreference,
} from "./sessionCanvasState";

const CANVASES: SessionCanvasSummary[] = [
  {
    canvas_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1",
    state_version: 4,
    state_schema_version: 1,
    created_at_unix_ms: 100,
    updated_at_unix_ms: 220,
    expires_at_unix_ms: 1_000,
    closed: false,
    runtime_status: "ready",
    reference: {},
  },
  {
    canvas_id: "01ARZ3NDEKTSV4RRFFQ69G5FB2",
    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1",
    state_version: 2,
    state_schema_version: 1,
    created_at_unix_ms: 110,
    updated_at_unix_ms: 210,
    expires_at_unix_ms: 1_000,
    closed: false,
    runtime_status: "ready",
    reference: {},
  },
];

describe("sessionCanvasState helpers", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  it("persists the last selected canvas per session", () => {
    expect(readSessionCanvasPreference("session-1")).toEqual({});

    rememberSessionCanvasPreference("session-1", "canvas-a");

    expect(readSessionCanvasPreference("session-1")).toEqual({
      lastCanvasId: "canvas-a",
    });
  });

  it("toggles the pinned canvas without leaking across sessions", () => {
    togglePinnedSessionCanvasPreference("session-1", "canvas-a");
    rememberSessionCanvasPreference("session-2", "canvas-b");

    expect(readSessionCanvasPreference("session-1")).toEqual({
      lastCanvasId: "canvas-a",
      pinnedCanvasId: "canvas-a",
    });
    expect(readSessionCanvasPreference("session-2")).toEqual({
      lastCanvasId: "canvas-b",
    });

    togglePinnedSessionCanvasPreference("session-1", "canvas-a");
    expect(readSessionCanvasPreference("session-1")).toEqual({
      lastCanvasId: "canvas-a",
    });
  });

  it("resolves explicit, pinned, and last canvas preferences in order", () => {
    expect(
      resolvePreferredSessionCanvasId(CANVASES, {
        lastCanvasId: "01ARZ3NDEKTSV4RRFFQ69G5FB2",
        pinnedCanvasId: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
      }),
    ).toBe("01ARZ3NDEKTSV4RRFFQ69G5FB1");

    expect(
      resolvePreferredSessionCanvasId(
        CANVASES,
        {
          lastCanvasId: "01ARZ3NDEKTSV4RRFFQ69G5FB2",
          pinnedCanvasId: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        },
        ["01ARZ3NDEKTSV4RRFFQ69G5FB2"],
      ),
    ).toBe("01ARZ3NDEKTSV4RRFFQ69G5FB2");
  });

  it("extracts canvas ids from relative and absolute frame urls", () => {
    expect(
      extractCanvasIdFromFrameUrl("/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test"),
    ).toBe("01ARZ3NDEKTSV4RRFFQ69G5FB1");
    expect(
      extractCanvasIdFromFrameUrl(
        "https://console.example.test/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB2?token=test",
      ),
    ).toBe("01ARZ3NDEKTSV4RRFFQ69G5FB2");
    expect(extractCanvasIdFromFrameUrl("/chat")).toBeNull();
  });

  it("builds canonical canvas handoff links and runtime frame urls", () => {
    expect(
      buildChatCanvasHref({
        sessionId: "01ARZ3NDEKTSV4RRFFQ69G5FA1",
        canvasId: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
      }),
    ).toBe(
      "/chat/canvas?sessionId=01ARZ3NDEKTSV4RRFFQ69G5FA1&canvasId=01ARZ3NDEKTSV4RRFFQ69G5FB1&intent=reopen-canvas",
    );

    expect(
      buildCanvasRuntimeFrameUrl({
        canvas_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        frame_url: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1",
        runtime_url: "/canvas/v1/runtime.js",
        auth_token: "token-123",
        expires_at_unix_ms: 1_000,
      }),
    ).toBe("/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=token-123");
  });
});
