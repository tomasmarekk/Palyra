import type { SessionCanvasRuntimeDescriptor, SessionCanvasSummary } from "../consoleApi";
import { buildConsoleHandoffHref, type CrossSurfaceHandoff } from "../console/contracts";

const SESSION_CANVAS_STORAGE_PREFIX = "palyra.chat.sessionCanvas";
const CANVAS_FRAME_PATH_SEGMENTS = ["canvas", "v1", "frame"] as const;

export type SessionCanvasPreference = {
  lastCanvasId?: string;
  pinnedCanvasId?: string;
};

export function readSessionCanvasPreference(sessionId: string): SessionCanvasPreference {
  const normalizedSessionId = normalizeStorageKeyPart(sessionId);
  if (normalizedSessionId === null || typeof window === "undefined") {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(storageKey(normalizedSessionId));
    if (raw === null) {
      return {};
    }
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return {};
    }
    return {
      lastCanvasId: normalizeCanvasId(readPreferenceValue(parsed, "lastCanvasId")),
      pinnedCanvasId: normalizeCanvasId(readPreferenceValue(parsed, "pinnedCanvasId")),
    };
  } catch {
    return {};
  }
}

export function rememberSessionCanvasPreference(
  sessionId: string,
  canvasId: string,
): SessionCanvasPreference {
  const normalizedSessionId = normalizeStorageKeyPart(sessionId);
  const normalizedCanvasId = normalizeCanvasId(canvasId);
  if (normalizedSessionId === null || normalizedCanvasId === undefined) {
    return {};
  }
  const current = readSessionCanvasPreference(normalizedSessionId);
  const next = {
    lastCanvasId: normalizedCanvasId,
    pinnedCanvasId: current.pinnedCanvasId,
  };
  writeSessionCanvasPreference(normalizedSessionId, next);
  return next;
}

export function togglePinnedSessionCanvasPreference(
  sessionId: string,
  canvasId: string,
): SessionCanvasPreference {
  const normalizedSessionId = normalizeStorageKeyPart(sessionId);
  const normalizedCanvasId = normalizeCanvasId(canvasId);
  if (normalizedSessionId === null || normalizedCanvasId === undefined) {
    return {};
  }
  const current = readSessionCanvasPreference(normalizedSessionId);
  const nextPinnedCanvasId =
    current.pinnedCanvasId === normalizedCanvasId ? undefined : normalizedCanvasId;
  const next = {
    lastCanvasId: normalizedCanvasId,
    pinnedCanvasId: nextPinnedCanvasId,
  };
  writeSessionCanvasPreference(normalizedSessionId, next);
  return next;
}

export function resolvePreferredSessionCanvasId(
  canvases: readonly SessionCanvasSummary[],
  preference: SessionCanvasPreference,
  candidateIds: readonly (string | null | undefined)[] = [],
): string | null {
  const knownCanvasIds = new Set(canvases.map((canvas) => canvas.canvas_id));
  for (const candidate of candidateIds) {
    const normalized = normalizeCanvasId(candidate);
    if (normalized !== undefined && knownCanvasIds.has(normalized)) {
      return normalized;
    }
  }
  const pinnedCanvasId = normalizeCanvasId(preference.pinnedCanvasId);
  if (pinnedCanvasId !== undefined && knownCanvasIds.has(pinnedCanvasId)) {
    return pinnedCanvasId;
  }
  const lastCanvasId = normalizeCanvasId(preference.lastCanvasId);
  if (lastCanvasId !== undefined && knownCanvasIds.has(lastCanvasId)) {
    return lastCanvasId;
  }
  return canvases[0]?.canvas_id ?? null;
}

export function extractCanvasIdFromFrameUrl(raw: string): string | null {
  const value = raw.trim();
  if (value.length === 0) {
    return null;
  }
  try {
    const parsed = new URL(value, resolveUrlBase());
    const segments = parsed.pathname.split("/").filter((segment) => segment.length > 0);
    if (
      segments.length !== CANVAS_FRAME_PATH_SEGMENTS.length + 1 ||
      !CANVAS_FRAME_PATH_SEGMENTS.every((segment, index) => segments[index] === segment)
    ) {
      return null;
    }
    return normalizeCanvasId(segments[CANVAS_FRAME_PATH_SEGMENTS.length]) ?? null;
  } catch {
    return null;
  }
}

export function buildChatCanvasHref(
  payload: Pick<CrossSurfaceHandoff, "canvasId" | "runId" | "sessionId" | "source"> = {},
): string {
  return buildConsoleHandoffHref({
    ...payload,
    section: "canvas",
    intent: payload.canvasId ? "reopen-canvas" : undefined,
  });
}

export function buildCanvasRuntimeFrameUrl(
  runtime: SessionCanvasRuntimeDescriptor | null | undefined,
): string | null {
  if (runtime === null || runtime === undefined) {
    return null;
  }
  const frameUrl = runtime.frame_url.trim();
  const authToken = runtime.auth_token.trim();
  if (frameUrl.length === 0 || authToken.length === 0) {
    return null;
  }
  try {
    const parsed = new URL(frameUrl, resolveUrlBase());
    parsed.searchParams.set("token", authToken);
    return parsed.origin === resolveUrlBase()
      ? `${parsed.pathname}${parsed.search}`
      : parsed.toString();
  } catch {
    const separator = frameUrl.includes("?") ? "&" : "?";
    return `${frameUrl}${separator}token=${encodeURIComponent(authToken)}`;
  }
}

function storageKey(sessionId: string): string {
  return `${SESSION_CANVAS_STORAGE_PREFIX}.${sessionId}`;
}

function writeSessionCanvasPreference(
  sessionId: string,
  preference: SessionCanvasPreference,
): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const payload: SessionCanvasPreference = {};
    const lastCanvasId = normalizeCanvasId(preference.lastCanvasId);
    const pinnedCanvasId = normalizeCanvasId(preference.pinnedCanvasId);
    if (lastCanvasId !== undefined) {
      payload.lastCanvasId = lastCanvasId;
    }
    if (pinnedCanvasId !== undefined) {
      payload.pinnedCanvasId = pinnedCanvasId;
    }
    if (payload.lastCanvasId === undefined && payload.pinnedCanvasId === undefined) {
      window.localStorage.removeItem(storageKey(sessionId));
      return;
    }
    window.localStorage.setItem(storageKey(sessionId), JSON.stringify(payload));
  } catch {
    // Ignore storage failures; canvas state still works for the current session.
  }
}

function normalizeCanvasId(value: string | null | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized?.length ? normalized : undefined;
}

function normalizeStorageKeyPart(value: string): string | null {
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function readPreferenceValue(
  source: object,
  key: "lastCanvasId" | "pinnedCanvasId",
): string | null {
  const candidate = (source as Record<string, unknown>)[key];
  return typeof candidate === "string" ? candidate : null;
}

function resolveUrlBase(): string {
  if (typeof window !== "undefined" && window.location.origin !== "null") {
    return window.location.origin;
  }
  return "https://palyra.local";
}
