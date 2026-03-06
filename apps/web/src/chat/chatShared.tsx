import type { JsonValue } from "../consoleApi";

const SENSITIVE_KEY_PATTERN =
  /(secret|token|password|cookie|authorization|credential|api[-_]?key|private[-_]?key|vault[-_]?ref)/i;
const SENSITIVE_VALUE_PATTERN =
  /^(Bearer\s+|sk-[a-z0-9]|ghp_[A-Za-z0-9]|xox[baprs]-|AIza[0-9A-Za-z\-_]{20,})/i;

export const MAX_TRANSCRIPT_RETENTION = 800;
export const MAX_RENDERED_TRANSCRIPT = 120;
export const DEFAULT_APPROVAL_SCOPE = "once" as const;
export const DEFAULT_APPROVAL_TTL_MS = "300000";

export type ApprovalScope = "once" | "session" | "timeboxed";
export type TranscriptEntryKind =
  | "meta"
  | "user"
  | "assistant"
  | "status"
  | "tool"
  | "approval_request"
  | "approval_response"
  | "a2ui"
  | "canvas"
  | "journal"
  | "error"
  | "complete"
  | "event";

export interface TranscriptEntry {
  readonly id: string;
  readonly kind: TranscriptEntryKind;
  readonly created_at_unix_ms: number;
  readonly run_id?: string;
  readonly session_id?: string;
  readonly title: string;
  readonly text?: string;
  readonly payload?: JsonValue;
  readonly approval_id?: string;
  readonly proposal_id?: string;
  readonly tool_name?: string;
  readonly surface?: string;
  readonly canvas_url?: string;
  readonly status?: string;
  readonly is_final?: boolean;
}

export interface ApprovalDraft {
  readonly scope: ApprovalScope;
  readonly reason: string;
  readonly ttl_ms: string;
  readonly busy: boolean;
}

type ApprovalRequestControlsProps = {
  approvalId: string;
  draft?: ApprovalDraft;
  onDraftChange: (next: ApprovalDraft) => void;
  onDecision: (approved: boolean) => void;
};

export function ApprovalRequestControls({
  approvalId,
  draft,
  onDraftChange,
  onDecision
}: ApprovalRequestControlsProps) {
  const effectiveDraft = draft ?? {
    scope: DEFAULT_APPROVAL_SCOPE,
    reason: "",
    ttl_ms: DEFAULT_APPROVAL_TTL_MS,
    busy: false
  };

  return (
    <section className="console-subpanel chat-approval-box">
      <h4>Approval required</h4>
      <p className="chat-muted">Approval ID: {approvalId}</p>
      <div className="console-grid-3">
        <label>
          Scope
          <select
            value={effectiveDraft.scope}
            onChange={(event) => {
              onDraftChange({
                ...effectiveDraft,
                scope: normalizeScope(event.target.value)
              });
            }}
            disabled={effectiveDraft.busy}
          >
            <option value="once">once</option>
            <option value="session">session</option>
            <option value="timeboxed">timeboxed</option>
          </select>
        </label>
        <label>
          TTL (ms)
          <input
            value={effectiveDraft.ttl_ms}
            onChange={(event) => {
              onDraftChange({
                ...effectiveDraft,
                ttl_ms: event.target.value
              });
            }}
            disabled={effectiveDraft.busy || effectiveDraft.scope !== "timeboxed"}
          />
        </label>
        <label>
          Reason
          <input
            value={effectiveDraft.reason}
            onChange={(event) => {
              onDraftChange({
                ...effectiveDraft,
                reason: event.target.value
              });
            }}
            disabled={effectiveDraft.busy}
          />
        </label>
        <div className="console-inline-actions">
          <button type="button" onClick={() => onDecision(true)} disabled={effectiveDraft.busy}>
            Approve
          </button>
          <button type="button" className="button--warn" onClick={() => onDecision(false)} disabled={effectiveDraft.busy}>
            Deny
          </button>
        </div>
      </div>
    </section>
  );
}

export function retainTranscriptWindow(values: TranscriptEntry[]): TranscriptEntry[] {
  if (values.length <= MAX_TRANSCRIPT_RETENTION) {
    return values;
  }
  return values.slice(values.length - MAX_TRANSCRIPT_RETENTION);
}

export function collectCanvasFrameUrls(value: JsonValue): string[] {
  if (typeof value === "string") {
    const normalized = normalizeCanvasFrameUrl(value);
    return normalized === null ? [] : [normalized];
  }
  if (Array.isArray(value)) {
    const rows: string[] = [];
    for (const entry of value) {
      rows.push(...collectCanvasFrameUrls(entry));
    }
    return rows;
  }
  if (isJsonObject(value)) {
    const rows: string[] = [];
    for (const entry of Object.values(value)) {
      rows.push(...collectCanvasFrameUrls(entry));
    }
    return rows;
  }
  return [];
}

export function normalizeCanvasFrameUrl(raw: string): string | null {
  if (!raw.startsWith("/canvas/v1/frame/")) {
    return null;
  }
  return raw;
}

export function parseTapePayload(payload: string): JsonValue {
  try {
    const parsed: unknown = JSON.parse(payload);
    return normalizePatchValue(parsed) ?? payload;
  } catch {
    return payload;
  }
}

export function normalizePatchValue(value: unknown): JsonValue | null {
  if (isJsonValue(value)) {
    return value;
  }
  if (Array.isArray(value)) {
    const rows = value.map((entry) => normalizePatchValue(entry));
    return rows.every((entry) => entry !== null) ? (rows as JsonValue[]) : null;
  }
  if (typeof value === "object" && value !== null) {
    const rows: Record<string, JsonValue> = {};
    for (const [key, entry] of Object.entries(value)) {
      const normalized = normalizePatchValue(entry);
      if (normalized === null) {
        return null;
      }
      rows[key] = normalized;
    }
    return rows;
  }
  return null;
}

export function shortId(value: string): string {
  if (value.length <= 12) {
    return value;
  }
  return `${value.slice(0, 6)}…${value.slice(-4)}`;
}

export function prettifyEventType(value: string): string {
  return value
    .split("_")
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");
}

export function normalizeScope(value: string): ApprovalScope {
  return value === "session" || value === "timeboxed" ? value : "once";
}

export function parseInteger(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

export function emptyToUndefined(raw: string): string | undefined {
  const trimmed = raw.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected failure.";
}

export function asObject(value: unknown): Record<string, JsonValue> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, JsonValue>)
    : null;
}

export function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

export function asBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

export function isJsonObject(value: JsonValue): value is { [key: string]: JsonValue } {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isJsonValue(value: unknown): value is JsonValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every((entry) => isJsonValue(entry));
  }
  if (typeof value === "object") {
    return Object.values(value as Record<string, unknown>).every((entry) => isJsonValue(entry));
  }
  return false;
}

function redactValue(value: JsonValue, revealSensitive: boolean): JsonValue {
  if (revealSensitive) {
    return value;
  }
  if (typeof value === "string") {
    return SENSITIVE_VALUE_PATTERN.test(value) ? "[redacted]" : value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactValue(entry, false));
  }
  if (isJsonObject(value)) {
    const sanitized: { [key: string]: JsonValue } = {};
    for (const [key, item] of Object.entries(value)) {
      sanitized[key] = SENSITIVE_KEY_PATTERN.test(key) ? "[redacted]" : redactValue(item, false);
    }
    return sanitized;
  }
  return value;
}

export function toPrettyJson(value: JsonValue, revealSensitive: boolean): string {
  return JSON.stringify(redactValue(value, revealSensitive), null, 2);
}
