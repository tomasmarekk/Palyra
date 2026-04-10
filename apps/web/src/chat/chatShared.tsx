import { memo, useMemo } from "react";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  SectionCard,
  SelectField,
  TextInputField,
} from "../console/components/ui";
import type { JsonValue, MediaDerivedArtifactRecord, SessionCatalogRecord } from "../consoleApi";
import {
  findChatSlashCommand,
} from "./chatCommandRegistry";
export {
  CHAT_SLASH_COMMANDS,
  type SlashCommandDefinition,
  type SlashCommandExecution,
  type SlashCommandSurface,
} from "./chatCommandRegistry";

const SENSITIVE_KEY_PATTERN =
  /(secret|token|password|cookie|authorization|credential|api[-_]?key|private[-_]?key|vault[-_]?ref)/i;
const SENSITIVE_VALUE_PATTERN =
  /^(Bearer\s+|sk-[a-z0-9]|ghp_[A-Za-z0-9]|xox[baprs]-|AIza[0-9A-Za-z\-_]{20,})/i;

export const MAX_TRANSCRIPT_RETENTION = 800;
export const MAX_RENDERED_TRANSCRIPT = 120;
export const DEFAULT_APPROVAL_SCOPE = "once" as const;
export const DEFAULT_APPROVAL_TTL_MS = "300000";
export const CONTEXT_BUDGET_SOFT_LIMIT = 12_000;
export const CONTEXT_BUDGET_HARD_LIMIT = 16_000;
const CANVAS_FRAME_PATH_SEGMENTS = ["canvas", "v1", "frame"] as const;
const MAX_CANVAS_SCAN_DEPTH = 12;
const MAX_CANVAS_SCAN_VISITS = 128;
const MAX_CANVAS_SCAN_RESULTS = 8;

export interface TranscriptAttachmentSummary {
  readonly id: string;
  readonly filename: string;
  readonly kind: string;
  readonly size_bytes: number;
  readonly budget_tokens?: number;
  readonly preview_url?: string;
}

export interface ComposerAttachment {
  readonly local_id: string;
  readonly artifact_id: string;
  readonly attachment_id: string;
  readonly filename: string;
  readonly declared_content_type: string;
  readonly content_hash: string;
  readonly size_bytes: number;
  readonly width_px?: number;
  readonly height_px?: number;
  readonly kind: "image" | "file" | "audio" | "video";
  readonly budget_tokens: number;
  readonly preview_url?: string;
  readonly derived_artifacts?: readonly MediaDerivedArtifactRecord[];
}

export interface ContextBudgetSummary {
  readonly baseline_tokens: number;
  readonly draft_tokens: number;
  readonly reference_tokens: number;
  readonly attachment_tokens: number;
  readonly estimated_total_tokens: number;
  readonly limit_tokens: number;
  readonly ratio: number;
  readonly tone: "default" | "warning" | "danger";
  readonly label: string;
  readonly warning?: string;
}

export type CompactCommandMode = "preview" | "apply";

export interface ParsedSlashCommand {
  readonly name: string;
  readonly args: string;
}

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
  readonly attachments?: TranscriptAttachmentSummary[];
}

export interface ApprovalDraft {
  readonly scope: ApprovalScope;
  readonly reason: string;
  readonly ttl_ms: string;
  readonly busy: boolean;
}

type AssistantTokenBatchEntry = readonly [
  runId: string,
  update: { token: string; isFinal: boolean },
];

type PrettyJsonBlockProps = {
  value: JsonValue;
  revealSensitiveValues: boolean;
  className?: string;
};

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
  onDecision,
}: ApprovalRequestControlsProps) {
  const effectiveDraft = draft ?? {
    scope: DEFAULT_APPROVAL_SCOPE,
    reason: "",
    ttl_ms: DEFAULT_APPROVAL_TTL_MS,
    busy: false,
  };

  return (
    <SectionCard
      className="chat-approval-box"
      description={`Approval ID: ${approvalId}`}
      title="Approval required"
    >
      <AppForm className="console-grid-3">
        <SelectField
          disabled={effectiveDraft.busy}
          label="Scope"
          options={[
            { key: "once", label: "once" },
            { key: "session", label: "session" },
            { key: "timeboxed", label: "timeboxed" },
          ]}
          value={effectiveDraft.scope}
          onChange={(value) =>
            onDraftChange({
              ...effectiveDraft,
              scope: normalizeScope(value),
            })
          }
        />
        <TextInputField
          description="Used only when scope is timeboxed."
          disabled={effectiveDraft.busy || effectiveDraft.scope !== "timeboxed"}
          label="TTL (ms)"
          value={effectiveDraft.ttl_ms}
          onChange={(value) =>
            onDraftChange({
              ...effectiveDraft,
              ttl_ms: value,
            })
          }
        />
        <TextInputField
          disabled={effectiveDraft.busy}
          label="Reason"
          value={effectiveDraft.reason}
          onChange={(value) =>
            onDraftChange({
              ...effectiveDraft,
              reason: value,
            })
          }
        />
        <ActionCluster>
          <ActionButton
            isDisabled={effectiveDraft.busy}
            type="button"
            variant="primary"
            onPress={() => onDecision(true)}
          >
            Approve
          </ActionButton>
          <ActionButton
            isDisabled={effectiveDraft.busy}
            type="button"
            variant="danger"
            onPress={() => onDecision(false)}
          >
            Deny
          </ActionButton>
        </ActionCluster>
      </AppForm>
    </SectionCard>
  );
}

export function retainTranscriptWindow(values: TranscriptEntry[]): TranscriptEntry[] {
  if (values.length <= MAX_TRANSCRIPT_RETENTION) {
    return values;
  }
  return values.slice(values.length - MAX_TRANSCRIPT_RETENTION);
}

export function applyAssistantTokenBatch(
  previous: TranscriptEntry[],
  assistantEntryByRun: Map<string, string>,
  queuedTokens: readonly AssistantTokenBatchEntry[],
  createdAtUnixMs: number,
): TranscriptEntry[] {
  if (queuedTokens.length === 0) {
    return previous;
  }

  let next = previous;
  for (const [runId, update] of queuedTokens) {
    const mappedEntryId = assistantEntryByRun.get(runId);
    if (mappedEntryId !== undefined) {
      const index = next.findIndex((entry) => entry.id === mappedEntryId);
      if (index >= 0) {
        const existing = next[index];
        const nextEntry: TranscriptEntry = {
          ...existing,
          text: `${existing.text ?? ""}${update.token}`,
          is_final: Boolean(existing.is_final) || update.isFinal,
        };
        const updated = [...next];
        updated[index] = nextEntry;
        next = updated;
        continue;
      }
    }

    const entryId = `assistant-${runId}-${createdAtUnixMs}`;
    assistantEntryByRun.set(runId, entryId);
    next = [
      ...next,
      {
        id: entryId,
        kind: "assistant",
        created_at_unix_ms: createdAtUnixMs,
        run_id: runId,
        title: "Assistant",
        text: update.token,
        is_final: update.isFinal,
      },
    ];
  }

  return retainTranscriptWindow(next);
}

export function collectCanvasFrameUrls(value: JsonValue): string[] {
  const rows: string[] = [];
  const seen = new Set<string>();
  const pending: Array<{ value: JsonValue; depth: number }> = [{ value, depth: 0 }];
  let visited = 0;

  while (
    pending.length > 0 &&
    visited < MAX_CANVAS_SCAN_VISITS &&
    rows.length < MAX_CANVAS_SCAN_RESULTS
  ) {
    const current = pending.pop();
    if (current === undefined) {
      break;
    }
    visited += 1;

    if (typeof current.value === "string") {
      const normalized = normalizeCanvasFrameUrl(current.value);
      if (normalized !== null && !seen.has(normalized)) {
        seen.add(normalized);
        rows.push(normalized);
      }
      continue;
    }
    if (current.depth >= MAX_CANVAS_SCAN_DEPTH) {
      continue;
    }
    if (Array.isArray(current.value)) {
      enqueueCanvasScanEntries(current.value, current.depth + 1, pending, visited);
      continue;
    }
    if (isJsonObject(current.value)) {
      enqueueCanvasScanEntries(Object.values(current.value), current.depth + 1, pending, visited);
    }
  }

  return rows;
}

function enqueueCanvasScanEntries(
  values: readonly JsonValue[],
  depth: number,
  pending: Array<{ value: JsonValue; depth: number }>,
  visited: number,
): void {
  const remainingBudget = MAX_CANVAS_SCAN_VISITS - visited - pending.length;
  if (remainingBudget <= 0) {
    return;
  }
  const childLimit = Math.min(values.length, remainingBudget);
  for (let index = childLimit - 1; index >= 0; index -= 1) {
    pending.push({ value: values[index], depth });
  }
}

export function normalizeCanvasFrameUrl(raw: string): string | null {
  if (typeof window === "undefined" || window.location.origin === "null") {
    return null;
  }

  try {
    const parsed = new URL(raw, window.location.origin);
    if (parsed.origin !== window.location.origin || parsed.hash.length > 0) {
      return null;
    }

    const segments = parsed.pathname.split("/").filter((segment) => segment.length > 0);
    if (
      segments.length !== CANVAS_FRAME_PATH_SEGMENTS.length + 1 ||
      !CANVAS_FRAME_PATH_SEGMENTS.every((segment, index) => segments[index] === segment)
    ) {
      return null;
    }

    const frameId = segments[CANVAS_FRAME_PATH_SEGMENTS.length];
    const tokens = parsed.searchParams.getAll("token").map((value) => value.trim());
    if (frameId.length === 0 || tokens.length !== 1 || tokens[0].length === 0) {
      return null;
    }
    if (Array.from(parsed.searchParams.keys()).some((key) => key !== "token")) {
      return null;
    }

    return `${parsed.pathname}${parsed.search}`;
  } catch {
    return null;
  }
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

export function describeBranchState(branchState: string): string {
  const normalized = branchState.trim().toLowerCase();
  if (normalized === "root") {
    return "Root session";
  }
  if (normalized === "branched" || normalized === "active_branch") {
    return "Active branch";
  }
  if (normalized === "branch_source") {
    return "Branch source";
  }
  if (normalized === "missing") {
    return "No lineage";
  }
  return branchState;
}

export function buildSessionLineageHint(session: SessionCatalogRecord | null): string {
  if (session === null) {
    return "Select a session to inspect lineage.";
  }
  const normalized = session.branch_state.trim().toLowerCase();
  const parent = session.parent_session_id?.trim();
  const originRunId = session.branch_origin_run_id?.trim();
  if (normalized === "root") {
    return originRunId ? `Root session anchored at run ${shortId(originRunId)}.` : "Root session.";
  }
  if (normalized === "branched" || normalized === "active_branch") {
    if (parent !== undefined && parent.length > 0) {
      return originRunId !== undefined && originRunId.length > 0
        ? `Active branch from ${shortId(parent)} at run ${shortId(originRunId)}.`
        : `Active branch from ${shortId(parent)}.`;
    }
    return originRunId !== undefined && originRunId.length > 0
      ? `Active branch anchored at run ${shortId(originRunId)}.`
      : "Active branch.";
  }
  if (normalized === "branch_source") {
    if (parent !== undefined && parent.length > 0) {
      return originRunId !== undefined && originRunId.length > 0
        ? `Branch source with upstream ${shortId(parent)} at run ${shortId(originRunId)}.`
        : `Branch source with upstream ${shortId(parent)}.`;
    }
    return originRunId !== undefined && originRunId.length > 0
      ? `Branch source anchored at run ${shortId(originRunId)}.`
      : "Branch source.";
  }
  const branchLabel = describeBranchState(session.branch_state);
  return parent !== undefined && parent.length > 0
    ? `${branchLabel} from ${shortId(parent)}.`
    : `${branchLabel}.`;
}

export function parseCompactCommandMode(raw: string): CompactCommandMode {
  const firstToken = raw.trim().split(/\s+/, 1)[0]?.toLowerCase() ?? "";
  return firstToken === "apply" ? "apply" : "preview";
}

export function estimateTextTokens(text: string): number {
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return 0;
  }
  return Math.max(1, Math.ceil(trimmed.length / 4));
}

export function formatApproxTokens(value: number): string {
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(value >= 10_000 ? 0 : 1)}k`;
  }
  return value.toLocaleString();
}

export function buildContextBudgetSummary(args: {
  baseline_tokens: number;
  draft_text: string;
  attachments: readonly ComposerAttachment[];
  reference_tokens?: number;
}): ContextBudgetSummary {
  const draft_tokens = estimateTextTokens(args.draft_text);
  const reference_tokens = Math.max(0, args.reference_tokens ?? 0);
  const attachment_tokens = args.attachments.reduce(
    (sum, attachment) => sum + attachment.budget_tokens,
    0,
  );
  const estimated_total_tokens =
    args.baseline_tokens + draft_tokens + reference_tokens + attachment_tokens;
  const ratio = estimated_total_tokens / CONTEXT_BUDGET_HARD_LIMIT;
  const tone =
    estimated_total_tokens >= CONTEXT_BUDGET_HARD_LIMIT
      ? "danger"
      : estimated_total_tokens >= CONTEXT_BUDGET_SOFT_LIMIT
        ? "warning"
        : "default";
  const warning =
    tone === "danger"
      ? "Estimated context is above the safe working budget. Consider branching or exporting before another large turn."
      : tone === "warning"
        ? "Estimated context is approaching the budget. Compact prompts or branch soon."
        : undefined;

  return {
    baseline_tokens: args.baseline_tokens,
    draft_tokens,
    reference_tokens,
    attachment_tokens,
    estimated_total_tokens,
    limit_tokens: CONTEXT_BUDGET_HARD_LIMIT,
    ratio,
    tone,
    label: `${formatApproxTokens(estimated_total_tokens)} / ${formatApproxTokens(CONTEXT_BUDGET_HARD_LIMIT)} est.`,
    warning,
  };
}

export function parseSlashCommand(raw: string): ParsedSlashCommand | null {
  const trimmed = raw.trim();
  if (!trimmed.startsWith("/")) {
    return null;
  }
  const withoutPrefix = trimmed.slice(1).trim();
  if (withoutPrefix.length === 0) {
    return {
      name: "help",
      args: "",
    };
  }

  const firstSpace = withoutPrefix.indexOf(" ");
  const name = (
    firstSpace === -1 ? withoutPrefix : withoutPrefix.slice(0, firstSpace)
  ).toLowerCase();
  const args = firstSpace === -1 ? "" : withoutPrefix.slice(firstSpace + 1).trim();

  if (findChatSlashCommand(name, "web") !== null) {
    return {
      name,
      args,
    };
  }

  return null;
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

export const PrettyJsonBlock = memo(function PrettyJsonBlock({
  value,
  revealSensitiveValues,
  className,
}: PrettyJsonBlockProps) {
  const formatted = useMemo(
    () => toPrettyJson(value, revealSensitiveValues),
    [value, revealSensitiveValues],
  );
  return <pre className={className}>{formatted}</pre>;
});
