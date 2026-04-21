import type { JsonValue, SessionCatalogRecord } from "../consoleApi";

type JsonObject = { readonly [key: string]: JsonValue };

export type DeliveryPresentationTone = "default" | "accent" | "success" | "warning" | "danger";

export interface DeliveryPresentationSummary {
  readonly title: string;
  readonly text: string;
  readonly status: string;
  readonly tone: DeliveryPresentationTone;
  readonly presentation?: string;
  readonly terminalState?: string;
  readonly hiddenCount?: number;
}

export function shortId(value: string): string {
  if (value.length <= 12) {
    return value;
  }
  return `${value.slice(0, 6)}…${value.slice(-4)}`;
}

export function describeBranchState(branchState: string | null | undefined): string {
  const normalized = branchState?.trim().toLowerCase() ?? "";
  if (normalized.length === 0) {
    return "Unknown lineage";
  }
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
  return branchState ?? "Unknown lineage";
}

export function describeTitleGenerationState(
  titleGenerationState: string | null | undefined,
  manualTitleLocked: boolean,
): string {
  if (manualTitleLocked) {
    return "Manual title";
  }
  const normalized = titleGenerationState?.trim().toLowerCase() ?? "";
  if (normalized.length === 0) {
    return "Auto title unavailable";
  }
  if (normalized === "ready") {
    return "Auto title ready";
  }
  if (normalized === "pending") {
    return "Auto title pending";
  }
  if (normalized === "failed") {
    return "Auto title failed";
  }
  if (normalized === "idle") {
    return "Auto title idle";
  }
  return titleGenerationState ?? "Auto title unavailable";
}

export function buildSessionLineageHint(session: SessionCatalogRecord | null): string {
  if (session === null) {
    return "Select a session to inspect lineage.";
  }
  const normalized = session.branch_state?.trim().toLowerCase() ?? "";
  if (normalized.length === 0) {
    return "Lineage metadata is unavailable for this session.";
  }
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

export function summarizeDeliveryPayload(payload: JsonValue): DeliveryPresentationSummary | null {
  const root = asJsonObject(payload);
  if (root === null) {
    return null;
  }
  const progress =
    readObjectPath(root, ["details", "delivery", "progress"]) ??
    readObjectPath(root, ["delivery", "progress"]) ??
    readObjectPath(root, ["payload_json", "details", "delivery", "progress"]);
  const arbitration =
    readObjectPath(root, ["delivery_arbitration"]) ??
    readObjectPath(root, ["payload", "decision"]) ??
    readObjectPath(root, ["payload_json", "delivery_arbitration"]) ??
    readObjectPath(root, ["payload_json", "payload", "decision"]) ??
    (readString(root, "decision") !== null && readObject(root, "policy") !== null ? root : null);

  if (progress !== null) {
    const terminalState = readString(progress, "terminal_state") ?? undefined;
    const title = readString(progress, "title") ?? "Delivery progress";
    return {
      title,
      text: readString(progress, "text") ?? "Merged delivery progress update.",
      status: terminalState ?? readString(progress, "presentation") ?? "progress",
      tone: deliveryToneForState(terminalState, readNumber(progress, "approval_wait_count") ?? 0),
      presentation: readString(progress, "presentation") ?? undefined,
      terminalState,
      hiddenCount: readNumber(progress, "hidden_count") ?? undefined,
    };
  }

  if (arbitration !== null) {
    const decision = readString(arbitration, "decision") ?? "delivery_arbitration";
    const reason = readString(arbitration, "reason") ?? "Delivery arbitration recorded.";
    const preferred = readBoolean(
      readObjectPath(arbitration, ["descendant_output"]) ?? {},
      "preferred",
    );
    const suppressed = readBoolean(
      readObjectPath(arbitration, ["parent_output"]) ?? {},
      "suppressed",
    );
    return {
      title: titleForDeliveryDecision(decision),
      text: reason.replaceAll("_", " "),
      status: suppressed === true ? "suppressed" : decision,
      tone: preferred === true ? "success" : decision.includes("review") ? "warning" : "accent",
    };
  }

  return null;
}

function titleForDeliveryDecision(decision: string): string {
  switch (decision) {
    case "prefer_terminal_descendant":
      return "Descendant output preferred";
    case "annotate_superseded_parent":
      return "Parent output superseded";
    case "hold_for_review":
      return "Delivery waiting for review";
    case "deliver_interim_parent":
      return "Interim parent output delivered";
    default:
      return "Delivery arbitration";
  }
}

function deliveryToneForState(
  terminalState: string | undefined,
  approvalWaitCount: number,
): DeliveryPresentationTone {
  if (terminalState === "failed" || terminalState === "transport_error") {
    return "danger";
  }
  if (terminalState === "cancelled" || terminalState === "canceled" || approvalWaitCount > 0) {
    return "warning";
  }
  if (terminalState !== undefined) {
    return "success";
  }
  return "accent";
}

function readObjectPath(root: JsonObject, path: readonly string[]): JsonObject | null {
  let current: JsonValue = root;
  for (const segment of path) {
    const currentObject = asJsonObject(current);
    if (currentObject === null) {
      return null;
    }
    current = currentObject[segment];
  }
  return asJsonObject(current);
}

function readObject(root: JsonObject, key: string): JsonObject | null {
  return asJsonObject(root[key]);
}

function readString(root: JsonObject, key: string): string | null {
  const value = root[key];
  return typeof value === "string" && value.trim().length > 0 ? value : null;
}

function readNumber(root: JsonObject, key: string): number | null {
  const value = root[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function readBoolean(root: JsonObject, key: string): boolean | null {
  const value = root[key];
  return typeof value === "boolean" ? value : null;
}

function asJsonObject(value: JsonValue | undefined): JsonObject | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonObject)
    : null;
}
