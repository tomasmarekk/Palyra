import { findSectionByPath, getSectionPath } from "./navigation";
import type { Section } from "./sectionMetadata";
import type { JsonValue } from "../consoleApi";

export type ConsoleUiMode = "basic" | "advanced";
export type ConsoleLocale = "en" | "qps-ploc";
export type TelemetrySurface = "web" | "desktop" | "tui" | "mobile";

export type HandoffIntent =
  | "approve"
  | "open-workspace"
  | "reopen-canvas"
  | "resume-session"
  | "inspect-run";

export type HandoffSection = Section | "home";

export type CrossSurfaceHandoff = {
  section?: HandoffSection;
  sessionId?: string;
  runId?: string;
  deviceId?: string;
  objectiveId?: string;
  canvasId?: string;
  intent?: HandoffIntent;
  source?: TelemetrySurface;
};

export type UxTelemetryEvent = {
  name:
    | "ux.surface.opened"
    | "ux.mode.changed"
    | "ux.handoff.opened"
    | "ux.onboarding.step"
    | "ux.chat.prompt_submitted"
    | "ux.approval.resolved"
    | "ux.run.inspected"
    | "ux.session.resumed"
    | "ux.voice.entry"
    | "ux.canvas.entry"
    | "ux.rollback.previewed";
  surface: TelemetrySurface;
  section?: string;
  mode?: ConsoleUiMode;
  locale?: ConsoleLocale;
  outcome?: "ok" | "blocked" | "error" | "cancelled";
  step?: string;
  toolName?: string;
  sessionId?: string;
  runId?: string;
  deviceId?: string;
  objectiveId?: string;
  canvasId?: string;
  intent?: HandoffIntent;
  latencyMs?: number;
  summary?: string;
};

export type SystemEventRecord = {
  operator_event?: string | null;
  timestamp_unix_ms?: number | null;
  payload_json?: JsonValue;
  session_id?: string | null;
  run_id?: string | null;
  device_id?: string | null;
};

export type UxTelemetryAggregate = {
  totalEvents: number;
  countsBySurface: Record<TelemetrySurface, number>;
  countsByName: Record<string, number>;
  approvalFatigueByTool: Record<string, number>;
  approvalFatigueBySession: Record<string, number>;
  frictionBySurface: Record<TelemetrySurface, number>;
  funnel: Record<
    | "setup_started"
    | "provider_verified"
    | "first_prompt_sent"
    | "first_approval_resolved"
    | "first_run_inspected"
    | "second_session_resumed",
    number
  >;
};

const HANDOFF_PARAM_ORDER: readonly (keyof CrossSurfaceHandoff)[] = [
  "sessionId",
  "runId",
  "deviceId",
  "objectiveId",
  "canvasId",
  "intent",
  "source",
] as const;

const UX_EVENT_PREFIX = "system.operator.ux.";

export function normalizeHandoffSection(section?: string | null): HandoffSection {
  const normalized = section?.trim();
  switch (normalized) {
    case "chat":
    case "overview":
    case "sessions":
    case "usage":
    case "logs":
    case "inventory":
    case "approvals":
    case "cron":
    case "channels":
    case "browser":
    case "agents":
    case "memory":
    case "skills":
    case "auth":
    case "config":
    case "secrets":
    case "access":
    case "operations":
    case "support":
    case "home":
      return normalized;
    default:
      return "overview";
  }
}

export function buildConsoleHandoffHref(payload: CrossSurfaceHandoff): string {
  const section = normalizeHandoffSection(payload.section);
  const params = new URLSearchParams();
  for (const key of HANDOFF_PARAM_ORDER) {
    const value = payload[key];
    if (typeof value === "string" && value.trim().length > 0) {
      params.set(key, value.trim());
    }
  }
  const basePath =
    section === "home" ? getSectionPath("overview") : getSectionPath(section as Section);
  const query = params.toString();
  return query.length > 0 ? `${basePath}?${query}` : basePath;
}

export function parseConsoleHandoff(raw: URLSearchParams | string): CrossSurfaceHandoff {
  const parsed = typeof raw === "string" ? new URL(raw, "https://palyra.local") : null;
  const params = typeof raw === "string" ? new URL(raw, "https://palyra.local").searchParams : raw;
  const pathSection = parsed === null ? null : findSectionByPath(parsed.pathname);
  const handoff: CrossSurfaceHandoff = {
    section: normalizeHandoffSection(params.get("section") ?? pathSection ?? undefined),
  };
  for (const key of HANDOFF_PARAM_ORDER) {
    const value = params.get(key);
    if (value !== null && value.trim().length > 0) {
      handoff[key] = value.trim() as never;
    }
  }
  return handoff;
}

export function nearestSupportedHandoffSection(payload: CrossSurfaceHandoff): Section {
  if (payload.section === "home") {
    return "overview";
  }
  if (payload.section === "chat") {
    return "chat";
  }
  if (payload.section === "approvals") {
    return "approvals";
  }
  if (payload.section === "access") {
    return "access";
  }
  if (payload.section === "browser" && payload.canvasId !== undefined) {
    return "browser";
  }
  return (payload.section as Section | undefined) ?? "overview";
}

export function isUxSystemEvent(record: SystemEventRecord): boolean {
  return (
    typeof record.operator_event === "string" && record.operator_event.startsWith(UX_EVENT_PREFIX)
  );
}

export function toSystemEventPayload(event: UxTelemetryEvent): {
  name: string;
  summary?: string;
  details: JsonValue;
} {
  const details = compactJsonObject({
    surface: event.surface,
    section: event.section,
    mode: event.mode,
    locale: event.locale,
    outcome: event.outcome,
    step: event.step,
    toolName: event.toolName,
    sessionId: event.sessionId,
    runId: event.runId,
    deviceId: event.deviceId,
    objectiveId: event.objectiveId,
    canvasId: event.canvasId,
    intent: event.intent,
    latencyMs: event.latencyMs,
  });
  return {
    name: event.name,
    summary: event.summary,
    details,
  };
}

export function aggregateUxTelemetry(records: SystemEventRecord[]): UxTelemetryAggregate {
  const aggregate: UxTelemetryAggregate = {
    totalEvents: 0,
    countsBySurface: { web: 0, desktop: 0, tui: 0, mobile: 0 },
    countsByName: {},
    approvalFatigueByTool: {},
    approvalFatigueBySession: {},
    frictionBySurface: { web: 0, desktop: 0, tui: 0, mobile: 0 },
    funnel: {
      setup_started: 0,
      provider_verified: 0,
      first_prompt_sent: 0,
      first_approval_resolved: 0,
      first_run_inspected: 0,
      second_session_resumed: 0,
    },
  };

  for (const record of records) {
    if (!isUxSystemEvent(record)) {
      continue;
    }
    const operatorEvent = record.operator_event as string;
    const name = operatorEvent.replace(UX_EVENT_PREFIX, "ux.");
    const details = readRecordDetails(record.payload_json);
    const surface = readSurface(details.surface);
    aggregate.totalEvents += 1;
    aggregate.countsBySurface[surface] += 1;
    aggregate.countsByName[name] = (aggregate.countsByName[name] ?? 0) + 1;
    if (details.outcome === "blocked" || details.outcome === "error") {
      aggregate.frictionBySurface[surface] += 1;
    }
    if (name === "ux.approval.resolved") {
      const toolName = readString(details.toolName) ?? "unknown";
      aggregate.approvalFatigueByTool[toolName] =
        (aggregate.approvalFatigueByTool[toolName] ?? 0) + 1;
      const sessionId =
        readString(details.sessionId) ?? readString(record.session_id) ?? "unknown-session";
      aggregate.approvalFatigueBySession[sessionId] =
        (aggregate.approvalFatigueBySession[sessionId] ?? 0) + 1;
    }
    if (name === "ux.onboarding.step") {
      const step = readString(details.step);
      if (step === "setup_started") {
        aggregate.funnel.setup_started += 1;
      }
      if (step === "provider_verified") {
        aggregate.funnel.provider_verified += 1;
      }
    }
    if (name === "ux.chat.prompt_submitted") {
      aggregate.funnel.first_prompt_sent += 1;
    }
    if (name === "ux.approval.resolved") {
      aggregate.funnel.first_approval_resolved += 1;
    }
    if (name === "ux.run.inspected") {
      aggregate.funnel.first_run_inspected += 1;
    }
    if (name === "ux.session.resumed") {
      aggregate.funnel.second_session_resumed += 1;
    }
  }

  return aggregate;
}

function readRecordDetails(payload: JsonValue | undefined): Record<string, JsonValue> {
  if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
    const details = payload.details;
    if (details !== null && typeof details === "object" && !Array.isArray(details)) {
      return details as Record<string, JsonValue>;
    }
  }
  return {};
}

function readSurface(value: JsonValue): TelemetrySurface {
  switch (value) {
    case "desktop":
    case "tui":
    case "mobile":
      return value;
    default:
      return "web";
  }
}

function readString(value: JsonValue | undefined | null): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function compactJsonObject(
  value: Record<string, JsonValue | undefined>,
): Record<string, JsonValue> {
  return Object.fromEntries(
    Object.entries(value).filter(([, entryValue]) => entryValue !== undefined),
  ) as Record<string, JsonValue>;
}
