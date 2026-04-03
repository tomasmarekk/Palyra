import {
  emptyToUndefined,
  isJsonObject,
  parseInteger,
  readBool,
  readNumber,
  readObject,
  readString,
  type JsonObject,
} from "../shared";

export type RoutineEditorForm = {
  routineId: string;
  name: string;
  prompt: string;
  triggerKind: "schedule" | "manual" | "hook" | "webhook" | "system_event";
  naturalLanguageSchedule: string;
  scheduleType: "cron" | "every" | "at";
  cronExpression: string;
  everyIntervalMs: string;
  atTimestampRfc3339: string;
  enabled: boolean;
  channel: string;
  sessionKey: string;
  sessionLabel: string;
  concurrencyPolicy: "forbid" | "replace" | "queue_one";
  retryMaxAttempts: string;
  retryBackoffMs: string;
  misfirePolicy: "skip" | "catch_up";
  jitterMs: string;
  deliveryMode: "same_channel" | "specific_channel" | "local_only" | "logs_only";
  deliveryChannel: string;
  quietHoursStart: string;
  quietHoursEnd: string;
  quietHoursTimezone: "local" | "utc";
  cooldownMs: string;
  approvalMode: "none" | "before_enable" | "before_first_run";
  templateId: string;
  hookId: string;
  webhookIntegrationId: string;
  webhookProvider: string;
  eventName: string;
  triggerPayloadText: string;
};

export const EMPTY_JSON_TEXT = "{\n  \n}";

export const SCHEDULE_OPTIONS = [
  { key: "every", label: "Every interval" },
  { key: "cron", label: "Cron expression" },
  { key: "at", label: "One-off at" },
] as const;

export const TRIGGER_OPTIONS = [
  { key: "schedule", label: "Schedule" },
  { key: "manual", label: "Manual" },
  { key: "hook", label: "Hook" },
  { key: "webhook", label: "Webhook" },
  { key: "system_event", label: "System event" },
] as const;

export const CONCURRENCY_OPTIONS = [
  { key: "forbid", label: "Forbid overlap" },
  { key: "replace", label: "Replace existing" },
  { key: "queue_one", label: "Queue one" },
] as const;

export const MISFIRE_OPTIONS = [
  { key: "skip", label: "Skip missed trigger" },
  { key: "catch_up", label: "Catch up once" },
] as const;

export const DELIVERY_OPTIONS = [
  { key: "same_channel", label: "Same channel" },
  { key: "specific_channel", label: "Specific channel" },
  { key: "local_only", label: "Local only" },
  { key: "logs_only", label: "Logs only" },
] as const;

export const APPROVAL_OPTIONS = [
  { key: "none", label: "No extra approval" },
  { key: "before_enable", label: "Before enable" },
  { key: "before_first_run", label: "Before first run" },
] as const;

export const TIMEZONE_OPTIONS = [
  { key: "local", label: "Local timezone" },
  { key: "utc", label: "UTC" },
] as const;

export function defaultRoutineForm(channel: string | null | undefined): RoutineEditorForm {
  return {
    routineId: "",
    name: "",
    prompt: "",
    triggerKind: "schedule",
    naturalLanguageSchedule: "every weekday at 9",
    scheduleType: "every",
    cronExpression: "",
    everyIntervalMs: "3600000",
    atTimestampRfc3339: "",
    enabled: true,
    channel: channel ?? "",
    sessionKey: "",
    sessionLabel: "",
    concurrencyPolicy: "forbid",
    retryMaxAttempts: "1",
    retryBackoffMs: "1000",
    misfirePolicy: "skip",
    jitterMs: "0",
    deliveryMode: "same_channel",
    deliveryChannel: "",
    quietHoursStart: "",
    quietHoursEnd: "",
    quietHoursTimezone: "local",
    cooldownMs: "0",
    approvalMode: "none",
    templateId: "",
    hookId: "",
    webhookIntegrationId: "",
    webhookProvider: "",
    eventName: "",
    triggerPayloadText: EMPTY_JSON_TEXT,
  };
}

export function routineFormFromRecord(
  routine: JsonObject,
  fallbackChannel: string | null | undefined,
): RoutineEditorForm {
  const schedulePayload = readObject(routine, "schedule_payload") ?? {};
  const triggerPayload = readObject(routine, "trigger_payload") ?? {};
  const quietHours = readObject(routine, "quiet_hours") ?? {};
  const form = defaultRoutineForm(fallbackChannel);
  return {
    ...form,
    routineId: readString(routine, "routine_id") ?? readString(routine, "job_id") ?? "",
    name: readString(routine, "name") ?? "",
    prompt: readString(routine, "prompt") ?? "",
    triggerKind:
      (readString(routine, "trigger_kind") as RoutineEditorForm["triggerKind"] | null) ??
      form.triggerKind,
    naturalLanguageSchedule: "",
    scheduleType:
      (readString(routine, "schedule_type") as RoutineEditorForm["scheduleType"] | null) ??
      form.scheduleType,
    cronExpression: readString(schedulePayload, "expression") ?? "",
    everyIntervalMs: String(readNumber(schedulePayload, "interval_ms") ?? 3_600_000),
    atTimestampRfc3339: readString(schedulePayload, "timestamp_rfc3339") ?? "",
    enabled: readBool(routine, "enabled"),
    channel: readString(routine, "channel") ?? form.channel,
    sessionKey: readString(routine, "session_key") ?? "",
    sessionLabel: readString(routine, "session_label") ?? "",
    concurrencyPolicy:
      (readString(routine, "concurrency_policy") as
        | RoutineEditorForm["concurrencyPolicy"]
        | null) ?? form.concurrencyPolicy,
    retryMaxAttempts: String(
      readNumber(readObject(routine, "retry_policy") ?? {}, "max_attempts") ?? 1,
    ),
    retryBackoffMs: String(
      readNumber(readObject(routine, "retry_policy") ?? {}, "backoff_ms") ?? 1000,
    ),
    misfirePolicy:
      (readString(routine, "misfire_policy") as RoutineEditorForm["misfirePolicy"] | null) ??
      form.misfirePolicy,
    jitterMs: String(readNumber(routine, "jitter_ms") ?? 0),
    deliveryMode:
      (readString(routine, "delivery_mode") as RoutineEditorForm["deliveryMode"] | null) ??
      form.deliveryMode,
    deliveryChannel: readString(routine, "delivery_channel") ?? "",
    quietHoursStart: minuteOfDayToClock(readNumber(quietHours, "start_minute_of_day")),
    quietHoursEnd: minuteOfDayToClock(readNumber(quietHours, "end_minute_of_day")),
    quietHoursTimezone:
      (readString(quietHours, "timezone") as RoutineEditorForm["quietHoursTimezone"] | null) ??
      form.quietHoursTimezone,
    cooldownMs: String(readNumber(routine, "cooldown_ms") ?? 0),
    approvalMode:
      (readString(routine, "approval_mode") as RoutineEditorForm["approvalMode"] | null) ??
      form.approvalMode,
    templateId: readString(routine, "template_id") ?? "",
    hookId: readString(triggerPayload, "hook_id") ?? "",
    webhookIntegrationId: readString(triggerPayload, "integration_id") ?? "",
    webhookProvider: readString(triggerPayload, "provider") ?? "",
    eventName: readString(triggerPayload, "event") ?? readString(triggerPayload, "name") ?? "",
    triggerPayloadText: formatJson(triggerPayload),
  };
}

export function applyTemplateToForm(
  template: JsonObject,
  base: RoutineEditorForm,
): RoutineEditorForm {
  return {
    ...base,
    name: readString(template, "default_name") ?? base.name,
    prompt: readString(template, "prompt") ?? base.prompt,
    triggerKind:
      (readString(template, "trigger_kind") as RoutineEditorForm["triggerKind"] | null) ??
      base.triggerKind,
    naturalLanguageSchedule: readString(template, "natural_language_schedule") ?? "",
    deliveryMode:
      (readString(template, "delivery_mode") as RoutineEditorForm["deliveryMode"] | null) ??
      base.deliveryMode,
    approvalMode:
      (readString(template, "approval_mode") as RoutineEditorForm["approvalMode"] | null) ??
      base.approvalMode,
    templateId: readString(template, "template_id") ?? base.templateId,
  };
}

export function buildRoutineUpsertPayload(
  form: RoutineEditorForm,
): Record<string, string | number | boolean | JsonObject | undefined> {
  const payload: Record<string, string | number | boolean | JsonObject | undefined> = {
    routine_id: emptyToUndefined(form.routineId),
    name: form.name.trim(),
    prompt: form.prompt.trim(),
    enabled: form.enabled,
    trigger_kind: form.triggerKind,
    channel: emptyToUndefined(form.channel),
    session_key: emptyToUndefined(form.sessionKey),
    session_label: emptyToUndefined(form.sessionLabel),
    concurrency_policy: form.concurrencyPolicy,
    retry_max_attempts: parseInteger(form.retryMaxAttempts) ?? 1,
    retry_backoff_ms: parseInteger(form.retryBackoffMs) ?? 1000,
    misfire_policy: form.misfirePolicy,
    jitter_ms: parseInteger(form.jitterMs) ?? 0,
    delivery_mode: form.deliveryMode,
    delivery_channel:
      form.deliveryMode === "specific_channel" ? emptyToUndefined(form.deliveryChannel) : undefined,
    quiet_hours_start: emptyToUndefined(form.quietHoursStart),
    quiet_hours_end: emptyToUndefined(form.quietHoursEnd),
    quiet_hours_timezone:
      emptyToUndefined(form.quietHoursStart) === undefined &&
      emptyToUndefined(form.quietHoursEnd) === undefined
        ? undefined
        : form.quietHoursTimezone,
    cooldown_ms: parseInteger(form.cooldownMs) ?? 0,
    approval_mode: form.approvalMode,
    template_id: emptyToUndefined(form.templateId),
  };

  if (form.triggerKind === "schedule") {
    payload.natural_language_schedule = emptyToUndefined(form.naturalLanguageSchedule);
    if (payload.natural_language_schedule === undefined) {
      payload.schedule_type = form.scheduleType;
      if (form.scheduleType === "every") {
        payload.every_interval_ms = parseInteger(form.everyIntervalMs) ?? undefined;
      }
      if (form.scheduleType === "cron") {
        payload.cron_expression = emptyToUndefined(form.cronExpression);
      }
      if (form.scheduleType === "at") {
        payload.at_timestamp_rfc3339 = emptyToUndefined(form.atTimestampRfc3339);
      }
    }
    return payload;
  }

  const parsedPayload = parseJsonObject(form.triggerPayloadText, "Trigger payload matcher");
  const triggerPayload: JsonObject = { ...parsedPayload };
  if (form.triggerKind === "hook") {
    if (form.hookId.trim().length === 0) {
      throw new Error("Hook routines require hook_id.");
    }
    triggerPayload.hook_id = form.hookId.trim();
    if (form.eventName.trim().length > 0) {
      triggerPayload.event = form.eventName.trim();
    }
  } else if (form.triggerKind === "webhook") {
    if (form.webhookIntegrationId.trim().length === 0 || form.eventName.trim().length === 0) {
      throw new Error("Webhook routines require integration id and event.");
    }
    triggerPayload.integration_id = form.webhookIntegrationId.trim();
    triggerPayload.event = form.eventName.trim();
    if (form.webhookProvider.trim().length > 0) {
      triggerPayload.provider = form.webhookProvider.trim();
    }
  } else if (form.triggerKind === "system_event") {
    if (form.eventName.trim().length === 0) {
      throw new Error("System-event routines require an event name.");
    }
    triggerPayload.event = normalizeSystemEventMatcher(form.eventName);
  }
  payload.trigger_payload = triggerPayload;
  return payload;
}

export function parseJsonObject(text: string, label: string): JsonObject {
  const trimmed = text.trim();
  if (trimmed.length === 0) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch (error) {
    throw new Error(`${label} must be valid JSON: ${(error as Error).message}`);
  }
  const candidate = parsed as JsonObject | null;
  if (!isJsonObject(candidate)) {
    throw new Error(`${label} must be a JSON object.`);
  }
  return candidate;
}

export function formatJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2);
}

export function resolveRoutineId(routine: JsonObject | null): string | null {
  if (routine === null) {
    return null;
  }
  return readString(routine, "routine_id") ?? readString(routine, "job_id");
}

export function routineSummary(routine: JsonObject): string {
  const triggerKind = readString(routine, "trigger_kind") ?? "manual";
  if (triggerKind !== "schedule") {
    const triggerPayload = readObject(routine, "trigger_payload") ?? {};
    return `${triggerKind} · ${readString(triggerPayload, "event") ?? readString(triggerPayload, "hook_id") ?? readString(triggerPayload, "integration_id") ?? "custom matcher"}`;
  }
  const scheduleType = readString(routine, "schedule_type") ?? "schedule";
  const schedulePayload = readObject(routine, "schedule_payload") ?? {};
  if (scheduleType === "every") {
    return `every ${millisecondsSummary(readNumber(schedulePayload, "interval_ms"))}`;
  }
  if (scheduleType === "cron") {
    return readString(schedulePayload, "expression") ?? "cron expression unavailable";
  }
  if (scheduleType === "at") {
    return readString(schedulePayload, "timestamp_rfc3339") ?? "one-off timestamp unavailable";
  }
  return scheduleType;
}

export function millisecondsSummary(value: number | null): string {
  if (value === null || value <= 0) {
    return "0 ms";
  }
  if (value % 3_600_000 === 0) {
    return `${value / 3_600_000}h`;
  }
  if (value % 60_000 === 0) {
    return `${value / 60_000}m`;
  }
  if (value % 1_000 === 0) {
    return `${value / 1_000}s`;
  }
  return `${value} ms`;
}

export function stripSystemEventPrefix(raw: string): string {
  return raw.startsWith("system.operator.") ? raw.slice("system.operator.".length) : raw;
}

function normalizeSystemEventMatcher(raw: string): string {
  const trimmed = raw.trim();
  if (trimmed.startsWith("system.operator.")) {
    return trimmed;
  }
  return `system.operator.${trimmed}`;
}

function minuteOfDayToClock(value: number | null): string {
  if (value === null || !Number.isFinite(value) || value < 0) {
    return "";
  }
  const total = Math.floor(value) % (24 * 60);
  const hours = String(Math.floor(total / 60)).padStart(2, "0");
  const minutes = String(total % 60).padStart(2, "0");
  return `${hours}:${minutes}`;
}
