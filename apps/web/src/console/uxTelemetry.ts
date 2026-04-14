import type { ConsoleApiClient, JsonValue } from "../consoleApi";
import {
  aggregateUxTelemetry,
  isUxSystemEvent,
  toSystemEventPayload,
  type SystemEventRecord,
  type UxTelemetryAggregate,
  type UxTelemetryEvent,
} from "./contracts";

export async function emitUxSystemEvent(
  api: ConsoleApiClient,
  event: UxTelemetryEvent,
): Promise<void> {
  await api.emitSystemEvent(toSystemEventPayload(event));
}

export async function loadUxTelemetryAggregate(
  api: ConsoleApiClient,
  limit: number = 250,
): Promise<{ aggregate: UxTelemetryAggregate; records: SystemEventRecord[] }> {
  const params = new URLSearchParams();
  params.set("limit", String(limit));
  params.set("contains", "system.operator.ux.");
  const response = await api.listSystemEvents(params);
  const records = toSystemEventRecords(response.events);
  return {
    aggregate: aggregateUxTelemetry(records),
    records,
  };
}

function toSystemEventRecords(events: JsonValue[]): SystemEventRecord[] {
  return events.filter(isSystemEventRecord);
}

function isSystemEventRecord(value: JsonValue): value is SystemEventRecord {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  return isUxSystemEvent(value as SystemEventRecord) || "payload_json" in value;
}
