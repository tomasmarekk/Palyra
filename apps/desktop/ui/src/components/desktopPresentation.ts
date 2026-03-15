import type {
  BrowserServiceSnapshot,
  ControlCenterSnapshot,
  RuntimeStatus,
  ServiceProcessSnapshot
} from "../lib/desktopApi";
import type { UiTone } from "./ui";

export type ActionName = "start" | "stop" | "restart" | "dashboard" | null;

export function formatUnixMs(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "-";
  }

  return new Date(value).toLocaleString();
}

export function formatUptime(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) {
    return "-";
  }

  const total = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(total / 3_600);
  const minutes = Math.floor((total % 3_600) / 60);
  const remainingSeconds = total % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${remainingSeconds}s`;
  }

  if (minutes > 0) {
    return `${minutes}m ${remainingSeconds}s`;
  }

  return `${remainingSeconds}s`;
}

export function fallbackText(value: string | null): string {
  if (value === null || value.trim().length === 0) {
    return "None recorded";
  }

  return value;
}

export function actionLabel(
  action: ActionName,
  name: Exclude<ActionName, null>,
  idle: string,
  busy: string
): string {
  return action === name ? busy : idle;
}

export function overallTone(status: RuntimeStatus): UiTone {
  if (status === "healthy") {
    return "success";
  }

  if (status === "degraded") {
    return "warning";
  }

  return "danger";
}

export function processTone(process: ServiceProcessSnapshot): UiTone {
  if (process.running) {
    return process.restart_attempt > 0 ? "warning" : "success";
  }

  return process.desired_running ? "danger" : "warning";
}

export function browserTone(snapshot: BrowserServiceSnapshot): UiTone {
  if (!snapshot.enabled) {
    return "warning";
  }

  return snapshot.healthy ? "success" : "danger";
}

export function attentionTone(attentionCount: number): UiTone {
  return attentionCount === 0 ? "success" : "warning";
}

export function processSummary(snapshot: ServiceProcessSnapshot): string {
  if (!snapshot.running) {
    return "Stopped";
  }

  const pid = snapshot.pid === null ? "pid n/a" : `pid ${snapshot.pid}`;
  const ports = snapshot.bound_ports.length === 0 ? "no ports" : `ports ${snapshot.bound_ports.join(", ")}`;
  return `${snapshot.liveness} · ${pid} · ${ports}`;
}

export function collectAttentionItems(snapshot: ControlCenterSnapshot): string[] {
  const unique = new Set<string>();

  for (const item of [...snapshot.warnings, ...snapshot.diagnostics.errors]) {
    const normalized = item.trim();
    if (normalized.length === 0 || unique.has(normalized)) {
      continue;
    }

    unique.add(normalized);
    if (unique.size >= 5) {
      break;
    }
  }

  return [...unique];
}
