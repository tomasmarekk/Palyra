import { Button, Card, CardContent, CardHeader, Chip } from "@heroui/react";
import { useEffect, useEffectEvent, useMemo, useRef, useState, type ReactNode } from "react";

import {
  isDesktopHostAvailable,
  openDashboard,
  restartPalyra,
  showMainWindow,
  startPalyra,
  stopPalyra,
  type ActionResult,
  type ControlCenterSnapshot
} from "./lib/desktopApi";
import { useDesktopSnapshot } from "./hooks/useDesktopSnapshot";

type ActionName = "start" | "stop" | "restart" | "dashboard" | null;

function formatUnixMs(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "-";
  }

  return new Date(value).toLocaleString();
}

function formatUptime(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) {
    return "-";
  }

  const total = Math.max(0, Math.floor(seconds));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remainingSeconds = total % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${remainingSeconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  return `${remainingSeconds}s`;
}

function statusColor(status: ControlCenterSnapshot["overall_status"]): "success" | "warning" | "danger" {
  if (status === "healthy") {
    return "success";
  }
  if (status === "degraded") {
    return "warning";
  }
  return "danger";
}

function processTone(process: ControlCenterSnapshot["gateway_process"]): "success" | "warning" | "danger" {
  if (process.running) {
    return process.restart_attempt > 0 ? "warning" : "success";
  }
  return process.desired_running ? "danger" : "warning";
}

function browserTone(snapshot: ControlCenterSnapshot["quick_facts"]["browser_service"]): "success" | "warning" | "danger" {
  if (!snapshot.enabled) {
    return "warning";
  }
  return snapshot.healthy ? "success" : "danger";
}

function processSummary(snapshot: ControlCenterSnapshot["gateway_process"]): string {
  if (!snapshot.running) {
    return "Stopped";
  }

  const pid = snapshot.pid === null ? "pid n/a" : `pid ${snapshot.pid}`;
  const ports = snapshot.bound_ports.length === 0 ? "no ports" : `ports ${snapshot.bound_ports.join(", ")}`;
  return `${snapshot.liveness} · ${pid} · ${ports}`;
}

function actionLabel(action: ActionName, name: Exclude<ActionName, null>, idle: string, busy: string): string {
  return action === name ? busy : idle;
}

function fallbackText(value: string | null): string {
  if (value === null || value.trim().length === 0) {
    return "None recorded";
  }
  return value;
}

function collectAttentionItems(snapshot: ControlCenterSnapshot): string[] {
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

function StatusCard({
  eyebrow,
  title,
  accent,
  children
}: {
  accent?: ReactNode;
  children: ReactNode;
  eyebrow: string;
  title: string;
}) {
  return (
    <Card className="desktop-card">
      <CardHeader className="desktop-card__header">
        <div>
          <p className="desktop-eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </div>
        {accent}
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  );
}

export function App() {
  const { snapshot, loading, error, previewMode, refresh } = useDesktopSnapshot();
  const [actionState, setActionState] = useState<ActionName>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const mainWindowShownRef = useRef(false);

  const attentionItems = useMemo(() => collectAttentionItems(snapshot), [snapshot]);

  const revealMainWindow = useEffectEvent(async () => {
    if (mainWindowShownRef.current || !isDesktopHostAvailable()) {
      return;
    }

    try {
      await showMainWindow();
      mainWindowShownRef.current = true;
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(`Desktop window handoff failed: ${message}`);
    }
  });

  useEffect(() => {
    void revealMainWindow();
  }, [revealMainWindow]);

  async function runAction(action: ActionName, execute: () => Promise<ActionResult>): Promise<void> {
    if (action === null) {
      return;
    }

    setActionState(action);
    try {
      const result = await execute();
      setNotice(result.message);
      await refresh();
    } catch (failure) {
      const message = failure instanceof Error ? failure.message : String(failure);
      setNotice(message);
    } finally {
      setActionState(null);
    }
  }

  return (
    <main className="desktop-root">
      <section className="desktop-hero" aria-busy={loading}>
        <div className="desktop-hero__copy">
          <p className="desktop-kicker">Desktop Control Center</p>
          <h1>Start the local runtime, watch it stabilize, then hand off to the dashboard.</h1>
          <p className="desktop-copy">
            This surface is intentionally small. It covers launcher controls, runtime health, and
            the shortest path into the operator dashboard without re-embedding onboarding or
            settings workflows in desktop.
          </p>
        </div>

        <div className="desktop-hero__summary">
          <div className="desktop-hero__summary-row">
            <span className="desktop-label">Overall state</span>
            <Chip color={statusColor(snapshot.overall_status)} variant="soft">
              {snapshot.overall_status}
            </Chip>
          </div>
          <div className="desktop-hero__summary-row">
            <span className="desktop-label">Dashboard mode</span>
            <strong>{snapshot.quick_facts.dashboard_access_mode}</strong>
          </div>
          <div className="desktop-hero__summary-row">
            <span className="desktop-label">Last snapshot</span>
            <strong>{loading ? "Refreshing…" : formatUnixMs(snapshot.generated_at_unix_ms)}</strong>
          </div>
        </div>
      </section>

      <section className="desktop-actions" aria-label="Desktop lifecycle actions">
        <div className="desktop-actions__group">
          <Button
            isDisabled={actionState !== null}
            variant="primary"
            onPress={() => void runAction("start", startPalyra)}
          >
            {actionLabel(actionState, "start", "Start Palyra", "Starting Palyra…")}
          </Button>
          <Button
            isDisabled={actionState !== null || !snapshot.gateway_process.running}
            variant="outline"
            onPress={() => void runAction("stop", stopPalyra)}
          >
            {actionLabel(actionState, "stop", "Stop Runtime", "Stopping Runtime…")}
          </Button>
          <Button
            isDisabled={actionState !== null || !snapshot.gateway_process.running}
            variant="secondary"
            onPress={() => void runAction("restart", restartPalyra)}
          >
            {actionLabel(actionState, "restart", "Restart Runtime", "Restarting Runtime…")}
          </Button>
        </div>

        <div className="desktop-actions__group">
          <Button
            isDisabled={actionState !== null}
            variant="ghost"
            onPress={() => void runAction("dashboard", openDashboard)}
          >
            {actionLabel(actionState, "dashboard", "Open Dashboard", "Opening Dashboard…")}
          </Button>
          <Button isDisabled={actionState !== null} variant="ghost" onPress={() => void refresh()}>
            Refresh Snapshot
          </Button>
        </div>
      </section>

      {(notice !== null || error !== null || previewMode) && (
        <Card className="desktop-notice">
          <CardContent className="desktop-notice__content">
            {previewMode ? (
              <p className="desktop-notice__line">
                Preview data is active because the Tauri host bridge is not available in this
                context.
              </p>
            ) : null}
            {notice !== null ? <p className="desktop-notice__line">{notice}</p> : null}
            {error !== null ? <p className="desktop-error">{error}</p> : null}
          </CardContent>
        </Card>
      )}

      <section className="desktop-overview">
        <StatusCard
          eyebrow="Launcher"
          title="Gateway process"
          accent={
            <Chip color={processTone(snapshot.gateway_process)} variant="soft">
              {snapshot.gateway_process.running ? "Running" : "Stopped"}
            </Chip>
          }
        >
          <p className="desktop-card__lede">{processSummary(snapshot.gateway_process)}</p>
          <dl className="desktop-pairs">
            <div>
              <dt>Desired</dt>
              <dd>{snapshot.gateway_process.desired_running ? "running" : "stopped"}</dd>
            </div>
            <div>
              <dt>Last start</dt>
              <dd>{formatUnixMs(snapshot.gateway_process.last_start_unix_ms)}</dd>
            </div>
            <div>
              <dt>Next restart</dt>
              <dd>{formatUnixMs(snapshot.gateway_process.next_restart_unix_ms)}</dd>
            </div>
          </dl>
        </StatusCard>

        <StatusCard
          eyebrow="Handoff"
          title="Dashboard target"
          accent={
            <Chip color="accent" variant="soft">
              {snapshot.quick_facts.dashboard_access_mode}
            </Chip>
          }
        >
          <p className="desktop-card__lede desktop-mono">{snapshot.quick_facts.dashboard_url}</p>
          <dl className="desktop-pairs">
            <div>
              <dt>Version</dt>
              <dd>{snapshot.quick_facts.gateway_version ?? "Unavailable"}</dd>
            </div>
            <div>
              <dt>Git hash</dt>
              <dd className="desktop-mono">{snapshot.quick_facts.gateway_git_hash ?? "-"}</dd>
            </div>
            <div>
              <dt>Gateway uptime</dt>
              <dd>{formatUptime(snapshot.quick_facts.gateway_uptime_seconds)}</dd>
            </div>
          </dl>
        </StatusCard>

        <StatusCard
          eyebrow="Sidecar"
          title="Browser service"
          accent={
            <Chip color={browserTone(snapshot.quick_facts.browser_service)} variant="soft">
              {snapshot.quick_facts.browser_service.status}
            </Chip>
          }
        >
          <p className="desktop-card__lede">
            {snapshot.quick_facts.browser_service.enabled ? "Enabled" : "Disabled by config"}
          </p>
          <dl className="desktop-pairs">
            <div>
              <dt>Health</dt>
              <dd>{snapshot.quick_facts.browser_service.healthy ? "healthy" : "needs attention"}</dd>
            </div>
            <div>
              <dt>Uptime</dt>
              <dd>{formatUptime(snapshot.quick_facts.browser_service.uptime_seconds)}</dd>
            </div>
            <div>
              <dt>Last error</dt>
              <dd>{fallbackText(snapshot.quick_facts.browser_service.last_error)}</dd>
            </div>
          </dl>
        </StatusCard>

        <StatusCard
          eyebrow="Observability"
          title="Diagnostics pulse"
          accent={
            <Chip
              color={attentionItems.length === 0 ? "success" : "warning"}
              variant="soft"
            >
              {attentionItems.length === 0 ? "Clear" : `${attentionItems.length} alerts`}
            </Chip>
          }
        >
          <p className="desktop-card__lede">
            {attentionItems.length === 0
              ? "No redacted warnings are currently asking for action."
              : "Recent warnings and diagnostics were folded into a single operator queue."}
          </p>
          <dl className="desktop-pairs">
            <div>
              <dt>Diagnostics time</dt>
              <dd>{formatUnixMs(snapshot.diagnostics.generated_at_unix_ms)}</dd>
            </div>
            <div>
              <dt>Dropped events</dt>
              <dd>{snapshot.diagnostics.dropped_log_events_total}</dd>
            </div>
            <div>
              <dt>Preview mode</dt>
              <dd>{previewMode ? "active" : "off"}</dd>
            </div>
          </dl>
        </StatusCard>
      </section>

      <section className="desktop-detail-grid">
        <StatusCard
          eyebrow="Runtime detail"
          title="Process monitor"
          accent={
            <Chip color={processTone(snapshot.browserd_process)} variant="soft">
              {snapshot.browserd_process.running ? "Browserd up" : "Browserd down"}
            </Chip>
          }
        >
          <div className="desktop-process-stack">
            <section className="desktop-process-panel">
              <h3>Gateway</h3>
              <dl className="desktop-pairs">
                <div>
                  <dt>PID</dt>
                  <dd>{snapshot.gateway_process.pid ?? "Unavailable"}</dd>
                </div>
                <div>
                  <dt>Restart attempts</dt>
                  <dd>{snapshot.gateway_process.restart_attempt}</dd>
                </div>
                <div>
                  <dt>Bound ports</dt>
                  <dd>{snapshot.gateway_process.bound_ports.join(", ") || "None"}</dd>
                </div>
                <div>
                  <dt>Last exit</dt>
                  <dd>{fallbackText(snapshot.gateway_process.last_exit)}</dd>
                </div>
              </dl>
            </section>

            <section className="desktop-process-panel">
              <h3>Browserd</h3>
              <dl className="desktop-pairs">
                <div>
                  <dt>PID</dt>
                  <dd>{snapshot.browserd_process.pid ?? "Unavailable"}</dd>
                </div>
                <div>
                  <dt>Restart attempts</dt>
                  <dd>{snapshot.browserd_process.restart_attempt}</dd>
                </div>
                <div>
                  <dt>Bound ports</dt>
                  <dd>{snapshot.browserd_process.bound_ports.join(", ") || "None"}</dd>
                </div>
                <div>
                  <dt>Last exit</dt>
                  <dd>{fallbackText(snapshot.browserd_process.last_exit)}</dd>
                </div>
              </dl>
            </section>
          </div>
        </StatusCard>

        <StatusCard
          eyebrow="Attention"
          title="Warnings and recovery notes"
          accent={
            <Chip color={attentionItems.length === 0 ? "success" : "warning"} variant="soft">
              {attentionItems.length === 0 ? "Stable" : "Review"}
            </Chip>
          }
        >
          {attentionItems.length === 0 ? (
            <p className="desktop-card__lede">
              Local runtime signals are currently clean. If the dashboard still refuses to open,
              refresh the snapshot once before retrying the handoff.
            </p>
          ) : (
            <ul className="desktop-issues">
              {attentionItems.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          )}
        </StatusCard>
      </section>
    </main>
  );
}
