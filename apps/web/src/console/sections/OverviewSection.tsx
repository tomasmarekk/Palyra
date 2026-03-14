import { Card, CardContent, Chip } from "@heroui/react";

import type { CapabilityCatalog } from "../../consoleApi";
import { capabilityModeCounts } from "../capabilityCatalog";
import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  readBool,
  readNumber,
  readObject,
  readString,
  toStringArray,
  type JsonObject
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type OverviewSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "overviewBusy"
    | "overviewCatalog"
    | "overviewDeployment"
    | "overviewApprovals"
    | "overviewDiagnostics"
    | "overviewSupportJobs"
    | "refreshOverview"
    | "setSection"
  >;
};

type Tone = "default" | "success" | "warning" | "danger" | "accent";

export function OverviewSection({ app }: OverviewSectionProps) {
  const catalog = readCapabilityCatalog(app.overviewCatalog);
  const deployment = app.overviewDeployment;
  const diagnostics = app.overviewDiagnostics;
  const observability = readObject(diagnostics ?? {}, "observability");
  const connector = readObject(observability ?? {}, "connector");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const supportBundle = readObject(observability ?? {}, "support_bundle");
  const browserd = readObject(diagnostics ?? {}, "browserd");
  const deploymentWarnings = toStringArray(Array.isArray(deployment?.warnings) ? deployment.warnings : []);
  const recentFailures = readJsonObjectArray(observability?.recent_failures);
  const recentConnectorErrors = readJsonObjectArray(connector?.recent_errors);
  const recentSignals = [...recentFailures, ...recentConnectorErrors].slice(0, 6);
  const pendingApprovals = app.overviewApprovals.filter(isPendingApproval).length;
  const supportQueuedJobs = app.overviewSupportJobs.filter((job) => {
    const state = readString(job, "state");
    return state === "queued" || state === "running";
  }).length;
  const supportFailedJobs = app.overviewSupportJobs.filter((job) => readString(job, "state") === "failed").length;
  const connectorDegradedCount = readNumber(connector ?? {}, "degraded_connectors") ?? 0;
  const connectorDeadLetters = readNumber(connector ?? {}, "dead_letters") ?? 0;
  const cliHandoffs = listCliHandoffs(catalog);
  const providerState =
    readString(providerAuth ?? {}, "state") ??
    readString(readObject(diagnostics ?? {}, "auth_profiles") ?? {}, "state") ??
    "unknown";
  const runtime = summarizeRuntime({
    warningCount: deploymentWarnings.length,
    pendingApprovals,
    failedSupportJobs: supportFailedJobs,
    connectorDegradedCount,
    providerState
  });
  const exposureCounts = capabilityModeCounts(catalog?.capabilities ?? []);
  const topCards = [
    {
      label: "Runtime health",
      value: runtime.label,
      tone: runtime.tone,
      hint:
        runtime.tone === "success"
          ? "No immediate operator blockers published."
          : `${runtime.issues.length} attention item${runtime.issues.length === 1 ? "" : "s"} published.`
    },
    {
      label: "Access posture",
      value: `${readString(deployment ?? {}, "mode") ?? "unknown"} / ${readString(deployment ?? {}, "bind_profile") ?? "n/a"}`,
      tone: deploymentWarnings.length > 0 ? "warning" : "default",
      hint: deploymentWarnings[0] ?? "Loopback-style posture with explicit auth gates."
    },
    {
      label: "Pending approvals",
      value: String(pendingApprovals),
      tone: pendingApprovals > 0 ? "warning" : "success",
      hint:
        pendingApprovals > 0
          ? "Review sensitive actions before they block active runs."
          : "Approval queue is currently clear."
    },
    {
      label: "Support status",
      value: `${supportQueuedJobs} queued / ${supportFailedJobs} failed`,
      tone: supportFailedJobs > 0 ? "danger" : supportQueuedJobs > 0 ? "warning" : "success",
      hint:
        readString(readObject(supportBundle ?? {}, "last_job") ?? {}, "state") ??
        "No recent support bundle job loaded."
    }
  ] as const;

  return (
    <main className="workspace-page">
      <ConsoleSectionHeader
        title="Overview"
        description="Check health, see what needs attention, and jump into the right workspace without wading through the full capability catalog."
        actions={
          <button type="button" onClick={() => void app.refreshOverview()} disabled={app.overviewBusy}>
            {app.overviewBusy ? "Refreshing..." : "Refresh overview"}
          </button>
        }
      />

      <section className="workspace-summary-grid">
        {topCards.map((card) => (
          <Card key={card.label} className="workspace-stat-card border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
            <CardContent className="gap-3 px-5 py-4">
              <div className="workspace-stat-card__header">
                <p className="workspace-kicker">{card.label}</p>
                <Chip color={card.tone} variant="soft">
                  {card.value}
                </Chip>
              </div>
              <p className="workspace-stat-card__value">{card.value}</p>
              <p className="chat-muted">{card.hint}</p>
            </CardContent>
          </Card>
        ))}
      </section>

      <section className="workspace-grid workspace-grid--two-up">
        <Card className="workspace-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="gap-4 px-5 py-5">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Needs Attention</p>
              <h3>Priority checks</h3>
              <p className="chat-muted">
                Overview only surfaces the items that can block operations or should change the next page you open.
              </p>
            </div>
            {runtime.issues.length === 0 ? (
              <p className="workspace-empty">No urgent warnings are currently published.</p>
            ) : (
              <ul className="workspace-bullet-list">
                {runtime.issues.map((issue) => (
                  <li key={issue}>{issue}</li>
                ))}
              </ul>
            )}
            {recentSignals.length > 0 && (
              <div className="workspace-tag-row">
                {recentSignals.slice(0, 3).map((signal, index) => {
                  const signalText =
                    readString(signal, "message") ??
                    readString(signal, "operation") ??
                    readString(signal, "reason") ??
                    "signal";
                  return (
                    <Chip key={`${signalText}-${index}`} size="sm" variant="secondary">
                      {signalText}
                    </Chip>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="workspace-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="gap-4 px-5 py-5">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Quick Actions</p>
              <h3>Go where the operator work is</h3>
              <p className="chat-muted">
                Jump straight into the workspace that matches the current signal instead of navigating the whole dashboard.
              </p>
            </div>
            <div className="workspace-action-grid">
              <QuickAction label="Open chat" detail="Continue the active operator conversation." onClick={() => app.setSection("chat")} />
              <QuickAction label="Review approvals" detail="Process sensitive-action requests." onClick={() => app.setSection("approvals")} />
              <QuickAction label="Check channels" detail="Inspect connector health and router state." onClick={() => app.setSection("channels")} />
              <QuickAction label="Open support" detail="Queue bundles and inspect recent failures." onClick={() => app.setSection("support")} />
            </div>
          </CardContent>
        </Card>

        <Card className="workspace-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="gap-4 px-5 py-5">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Published Commands</p>
              <h3>CLI handoff surface</h3>
              <p className="chat-muted">
                Keep the approved operator CLI escapes visible here without turning the page into a
                full capability catalog.
              </p>
            </div>
            {cliHandoffs.length === 0 ? (
              <p className="workspace-empty">No CLI handoffs are currently published.</p>
            ) : (
              <div className="workspace-list">
                {cliHandoffs.map((command) => (
                  <article key={command} className="workspace-list-card">
                    <strong>{command}</strong>
                    <p>Published from the live capability catalog for explicit operator handoff.</p>
                  </article>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </section>

      <section className="workspace-grid workspace-grid--two-up">
        <Card className="workspace-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="gap-4 px-5 py-5">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Deployment And Access</p>
              <h3>Current posture</h3>
            </div>
            <dl className="workspace-detail-grid">
              <div>
                <dt>Mode</dt>
                <dd>{readString(deployment ?? {}, "mode") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Bind profile</dt>
                <dd>{readString(deployment ?? {}, "bind_profile") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Admin auth</dt>
                <dd>{readBool(deployment ?? {}, "admin_auth_required") ? "required" : "unknown"}</dd>
              </div>
              <div>
                <dt>Remote bind</dt>
                <dd>{readBool(deployment ?? {}, "remote_bind_detected") ? "detected" : "not detected"}</dd>
              </div>
              <div>
                <dt>Browser service</dt>
                <dd>{readString(browserd ?? {}, "state") ?? "unknown"}</dd>
              </div>
              <div>
                <dt>Operator surfaces</dt>
                <dd>{catalog?.capabilities.length ?? 0} published</dd>
              </div>
            </dl>
            {deploymentWarnings.length > 0 && (
              <ul className="workspace-bullet-list">
                {deploymentWarnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card className="workspace-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="gap-4 px-5 py-5">
            <div className="workspace-panel__intro">
              <p className="workspace-kicker">Recent Operational Signals</p>
              <h3>What changed recently</h3>
            </div>
            <dl className="workspace-detail-grid">
              <div>
                <dt>Direct actions</dt>
                <dd>{exposureCounts.direct_action}</dd>
              </div>
              <div>
                <dt>CLI handoffs</dt>
                <dd>{exposureCounts.cli_handoff}</dd>
              </div>
              <div>
                <dt>Degraded connectors</dt>
                <dd>{connectorDegradedCount}</dd>
              </div>
              <div>
                <dt>Dead letters</dt>
                <dd>{connectorDeadLetters}</dd>
              </div>
              <div>
                <dt>Provider auth</dt>
                <dd>{providerState}</dd>
              </div>
              <div>
                <dt>Last support bundle</dt>
                <dd>{readString(readObject(supportBundle ?? {}, "last_job") ?? {}, "job_id") ?? "none"}</dd>
              </div>
            </dl>
            {recentSignals.length === 0 ? (
              <p className="workspace-empty">No recent failures or connector warnings were published.</p>
            ) : (
              <div className="workspace-list">
                {recentSignals.map((signal, index) => {
                  const title =
                    readString(signal, "failure_class") ??
                    readString(signal, "connector_id") ??
                    readString(signal, "operation") ??
                    `Signal ${index + 1}`;
                  const detail =
                    readString(signal, "message") ??
                    readString(signal, "reason") ??
                    readString(signal, "operation") ??
                    "No additional detail published.";
                  return (
                    <article key={`${title}-${index}`} className="workspace-list-card">
                      <strong>{title}</strong>
                      <p>{detail}</p>
                    </article>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </section>
    </main>
  );
}

function QuickAction({
  label,
  detail,
  onClick
}: {
  label: string;
  detail: string;
  onClick: () => void;
}) {
  return (
    <button type="button" className="workspace-action-card" onClick={onClick}>
      <strong>{label}</strong>
      <span>{detail}</span>
    </button>
  );
}

function isPendingApproval(approval: JsonObject): boolean {
  const decision = readString(approval, "decision");
  return decision === null || decision === "pending" || decision.length === 0;
}

function readCapabilityCatalog(value: JsonObject | null): CapabilityCatalog | null {
  if (value === null || !Array.isArray(value.capabilities)) {
    return null;
  }
  return value as unknown as CapabilityCatalog;
}

function readJsonObjectArray(value: unknown): JsonObject[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is JsonObject => isJsonObject(entry));
}

function listCliHandoffs(catalog: CapabilityCatalog | null): string[] {
  if (catalog === null) {
    return [];
  }
  const commands = new Set<string>();
  for (const capability of catalog.capabilities) {
    if (capability.dashboard_exposure !== "cli_handoff") {
      continue;
    }
    for (const command of capability.cli_handoff_commands) {
      if (command.trim().length > 0) {
        commands.add(command);
      }
    }
  }
  return [...commands];
}

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function summarizeRuntime({
  warningCount,
  pendingApprovals,
  failedSupportJobs,
  connectorDegradedCount,
  providerState
}: {
  warningCount: number;
  pendingApprovals: number;
  failedSupportJobs: number;
  connectorDegradedCount: number;
  providerState: string;
}): { label: string; tone: Tone; issues: string[] } {
  const issues: string[] = [];
  if (warningCount > 0) {
    issues.push(`${warningCount} deployment warning${warningCount === 1 ? "" : "s"} published.`);
  }
  if (pendingApprovals > 0) {
    issues.push(`${pendingApprovals} approval${pendingApprovals === 1 ? "" : "s"} waiting for review.`);
  }
  if (connectorDegradedCount > 0) {
    issues.push(`${connectorDegradedCount} connector${connectorDegradedCount === 1 ? "" : "s"} degraded.`);
  }
  if (failedSupportJobs > 0) {
    issues.push(`${failedSupportJobs} support bundle job${failedSupportJobs === 1 ? "" : "s"} failed.`);
  }
  if (providerState === "degraded" || providerState === "expired" || providerState === "missing") {
    issues.push(`Provider auth state is ${providerState}.`);
  }

  if (issues.length === 0) {
    return { label: "Ready", tone: "success", issues };
  }
  if (failedSupportJobs > 0 || providerState === "missing" || providerState === "expired") {
    return { label: "Attention required", tone: "danger", issues };
  }
  return { label: "Degraded", tone: "warning", issues };
}
