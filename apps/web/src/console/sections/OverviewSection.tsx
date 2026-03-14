import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceEmptyState, WorkspaceInlineNotice, workspaceToneForState } from "../components/workspace/WorkspacePatterns";
import { readBool, readObject, readString, toStringArray } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type OverviewSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "overviewBusy"
    | "overviewDeployment"
    | "overviewApprovals"
    | "overviewDiagnostics"
    | "overviewSupportJobs"
    | "refreshOverview"
    | "setSection"
  >;
};

export function OverviewSection({ app }: OverviewSectionProps) {
  const deployment = app.overviewDeployment;
  const diagnostics = app.overviewDiagnostics;
  const observability = readObject(diagnostics ?? {}, "observability");
  const connector = readObject(observability ?? {}, "connector");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const warnings = toStringArray(Array.isArray(deployment?.warnings) ? deployment.warnings : []);
  const pendingApprovals = app.overviewApprovals.filter((approval) => {
    const decision = readString(approval, "decision");
    return decision === null || decision === "pending" || decision.length === 0;
  }).length;
  const failedSupportJobs = app.overviewSupportJobs.filter((job) => readString(job, "state") === "failed").length;
  const connectorDegraded = Number(readString(connector ?? {}, "degraded_connectors") ?? "0");
  const providerState = readString(providerAuth ?? {}, "state") ?? readString(readObject(diagnostics ?? {}, "auth_profiles") ?? {}, "state") ?? "unknown";
  const attentionItems = buildAttentionItems({ warnings, pendingApprovals, failedSupportJobs, connectorDegraded, providerState });

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Overview"
        description="Stay focused on product posture, operator blockers, and the next page to open. Deep diagnostics now live in Settings / Diagnostics."
        status={
          <>
            <WorkspaceStatusChip tone={attentionItems.length > 0 ? "warning" : "success"}>
              {attentionItems.length > 0 ? `${attentionItems.length} attention items` : "Ready"}
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} deployment warnings
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <button type="button" onClick={() => void app.refreshOverview()} disabled={app.overviewBusy}>
            {app.overviewBusy ? "Refreshing..." : "Refresh overview"}
          </button>
        )}
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard label="Runtime posture" value={attentionItems.length > 0 ? "Attention required" : "Ready"} detail={attentionItems[0] ?? "No immediate operator blockers are published."} tone={attentionItems.length > 0 ? "warning" : "success"} />
        <WorkspaceMetricCard label="Access posture" value={`${readString(deployment ?? {}, "mode") ?? "unknown"} / ${readString(deployment ?? {}, "bind_profile") ?? "n/a"}`} detail={warnings[0] ?? "Remote access posture looks stable."} tone={warnings.length > 0 ? "warning" : "default"} />
        <WorkspaceMetricCard label="Pending approvals" value={pendingApprovals} detail={pendingApprovals > 0 ? "Review sensitive actions before they block runs." : "Approval queue is clear."} tone={pendingApprovals > 0 ? "warning" : "success"} />
        <WorkspaceMetricCard label="Support failures" value={failedSupportJobs} detail={failedSupportJobs > 0 ? "Recent bundle jobs failed and may need follow-up." : "No failed support jobs are loaded."} tone={failedSupportJobs > 0 ? "danger" : "default"} />
      </section>

      {attentionItems.length > 0 ? (
        <WorkspaceInlineNotice title="Needs attention" tone={workspaceToneForState("warning")}>
          <ul className="console-compact-list">{attentionItems.map((item) => <li key={item}>{item}</li>)}</ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-two-column">
        <WorkspaceSectionCard title="Next workspace" description="Jump directly to the place that matches the current signal instead of navigating the full dashboard.">
          <div className="workspace-stack">
            <QuickAction label="Open chat" detail="Continue the active operator conversation." onClick={() => app.setSection("chat")} />
            <QuickAction label="Review approvals" detail="Process sensitive-action requests." onClick={() => app.setSection("approvals")} />
            <QuickAction label="Inspect channels" detail="Check connector health and router posture." onClick={() => app.setSection("channels")} />
            <QuickAction label="Open diagnostics" detail="Troubleshoot runtime state, audit, and CLI handoffs." onClick={() => app.setSection("operations")} />
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard title="Product posture" description="Keep only the high-signal operational posture here; lower-level details live on the dedicated settings pages.">
          {deployment === null ? (
            <WorkspaceEmptyState title="No deployment posture loaded" description="Refresh overview to load the current mode, bind profile, and auth gates." compact />
          ) : (
            <dl className="workspace-key-value-grid">
              <div><dt>Mode</dt><dd>{readString(deployment, "mode") ?? "n/a"}</dd></div>
              <div><dt>Bind profile</dt><dd>{readString(deployment, "bind_profile") ?? "n/a"}</dd></div>
              <div><dt>Admin auth</dt><dd>{readBool(deployment, "admin_auth_required") ? "required" : "unknown"}</dd></div>
              <div><dt>Remote bind</dt><dd>{readBool(deployment, "remote_bind_detected") ? "detected" : "not detected"}</dd></div>
              <div><dt>Provider auth</dt><dd>{providerState}</dd></div>
              <div><dt>Degraded connectors</dt><dd>{connectorDegraded}</dd></div>
            </dl>
          )}
        </WorkspaceSectionCard>
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
    <button type="button" className="workspace-action-button" onClick={onClick}>
      <strong>{label}</strong>
      <span>{detail}</span>
    </button>
  );
}

function buildAttentionItems({
  warnings,
  pendingApprovals,
  failedSupportJobs,
  connectorDegraded,
  providerState
}: {
  warnings: string[];
  pendingApprovals: number;
  failedSupportJobs: number;
  connectorDegraded: number;
  providerState: string;
}): string[] {
  const items = [...warnings];
  if (pendingApprovals > 0) items.push(`${pendingApprovals} approvals waiting for review.`);
  if (failedSupportJobs > 0) items.push(`${failedSupportJobs} support bundle jobs failed.`);
  if (connectorDegraded > 0) items.push(`${connectorDegraded} connectors are degraded.`);
  if (providerState === "degraded" || providerState === "expired" || providerState === "missing") {
    items.push(`Provider auth state is ${providerState}.`);
  }
  return items;
}
