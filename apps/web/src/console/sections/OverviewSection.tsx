import { ActionButton } from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  workspaceToneForState
} from "../components/workspace/WorkspacePatterns";
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
  const failedSupportJobs = app.overviewSupportJobs.filter(
    (job) => readString(job, "state") === "failed"
  ).length;
  const connectorDegraded = Number(readString(connector ?? {}, "degraded_connectors") ?? "0");
  const providerState =
    readString(providerAuth ?? {}, "state") ??
    readString(readObject(diagnostics ?? {}, "auth_profiles") ?? {}, "state") ??
    "unknown";
  const attentionItems = buildAttentionItems({
    warnings,
    pendingApprovals,
    failedSupportJobs,
    connectorDegraded,
    providerState
  });

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
        actions={
          <ActionButton
            isDisabled={app.overviewBusy}
            type="button"
            variant="primary"
            onPress={() => void app.refreshOverview()}
          >
            {app.overviewBusy ? "Refreshing..." : "Refresh overview"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          detail={attentionItems[0] ?? "No immediate operator blockers are published."}
          label="Runtime posture"
          tone={attentionItems.length > 0 ? "warning" : "success"}
          value={attentionItems.length > 0 ? "Attention required" : "Ready"}
        />
        <WorkspaceMetricCard
          detail={warnings[0] ?? "Remote access posture looks stable."}
          label="Access posture"
          tone={warnings.length > 0 ? "warning" : "default"}
          value={`${readString(deployment ?? {}, "mode") ?? "unknown"} / ${readString(deployment ?? {}, "bind_profile") ?? "n/a"}`}
        />
        <WorkspaceMetricCard
          detail={
            pendingApprovals > 0
              ? "Review sensitive actions before they block runs."
              : "Approval queue is clear."
          }
          label="Pending approvals"
          tone={pendingApprovals > 0 ? "warning" : "success"}
          value={pendingApprovals}
        />
        <WorkspaceMetricCard
          detail={
            failedSupportJobs > 0
              ? "Recent bundle jobs failed and may need follow-up."
              : "No failed support jobs are loaded."
          }
          label="Support failures"
          tone={failedSupportJobs > 0 ? "danger" : "default"}
          value={failedSupportJobs}
        />
      </section>

      {attentionItems.length > 0 ? (
        <WorkspaceInlineNotice title="Needs attention" tone={workspaceToneForState("warning")}>
          <ul className="console-compact-list">
            {attentionItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </WorkspaceInlineNotice>
      ) : null}

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          description="Jump directly to the place that matches the current signal instead of navigating the full dashboard."
          title="Next workspace"
        >
          <div className="workspace-stack">
            <QuickAction
              detail="Continue the active operator conversation."
              label="Open chat"
              onClick={() => app.setSection("chat")}
            />
            <QuickAction
              detail="Process sensitive-action requests."
              label="Review approvals"
              onClick={() => app.setSection("approvals")}
            />
            <QuickAction
              detail="Check connector health and router posture."
              label="Inspect channels"
              onClick={() => app.setSection("channels")}
            />
            <QuickAction
              detail="Troubleshoot runtime state, audit, and CLI handoffs."
              label="Open diagnostics"
              onClick={() => app.setSection("operations")}
            />
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          description="Keep only the high-signal operational posture here; lower-level details live on the dedicated settings pages."
          title="Product posture"
        >
          {deployment === null ? (
            <WorkspaceEmptyState
              compact
              description="Refresh overview to load the current mode, bind profile, and auth gates."
              title="No deployment posture loaded"
            />
          ) : (
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Mode</dt>
                <dd>{readString(deployment, "mode") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Bind profile</dt>
                <dd>{readString(deployment, "bind_profile") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Admin auth</dt>
                <dd>{readBool(deployment, "admin_auth_required") ? "required" : "unknown"}</dd>
              </div>
              <div>
                <dt>Remote bind</dt>
                <dd>{readBool(deployment, "remote_bind_detected") ? "detected" : "not detected"}</dd>
              </div>
              <div>
                <dt>Provider auth</dt>
                <dd>{providerState}</dd>
              </div>
              <div>
                <dt>Degraded connectors</dt>
                <dd>{connectorDegraded}</dd>
              </div>
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
    <ActionButton
      className="workspace-action-button"
      fullWidth
      type="button"
      variant="ghost"
      onPress={onClick}
    >
      <span className="flex flex-col items-start gap-1 text-left">
        <strong>{label}</strong>
        <span>{detail}</span>
      </span>
    </ActionButton>
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
