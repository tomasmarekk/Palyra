import {
  ActionButton,
  ActionCluster,
  EmptyState,
  EntityTable,
  InlineNotice,
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import {
  PrettyJsonBlock,
  formatUnixMs,
  readNumber,
  readObject,
  readString,
  toStringArray,
  type JsonObject
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SupportSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "supportBusy"
    | "supportDeployment"
    | "supportDiagnosticsSnapshot"
    | "supportBundleRetainJobs"
    | "setSupportBundleRetainJobs"
    | "supportBundleJobs"
    | "supportSelectedBundleJobId"
    | "setSupportSelectedBundleJobId"
    | "supportSelectedBundleJob"
    | "refreshSupport"
    | "createSupportBundle"
    | "loadSupportBundleJob"
    | "setSection"
    | "revealSensitiveValues"
  >;
};

type SupportJobRow = {
  jobId: string;
  state: string;
  requestedAt: string;
};

export function SupportSection({ app }: SupportSectionProps) {
  const deployment = app.supportDeployment ?? {};
  const warnings = toStringArray(Array.isArray(deployment.warnings) ? deployment.warnings : []);
  const observability = readObject(app.supportDiagnosticsSnapshot ?? {}, "observability");
  const supportBundle = readObject(observability ?? {}, "support_bundle");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const recentFailures = toJsonObjectArray(observability?.recent_failures);
  const latestFailure = recentFailures[0] ?? null;
  const failedJobs = app.supportBundleJobs.filter((job) => readString(job, "state") === "failed");
  const providerAuthState = readString(providerAuth ?? {}, "state") ?? "unknown";
  const recoveryBacklog = readNumber(providerAuth ?? {}, "degraded_profiles") ?? 0;

  const supportJobRows: SupportJobRow[] = app.supportBundleJobs.map((job) => ({
    jobId: readString(job, "job_id") ?? "unknown",
    state: readString(job, "state") ?? "unknown",
    requestedAt: formatUnixMs(readUnixMillis(job, "requested_at_unix_ms"))
  }));

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Support"
        headingLabel="Support and Recovery"
        description="Queue support bundles, inspect recent failures, and move into diagnostics or recovery flows without relying on the desktop surface."
        status={
          <>
            <WorkspaceStatusChip tone={failedJobs.length > 0 ? "warning" : "success"}>
              {failedJobs.length} failed jobs
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={warnings.length > 0 ? "warning" : "default"}>
              {warnings.length} deployment warnings
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={latestFailure === null ? "default" : "warning"}>
              {latestFailure === null ? "No recent failure" : "Recent failure published"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionButton
            variant="secondary"
            onPress={() => void app.refreshSupport()}
            isDisabled={app.supportBusy}
          >
            {app.supportBusy ? "Refreshing..." : "Refresh support"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid">
        <WorkspaceMetricCard
          label="Support queue"
          value={app.supportBundleJobs.length}
          detail={
            app.supportBundleJobs[0] === undefined
              ? "No queued jobs"
              : readString(app.supportBundleJobs[0], "state") ?? "unknown"
          }
          tone={failedJobs.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Bundle reliability"
          value={formatRate(readNumber(supportBundle ?? {}, "success_rate_bps"))}
          detail={`${readString(supportBundle ?? {}, "attempts") ?? "0"} attempts`}
          tone={failedJobs.length > 0 ? "warning" : "success"}
        />
        <WorkspaceMetricCard
          label="Deployment posture"
          value={readString(deployment, "bind_profile") ?? "unknown"}
          detail={readString(deployment, "mode") ?? "Mode unavailable"}
        />
        <WorkspaceMetricCard
          label="Latest failure"
          value={
            latestFailure === null
              ? "None"
              : readString(latestFailure, "failure_class") ?? "Unknown"
          }
          detail={
            latestFailure === null
              ? "No recent failure signal."
              : readString(latestFailure, "operation") ?? "Operation unavailable"
          }
          tone={latestFailure === null ? "default" : "warning"}
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Queue support bundle"
          description="Support bundle work now lives here, with queue-backed execution that survives browser disconnects."
        >
          <div className="workspace-stack">
            <TextInputField
              label="Retain jobs"
              value={app.supportBundleRetainJobs}
              onChange={app.setSupportBundleRetainJobs}
            />
            <ActionCluster>
              <ActionButton
                onPress={() => void app.createSupportBundle()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? "Queueing..." : "Queue support bundle"}
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("operations")}>
                Open diagnostics
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("config")}>
                Open config
              </ActionButton>
            </ActionCluster>
            {warnings.length > 0 ? (
              <InlineNotice title="Current warnings" tone="warning">
                <ul className="console-compact-list">
                  {warnings.map((warning) => (
                    <li key={warning}>{warning}</li>
                  ))}
                </ul>
              </InlineNotice>
            ) : null}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Recent degraded signals"
          description="Keep the latest failure classes and messages close to support actions."
        >
          {latestFailure === null ? (
            <EmptyState
              compact
              title="No recent failures"
              description="No recent failures published by diagnostics."
            />
          ) : (
            <div className="workspace-stack">
              <InlineNotice title={readString(latestFailure, "failure_class") ?? "Unknown failure"} tone="danger">
                {readString(latestFailure, "operation") ?? "Operation unavailable"} ·{" "}
                {readString(latestFailure, "message_redacted") ??
                  readString(latestFailure, "message") ??
                  "No redacted message published."}
              </InlineNotice>
              <PrettyJsonBlock
                value={latestFailure}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            </div>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Provider auth recovery"
          description="Keep provider-auth degradation and next recovery motion visible next to support workflows."
        >
          <div className="workspace-stack">
            <div className="workspace-inline">
              <WorkspaceStatusChip
                tone={
                  providerAuthState === "missing" || providerAuthState === "expired"
                    ? "danger"
                    : providerAuthState === "degraded"
                      ? "warning"
                      : "success"
                }
              >
                {providerAuthState}
              </WorkspaceStatusChip>
              <WorkspaceStatusChip tone={recoveryBacklog > 0 ? "warning" : "default"}>
                {recoveryBacklog} degraded profiles
              </WorkspaceStatusChip>
            </div>
            <p className="chat-muted">
              Recovery stays explicit: move into diagnostics for current failures or auth/config
              settings when profile posture needs operator intervention.
            </p>
            <ActionCluster>
              <ActionButton variant="secondary" onPress={() => app.setSection("operations")}>
                Open diagnostics
              </ActionButton>
              <ActionButton variant="secondary" onPress={() => app.setSection("auth")}>
                Open auth profiles
              </ActionButton>
            </ActionCluster>
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Triage playbook"
          description="Keep the support handoff order visible so the dashboard stays the primary recovery surface."
        >
          <div className="workspace-stack">
            <ol className="workspace-bullet-list">
              <li>Check deployment warnings and provider auth state.</li>
              <li>Queue or load the latest support bundle job.</li>
              <li>Inspect diagnostics before changing config or auth posture.</li>
            </ol>
            <InlineNotice title="Reference" tone="default">
              docs/operations/observability-supportability-v1.md
            </InlineNotice>
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Queued jobs"
          description="Support bundle jobs remain visible after completion so operators can verify output paths and failure reasons."
        >
          <EntityTable
            ariaLabel="Support bundle jobs"
            columns={[
              {
                key: "job",
                label: "Job",
                isRowHeader: true,
                render: (row: SupportJobRow) => (
                  <div className="workspace-stack">
                    <strong>{row.jobId}</strong>
                    <span className="chat-muted">requested {row.requestedAt}</span>
                  </div>
                )
              },
              {
                key: "state",
                label: "State",
                render: (row: SupportJobRow) => (
                  <WorkspaceStatusChip tone={row.state === "failed" ? "danger" : "default"}>
                    {row.state}
                  </WorkspaceStatusChip>
                )
              },
              {
                key: "actions",
                label: "Actions",
                align: "end",
                render: (row: SupportJobRow) => (
                  <ActionButton
                    variant="secondary"
                    size="sm"
                    onPress={() => app.setSupportSelectedBundleJobId(row.jobId)}
                  >
                    Select
                  </ActionButton>
                )
              }
            ]}
            rows={supportJobRows}
            getRowId={(row) => row.jobId}
            emptyTitle="No support bundle jobs queued"
            emptyDescription="Queue a support bundle to inspect command output and artifact paths."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Selected job"
          description="Load command output, output path, and failure detail for the chosen support bundle job."
          actions={
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void app.loadSupportBundleJob()}
              isDisabled={app.supportBusy}
            >
              {app.supportBusy ? "Loading..." : "Load job"}
            </ActionButton>
          }
        >
          <div className="workspace-stack">
            <TextInputField
              label="Job ID"
              value={app.supportSelectedBundleJobId}
              onChange={app.setSupportSelectedBundleJobId}
            />

            {app.supportSelectedBundleJob === null ? (
              <EmptyState
                compact
                title="No support bundle job selected"
                description="Select a job and load it to inspect details."
              />
            ) : (
              <PrettyJsonBlock
                value={app.supportSelectedBundleJob}
                revealSensitiveValues={app.revealSensitiveValues}
              />
            )}
          </div>
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function formatRate(value: number | null): string {
  if (value === null) {
    return "n/a";
  }
  return `${(value / 100).toFixed(2)}%`;
}

function toJsonObjectArray(value: unknown): JsonObject[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((entry): entry is JsonObject => {
    return entry !== null && typeof entry === "object" && !Array.isArray(entry);
  });
}
