import {
  ActionButton,
  ActionCluster,
  CheckboxField,
  EmptyState,
  EntityTable,
  InlineNotice,
  TextInputField,
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip,
} from "../components/workspace/WorkspaceChrome";
import {
  PrettyJsonBlock,
  formatUnixMs,
  readNumber,
  readObject,
  readString,
  toStringArray,
  type JsonObject,
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
    | "supportDoctorRetainJobs"
    | "setSupportDoctorRetainJobs"
    | "supportDoctorOnly"
    | "setSupportDoctorOnly"
    | "supportDoctorSkip"
    | "setSupportDoctorSkip"
    | "supportDoctorRollbackRunId"
    | "setSupportDoctorRollbackRunId"
    | "supportDoctorForce"
    | "setSupportDoctorForce"
    | "supportDoctorJobs"
    | "supportSelectedDoctorJobId"
    | "setSupportSelectedDoctorJobId"
    | "supportSelectedDoctorJob"
    | "refreshSupport"
    | "createSupportBundle"
    | "loadSupportBundleJob"
    | "queueDoctorRecoveryPreview"
    | "queueDoctorRecoveryApply"
    | "queueDoctorRollbackPreview"
    | "queueDoctorRollbackApply"
    | "loadDoctorRecoveryJob"
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
  const doctorRecovery = readObject(observability ?? {}, "doctor_recovery");
  const providerAuth = readObject(observability ?? {}, "provider_auth");
  const recentFailures = toJsonObjectArray(observability?.recent_failures);
  const latestFailure = recentFailures[0] ?? null;
  const failedJobs = app.supportBundleJobs.filter((job) => readString(job, "state") === "failed");
  const failedDoctorJobs = app.supportDoctorJobs.filter(
    (job) => readString(job, "state") === "failed",
  );
  const providerAuthState = readString(providerAuth ?? {}, "state") ?? "unknown";
  const recoveryBacklog = readNumber(providerAuth ?? {}, "degraded_profiles") ?? 0;
  const latestDoctorRecovery = readObject(doctorRecovery ?? {}, "last_job");
  const latestDoctorRecoveryState = readString(latestDoctorRecovery ?? {}, "state") ?? "unknown";
  const selectedDoctorReport = readObject(app.supportSelectedDoctorJob ?? {}, "report");
  const selectedDoctorRecovery = readObject(selectedDoctorReport ?? {}, "recovery");
  const selectedDoctorPlannedSteps = toJsonObjectArray(selectedDoctorRecovery?.planned_steps);
  const selectedDoctorAppliedSteps = toJsonObjectArray(selectedDoctorRecovery?.applied_steps);
  const selectedDoctorNextSteps = toStringArray(
    Array.isArray(selectedDoctorRecovery?.next_steps) ? selectedDoctorRecovery.next_steps : [],
  );

  const supportJobRows: SupportJobRow[] = app.supportBundleJobs.map((job) => ({
    jobId: readString(job, "job_id") ?? "unknown",
    state: readString(job, "state") ?? "unknown",
    requestedAt: formatUnixMs(readUnixMillis(job, "requested_at_unix_ms")),
  }));
  const recoveryJobRows: SupportJobRow[] = app.supportDoctorJobs.map((job) => ({
    jobId: readString(job, "job_id") ?? "unknown",
    state: readString(job, "state") ?? "unknown",
    requestedAt: formatUnixMs(readUnixMillis(job, "requested_at_unix_ms")),
  }));

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Support"
        headingLabel="Support and Recovery"
        description="Queue support bundles, inspect queued doctor recovery plans, and move into rollback or diagnostics without leaving the dashboard."
        status={
          <>
            <WorkspaceStatusChip tone={failedJobs.length > 0 ? "warning" : "success"}>
              {failedJobs.length} failed bundle jobs
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={failedDoctorJobs.length > 0 ? "warning" : "default"}>
              {failedDoctorJobs.length} failed recovery jobs
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
              : (readString(app.supportBundleJobs[0], "state") ?? "unknown")
          }
          tone={failedJobs.length > 0 ? "warning" : "default"}
        />
        <WorkspaceMetricCard
          label="Recovery queue"
          value={app.supportDoctorJobs.length}
          detail={
            latestDoctorRecovery === null
              ? "No recovery jobs"
              : `${latestDoctorRecoveryState} · ${readString(latestDoctorRecovery, "mode") ?? "mode unavailable"}`
          }
          tone={failedDoctorJobs.length > 0 ? "warning" : "default"}
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
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Queue support bundle"
          description="Support bundle work remains queue-backed so command execution survives browser disconnects."
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
          title="Doctor recovery planner"
          description="Queue repair previews, apply changes, or rehearse rollback against a recorded recovery run."
        >
          <div className="workspace-stack">
            <div className="workspace-form-grid">
              <TextInputField
                label="Retain recovery jobs"
                value={app.supportDoctorRetainJobs}
                onChange={app.setSupportDoctorRetainJobs}
              />
              <TextInputField
                label="Rollback run ID"
                value={app.supportDoctorRollbackRunId}
                onChange={app.setSupportDoctorRollbackRunId}
                description="Required only for rollback preview/apply."
              />
              <TextInputField
                label="Only checks"
                value={app.supportDoctorOnly}
                onChange={app.setSupportDoctorOnly}
                description="Comma or newline separated doctor step IDs."
              />
              <TextInputField
                label="Skip checks"
                value={app.supportDoctorSkip}
                onChange={app.setSupportDoctorSkip}
                description="Comma or newline separated doctor step IDs."
              />
            </div>
            <CheckboxField
              label="Force destructive recovery paths"
              description="Needed only when rollback hash validation or destructive repair steps require explicit operator acknowledgement."
              checked={app.supportDoctorForce}
              onChange={app.setSupportDoctorForce}
            />
            <ActionCluster>
              <ActionButton
                onPress={() => void app.queueDoctorRecoveryPreview()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? "Queueing..." : "Queue preview"}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRecoveryApply()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? "Queueing..." : "Apply repairs"}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRollbackPreview()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? "Queueing..." : "Preview rollback"}
              </ActionButton>
              <ActionButton
                variant="secondary"
                onPress={() => void app.queueDoctorRollbackApply()}
                isDisabled={app.supportBusy}
              >
                {app.supportBusy ? "Queueing..." : "Apply rollback"}
              </ActionButton>
            </ActionCluster>
            {latestDoctorRecovery === null ? null : (
              <InlineNotice title="Latest recovery job" tone="default">
                {readString(latestDoctorRecovery, "mode") ?? "unknown mode"} ·{" "}
                {latestDoctorRecoveryState} · planned{" "}
                {readNumber(latestDoctorRecovery, "planned_step_count") ?? 0} / applied{" "}
                {readNumber(latestDoctorRecovery, "applied_step_count") ?? 0}
              </InlineNotice>
            )}
          </div>
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
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
              <InlineNotice
                title={readString(latestFailure, "failure_class") ?? "Unknown failure"}
                tone="danger"
              >
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
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Triage playbook"
          description="Keep the support handoff order visible so the dashboard stays the primary recovery surface."
        >
          <div className="workspace-stack">
            <ol className="workspace-bullet-list">
              <li>Check deployment warnings and provider auth state.</li>
              <li>Queue a doctor preview before applying repair or rollback.</li>
              <li>Load the latest support bundle and recovery jobs to inspect command output.</li>
              <li>Inspect diagnostics before changing config or auth posture.</li>
            </ol>
            <InlineNotice title="Reference" tone="default">
              docs-codebase/docs-tree/web_console_operator_dashboard/console_sections_and_navigation/support_recovery.md
            </InlineNotice>
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Latest recovery summary"
          description="Surface the latest published doctor summary directly from diagnostics."
        >
          {latestDoctorRecovery === null ? (
            <EmptyState
              compact
              title="No recovery summary published"
              description="Queue a doctor preview to populate recovery telemetry."
            />
          ) : (
            <PrettyJsonBlock
              value={latestDoctorRecovery}
              revealSensitiveValues={app.revealSensitiveValues}
            />
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Queued bundle jobs"
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
                ),
              },
              {
                key: "state",
                label: "State",
                render: (row: SupportJobRow) => (
                  <WorkspaceStatusChip tone={row.state === "failed" ? "danger" : "default"}>
                    {row.state}
                  </WorkspaceStatusChip>
                ),
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
                ),
              },
            ]}
            rows={supportJobRows}
            getRowId={(row) => row.jobId}
            emptyTitle="No support bundle jobs queued"
            emptyDescription="Queue a support bundle to inspect command output and artifact paths."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Selected bundle job"
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

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Recovery jobs"
          description="Queue-backed doctor runs keep preview/apply/rollback history visible after the browser disconnects."
        >
          <EntityTable
            ariaLabel="Doctor recovery jobs"
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
                ),
              },
              {
                key: "state",
                label: "State",
                render: (row: SupportJobRow) => (
                  <WorkspaceStatusChip tone={row.state === "failed" ? "danger" : "default"}>
                    {row.state}
                  </WorkspaceStatusChip>
                ),
              },
              {
                key: "actions",
                label: "Actions",
                align: "end",
                render: (row: SupportJobRow) => (
                  <ActionButton
                    variant="secondary"
                    size="sm"
                    onPress={() => app.setSupportSelectedDoctorJobId(row.jobId)}
                  >
                    Select
                  </ActionButton>
                ),
              },
            ]}
            rows={recoveryJobRows}
            getRowId={(row) => row.jobId}
            emptyTitle="No recovery jobs queued"
            emptyDescription="Queue a doctor preview or rollback rehearsal to inspect the recovery plan."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Selected recovery job"
          description="Load the selected doctor job to inspect parsed recovery output, available rollback runs, and command stderr/stdout."
          actions={
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void app.loadDoctorRecoveryJob()}
              isDisabled={app.supportBusy}
            >
              {app.supportBusy ? "Loading..." : "Load recovery job"}
            </ActionButton>
          }
        >
          <div className="workspace-stack">
            <TextInputField
              label="Recovery job ID"
              value={app.supportSelectedDoctorJobId}
              onChange={app.setSupportSelectedDoctorJobId}
            />

            {app.supportSelectedDoctorJob === null ? (
              <EmptyState
                compact
                title="No recovery job selected"
                description="Select a doctor recovery job and load it to inspect details."
              />
            ) : (
              <>
                {selectedDoctorRecovery === null ? null : (
                  <div className="workspace-stack">
                    <InlineNotice
                      title={readString(selectedDoctorReport ?? {}, "mode") ?? "Recovery summary"}
                      tone={
                        readString(app.supportSelectedDoctorJob, "state") === "failed"
                          ? "danger"
                          : "default"
                      }
                    >
                      run {readString(selectedDoctorRecovery, "run_id") ?? "preview-only"} ·
                      planned {selectedDoctorPlannedSteps.length} · applied{" "}
                      {selectedDoctorAppliedSteps.length}
                    </InlineNotice>
                    {selectedDoctorPlannedSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Planned steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorPlannedSteps.map((step, index) => (
                            <li key={`${readString(step, "id") ?? "planned"}-${index}`}>
                              {readString(step, "title") ?? readString(step, "id") ?? "Unnamed step"}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    {selectedDoctorAppliedSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Applied steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorAppliedSteps.map((step, index) => (
                            <li key={`${readString(step, "id") ?? "applied"}-${index}`}>
                              {(readString(step, "message") ?? readString(step, "id") ?? "Unnamed step")}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                    {selectedDoctorNextSteps.length > 0 ? (
                      <div className="workspace-stack">
                        <strong>Next steps</strong>
                        <ul className="console-compact-list">
                          {selectedDoctorNextSteps.map((step) => (
                            <li key={step}>{step}</li>
                          ))}
                        </ul>
                      </div>
                    ) : null}
                  </div>
                )}
                <PrettyJsonBlock
                  value={app.supportSelectedDoctorJob}
                  revealSensitiveValues={app.revealSensitiveValues}
                />
              </>
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
