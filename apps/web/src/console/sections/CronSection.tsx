import { useMemo, useState } from "react";

import {
  ActionButton,
  ActionCluster,
  AppForm,
  EmptyState,
  EntityTable,
  KeyValueList,
  SelectField,
  TextAreaField,
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import { formatUnixMs, readBool, readString, type JsonObject } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type CronSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "cronBusy"
    | "cronJobs"
    | "cronRuns"
    | "cronJobId"
    | "setCronJobId"
    | "cronForm"
    | "setCronForm"
    | "refreshCron"
    | "createCron"
    | "toggleCron"
    | "runCronNow"
    | "refreshCronRuns"
  >;
};

type CronJobRow = {
  record: JsonObject;
  jobId: string;
  name: string;
  enabled: boolean;
  scheduleType: string;
  nextRun: string;
  isSelected: boolean;
};

type CronRunRow = {
  runId: string;
  status: string;
  startedAt: string;
  toolCalls: string;
};

const SCHEDULE_OPTIONS = [
  { key: "every", label: "every" },
  { key: "cron", label: "cron" },
  { key: "at", label: "at" }
] as const;

export function CronSection({ app }: CronSectionProps) {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const selectedJob =
    app.cronJobs.find((job) => readString(job, "job_id") === app.cronJobId) ?? app.cronJobs[0] ?? null;
  const enabledJobs = app.cronJobs.filter((job) => readBool(job, "enabled"));

  const jobRows = useMemo<CronJobRow[]>(
    () =>
      app.cronJobs.map((job) => {
        const jobId = readString(job, "job_id") ?? "unknown";
        return {
          record: job,
          jobId,
          name: readString(job, "name") ?? jobId,
          enabled: readBool(job, "enabled"),
          scheduleType: readString(job, "schedule_type") ?? "schedule unavailable",
          nextRun: formatUnixMs(readUnixMillis(job, "next_run_at_unix_ms")),
          isSelected: selectedJob !== null && readString(selectedJob, "job_id") === jobId
        };
      }),
    [app.cronJobs, selectedJob]
  );

  const runRows = useMemo<CronRunRow[]>(
    () =>
      app.cronRuns.map((run) => ({
        runId: readString(run, "run_id") ?? "unknown",
        status: readString(run, "status") ?? "unknown",
        startedAt: formatUnixMs(readUnixMillis(run, "started_at_unix_ms")),
        toolCalls: readString(run, "tool_calls") ?? "0"
      })),
    [app.cronRuns]
  );

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Control"
        title="Automation"
        headingLabel="Cron"
        description="Keep scheduled prompt execution scannable first, then open create or run-history detail only when you need it."
        status={
          <>
            <WorkspaceStatusChip tone="default">{app.cronJobs.length} jobs</WorkspaceStatusChip>
            <WorkspaceStatusChip tone={enabledJobs.length > 0 ? "success" : "default"}>
              {enabledJobs.length} enabled
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={selectedJob === null ? "default" : "success"}>
              {selectedJob === null ? "No selection" : "Job selected"}
            </WorkspaceStatusChip>
          </>
        }
        actions={
          <ActionCluster>
            <ActionButton
              variant="secondary"
              onPress={() => setShowCreateForm((current) => !current)}
            >
              {showCreateForm ? "Hide create form" : "New automation"}
            </ActionButton>
            <ActionButton
              variant="secondary"
              onPress={() => void app.refreshCron()}
              isDisabled={app.cronBusy}
            >
              {app.cronBusy ? "Refreshing..." : "Refresh automation"}
            </ActionButton>
          </ActionCluster>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Active schedules"
          value={enabledJobs.length}
          detail={
            enabledJobs.length === 0
              ? "No enabled automation jobs."
              : "Jobs are eligible to run on schedule."
          }
          tone={enabledJobs.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Paused schedules"
          value={app.cronJobs.length - enabledJobs.length}
          detail="Disabled jobs stay visible for maintenance and audit context."
        />
        <WorkspaceMetricCard
          label="Selected job"
          value={
            selectedJob === null
              ? "None"
              : readString(selectedJob, "name") ?? readString(selectedJob, "job_id") ?? "Unknown"
          }
          detail={
            selectedJob === null
              ? "Choose a job to inspect next run windows and recent runs."
              : readString(selectedJob, "schedule_type") ?? "Schedule type unavailable"
          }
        />
      </section>

      <section className="workspace-two-column">
        <WorkspaceSectionCard
          title="Automation jobs"
          description="Scan the current schedule set first, then dispatch run-now or toggle enablement per job."
        >
          <EntityTable
            ariaLabel="Automation jobs"
            columns={[
              {
                key: "job",
                label: "Job",
                isRowHeader: true,
                render: (row: CronJobRow) => (
                  <div className="workspace-stack">
                    <strong>{row.name}</strong>
                    <span className="chat-muted">
                      {row.scheduleType} · next {row.nextRun}
                    </span>
                  </div>
                )
              },
              {
                key: "state",
                label: "State",
                render: (row: CronJobRow) => (
                  <div className="workspace-inline">
                    <WorkspaceStatusChip tone={row.enabled ? "success" : "default"}>
                      {row.enabled ? "enabled" : "disabled"}
                    </WorkspaceStatusChip>
                    {row.isSelected ? (
                      <WorkspaceStatusChip tone="accent">selected</WorkspaceStatusChip>
                    ) : null}
                  </div>
                )
              },
              {
                key: "actions",
                label: "Actions",
                align: "end",
                render: (row: CronJobRow) => (
                  <ActionCluster>
                    <ActionButton
                      aria-label={`Select ${row.name}`}
                      variant="secondary"
                      size="sm"
                      onPress={() => app.setCronJobId(row.jobId)}
                    >
                      Select
                    </ActionButton>
                    <ActionButton
                      aria-label={`${row.enabled ? "Disable" : "Enable"} ${row.name}`}
                      size="sm"
                      onPress={() => void app.toggleCron(row.record, !row.enabled)}
                      isDisabled={app.cronBusy}
                    >
                      {row.enabled ? "Disable" : "Enable"}
                    </ActionButton>
                    <ActionButton
                      aria-label={`Run ${row.name} now`}
                      size="sm"
                      variant="secondary"
                      onPress={() => void app.runCronNow(row.record)}
                      isDisabled={app.cronBusy}
                    >
                      Run now
                    </ActionButton>
                  </ActionCluster>
                )
              }
            ]}
            rows={jobRows}
            getRowId={(row) => row.jobId}
            emptyTitle="No automation jobs configured"
            emptyDescription="Create the first automation job to schedule prompt execution."
          />
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Create automation"
          description="The create form stays hidden until explicitly needed so the workspace remains list-first."
        >
          {!showCreateForm ? (
            <EmptyState
              compact
              title="Create form hidden"
              description="Open the create form only when you are ready to add a new scheduled prompt."
              action={
                <ActionButton variant="secondary" onPress={() => setShowCreateForm(true)}>
                  Open create form
                </ActionButton>
              }
            />
          ) : (
            <AppForm onSubmit={(event) => void app.createCron(event)}>
              <div className="workspace-form-grid">
                <TextInputField
                  label="Name"
                  value={app.cronForm.name}
                  onChange={(name) =>
                    app.setCronForm((previous) => ({ ...previous, name }))
                  }
                />
                <TextInputField
                  label="Channel"
                  value={app.cronForm.channel}
                  onChange={(channel) =>
                    app.setCronForm((previous) => ({ ...previous, channel }))
                  }
                />
                <SelectField
                  label="Schedule type"
                  value={app.cronForm.scheduleType}
                  onChange={(scheduleType) =>
                    app.setCronForm((previous) => ({
                      ...previous,
                      scheduleType: scheduleType as "cron" | "every" | "at"
                    }))
                  }
                  options={SCHEDULE_OPTIONS}
                />
                <TextInputField
                  label="Every interval (ms)"
                  value={app.cronForm.everyIntervalMs}
                  onChange={(everyIntervalMs) =>
                    app.setCronForm((previous) => ({ ...previous, everyIntervalMs }))
                  }
                />
                <TextInputField
                  label="Cron expression"
                  value={app.cronForm.cronExpression}
                  onChange={(cronExpression) =>
                    app.setCronForm((previous) => ({ ...previous, cronExpression }))
                  }
                />
                <TextInputField
                  label="At timestamp"
                  value={app.cronForm.atTimestampRfc3339}
                  onChange={(atTimestampRfc3339) =>
                    app.setCronForm((previous) => ({ ...previous, atTimestampRfc3339 }))
                  }
                />
              </div>

              <TextAreaField
                label="Prompt"
                rows={4}
                value={app.cronForm.prompt}
                onChange={(prompt) => app.setCronForm((previous) => ({ ...previous, prompt }))}
              />

              <ActionCluster>
                <ActionButton type="submit" isDisabled={app.cronBusy}>
                  {app.cronBusy ? "Creating..." : "Create automation"}
                </ActionButton>
                <ActionButton
                  type="button"
                  variant="secondary"
                  onPress={() => setShowCreateForm(false)}
                >
                  Collapse
                </ActionButton>
              </ActionCluster>
            </AppForm>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column workspace-two-column--history">
        <WorkspaceSectionCard
          title="Selected automation"
          description="Keep the currently selected job in view before loading its recent runs."
        >
          {selectedJob === null ? (
            <EmptyState
              compact
              title="No job selected"
              description="Select a job to inspect schedule and recent execution history."
            />
          ) : (
            <KeyValueList
              items={[
                { label: "Job ID", value: readString(selectedJob, "job_id") ?? "n/a" },
                {
                  label: "Schedule type",
                  value: readString(selectedJob, "schedule_type") ?? "n/a"
                },
                {
                  label: "Next run",
                  value: formatUnixMs(readUnixMillis(selectedJob, "next_run_at_unix_ms"))
                },
                {
                  label: "Last run",
                  value: formatUnixMs(readUnixMillis(selectedJob, "last_run_at_unix_ms"))
                },
                {
                  label: "Owner",
                  value: readString(selectedJob, "owner_principal") ?? "n/a"
                },
                {
                  label: "Channel",
                  value: readString(selectedJob, "channel") ?? "n/a"
                }
              ]}
            />
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Recent runs"
          description="Inspect recent automation outcomes without turning the page into a raw JSON dump."
          actions={
            <ActionButton
              variant="secondary"
              size="sm"
              onPress={() => void app.refreshCronRuns()}
              isDisabled={app.cronBusy || app.cronJobId.trim().length === 0}
            >
              {app.cronBusy ? "Loading..." : "Load selected runs"}
            </ActionButton>
          }
        >
          <EntityTable
            ariaLabel="Automation run history"
            columns={[
              {
                key: "run",
                label: "Run",
                isRowHeader: true,
                render: (row: CronRunRow) => (
                  <div className="workspace-stack">
                    <strong>{row.runId}</strong>
                    <span className="chat-muted">started {row.startedAt}</span>
                  </div>
                )
              },
              {
                key: "status",
                label: "Status",
                render: (row: CronRunRow) => (
                  <WorkspaceStatusChip
                    tone={row.status === "succeeded" ? "success" : "warning"}
                  >
                    {row.status}
                  </WorkspaceStatusChip>
                )
              },
              {
                key: "tools",
                label: "Tool calls",
                render: (row: CronRunRow) => `${row.toolCalls} tool calls`
              }
            ]}
            rows={runRows}
            getRowId={(row) => row.runId}
            emptyTitle="No run history loaded"
            emptyDescription="Load runs for the selected job to inspect recent automation outcomes."
          />
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
