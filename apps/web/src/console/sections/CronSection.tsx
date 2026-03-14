import { Button } from "@heroui/react";
import { useState } from "react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
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

export function CronSection({ app }: CronSectionProps) {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const selectedJob =
    app.cronJobs.find((job) => readString(job, "job_id") === app.cronJobId) ?? app.cronJobs[0] ?? null;
  const enabledJobs = app.cronJobs.filter((job) => readBool(job, "enabled"));

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
          <div className="console-inline-actions">
            <Button
              variant="secondary"
              onPress={() => setShowCreateForm((current) => !current)}
            >
              {showCreateForm ? "Hide create form" : "New automation"}
            </Button>
            <Button
              variant="secondary"
              onPress={() => void app.refreshCron()}
              isDisabled={app.cronBusy}
            >
              {app.cronBusy ? "Refreshing..." : "Refresh automation"}
            </Button>
          </div>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Active schedules"
          value={enabledJobs.length}
          detail={enabledJobs.length === 0 ? "No enabled automation jobs." : "Jobs are eligible to run on schedule."}
          tone={enabledJobs.length > 0 ? "success" : "default"}
        />
        <WorkspaceMetricCard
          label="Paused schedules"
          value={app.cronJobs.length - enabledJobs.length}
          detail="Disabled jobs stay visible for maintenance and audit context."
        />
        <WorkspaceMetricCard
          label="Selected job"
          value={selectedJob === null ? "None" : readString(selectedJob, "name") ?? readString(selectedJob, "job_id") ?? "Unknown"}
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
          <div className="workspace-list">
            {app.cronJobs.length === 0 ? (
              <p className="chat-muted">No automation jobs configured yet.</p>
            ) : (
              app.cronJobs.map((job) => {
                const jobId = readString(job, "job_id") ?? "unknown";
                const enabled = readBool(job, "enabled");
                const isSelected = selectedJob !== null && readString(selectedJob, "job_id") === jobId;
                return (
                  <article key={jobId} className={`workspace-list-item workspace-list-item--job${isSelected ? " is-active" : ""}`}>
                    <button
                      type="button"
                      className="workspace-list-button workspace-list-button--flat"
                      onClick={() => app.setCronJobId(jobId)}
                    >
                      <div>
                        <strong>{readString(job, "name") ?? jobId}</strong>
                        <p className="chat-muted">
                          {readString(job, "schedule_type") ?? "schedule unavailable"} · next{" "}
                          {formatUnixMs(readUnixMillis(job, "next_run_at_unix_ms"))}
                        </p>
                      </div>
                      <WorkspaceStatusChip tone={enabled ? "success" : "default"}>
                        {enabled ? "enabled" : "disabled"}
                      </WorkspaceStatusChip>
                    </button>
                    <div className="console-inline-actions">
                      <Button
                        variant="secondary"
                        size="sm"
                        onPress={() => app.setCronJobId(jobId)}
                      >
                        Select
                      </Button>
                      <Button
                        size="sm"
                        onPress={() => void app.toggleCron(job, !enabled)}
                        isDisabled={app.cronBusy}
                      >
                        {enabled ? "Disable" : "Enable"}
                      </Button>
                      <Button
                        size="sm"
                        variant="secondary"
                        onPress={() => void app.runCronNow(job)}
                        isDisabled={app.cronBusy}
                      >
                        Run now
                      </Button>
                    </div>
                  </article>
                );
              })
            )}
          </div>
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Create automation"
          description="The create form stays hidden until explicitly needed so the workspace remains list-first."
        >
          {!showCreateForm ? (
            <div className="workspace-callout">
              <p className="chat-muted">
                Open the create form only when you are ready to add a new scheduled prompt.
              </p>
              <Button variant="secondary" onPress={() => setShowCreateForm(true)}>
                Open create form
              </Button>
            </div>
          ) : (
            <form className="workspace-form" onSubmit={(event) => void app.createCron(event)}>
              <div className="workspace-form-grid">
                <label>
                  Name
                  <input
                    value={app.cronForm.name}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({ ...previous, name: event.target.value }))
                    }
                  />
                </label>
                <label>
                  Channel
                  <input
                    value={app.cronForm.channel}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({ ...previous, channel: event.target.value }))
                    }
                  />
                </label>
                <label>
                  Schedule type
                  <select
                    value={app.cronForm.scheduleType}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({
                        ...previous,
                        scheduleType: event.target.value as "cron" | "every" | "at"
                      }))
                    }
                  >
                    <option value="every">every</option>
                    <option value="cron">cron</option>
                    <option value="at">at</option>
                  </select>
                </label>
                <label>
                  Every interval (ms)
                  <input
                    value={app.cronForm.everyIntervalMs}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({
                        ...previous,
                        everyIntervalMs: event.target.value
                      }))
                    }
                  />
                </label>
                <label>
                  Cron expression
                  <input
                    value={app.cronForm.cronExpression}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({
                        ...previous,
                        cronExpression: event.target.value
                      }))
                    }
                  />
                </label>
                <label>
                  At timestamp
                  <input
                    value={app.cronForm.atTimestampRfc3339}
                    onChange={(event) =>
                      app.setCronForm((previous) => ({
                        ...previous,
                        atTimestampRfc3339: event.target.value
                      }))
                    }
                  />
                </label>
              </div>

              <label>
                Prompt
                <textarea
                  rows={4}
                  value={app.cronForm.prompt}
                  onChange={(event) =>
                    app.setCronForm((previous) => ({ ...previous, prompt: event.target.value }))
                  }
                />
              </label>

              <div className="console-inline-actions">
                <Button type="submit" isDisabled={app.cronBusy}>
                  {app.cronBusy ? "Creating..." : "Create automation"}
                </Button>
                <Button variant="secondary" onPress={() => setShowCreateForm(false)}>
                  Collapse
                </Button>
              </div>
            </form>
          )}
        </WorkspaceSectionCard>
      </section>

      <section className="workspace-two-column workspace-two-column--history">
        <WorkspaceSectionCard
          title="Selected automation"
          description="Keep the currently selected job in view before loading its recent runs."
        >
          {selectedJob === null ? (
            <p className="chat-muted">Select a job to inspect schedule and recent execution history.</p>
          ) : (
            <dl className="workspace-key-value-grid">
              <div>
                <dt>Job ID</dt>
                <dd>{readString(selectedJob, "job_id") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Schedule type</dt>
                <dd>{readString(selectedJob, "schedule_type") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Next run</dt>
                <dd>{formatUnixMs(readUnixMillis(selectedJob, "next_run_at_unix_ms"))}</dd>
              </div>
              <div>
                <dt>Last run</dt>
                <dd>{formatUnixMs(readUnixMillis(selectedJob, "last_run_at_unix_ms"))}</dd>
              </div>
              <div>
                <dt>Owner</dt>
                <dd>{readString(selectedJob, "owner_principal") ?? "n/a"}</dd>
              </div>
              <div>
                <dt>Channel</dt>
                <dd>{readString(selectedJob, "channel") ?? "n/a"}</dd>
              </div>
            </dl>
          )}
        </WorkspaceSectionCard>

        <WorkspaceSectionCard
          title="Recent runs"
          description="Inspect recent automation outcomes without turning the page into a raw JSON dump."
          actions={
            <Button
              variant="secondary"
              size="sm"
              onPress={() => void app.refreshCronRuns()}
              isDisabled={app.cronBusy || app.cronJobId.trim().length === 0}
            >
              {app.cronBusy ? "Loading..." : "Load selected runs"}
            </Button>
          }
        >
          {app.cronRuns.length === 0 ? (
            <p className="chat-muted">No run history loaded for the selected job.</p>
          ) : (
            <div className="workspace-list">
              {app.cronRuns.map((run) => {
                const runId = readString(run, "run_id") ?? "unknown";
                return (
                  <article key={runId} className="workspace-list-item">
                    <div>
                      <strong>{runId}</strong>
                      <p className="chat-muted">
                        {readString(run, "status") ?? "unknown"} · started{" "}
                        {formatUnixMs(readUnixMillis(run, "started_at_unix_ms"))}
                      </p>
                    </div>
                    <div className="workspace-inline">
                      <WorkspaceStatusChip
                        tone={readString(run, "status") === "succeeded" ? "success" : "warning"}
                      >
                        {readString(run, "status") ?? "unknown"}
                      </WorkspaceStatusChip>
                      <WorkspaceStatusChip tone="default">
                        {readString(run, "tool_calls") ?? "0"} tool calls
                      </WorkspaceStatusChip>
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </WorkspaceSectionCard>
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
