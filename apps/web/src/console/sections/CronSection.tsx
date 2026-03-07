import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { readBool, readString, toPrettyJson } from "../shared";
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
    | "revealSensitiveValues"
  >;
};

export function CronSection({ app }: CronSectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Cron"
        description="Create schedules, toggle jobs, dispatch run-now, and inspect recent run logs from one operator surface."
        actions={(
          <button type="button" onClick={() => void app.refreshCron()} disabled={app.cronBusy}>
            {app.cronBusy ? "Refreshing..." : "Refresh cron"}
          </button>
        )}
      />

      <form className="console-form" onSubmit={(event) => void app.createCron(event)}>
        <div className="console-grid-3">
          <label>
            Name
            <input value={app.cronForm.name} onChange={(event) => app.setCronForm((previous) => ({ ...previous, name: event.target.value }))} />
          </label>
          <label>
            Prompt
            <textarea value={app.cronForm.prompt} onChange={(event) => app.setCronForm((previous) => ({ ...previous, prompt: event.target.value }))} rows={3} />
          </label>
          <label>
            Channel
            <input value={app.cronForm.channel} onChange={(event) => app.setCronForm((previous) => ({ ...previous, channel: event.target.value }))} />
          </label>
        </div>
        <div className="console-grid-4">
          <label>
            Schedule type
            <select value={app.cronForm.scheduleType} onChange={(event) => app.setCronForm((previous) => ({ ...previous, scheduleType: event.target.value as "cron" | "every" | "at" }))}>
              <option value="every">every</option>
              <option value="cron">cron</option>
              <option value="at">at</option>
            </select>
          </label>
          <label>
            Every interval (ms)
            <input value={app.cronForm.everyIntervalMs} onChange={(event) => app.setCronForm((previous) => ({ ...previous, everyIntervalMs: event.target.value }))} />
          </label>
          <label>
            Cron expression
            <input value={app.cronForm.cronExpression} onChange={(event) => app.setCronForm((previous) => ({ ...previous, cronExpression: event.target.value }))} />
          </label>
          <label>
            At timestamp
            <input value={app.cronForm.atTimestampRfc3339} onChange={(event) => app.setCronForm((previous) => ({ ...previous, atTimestampRfc3339: event.target.value }))} />
          </label>
        </div>
        <button type="submit" disabled={app.cronBusy}>
          {app.cronBusy ? "Creating..." : "Create job"}
        </button>
      </form>

      <div className="console-table-wrap">
        <table className="console-table">
          <thead>
            <tr>
              <th>Job ID</th>
              <th>Name</th>
              <th>Enabled</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {app.cronJobs.length === 0 && <tr><td colSpan={4}>No cron jobs found.</td></tr>}
            {app.cronJobs.map((job) => {
              const id = readString(job, "job_id") ?? "(missing)";
              const enabled = readBool(job, "enabled");
              return (
                <tr key={id}>
                  <td>{id}</td>
                  <td>{readString(job, "name") ?? "-"}</td>
                  <td>{enabled ? "yes" : "no"}</td>
                  <td className="console-action-cell">
                    <button type="button" className="secondary" onClick={() => app.setCronJobId(id)}>Select</button>
                    <button type="button" onClick={() => void app.toggleCron(job, !enabled)}>
                      {enabled ? "Disable" : "Enable"}
                    </button>
                    <button type="button" onClick={() => void app.runCronNow(job)}>Run now</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Run logs</h3>
            <p className="chat-muted">
              Select a job, then load recent runs to verify run-now dispatches and scheduled execution history.
            </p>
          </div>
          <div className="console-inline-actions">
            <label>
              Job ID
              <input value={app.cronJobId} onChange={(event) => app.setCronJobId(event.target.value)} />
            </label>
            <button type="button" onClick={() => void app.refreshCronRuns()} disabled={app.cronBusy}>
              {app.cronBusy ? "Loading..." : "Load runs"}
            </button>
          </div>
        </div>
        {app.cronRuns.length === 0 ? (
          <p>No cron runs loaded.</p>
        ) : (
          <pre>{toPrettyJson(app.cronRuns, app.revealSensitiveValues)}</pre>
        )}
      </section>
    </main>
  );
}
