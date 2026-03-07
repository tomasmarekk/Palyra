import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import {
  formatUnixMs,
  readString,
  toPrettyJson,
  toStringArray,
  type JsonObject,
} from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type SupportSectionProps = {
  app: Pick<
    ConsoleAppState,
    | "supportBusy"
    | "supportDeployment"
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

export function SupportSection({ app }: SupportSectionProps) {
  const warnings = toStringArray(
    Array.isArray(app.supportDeployment?.warnings) ? app.supportDeployment.warnings : []
  );

  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Support and Recovery"
        description="Support bundle export, last-known posture, and operator recovery shortcuts live here so incident response stays visible from the dashboard."
        actions={(
          <button type="button" onClick={() => void app.refreshSupport()} disabled={app.supportBusy}>
            {app.supportBusy ? "Refreshing..." : "Refresh recovery"}
          </button>
        )}
      />

      <section className="console-grid-4 console-summary-grid">
        <article className="console-subpanel">
          <h3>Deployment snapshot</h3>
          <p><strong>Mode:</strong> {readString(app.supportDeployment ?? {}, "mode") ?? "n/a"}</p>
          <p><strong>Bind profile:</strong> {readString(app.supportDeployment ?? {}, "bind_profile") ?? "n/a"}</p>
          <p><strong>Warnings:</strong> {warnings.length}</p>
        </article>
        <article className="console-subpanel">
          <h3>Support queue</h3>
          <p><strong>Jobs:</strong> {app.supportBundleJobs.length}</p>
          <p><strong>Selected job:</strong> {app.supportSelectedBundleJobId || "none"}</p>
        </article>
        <article className="console-subpanel">
          <h3>Recovery shortcuts</h3>
          <div className="console-inline-actions">
            <button type="button" className="secondary" onClick={() => app.setSection("auth")}>
              Provider auth recovery
            </button>
            <button type="button" className="secondary" onClick={() => app.setSection("channels")}>
              Connector recovery
            </button>
            <button type="button" className="secondary" onClick={() => app.setSection("config")}>
              Config recovery
            </button>
          </div>
        </article>
        <article className="console-subpanel">
          <h3>Restart guidance</h3>
          <p>Use the desktop control center for local sidecar restarts, then re-check diagnostics and channel health from the dashboard.</p>
          <p className="chat-muted">
            Detached operators can fall back to the CLI doctor and support-bundle commands published in the operations domain.
          </p>
        </article>
      </section>

      <section className="console-grid-2">
        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Queue support bundle export</h3>
              <p className="chat-muted">
                Support bundles stay queue-backed so collection can continue even when a browser tab disconnects.
              </p>
            </div>
          </div>
          <div className="console-grid-2">
            <label>
              Retain jobs
              <input value={app.supportBundleRetainJobs} onChange={(event) => app.setSupportBundleRetainJobs(event.target.value)} />
            </label>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void app.createSupportBundle()} disabled={app.supportBusy}>
                {app.supportBusy ? "Queueing..." : "Queue support bundle"}
              </button>
            </div>
          </div>
          {warnings.length > 0 && (
            <>
              <p><strong>Last known warnings</strong></p>
              <ul className="console-compact-list">
                {warnings.map((warning) => (
                  <li key={warning}>{warning}</li>
                ))}
              </ul>
            </>
          )}
        </article>

        <article className="console-subpanel">
          <div className="console-subpanel__header">
            <div>
              <h3>Inspect queued job</h3>
              <p className="chat-muted">
                Load the latest export status, command output, and destination path without leaving the dashboard.
              </p>
            </div>
          </div>
          <div className="console-grid-2">
            <label>
              Job ID
              <input value={app.supportSelectedBundleJobId} onChange={(event) => app.setSupportSelectedBundleJobId(event.target.value)} />
            </label>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void app.loadSupportBundleJob()} disabled={app.supportBusy}>
                {app.supportBusy ? "Loading..." : "Load job"}
              </button>
            </div>
          </div>
          {app.supportSelectedBundleJob === null ? (
            <p>No support bundle job selected.</p>
          ) : (
            <pre>{toPrettyJson(app.supportSelectedBundleJob, app.revealSensitiveValues)}</pre>
          )}
        </article>
      </section>

      <section className="console-subpanel">
        <div className="console-subpanel__header">
          <div>
            <h3>Queued support bundle jobs</h3>
            <p className="chat-muted">
              Export jobs remain visible after completion so operators can confirm output paths and failure reasons.
            </p>
          </div>
        </div>
        {app.supportBundleJobs.length === 0 ? (
          <p>No support bundle jobs queued.</p>
        ) : (
          <div className="console-table-wrap">
            <table className="console-table">
              <thead>
                <tr>
                  <th>Job ID</th>
                  <th>State</th>
                  <th>Requested</th>
                  <th>Output</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {app.supportBundleJobs.map((job) => {
                  const jobId = readString(job, "job_id") ?? "unknown";
                  return (
                    <tr key={jobId}>
                      <td>{jobId}</td>
                      <td>{readString(job, "state") ?? "unknown"}</td>
                      <td>{formatUnixMs(readUnixMillis(job, "requested_at_unix_ms"))}</td>
                      <td>{readString(job, "output_path") ?? "-"}</td>
                      <td>
                        <button
                          type="button"
                          className="secondary"
                          onClick={() => app.setSupportSelectedBundleJobId(jobId)}
                        >
                          Select
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="console-subpanel">
        <h3>Redacted health and posture snapshot</h3>
        {app.supportDeployment === null ? (
          <p>No deployment posture loaded.</p>
        ) : (
          <pre>{toPrettyJson(app.supportDeployment, app.revealSensitiveValues)}</pre>
        )}
      </section>
    </main>
  );
}

function readUnixMillis(record: JsonObject, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}
