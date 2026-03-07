import { ConsoleSectionHeader } from "../components/ConsoleSectionHeader";
import { toPrettyJson } from "../shared";
import type { ConsoleAppState } from "../useConsoleAppState";

type MemorySectionProps = {
  app: Pick<
    ConsoleAppState,
    | "memoryBusy"
    | "memoryQuery"
    | "setMemoryQuery"
    | "memoryChannel"
    | "setMemoryChannel"
    | "memoryPurgeChannel"
    | "setMemoryPurgeChannel"
    | "memoryPurgeSessionId"
    | "setMemoryPurgeSessionId"
    | "memoryPurgeAll"
    | "setMemoryPurgeAll"
    | "memoryHits"
    | "memoryStatusBusy"
    | "memoryStatus"
    | "refreshMemoryStatus"
    | "searchMemory"
    | "purgeMemory"
    | "revealSensitiveValues"
  >;
};

export function MemorySection({ app }: MemorySectionProps) {
  return (
    <main className="console-card">
      <ConsoleSectionHeader
        title="Memory"
        description="Inspect retention posture, search scoped memory records, and purge channel or session data explicitly."
        actions={(
          <button type="button" onClick={() => void app.refreshMemoryStatus()} disabled={app.memoryStatusBusy}>
            {app.memoryStatusBusy ? "Refreshing..." : "Refresh status"}
          </button>
        )}
      />

      <section className="console-grid-2">
        <article className="console-subpanel">
          <h3>Retention and maintenance</h3>
          {app.memoryStatus === null ? <p>No memory status loaded.</p> : <pre>{toPrettyJson(app.memoryStatus, app.revealSensitiveValues)}</pre>}
        </article>
        <article className="console-subpanel">
          <h3>Purge memory</h3>
          <div className="console-grid-3">
            <label>
              Channel
              <input value={app.memoryPurgeChannel} onChange={(event) => app.setMemoryPurgeChannel(event.target.value)} />
            </label>
            <label>
              Session ID
              <input value={app.memoryPurgeSessionId} onChange={(event) => app.setMemoryPurgeSessionId(event.target.value)} />
            </label>
            <label className="console-checkbox-inline">
              <input type="checkbox" checked={app.memoryPurgeAll} onChange={(event) => app.setMemoryPurgeAll(event.target.checked)} />
              Purge all principal memory
            </label>
          </div>
          <button type="button" className="button--warn" onClick={() => void app.purgeMemory()} disabled={app.memoryBusy}>
            {app.memoryBusy ? "Purging..." : "Purge memory"}
          </button>
        </article>
      </section>

      <section className="console-subpanel">
        <h3>Search memory</h3>
        <form className="console-form" onSubmit={(event) => void app.searchMemory(event)}>
          <div className="console-grid-3">
            <label>
              Query
              <input value={app.memoryQuery} onChange={(event) => app.setMemoryQuery(event.target.value)} />
            </label>
            <label>
              Channel
              <input value={app.memoryChannel} onChange={(event) => app.setMemoryChannel(event.target.value)} />
            </label>
            <button type="submit" disabled={app.memoryBusy}>{app.memoryBusy ? "Searching..." : "Search memory"}</button>
          </div>
        </form>
        {app.memoryHits.length === 0 ? <p>No memory hits loaded.</p> : <pre>{toPrettyJson(app.memoryHits, app.revealSensitiveValues)}</pre>}
      </section>
    </main>
  );
}
