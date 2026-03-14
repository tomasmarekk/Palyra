import { useState } from "react";

import { WorkspaceMetricCard, WorkspacePageHeader, WorkspaceSectionCard, WorkspaceStatusChip } from "../components/workspace/WorkspaceChrome";
import { WorkspaceConfirmDialog, WorkspaceEmptyState, WorkspaceInlineNotice, WorkspaceTable } from "../components/workspace/WorkspacePatterns";
import { formatUnixMs, readNumber, readObject, readString, type JsonObject } from "../shared";
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
  >;
};

export function MemorySection({ app }: MemorySectionProps) {
  const [confirmingPurge, setConfirmingPurge] = useState(false);
  const usage = readObject(app.memoryStatus ?? {}, "usage");
  const retention = readObject(app.memoryStatus ?? {}, "retention");
  const maintenance = readObject(app.memoryStatus ?? {}, "maintenance");

  return (
    <main className="workspace-page">
      <WorkspacePageHeader
        eyebrow="Agent"
        title="Memory"
        description="Search scoped memory quickly, understand retention posture, and keep purge flows clearly destructive instead of blending them into search."
        status={
          <>
            <WorkspaceStatusChip tone={app.memoryHits.length > 0 ? "success" : "default"}>
              {app.memoryHits.length} hits loaded
            </WorkspaceStatusChip>
            <WorkspaceStatusChip tone={app.memoryPurgeAll ? "danger" : "default"}>
              {app.memoryPurgeAll ? "Purge-all armed" : "Scoped purge"}
            </WorkspaceStatusChip>
          </>
        }
        actions={(
          <button type="button" onClick={() => void app.refreshMemoryStatus()} disabled={app.memoryStatusBusy}>
            {app.memoryStatusBusy ? "Refreshing..." : "Refresh status"}
          </button>
        )}
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          label="Stored items"
          value={readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0}
          detail="Current retained memory entries visible to the maintenance surface."
          tone={(readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0) > 0 ? "accent" : "default"}
        />
        <WorkspaceMetricCard
          label="Vectors"
          value={readNumber(usage ?? {}, "vector_count") ?? 0}
          detail="Vector rows currently stored for hybrid retrieval."
        />
        <WorkspaceMetricCard
          label="Retention TTL"
          value={`${readNumber(retention ?? {}, "ttl_days") ?? 0} days`}
          detail="Retention policy remains visible so search and purge decisions stay grounded."
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Search memory"
            description="Use a short query and optional channel scope to find relevant memory without needing internal retrieval jargon."
          >
            <form className="workspace-stack" onSubmit={(event) => void app.searchMemory(event)}>
              <div className="workspace-form-grid">
                <label>
                  Query
                  <input
                    value={app.memoryQuery}
                    onChange={(event) => app.setMemoryQuery(event.target.value)}
                  />
                </label>
                <label>
                  Channel
                  <input
                    value={app.memoryChannel}
                    onChange={(event) => app.setMemoryChannel(event.target.value)}
                  />
                </label>
                <div className="workspace-inline">
                  <button type="submit" disabled={app.memoryBusy}>
                    {app.memoryBusy ? "Searching..." : "Search"}
                  </button>
                </div>
              </div>
            </form>

            {app.memoryHits.length === 0 ? (
              <WorkspaceEmptyState
                title="No memory hits loaded"
                description="Search by query and optional channel to inspect what the current principal can retrieve."
              />
            ) : (
              <WorkspaceTable
                ariaLabel="Memory search results"
                columns={["Memory", "Channel", "Snippet", "Score"]}
              >
                {app.memoryHits.map((hit, index) => (
                  <tr key={readString(hit, "memory_id") ?? `memory-hit-${index}`}>
                    <td>
                      <div className="workspace-table__meta">
                        <strong>{readMemoryId(hit, index)}</strong>
                        <span className="chat-muted">
                          {readString(hit, "session_id") ?? readString(readObject(hit, "item") ?? {}, "session_id") ?? "No session"}
                        </span>
                      </div>
                    </td>
                    <td>{readString(hit, "channel") ?? readString(readObject(hit, "item") ?? {}, "channel") ?? "n/a"}</td>
                    <td>{readString(hit, "snippet") ?? readString(hit, "content") ?? readString(readObject(hit, "item") ?? {}, "content_text") ?? "No snippet"}</td>
                    <td>{formatScore(hit)}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard
            title="Retention and maintenance"
            description="Keep maintenance timings and retention policy visible so purge stays a deliberate operational choice."
          >
            {app.memoryStatus === null ? (
              <WorkspaceEmptyState
                title="No memory status loaded"
                description="Refresh status to inspect current usage, TTL policy, and maintenance timing."
                compact
              />
            ) : (
              <dl className="workspace-key-value-grid">
                <div>
                  <dt>Entries</dt>
                  <dd>{readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0}</dd>
                </div>
                <div>
                  <dt>Approx bytes</dt>
                  <dd>{readNumber(usage ?? {}, "approx_bytes") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>TTL days</dt>
                  <dd>{readNumber(retention ?? {}, "ttl_days") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Vacuum schedule</dt>
                  <dd>{readString(retention ?? {}, "vacuum_schedule") ?? "n/a"}</dd>
                </div>
                <div>
                  <dt>Last vacuum</dt>
                  <dd>
                    {formatUnixMs(
                      readNumber(maintenance ?? {}, "last_vacuum_at_unix_ms") ??
                      readNumber(app.memoryStatus, "last_vacuum_at_unix_ms")
                    )}
                  </dd>
                </div>
                <div>
                  <dt>Next maintenance</dt>
                  <dd>{formatUnixMs(readNumber(app.memoryStatus, "next_maintenance_run_at_unix_ms"))}</dd>
                </div>
              </dl>
            )}
          </WorkspaceSectionCard>

          <WorkspaceSectionCard
            title="Purge memory"
            description="Purge stays secondary and destructive. Narrow the scope when possible before choosing purge-all."
          >
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <label>
                  Channel
                  <input
                    value={app.memoryPurgeChannel}
                    onChange={(event) => app.setMemoryPurgeChannel(event.target.value)}
                  />
                </label>
                <label>
                  Session ID
                  <input
                    value={app.memoryPurgeSessionId}
                    onChange={(event) => app.setMemoryPurgeSessionId(event.target.value)}
                  />
                </label>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={app.memoryPurgeAll}
                    onChange={(event) => app.setMemoryPurgeAll(event.target.checked)}
                  />
                  Purge all principal memory
                </label>
              </div>
              <div className="workspace-inline">
                <button
                  type="button"
                  className="button--warn"
                  onClick={() => setConfirmingPurge(true)}
                  disabled={app.memoryBusy}
                >
                  {app.memoryBusy ? "Purging..." : "Purge memory"}
                </button>
              </div>
            </div>
          </WorkspaceSectionCard>

          <WorkspaceInlineNotice title="Operator guidance" tone="warning">
            <p>
              Search first, purge second. The memory service is scoped per principal/channel/session,
              so broad deletes should be rare and deliberate.
            </p>
          </WorkspaceInlineNotice>
        </div>
      </section>

      <WorkspaceConfirmDialog
        isOpen={confirmingPurge}
        onOpenChange={setConfirmingPurge}
        title="Purge memory"
        description={
          app.memoryPurgeAll
            ? "Delete all memory for the current principal? This is the broadest purge path."
            : `Delete memory for channel ${app.memoryPurgeChannel || "n/a"} and session ${app.memoryPurgeSessionId || "n/a"}?`
        }
        confirmLabel="Purge memory"
        confirmTone="danger"
        isBusy={app.memoryBusy}
        onConfirm={() => {
          setConfirmingPurge(false);
          void app.purgeMemory();
        }}
      />
    </main>
  );
}

function readMemoryId(hit: JsonObject, index: number): string {
  return (
    readString(hit, "memory_id") ??
    readString(readObject(hit, "item") ?? {}, "memory_id") ??
    `memory-${index + 1}`
  );
}

function formatScore(hit: JsonObject): string {
  const score = readNumber(hit, "score") ?? readNumber(readObject(hit, "breakdown") ?? {}, "final_score");
  return score === null ? "n/a" : score.toFixed(2);
}
