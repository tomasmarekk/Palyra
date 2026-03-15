import { useState } from "react";

import {
  ActionButton,
  CheckboxField,
  TextInputField
} from "../components/ui";
import {
  WorkspaceMetricCard,
  WorkspacePageHeader,
  WorkspaceSectionCard,
  WorkspaceStatusChip
} from "../components/workspace/WorkspaceChrome";
import {
  WorkspaceConfirmDialog,
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceTable
} from "../components/workspace/WorkspacePatterns";
import {
  formatUnixMs,
  readNumber,
  readObject,
  readString,
  type JsonObject
} from "../shared";
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
        actions={
          <ActionButton
            isDisabled={app.memoryStatusBusy}
            type="button"
            variant="primary"
            onPress={() => void app.refreshMemoryStatus()}
          >
            {app.memoryStatusBusy ? "Refreshing..." : "Refresh status"}
          </ActionButton>
        }
      />

      <section className="workspace-metric-grid workspace-metric-grid--compact">
        <WorkspaceMetricCard
          detail="Current retained memory entries visible to the maintenance surface."
          label="Stored items"
          tone={
            (readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0) > 0
              ? "accent"
              : "default"
          }
          value={readNumber(usage ?? {}, "item_count") ?? readNumber(usage ?? {}, "entries") ?? 0}
        />
        <WorkspaceMetricCard
          detail="Vector rows currently stored for hybrid retrieval."
          label="Vectors"
          value={readNumber(usage ?? {}, "vector_count") ?? 0}
        />
        <WorkspaceMetricCard
          detail="Retention policy remains visible so search and purge decisions stay grounded."
          label="Retention TTL"
          value={`${readNumber(retention ?? {}, "ttl_days") ?? 0} days`}
        />
      </section>

      <section className="workspace-aside-grid">
        <div className="workspace-stack">
          <WorkspaceSectionCard
            description="Use a short query and optional channel scope to find relevant memory without needing internal retrieval jargon."
            title="Search memory"
          >
            <form className="workspace-stack" onSubmit={(event) => void app.searchMemory(event)}>
              <div className="workspace-form-grid">
                <TextInputField
                  label="Query"
                  value={app.memoryQuery}
                  onChange={app.setMemoryQuery}
                />
                <TextInputField
                  label="Channel"
                  value={app.memoryChannel}
                  onChange={app.setMemoryChannel}
                />
                <div className="workspace-inline">
                  <ActionButton isDisabled={app.memoryBusy} type="submit" variant="primary">
                    {app.memoryBusy ? "Searching..." : "Search"}
                  </ActionButton>
                </div>
              </div>
            </form>

            {app.memoryHits.length === 0 ? (
              <WorkspaceEmptyState
                description="Search by query and optional channel to inspect what the current principal can retrieve."
                title="No memory hits loaded"
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
                          {readString(hit, "session_id") ??
                            readString(readObject(hit, "item") ?? {}, "session_id") ??
                            "No session"}
                        </span>
                      </div>
                    </td>
                    <td>
                      {readString(hit, "channel") ??
                        readString(readObject(hit, "item") ?? {}, "channel") ??
                        "n/a"}
                    </td>
                    <td>
                      {readString(hit, "snippet") ??
                        readString(hit, "content") ??
                        readString(readObject(hit, "item") ?? {}, "content_text") ??
                        "No snippet"}
                    </td>
                    <td>{formatScore(hit)}</td>
                  </tr>
                ))}
              </WorkspaceTable>
            )}
          </WorkspaceSectionCard>
        </div>

        <div className="workspace-stack">
          <WorkspaceSectionCard
            description="Keep maintenance timings and retention policy visible so purge stays a deliberate operational choice."
            title="Retention and maintenance"
          >
            {app.memoryStatus === null ? (
              <WorkspaceEmptyState
                compact
                description="Refresh status to inspect current usage, TTL policy, and maintenance timing."
                title="No memory status loaded"
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
            description="Purge stays secondary and destructive. Narrow the scope when possible before choosing purge-all."
            title="Purge memory"
          >
            <div className="workspace-stack">
              <div className="workspace-form-grid">
                <TextInputField
                  label="Channel"
                  value={app.memoryPurgeChannel}
                  onChange={app.setMemoryPurgeChannel}
                />
                <TextInputField
                  label="Session ID"
                  value={app.memoryPurgeSessionId}
                  onChange={app.setMemoryPurgeSessionId}
                />
                <CheckboxField
                  checked={app.memoryPurgeAll}
                  description="Delete all memory visible to the current principal."
                  label="Purge all principal memory"
                  onChange={app.setMemoryPurgeAll}
                />
              </div>
              <div className="workspace-inline">
                <ActionButton
                  isDisabled={app.memoryBusy}
                  type="button"
                  variant="danger"
                  onPress={() => setConfirmingPurge(true)}
                >
                  {app.memoryBusy ? "Purging..." : "Purge memory"}
                </ActionButton>
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
        isBusy={app.memoryBusy}
        isOpen={confirmingPurge}
        confirmLabel="Purge memory"
        confirmTone="danger"
        description={
          app.memoryPurgeAll
            ? "Delete all memory for the current principal? This is the broadest purge path."
            : `Delete memory for channel ${app.memoryPurgeChannel || "n/a"} and session ${app.memoryPurgeSessionId || "n/a"}?`
        }
        title="Purge memory"
        onConfirm={() => {
          setConfirmingPurge(false);
          void app.purgeMemory();
        }}
        onOpenChange={setConfirmingPurge}
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
  const score =
    readNumber(hit, "score") ?? readNumber(readObject(hit, "breakdown") ?? {}, "final_score");
  return score === null ? "n/a" : score.toFixed(2);
}
