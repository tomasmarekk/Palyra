import {
  ActionButton,
  ActionCluster,
  EmptyState,
  InlineNotice,
  KeyValueList,
  SelectField,
  SectionCard,
} from "../console/components/ui";
import type {
  ChatRunLineage,
  ChatRunStatusRecord,
  ChatRunTapeSnapshot,
  ConsoleApiClient,
  JsonValue,
  WorkspaceRestoreResponseEnvelope,
} from "../consoleApi";
import { Tabs } from "@heroui/react";

import { PrettyJsonBlock, parseTapePayload, shortId, toPrettyJson } from "./chatShared";
import { ChatRunWorkspaceTab } from "./ChatRunWorkspaceTab";

export type RunDrawerTab = "status" | "lineage" | "tape" | "workspace";

type ChatRunDrawerProps = {
  open: boolean;
  runIds: string[];
  runDrawerId: string;
  setRunDrawerId: (runId: string) => void;
  runDrawerBusy: boolean;
  runStatus: ChatRunStatusRecord | null;
  runTape: ChatRunTapeSnapshot | null;
  runLineage: ChatRunLineage | null;
  activeTab: RunDrawerTab;
  setActiveTab: (tab: RunDrawerTab) => void;
  api: ConsoleApiClient;
  revealSensitiveValues: boolean;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  onInspectCompaction: (artifactId: string) => void;
  onInspectSessionCheckpoint: (checkpointId: string) => void;
  onWorkspaceRestore: (response: WorkspaceRestoreResponseEnvelope) => Promise<void>;
  openMemorySection: () => void;
  openSupportSection: () => void;
  refreshRun: () => void;
  close: () => void;
};

export function ChatRunDrawer({
  open,
  runIds,
  runDrawerId,
  setRunDrawerId,
  runDrawerBusy,
  runStatus,
  runTape,
  runLineage,
  activeTab,
  setActiveTab,
  api,
  revealSensitiveValues,
  setError,
  setNotice,
  onInspectCompaction,
  onInspectSessionCheckpoint,
  onWorkspaceRestore,
  openMemorySection,
  openSupportSection,
  refreshRun,
  close,
}: ChatRunDrawerProps) {
  if (!open) {
    return null;
  }

  return (
    <aside className="chat-run-drawer" aria-label="Run details inspector">
      <div className="workspace-field-grid workspace-field-grid--double">
        <SelectField
          label="Run"
          options={runIds.map((runId) => ({ key: runId, label: runId }))}
          placeholder="Select run"
          value={runDrawerId}
          onChange={setRunDrawerId}
        />
        <ActionCluster className="items-end">
          <ActionButton
            isDisabled={runDrawerId.trim().length === 0 || runDrawerBusy}
            type="button"
            variant="primary"
            onPress={refreshRun}
          >
            {runDrawerBusy ? "Loading..." : "Refresh run"}
          </ActionButton>
          <ActionButton type="button" variant="secondary" onPress={close}>
            Hide inspector
          </ActionButton>
        </ActionCluster>
      </div>

      {runDrawerId.trim().length === 0 ? (
        <EmptyState
          compact
          description="Select a run to inspect status and tape events."
          title="No run selected"
        />
      ) : (
        <>
          <Tabs
            selectedKey={activeTab}
            variant="secondary"
            onSelectionChange={(key) => setActiveTab(String(key) as RunDrawerTab)}
          >
            <Tabs.ListContainer>
              <Tabs.List aria-label="Run inspector sections" className="w-full">
                <Tabs.Tab id="status">
                  Status
                  <Tabs.Indicator />
                </Tabs.Tab>
                <Tabs.Tab id="lineage">
                  Lineage
                  <Tabs.Indicator />
                </Tabs.Tab>
                <Tabs.Tab id="tape">
                  Tape
                  <Tabs.Indicator />
                </Tabs.Tab>
                <Tabs.Tab id="workspace">
                  Workspace
                  <Tabs.Indicator />
                </Tabs.Tab>
              </Tabs.List>
            </Tabs.ListContainer>
            <Tabs.Panel className="pt-4" id="status">
              {runStatus === null ? (
                <EmptyState
                  compact
                  description="Refresh a run to inspect its current status."
                  title="No run status loaded yet"
                />
              ) : (
                <SectionCard title="Run status" variant="transparent">
                  <KeyValueList
                    items={[
                      { label: "State", value: runStatus.state },
                      { label: "Origin", value: runStatus.origin_kind },
                      {
                        label: "Origin run",
                        value:
                          runStatus.origin_run_id !== undefined
                            ? shortId(runStatus.origin_run_id)
                            : "none",
                      },
                      {
                        label: "Parent run",
                        value:
                          runStatus.parent_run_id !== undefined
                            ? shortId(runStatus.parent_run_id)
                            : "none",
                      },
                      {
                        label: "Triggered by",
                        value: runStatus.triggered_by_principal ?? "n/a",
                      },
                      {
                        label: "Delegation profile",
                        value: runStatus.delegation?.display_name ?? "none",
                      },
                      {
                        label: "Execution mode",
                        value: runStatus.delegation?.execution_mode ?? "n/a",
                      },
                      {
                        label: "Merge strategy",
                        value: runStatus.merge_result?.strategy ?? "n/a",
                      },
                      { label: "Prompt tokens", value: runStatus.prompt_tokens },
                      { label: "Completion tokens", value: runStatus.completion_tokens },
                      { label: "Total tokens", value: runStatus.total_tokens },
                      { label: "Tape events", value: runStatus.tape_events },
                      {
                        label: "Updated",
                        value: new Date(runStatus.updated_at_unix_ms).toLocaleString(),
                      },
                    ]}
                  />
                  {runStatus.parameter_delta_json !== undefined &&
                  runStatus.parameter_delta_json.length > 0 ? (
                    <SectionCard
                      description="Retry and branch runs can carry deltas from the parent execution."
                      title="Parameter delta"
                      variant="transparent"
                    >
                      <PrettyJsonBlock
                        revealSensitiveValues={revealSensitiveValues}
                        value={parseTapePayload(runStatus.parameter_delta_json)}
                      />
                    </SectionCard>
                  ) : null}
                  {runStatus.delegation !== undefined ? (
                    <SectionCard
                      description="Delegated child runs keep the resolved profile, scope, and budget snapshot here."
                      title="Delegation"
                      variant="transparent"
                    >
                      <PrettyJsonBlock
                        revealSensitiveValues={revealSensitiveValues}
                        value={runStatus.delegation as unknown as JsonValue}
                      />
                    </SectionCard>
                  ) : null}
                  {runStatus.merge_result !== undefined ? (
                    <SectionCard
                      description="Merged child-run output stays attached to the child run with provenance."
                      title="Merge result"
                      variant="transparent"
                    >
                      <PrettyJsonBlock
                        revealSensitiveValues={revealSensitiveValues}
                        value={runStatus.merge_result as unknown as JsonValue}
                      />
                    </SectionCard>
                  ) : null}
                  {runStatus.last_error !== undefined && runStatus.last_error.length > 0 ? (
                    <InlineNotice title="Run error" tone="danger">
                      {runStatus.last_error}
                    </InlineNotice>
                  ) : null}
                </SectionCard>
              )}
            </Tabs.Panel>
            <Tabs.Panel className="pt-4" id="lineage">
              {runLineage === null ? (
                <EmptyState
                  compact
                  description="Refresh a run to load its lineage tree."
                  title="No lineage loaded"
                />
              ) : (
                <SectionCard
                  title={`Run lineage (${runLineage.runs.length})`}
                  variant="transparent"
                >
                  <KeyValueList
                    items={[
                      { label: "Focus run", value: shortId(runLineage.focus_run_id) },
                      { label: "Root run", value: shortId(runLineage.root_run_id) },
                    ]}
                  />
                  <div className="chat-tape-list">
                    {buildLineageRows(runLineage).map((row) => (
                      <article
                        key={row.run.run_id}
                        className="chat-tape-item"
                        style={{ marginInlineStart: `${row.depth * 18}px` }}
                      >
                        <header>
                          <strong>{shortId(row.run.run_id)}</strong>
                          <span>{row.run.state}</span>
                        </header>
                        <p className="chat-muted">
                          {row.run.delegation?.display_name ?? row.run.origin_kind}
                          {row.run.parent_run_id !== undefined
                            ? ` · parent ${shortId(row.run.parent_run_id)}`
                            : ""}
                        </p>
                      </article>
                    ))}
                  </div>
                </SectionCard>
              )}
            </Tabs.Panel>
            <Tabs.Panel className="pt-4" id="tape">
              {runTape === null ? (
                <EmptyState
                  compact
                  description="Refresh a run to load its tape snapshot."
                  title="No tape snapshot loaded"
                />
              ) : (
                <SectionCard title={`Tape events (${runTape.events.length})`} variant="transparent">
                  <div className="chat-tape-list">
                    {runTape.events.map((event) => (
                      <article key={`${event.seq}-${event.event_type}`} className="chat-tape-item">
                        <header>
                          <strong>#{event.seq}</strong>
                          <span>{event.event_type}</span>
                        </header>
                        <pre>
                          {toPrettyJson(
                            parseTapePayload(event.payload_json),
                            revealSensitiveValues,
                          )}
                        </pre>
                      </article>
                    ))}
                  </div>
                </SectionCard>
              )}
            </Tabs.Panel>
            <Tabs.Panel className="pt-4" id="workspace">
              <ChatRunWorkspaceTab
                active={activeTab === "workspace"}
                api={api}
                openMemorySection={openMemorySection}
                openSupportSection={openSupportSection}
                onInspectCompaction={onInspectCompaction}
                onInspectSessionCheckpoint={onInspectSessionCheckpoint}
                onOpenRun={(nextRunId, nextTab = "status") => {
                  setActiveTab(nextTab);
                  setRunDrawerId(nextRunId);
                }}
                onWorkspaceRestore={onWorkspaceRestore}
                revealSensitiveValues={revealSensitiveValues}
                runId={runDrawerId}
                runIds={runIds}
                runStatus={runStatus}
                setError={setError}
                setNotice={setNotice}
              />
            </Tabs.Panel>
          </Tabs>
        </>
      )}
    </aside>
  );
}

function buildLineageRows(
  lineage: ChatRunLineage,
): Array<{ run: ChatRunStatusRecord; depth: number }> {
  const runsById = new Map(lineage.runs.map((run) => [run.run_id, run] as const));
  return lineage.runs.map((run) => ({
    run,
    depth: computeLineageDepth(run, runsById),
  }));
}

function computeLineageDepth(
  run: ChatRunStatusRecord,
  runsById: ReadonlyMap<string, ChatRunStatusRecord>,
): number {
  let depth = 0;
  let cursor = run.parent_run_id;
  const visited = new Set<string>();
  while (cursor !== undefined && visited.has(cursor) === false) {
    visited.add(cursor);
    const parent = runsById.get(cursor);
    if (parent === undefined) {
      break;
    }
    depth += 1;
    cursor = parent.parent_run_id;
  }
  return depth;
}
