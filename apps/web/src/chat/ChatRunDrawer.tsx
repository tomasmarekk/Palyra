import {
  ActionButton,
  ActionCluster,
  EmptyState,
  InlineNotice,
  KeyValueList,
  SelectField,
  SectionCard
} from "../console/components/ui";
import type { ChatRunStatusRecord, ChatRunTapeSnapshot } from "../consoleApi";

import { parseTapePayload, toPrettyJson } from "./chatShared";

type ChatRunDrawerProps = {
  open: boolean;
  runIds: string[];
  runDrawerId: string;
  setRunDrawerId: (runId: string) => void;
  runDrawerBusy: boolean;
  runStatus: ChatRunStatusRecord | null;
  runTape: ChatRunTapeSnapshot | null;
  revealSensitiveValues: boolean;
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
  revealSensitiveValues,
  refreshRun,
  close
}: ChatRunDrawerProps) {
  if (!open) {
    return null;
  }

  return (
    <aside className="chat-run-drawer" aria-label="Run details inspector">
      <div className="workspace-panel__intro">
        <p className="workspace-kicker">Inspector</p>
        <h3>Run details</h3>
        <p className="chat-muted">Inspect status, tape events, and token accounting only when you need to.</p>
      </div>

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
          {runStatus === null ? (
            <EmptyState
              compact
              description="Refresh a run to inspect its current status."
              title="No run status loaded yet"
            />
          ) : (
            <SectionCard title="Status">
              <KeyValueList
                items={[
                  { label: "State", value: runStatus.state },
                  { label: "Prompt tokens", value: runStatus.prompt_tokens },
                  { label: "Completion tokens", value: runStatus.completion_tokens },
                  { label: "Total tokens", value: runStatus.total_tokens },
                  { label: "Tape events", value: runStatus.tape_events },
                  {
                    label: "Updated",
                    value: new Date(runStatus.updated_at_unix_ms).toLocaleString()
                  }
                ]}
              />
              {runStatus.last_error !== undefined && runStatus.last_error.length > 0 ? (
                <InlineNotice title="Run error" tone="danger">
                  {runStatus.last_error}
                </InlineNotice>
              ) : null}
            </SectionCard>
          )}

          {runTape === null ? (
            <EmptyState
              compact
              description="Refresh a run to load its tape snapshot."
              title="No tape snapshot loaded"
            />
          ) : (
            <SectionCard title={`Tape events (${runTape.events.length})`}>
              <div className="chat-tape-list">
                {runTape.events.map((event) => (
                  <article key={`${event.seq}-${event.event_type}`} className="chat-tape-item">
                    <header>
                      <strong>#{event.seq}</strong>
                      <span>{event.event_type}</span>
                    </header>
                    <pre>{toPrettyJson(parseTapePayload(event.payload_json), revealSensitiveValues)}</pre>
                  </article>
                ))}
              </div>
            </SectionCard>
          )}
        </>
      )}
    </aside>
  );
}
