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
    <aside className="chat-run-drawer" aria-label="Run details drawer">
      <header className="chat-run-drawer__header">
        <h3>Run details</h3>
        <div className="console-inline-actions">
          <select value={runDrawerId} onChange={(event) => setRunDrawerId(event.target.value)}>
            <option value="">Select run</option>
            {runIds.map((runId) => (
              <option key={runId} value={runId}>{runId}</option>
            ))}
          </select>
          <button type="button" onClick={refreshRun} disabled={runDrawerId.trim().length === 0 || runDrawerBusy}>
            {runDrawerBusy ? "Loading..." : "Refresh run"}
          </button>
          <button type="button" onClick={close}>Close</button>
        </div>
      </header>

      {runDrawerId.trim().length === 0 ? (
        <p className="chat-muted">Select a run to inspect status and tape events.</p>
      ) : (
        <>
          {runStatus === null ? (
            <p className="chat-muted">No run status loaded yet.</p>
          ) : (
            <section className="console-subpanel">
              <h4>Status</h4>
              <div className="console-grid-3">
                <p><strong>State:</strong> {runStatus.state}</p>
                <p><strong>Prompt tokens:</strong> {runStatus.prompt_tokens}</p>
                <p><strong>Completion tokens:</strong> {runStatus.completion_tokens}</p>
                <p><strong>Total tokens:</strong> {runStatus.total_tokens}</p>
                <p><strong>Tape events:</strong> {runStatus.tape_events}</p>
                <p><strong>Updated:</strong> {new Date(runStatus.updated_at_unix_ms).toLocaleString()}</p>
              </div>
              {runStatus.last_error !== undefined && runStatus.last_error.length > 0 && (
                <p className="console-banner console-banner--error">{runStatus.last_error}</p>
              )}
            </section>
          )}

          {runTape === null ? (
            <p className="chat-muted">No tape snapshot loaded.</p>
          ) : (
            <section className="console-subpanel">
              <h4>Tape events ({runTape.events.length})</h4>
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
            </section>
          )}
        </>
      )}
    </aside>
  );
}
