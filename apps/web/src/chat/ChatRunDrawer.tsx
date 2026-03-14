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
        <label>
          Run
          <select value={runDrawerId} onChange={(event) => setRunDrawerId(event.target.value)}>
            <option value="">Select run</option>
            {runIds.map((runId) => (
              <option key={runId} value={runId}>
                {runId}
              </option>
            ))}
          </select>
        </label>
        <div className="workspace-inline-actions">
          <button type="button" onClick={refreshRun} disabled={runDrawerId.trim().length === 0 || runDrawerBusy}>
            {runDrawerBusy ? "Loading..." : "Refresh run"}
          </button>
          <button type="button" className="secondary" onClick={close}>
            Hide inspector
          </button>
        </div>
      </div>

      {runDrawerId.trim().length === 0 ? (
        <p className="workspace-empty">Select a run to inspect status and tape events.</p>
      ) : (
        <>
          {runStatus === null ? (
            <p className="workspace-empty">No run status loaded yet.</p>
          ) : (
            <section className="chat-run-section">
              <h4>Status</h4>
              <dl className="workspace-detail-grid">
                <div>
                  <dt>State</dt>
                  <dd>{runStatus.state}</dd>
                </div>
                <div>
                  <dt>Prompt tokens</dt>
                  <dd>{runStatus.prompt_tokens}</dd>
                </div>
                <div>
                  <dt>Completion tokens</dt>
                  <dd>{runStatus.completion_tokens}</dd>
                </div>
                <div>
                  <dt>Total tokens</dt>
                  <dd>{runStatus.total_tokens}</dd>
                </div>
                <div>
                  <dt>Tape events</dt>
                  <dd>{runStatus.tape_events}</dd>
                </div>
                <div>
                  <dt>Updated</dt>
                  <dd>{new Date(runStatus.updated_at_unix_ms).toLocaleString()}</dd>
                </div>
              </dl>
              {runStatus.last_error !== undefined && runStatus.last_error.length > 0 && (
                <p className="console-banner console-banner--error">{runStatus.last_error}</p>
              )}
            </section>
          )}

          {runTape === null ? (
            <p className="workspace-empty">No tape snapshot loaded.</p>
          ) : (
            <section className="chat-run-section">
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
