import type { ChatSessionRecord } from "../consoleApi";

import { shortId } from "./chatShared";

type ChatSessionsSidebarProps = {
  sessionsBusy: boolean;
  newSessionLabel: string;
  setNewSessionLabel: (value: string) => void;
  createSession: () => void;
  sessionLabelDraft: string;
  setSessionLabelDraft: (value: string) => void;
  selectedSession: ChatSessionRecord | null;
  renameSession: () => void;
  resetSession: () => void;
  sortedSessions: ChatSessionRecord[];
  activeSessionId: string;
  setActiveSessionId: (sessionId: string) => void;
};

export function ChatSessionsSidebar({
  sessionsBusy,
  newSessionLabel,
  setNewSessionLabel,
  createSession,
  sessionLabelDraft,
  setSessionLabelDraft,
  selectedSession,
  renameSession,
  resetSession,
  sortedSessions,
  activeSessionId,
  setActiveSessionId
}: ChatSessionsSidebarProps) {
  return (
    <aside className="chat-sessions" aria-label="Chat sessions">
      <div className="workspace-panel__intro">
        <p className="workspace-kicker">Sessions</p>
        <h3>Conversation rail</h3>
        <p className="chat-muted">Keep sessions secondary to the conversation, but always one click away.</p>
      </div>

      <section className="chat-session-editor">
        <h4>New session</h4>
        <label>
          New label
          <input
            value={newSessionLabel}
            onChange={(event) => setNewSessionLabel(event.target.value)}
            placeholder="optional"
          />
        </label>
        <button type="button" onClick={createSession} disabled={sessionsBusy}>
          {sessionsBusy ? "Creating..." : "Create"}
        </button>
      </section>

      <section className="chat-session-editor">
        <h4>Selected session</h4>
        <label>
          Active label
          <input
            value={sessionLabelDraft}
            onChange={(event) => setSessionLabelDraft(event.target.value)}
            disabled={selectedSession === null || sessionsBusy}
          />
        </label>
        <div className="workspace-inline-actions">
          <button type="button" onClick={renameSession} disabled={selectedSession === null || sessionsBusy}>
            Rename
          </button>
          <button type="button" className="button--warn" onClick={resetSession} disabled={selectedSession === null || sessionsBusy}>
            Reset
          </button>
        </div>
      </section>

      <div className="chat-session-list" role="listbox" aria-label="Conversation sessions">
        {sortedSessions.length === 0 ? (
          <p className="workspace-empty">Create a session to start a conversation.</p>
        ) : (
          sortedSessions.map((session) => {
            const active = session.session_id === activeSessionId;
            const label = session.session_label?.trim().length ? session.session_label : shortId(session.session_id);
            return (
              <button
                key={session.session_id}
                type="button"
                className={`chat-session-item${active ? " is-active" : ""}`}
                onClick={() => setActiveSessionId(session.session_id)}
                aria-selected={active}
              >
                <span className="chat-session-item__title">{label}</span>
                <small>
                  Updated {new Date(session.updated_at_unix_ms).toLocaleTimeString()} · {shortId(session.session_id)}
                </small>
              </button>
            );
          })
        )}
      </div>
    </aside>
  );
}
