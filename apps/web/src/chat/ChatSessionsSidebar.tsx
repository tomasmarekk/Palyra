import { Button } from "@heroui/react";

import { ActionButton, ActionCluster, EmptyState, TextInputField } from "../console/components/ui";
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
  archiveSession: () => void;
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
  archiveSession,
  sortedSessions,
  activeSessionId,
  setActiveSessionId,
}: ChatSessionsSidebarProps) {
  return (
    <aside className="chat-sessions" aria-label="Chat sessions">
      <div className="workspace-panel__intro">
        <p className="workspace-kicker">Session rail</p>
        <h3>Conversation rail</h3>
        <p className="chat-muted">
          Keep the rail compact and operational: create, rename, reset, then switch sessions fast.
        </p>
      </div>

      <div className="workspace-stack">
        <TextInputField
          label="New label"
          placeholder="optional"
          value={newSessionLabel}
          onChange={setNewSessionLabel}
        />
        <ActionCluster>
          <ActionButton
            isDisabled={sessionsBusy}
            type="button"
            variant="primary"
            onPress={createSession}
          >
            {sessionsBusy ? "Creating..." : "Create"}
          </ActionButton>
        </ActionCluster>
      </div>

      <div className="workspace-callout">
        <div className="workspace-panel__intro">
          <p className="workspace-kicker">Selected session</p>
          <h3>
            {selectedSession?.session_label?.trim() ||
              (selectedSession ? shortId(selectedSession.session_id) : "None")}
          </h3>
        </div>
        <TextInputField
          disabled={selectedSession === null || sessionsBusy}
          label="Active label"
          value={sessionLabelDraft}
          onChange={setSessionLabelDraft}
        />
        <ActionCluster>
          <ActionButton
            isDisabled={selectedSession === null || sessionsBusy}
            type="button"
            variant="primary"
            onPress={renameSession}
          >
            Rename
          </ActionButton>
          <ActionButton
            isDisabled={selectedSession === null || sessionsBusy}
            type="button"
            variant="danger"
            onPress={resetSession}
          >
            Reset
          </ActionButton>
          <ActionButton
            isDisabled={selectedSession === null || sessionsBusy}
            type="button"
            variant="danger"
            onPress={archiveSession}
          >
            Archive
          </ActionButton>
        </ActionCluster>
      </div>

      <div className="chat-session-list" role="listbox">
        {sortedSessions.length === 0 ? (
          <EmptyState
            compact
            description="Create a session to start a conversation."
            title="No sessions yet"
          />
        ) : (
          sortedSessions.map((session) => {
            const active = session.session_id === activeSessionId;
            const label = session.session_label?.trim().length
              ? session.session_label
              : shortId(session.session_id);

            return (
              <Button
                key={session.session_id}
                aria-selected={active}
                className="chat-session-item"
                fullWidth
                type="button"
                variant={active ? "secondary" : "ghost"}
                onPress={() => setActiveSessionId(session.session_id)}
              >
                <span className="flex w-full flex-col items-start gap-1 text-left">
                  <span className="chat-session-item__title">{label}</span>
                  <small>
                    Updated {new Date(session.updated_at_unix_ms).toLocaleTimeString()} ·{" "}
                    {shortId(session.session_id)}
                  </small>
                </span>
              </Button>
            );
          })
        )}
      </div>
    </aside>
  );
}
