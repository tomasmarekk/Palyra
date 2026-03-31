import { Button } from "@heroui/react";

import {
  ActionButton,
  ActionCluster,
  EmptyState,
  SwitchField,
  TextInputField,
} from "../console/components/ui";
import type { SessionCatalogRecord } from "../consoleApi";

import { shortId } from "./chatShared";

type ChatSessionsSidebarProps = {
  sessionsBusy: boolean;
  newSessionLabel: string;
  setNewSessionLabel: (value: string) => void;
  createSession: () => void;
  searchQuery: string;
  setSearchQuery: (value: string) => void;
  includeArchived: boolean;
  setIncludeArchived: (value: boolean) => void;
  sessionLabelDraft: string;
  setSessionLabelDraft: (value: string) => void;
  selectedSession: SessionCatalogRecord | null;
  renameSession: () => void;
  resetSession: () => void;
  archiveSession: () => void;
  sortedSessions: SessionCatalogRecord[];
  activeSessionId: string;
  setActiveSessionId: (sessionId: string) => void;
};

export function ChatSessionsSidebar({
  sessionsBusy,
  newSessionLabel,
  setNewSessionLabel,
  createSession,
  searchQuery,
  setSearchQuery,
  includeArchived,
  setIncludeArchived,
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
        <TextInputField
          label="History search"
          placeholder="title, preview, or snippet"
          value={searchQuery}
          onChange={setSearchQuery}
        />
        <SwitchField
          checked={includeArchived}
          description="Include archived sessions in search and resume results."
          label="Show archived"
          onChange={setIncludeArchived}
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
          <h3>{selectedSession?.title ?? "None"}</h3>
          {selectedSession !== null && (
            <p className="chat-muted">
              {selectedSession.title_source} ·{" "}
              {selectedSession.archived ? "archived" : "active"} ·{" "}
              {shortId(selectedSession.session_id)}
            </p>
          )}
          {selectedSession?.preview !== undefined && (
            <p className="chat-muted">{selectedSession.preview}</p>
          )}
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
                  <span className="chat-session-item__title">{session.title}</span>
                  {session.preview !== undefined && <small>{session.preview}</small>}
                  <small>
                    Updated {new Date(session.updated_at_unix_ms).toLocaleTimeString()} ·{" "}
                    {session.archived ? "archived" : session.title_source} ·{" "}
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
