import { Button, ScrollShadow } from "@heroui/react";

import {
  ActionButton,
  ActionCluster,
  EmptyState,
  SectionCard,
  TextInputField
} from "../console/components/ui";
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

      <SectionCard title="New session">
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
      </SectionCard>

      <SectionCard title="Selected session">
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
        </ActionCluster>
      </SectionCard>

      <ScrollShadow className="chat-session-list" role="listbox">
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
                variant={active ? "secondary" : "tertiary"}
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
      </ScrollShadow>
    </aside>
  );
}
