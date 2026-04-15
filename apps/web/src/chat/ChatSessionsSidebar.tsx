import { Button, Chip } from "@heroui/react";

import {
  ActionButton,
  ActionCluster,
  EmptyState,
  SwitchField,
  TextInputField,
} from "../console/components/ui";
import type { SessionCatalogRecord } from "../consoleApi";

import {
  buildSessionLineageHint,
  describeBranchState,
  describeTitleGenerationState,
  shortId,
} from "./chatShared";

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
          placeholder="title, family, agent, model, file, or recap"
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
              {describeTitleGenerationState(
                selectedSession.title_generation_state,
                selectedSession.manual_title_locked,
              )}{" "}
              · {selectedSession.archived ? "archived" : "active"} ·{" "}
              {shortId(selectedSession.session_id)}
            </p>
          )}
          {selectedSession !== null ? (
            <div className="workspace-chip-row">
              <Chip size="sm" variant="secondary">
                {describeBranchState(selectedSession.branch_state)}
              </Chip>
              {selectedSession.last_run_state !== undefined ? (
                <Chip size="sm" variant="secondary">
                  Last run {selectedSession.last_run_state}
                </Chip>
              ) : null}
              {selectedSession.family.family_size > 1 ? (
                <Chip size="sm" variant="secondary">
                  Family {selectedSession.family.sequence}/{selectedSession.family.family_size}
                </Chip>
              ) : null}
              {selectedSession.has_context_files ? (
                <Chip size="sm" variant="secondary">
                  {selectedSession.recap.active_context_files.length} context file
                  {selectedSession.recap.active_context_files.length === 1 ? "" : "s"}
                </Chip>
              ) : null}
            </div>
          ) : null}
          {selectedSession !== null &&
          selectedSession.family.root_title.length > 0 &&
          selectedSession.family.root_title !== selectedSession.title ? (
            <p className="chat-muted">Family root: {selectedSession.family.root_title}</p>
          ) : null}
          {selectedSession?.preview !== undefined && (
            <p className="chat-muted">{selectedSession.preview}</p>
          )}
          {selectedSession?.preview === undefined && selectedSession?.last_summary !== undefined ? (
            <p className="chat-muted">{selectedSession.last_summary}</p>
          ) : null}
          {selectedSession !== null ? (
            <p className="chat-muted">{buildSessionLineageHint(selectedSession)}</p>
          ) : null}
        </div>
        <TextInputField
          disabled={selectedSession === null || sessionsBusy}
          description="Leave empty to return to automatic titles."
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
                  {session.preview === undefined && session.last_summary !== undefined ? (
                    <small>{session.last_summary}</small>
                  ) : null}
                  <span className="chat-session-item__meta">
                    <Chip size="sm" variant="secondary">
                      {describeBranchState(session.branch_state)}
                    </Chip>
                    <Chip size="sm" variant="secondary">
                      {describeTitleGenerationState(
                        session.title_generation_state,
                        session.manual_title_locked,
                      )}
                    </Chip>
                    {session.family.family_size > 1 ? (
                      <Chip size="sm" variant="secondary">
                        Family {session.family.sequence}/{session.family.family_size}
                      </Chip>
                    ) : null}
                    {session.pending_approvals > 0 ? (
                      <Chip color="warning" size="sm" variant="soft">
                        {session.pending_approvals} approval
                        {session.pending_approvals === 1 ? "" : "s"}
                      </Chip>
                    ) : null}
                  </span>
                  {session.family.root_title !== session.title ? (
                    <small>Root: {session.family.root_title}</small>
                  ) : null}
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
