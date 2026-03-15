import { Chip } from "@heroui/react";
import { useEffect, useMemo } from "react";

import type { ConsoleApiClient } from "../consoleApi";
import {
  ActionButton,
  EmptyState,
  KeyValueList,
  MetricCard,
  SectionCard,
  StatusChip,
  SwitchField
} from "../console/components/ui";

import { ChatComposer } from "./ChatComposer";
import { ChatRunDrawer } from "./ChatRunDrawer";
import { ChatSessionsSidebar } from "./ChatSessionsSidebar";
import { ChatTranscript } from "./ChatTranscript";
import { shortId } from "./chatShared";
import { useChatRunStream } from "./useChatRunStream";
import { useChatSessions } from "./useChatSessions";

interface ChatConsolePanelProps {
  readonly api: ConsoleApiClient;
  readonly revealSensitiveValues: boolean;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
}

export function ChatConsolePanel({
  api,
  revealSensitiveValues,
  setError,
  setNotice
}: ChatConsolePanelProps) {
  const sessions = useChatSessions({
    api,
    setError,
    setNotice
  });

  const {
    composerText,
    setComposerText,
    allowSensitiveTools,
    setAllowSensitiveTools,
    streaming,
    activeRunId,
    runDrawerOpen,
    runDrawerBusy,
    runDrawerId,
    runStatus,
    runTape,
    transcriptBoxRef,
    approvalDrafts,
    a2uiDocuments,
    runIds,
    hiddenTranscriptItems,
    visibleTranscript,
    sendMessage,
    cancelStreaming,
    clearTranscriptState,
    openRunDetails,
    closeRunDrawer,
    refreshRunDetails,
    setRunDrawerId,
    updateApprovalDraftValue,
    decideInlineApproval,
    dispose
  } = useChatRunStream({
    api,
    activeSessionId: sessions.activeSessionId,
    sessionLabelDraft: sessions.sessionLabelDraft,
    setError,
    setNotice
  });

  const pendingApprovalCount = useMemo(
    () =>
      visibleTranscript.filter(
        (entry) => entry.kind === "approval_request" && typeof entry.approval_id === "string"
      ).length,
    [visibleTranscript]
  );
  const a2uiSurfaces = useMemo(() => Object.keys(a2uiDocuments), [a2uiDocuments]);
  const inspectorVisible = runDrawerOpen || runIds.length > 0;

  useEffect(() => {
    void sessions.refreshSessions(true);
    return () => {
      dispose();
    };
  }, []);

  async function resetSessionAndTranscript(): Promise<void> {
    const resetApplied = await sessions.resetSession();
    if (!resetApplied) {
      return;
    }
    clearTranscriptState();
    setNotice("Session reset applied. Local transcript cleared.");
  }

  return (
    <main className="workspace-page chat-workspace">
      <section className="workspace-summary-grid">
        <MetricCard
          detail="Current working conversation."
          label="Session"
          tone={sessions.selectedSession ? "success" : "warning"}
          value={
            sessions.selectedSession?.session_label?.trim() ||
            (sessions.selectedSession ? shortId(sessions.selectedSession.session_id) : "none")
          }
        />
        <MetricCard
          detail="Most recent run in focus."
          label="Active run"
          tone={activeRunId ? "default" : "warning"}
          value={activeRunId ?? "none"}
        />
        <MetricCard
          detail="Inline approval requests awaiting a decision."
          label="Pending approvals"
          tone={pendingApprovalCount > 0 ? "warning" : "success"}
          value={String(pendingApprovalCount)}
        />
        <MetricCard
          detail="Published A2UI surfaces for the current session."
          label="A2UI surfaces"
          tone={a2uiSurfaces.length > 0 ? "default" : "success"}
          value={String(a2uiSurfaces.length)}
        />
      </section>

      <section className="chat-workspace__layout">
        <SectionCard
          className="chat-panel"
          description="Create, rename, reset, and switch sessions."
          title="Sessions"
        >
          <ChatSessionsSidebar
            sessionsBusy={sessions.sessionsBusy}
            newSessionLabel={sessions.newSessionLabel}
            setNewSessionLabel={sessions.setNewSessionLabel}
            createSession={() => {
              void sessions.createSession();
            }}
            sessionLabelDraft={sessions.sessionLabelDraft}
            setSessionLabelDraft={sessions.setSessionLabelDraft}
            selectedSession={sessions.selectedSession}
            renameSession={() => {
              void sessions.renameSession();
            }}
            resetSession={() => {
              void resetSessionAndTranscript();
            }}
            sortedSessions={sessions.sortedSessions}
            activeSessionId={sessions.activeSessionId}
            setActiveSessionId={sessions.setActiveSessionId}
          />
        </SectionCard>

        <SectionCard
          className="chat-panel chat-panel--conversation"
          description="Conversation state, streaming output, and operator controls."
          title={
            sessions.selectedSession === null
              ? "No active session"
              : sessions.selectedSession.session_label?.trim().length
                ? sessions.selectedSession.session_label
                : shortId(sessions.selectedSession.session_id)
          }
          actions={
            <div className="workspace-inline-actions">
              <StatusChip tone={streaming ? "warning" : "success"}>
                {streaming ? "Streaming" : "Idle"}
              </StatusChip>
              <Chip variant="secondary">
                {activeRunId === null ? "No active run" : `Active run: ${activeRunId}`}
              </Chip>
            </div>
          }
        >
          <div className="chat-panel__body">
            <div className="chat-main-header">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">Chat</p>
                <h2>
                  {sessions.selectedSession === null
                    ? "No active session"
                    : sessions.selectedSession.session_label?.trim().length
                      ? sessions.selectedSession.session_label
                      : shortId(sessions.selectedSession.session_id)}
                </h2>
                <p className="chat-muted">
                  {activeRunId === null ? "No active run" : `Active run: ${activeRunId}`}
                </p>
              </div>
              <div className="workspace-inline-actions">
                <SwitchField
                  checked={allowSensitiveTools}
                  description="Applies to the next run only."
                  label="Allow sensitive tools"
                  onChange={setAllowSensitiveTools}
                />
                <ActionButton
                  isDisabled={sessions.sessionsBusy}
                  type="button"
                  variant="secondary"
                  onPress={() => void sessions.refreshSessions(false)}
                >
                  {sessions.sessionsBusy ? "Refreshing..." : "Refresh sessions"}
                </ActionButton>
                <ActionButton
                  isDisabled={(activeRunId ?? runIds[0] ?? null) === null}
                  type="button"
                  variant="primary"
                  onPress={() => {
                    const targetRunId = activeRunId ?? runIds[0] ?? null;
                    if (targetRunId === null) {
                      setError("No run is available for inspection.");
                      return;
                    }
                    openRunDetails(targetRunId);
                  }}
                >
                  Run details
                </ActionButton>
              </div>
            </div>

            <ChatTranscript
              visibleTranscript={visibleTranscript}
              hiddenTranscriptItems={hiddenTranscriptItems}
              transcriptBoxRef={transcriptBoxRef}
              approvalDrafts={approvalDrafts}
              a2uiDocuments={a2uiDocuments}
              revealSensitiveValues={revealSensitiveValues}
              updateApprovalDraft={updateApprovalDraftValue}
              decideInlineApproval={(approvalId, approved) => {
                void decideInlineApproval(approvalId, approved);
              }}
              openRunDetails={openRunDetails}
            />

            <ChatComposer
              composerText={composerText}
              setComposerText={setComposerText}
              streaming={streaming}
              activeSessionId={sessions.activeSessionId}
              submitMessage={() => {
                void sendMessage(() => sessions.refreshSessions(false));
              }}
              cancelStreaming={cancelStreaming}
              clearTranscript={() => {
                clearTranscriptState();
                setNotice("Local transcript cleared.");
              }}
            />
          </div>
        </SectionCard>

        <div className="chat-inspector-column">
          <SectionCard
            className="chat-panel chat-panel--sticky"
            description="Fast session context while you work."
            title={sessions.selectedSession?.session_label?.trim() || "Session summary"}
          >
            <KeyValueList
              items={[
                {
                  label: "Session ID",
                  value: sessions.selectedSession
                    ? shortId(sessions.selectedSession.session_id)
                    : "none"
                },
                {
                  label: "Updated",
                  value: sessions.selectedSession
                    ? new Date(sessions.selectedSession.updated_at_unix_ms).toLocaleString()
                    : "n/a"
                },
                { label: "Visible transcript", value: visibleTranscript.length },
                { label: "Known runs", value: runIds.length }
              ]}
            />
            <div className="workspace-inline-actions">
              <ActionButton
                isDisabled={sessions.selectedSession === null || sessions.sessionsBusy}
                type="button"
                variant="danger"
                onPress={() => {
                  void resetSessionAndTranscript();
                }}
              >
                Reset session
              </ActionButton>
            </div>
          </SectionCard>

          <SectionCard
            className="chat-panel"
            description="Approval and surface signals stay visible without taking over the main transcript."
            title="Workspace signals"
          >
            <div className="workspace-tag-row">
              <Chip color={pendingApprovalCount > 0 ? "warning" : "success"} variant="soft">
                {pendingApprovalCount} approval{pendingApprovalCount === 1 ? "" : "s"}
              </Chip>
              <Chip variant="secondary">
                {a2uiSurfaces.length} A2UI surface{a2uiSurfaces.length === 1 ? "" : "s"}
              </Chip>
            </div>
            {a2uiSurfaces.length === 0 ? (
              <EmptyState
                compact
                description="No A2UI documents published for this session yet."
                title="No A2UI surfaces"
              />
            ) : (
              <ul className="workspace-bullet-list">
                {a2uiSurfaces.map((surface) => (
                  <li key={surface}>{surface}</li>
                ))}
              </ul>
            )}
          </SectionCard>

          {inspectorVisible ? (
            <SectionCard
              className="chat-panel"
              description="Status, tape, and token usage for the selected run."
              title="Run inspector"
            >
              <ChatRunDrawer
                open
                runIds={runIds}
                runDrawerId={runDrawerId}
                setRunDrawerId={setRunDrawerId}
                runDrawerBusy={runDrawerBusy}
                runStatus={runStatus}
                runTape={runTape}
                revealSensitiveValues={revealSensitiveValues}
                refreshRun={refreshRunDetails}
                close={closeRunDrawer}
              />
            </SectionCard>
          ) : (
            <SectionCard
              className="chat-panel"
              description="Run details become available after the first streamed response."
              title="Inspector"
            >
              <EmptyState
                compact
                description="Open a run after the first streamed response to inspect status, tape, and token usage."
                title="Run details will appear here"
              />
            </SectionCard>
          )}
        </div>
      </section>
    </main>
  );
}
