import { Chip } from "@heroui/react";
import { useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import type { ConsoleApiClient } from "../consoleApi";
import {
  ActionButton,
  EmptyState,
  KeyValueList,
  PageHeader,
  SectionCard,
  StatusChip,
  SwitchField,
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
  setNotice,
}: ChatConsolePanelProps) {
  const [searchParams] = useSearchParams();
  const preferredSessionId = searchParams.get("sessionId");
  const preferredRunId = searchParams.get("runId");
  const deepLinkedRunRef = useRef<string | null>(null);
  const [runActionBusy, setRunActionBusy] = useState(false);
  const sessions = useChatSessions({
    api,
    setError,
    setNotice,
    preferredSessionId,
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
    dispose,
  } = useChatRunStream({
    api,
    activeSessionId: sessions.activeSessionId,
    sessionLabelDraft: sessions.sessionLabelDraft,
    setError,
    setNotice,
  });

  const pendingApprovalCount = useMemo(
    () =>
      visibleTranscript.filter(
        (entry) => entry.kind === "approval_request" && typeof entry.approval_id === "string",
      ).length,
    [visibleTranscript],
  );
  const a2uiSurfaces = useMemo(() => Object.keys(a2uiDocuments), [a2uiDocuments]);
  const inspectorVisible = runDrawerOpen || runIds.length > 0;
  const actionableRunId =
    activeRunId ?? (runDrawerId.trim().length > 0 ? runDrawerId.trim() : null) ?? runIds[0] ?? null;

  useEffect(() => {
    void sessions.refreshSessions(true);
    return () => {
      dispose();
    };
  }, []);

  useEffect(() => {
    if (preferredRunId === null || preferredRunId.trim().length === 0) {
      deepLinkedRunRef.current = null;
      return;
    }
    if (sessions.activeSessionId.trim().length === 0) {
      return;
    }
    if (
      preferredSessionId !== null &&
      preferredSessionId.trim().length > 0 &&
      sessions.activeSessionId !== preferredSessionId
    ) {
      return;
    }
    if (deepLinkedRunRef.current === preferredRunId) {
      return;
    }
    deepLinkedRunRef.current = preferredRunId;
    openRunDetails(preferredRunId);
  }, [openRunDetails, preferredRunId, preferredSessionId, sessions.activeSessionId]);

  async function resetSessionAndTranscript(): Promise<void> {
    const resetApplied = await sessions.resetSession();
    if (!resetApplied) {
      return;
    }
    clearTranscriptState();
    setNotice("Session reset applied. Local transcript cleared.");
  }

  async function archiveSessionAndTranscript(): Promise<void> {
    const archived = await sessions.archiveSession();
    if (!archived) {
      return;
    }
    clearTranscriptState();
    setNotice("Session archived. Local transcript cleared.");
  }

  async function abortCurrentRun(): Promise<void> {
    const targetRunId = actionableRunId;
    if (targetRunId === null || targetRunId.trim().length === 0) {
      setError("No run is available for cancellation.");
      return;
    }
    setRunActionBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.abortSessionRun(targetRunId);
      setNotice(
        response.cancel_requested ? "Run cancellation requested." : "Run was already idle.",
      );
      await sessions.refreshSessions(false);
      if (runDrawerOpen && runDrawerId.trim() === targetRunId) {
        refreshRunDetails();
      }
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setRunActionBusy(false);
    }
  }

  return (
    <main className="workspace-page chat-workspace">
      <PageHeader
        eyebrow="Chat"
        title={
          sessions.selectedSession?.title ??
          (sessions.selectedSession
            ? shortId(sessions.selectedSession.session_id)
            : "Operator workspace")
        }
        description="Sessions, transcript, approvals, and run inspection stay on one operator surface without duplicate hero headers or consumer chat chrome."
        status={
          <>
            <StatusChip tone={streaming ? "warning" : "success"}>
              {streaming ? "Streaming" : "Idle"}
            </StatusChip>
            <StatusChip tone={pendingApprovalCount > 0 ? "warning" : "default"}>
              {pendingApprovalCount} pending approval{pendingApprovalCount === 1 ? "" : "s"}
            </StatusChip>
            <StatusChip tone={runIds.length > 0 ? "accent" : "default"}>
              {runIds.length} known run{runIds.length === 1 ? "" : "s"}
            </StatusChip>
            <Chip size="sm" variant="secondary">
              {a2uiSurfaces.length} A2UI surface{a2uiSurfaces.length === 1 ? "" : "s"}
            </Chip>
          </>
        }
        actions={
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
            <ActionButton
              isDisabled={runActionBusy || actionableRunId === null}
              type="button"
              variant="ghost"
              onPress={() => void abortCurrentRun()}
            >
              {runActionBusy ? "Aborting..." : "Abort run"}
            </ActionButton>
          </div>
        }
      />

      <section className="chat-workspace__layout">
        <SectionCard
          className="chat-panel"
          description="Create, rename, reset, and switch sessions without leaving the active conversation."
          title="Sessions"
          actions={
            <StatusChip tone={sessions.selectedSession ? "success" : "warning"}>
              {sessions.selectedSession ? "Active session" : "No session"}
            </StatusChip>
          }
        >
          <ChatSessionsSidebar
            sessionsBusy={sessions.sessionsBusy}
            newSessionLabel={sessions.newSessionLabel}
            setNewSessionLabel={sessions.setNewSessionLabel}
            createSession={() => {
              void sessions.createSession();
            }}
            searchQuery={sessions.searchQuery}
            setSearchQuery={sessions.setSearchQuery}
            includeArchived={sessions.includeArchived}
            setIncludeArchived={sessions.setIncludeArchived}
            sessionLabelDraft={sessions.sessionLabelDraft}
            setSessionLabelDraft={sessions.setSessionLabelDraft}
            selectedSession={sessions.selectedSession}
            renameSession={() => {
              void sessions.renameSession();
            }}
            resetSession={() => {
              void resetSessionAndTranscript();
            }}
            archiveSession={() => {
              void archiveSessionAndTranscript();
            }}
            sortedSessions={sessions.sortedSessions}
            activeSessionId={sessions.activeSessionId}
            setActiveSessionId={sessions.setActiveSessionId}
          />
        </SectionCard>

        <SectionCard
          className="chat-panel chat-panel--conversation"
          description="Transcript, approvals, and composer for the current working session."
          title="Conversation"
          actions={
            <div className="workspace-inline-actions">
              <StatusChip tone={streaming ? "warning" : "success"}>
                {streaming ? "Streaming" : "Idle"}
              </StatusChip>
              <StatusChip tone={pendingApprovalCount > 0 ? "warning" : "default"}>
                {pendingApprovalCount} approval{pendingApprovalCount === 1 ? "" : "s"}
              </StatusChip>
              <Chip variant="secondary">
                {activeRunId === null ? "No active run" : `Active run: ${activeRunId}`}
              </Chip>
            </div>
          }
        >
          <div className="chat-panel__body">
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
            description="Approval backlog, surface count, and run inventory stay visible without turning the chat into a dashboard."
            title="Workspace signals"
          >
            <div className="workspace-tag-row">
              <Chip color={pendingApprovalCount > 0 ? "warning" : "success"} variant="soft">
                {pendingApprovalCount} approval{pendingApprovalCount === 1 ? "" : "s"}
              </Chip>
              <Chip variant="secondary">
                {a2uiSurfaces.length} A2UI surface{a2uiSurfaces.length === 1 ? "" : "s"}
              </Chip>
              <Chip variant="secondary">
                {runIds.length} known run{runIds.length === 1 ? "" : "s"}
              </Chip>
            </div>
            <KeyValueList
              items={[
                {
                  label: "Session",
                  value:
                    sessions.selectedSession?.title ||
                    (sessions.selectedSession
                      ? shortId(sessions.selectedSession.session_id)
                      : "none"),
                },
                {
                  label: "Preview",
                  value: sessions.selectedSession?.preview ?? "n/a",
                },
                {
                  label: "Updated",
                  value: sessions.selectedSession
                    ? new Date(sessions.selectedSession.updated_at_unix_ms).toLocaleString()
                    : "n/a",
                },
                { label: "Visible transcript", value: visibleTranscript.length },
              ]}
            />
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
              description="Status, tape, and token usage stay secondary to the transcript but available on demand."
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
              title="Run inspector"
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
