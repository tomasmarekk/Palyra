import { Card, CardContent, Chip } from "@heroui/react";
import { useEffect, useMemo } from "react";

import type { ConsoleApiClient } from "../consoleApi";

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
        <ChatMetric label="Session" value={sessions.selectedSession?.session_label?.trim() || (sessions.selectedSession ? shortId(sessions.selectedSession.session_id) : "none")} tone={sessions.selectedSession ? "success" : "warning"} />
        <ChatMetric label="Active run" value={activeRunId ?? "none"} tone={activeRunId ? "default" : "warning"} />
        <ChatMetric label="Pending approvals" value={String(pendingApprovalCount)} tone={pendingApprovalCount > 0 ? "warning" : "success"} />
        <ChatMetric label="A2UI surfaces" value={String(a2uiSurfaces.length)} tone={a2uiSurfaces.length > 0 ? "default" : "success"} />
      </section>

      <section className="chat-workspace__layout">
        <Card className="chat-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="px-4 py-4">
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
          </CardContent>
        </Card>

        <Card className="chat-panel chat-panel--conversation border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
          <CardContent className="chat-panel__body px-4 py-4">
            <header className="chat-main-header">
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
                <Chip color={streaming ? "warning" : "success"} variant="soft">
                  {streaming ? "Streaming" : "Idle"}
                </Chip>
                <label className="console-checkbox-inline">
                  <input
                    type="checkbox"
                    checked={allowSensitiveTools}
                    onChange={(event) => setAllowSensitiveTools(event.target.checked)}
                  />
                  Allow sensitive tools for next run
                </label>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => void sessions.refreshSessions(false)}
                  disabled={sessions.sessionsBusy}
                >
                  {sessions.sessionsBusy ? "Refreshing..." : "Refresh sessions"}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    const targetRunId = activeRunId ?? runIds[0] ?? null;
                    if (targetRunId === null) {
                      setError("No run is available for inspection.");
                      return;
                    }
                    openRunDetails(targetRunId);
                  }}
                  disabled={(activeRunId ?? runIds[0] ?? null) === null}
                >
                  Run details
                </button>
              </div>
            </header>

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
          </CardContent>
        </Card>

        <div className="chat-inspector-column">
          <Card className="chat-panel chat-panel--sticky border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
            <CardContent className="gap-4 px-4 py-4">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">Session Context</p>
                <h3>{sessions.selectedSession?.session_label?.trim() || "Session summary"}</h3>
              </div>
              <dl className="workspace-detail-grid">
                <div>
                  <dt>Session ID</dt>
                  <dd>{sessions.selectedSession ? shortId(sessions.selectedSession.session_id) : "none"}</dd>
                </div>
                <div>
                  <dt>Updated</dt>
                  <dd>
                    {sessions.selectedSession
                      ? new Date(sessions.selectedSession.updated_at_unix_ms).toLocaleString()
                      : "n/a"}
                  </dd>
                </div>
                <div>
                  <dt>Visible transcript</dt>
                  <dd>{visibleTranscript.length}</dd>
                </div>
                <div>
                  <dt>Known runs</dt>
                  <dd>{runIds.length}</dd>
                </div>
              </dl>
              <div className="workspace-inline-actions">
                <button type="button" className="button--warn" onClick={() => {
                  void resetSessionAndTranscript();
                }} disabled={sessions.selectedSession === null || sessions.sessionsBusy}>
                  Reset session
                </button>
              </div>
            </CardContent>
          </Card>

          <Card className="chat-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
            <CardContent className="gap-4 px-4 py-4">
              <div className="workspace-panel__intro">
                <p className="workspace-kicker">Workspace Signals</p>
                <h3>Approvals and A2UI</h3>
              </div>
              <div className="workspace-tag-row">
                <Chip color={pendingApprovalCount > 0 ? "warning" : "success"} variant="soft">
                  {pendingApprovalCount} approval{pendingApprovalCount === 1 ? "" : "s"}
                </Chip>
                <Chip variant="secondary">{a2uiSurfaces.length} A2UI surface{a2uiSurfaces.length === 1 ? "" : "s"}</Chip>
              </div>
              {a2uiSurfaces.length === 0 ? (
                <p className="workspace-empty">No A2UI documents published for this session yet.</p>
              ) : (
                <ul className="workspace-bullet-list">
                  {a2uiSurfaces.map((surface) => (
                    <li key={surface}>{surface}</li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>

          {inspectorVisible ? (
            <Card className="chat-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
              <CardContent className="px-4 py-4">
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
              </CardContent>
            </Card>
          ) : (
            <Card className="chat-panel border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
              <CardContent className="gap-3 px-4 py-4">
                <div className="workspace-panel__intro">
                  <p className="workspace-kicker">Inspector</p>
                  <h3>Run details will appear here</h3>
                </div>
                <p className="workspace-empty">Open a run after the first streamed response to inspect status, tape, and token usage.</p>
              </CardContent>
            </Card>
          )}
        </div>
      </section>
    </main>
  );
}

function ChatMetric({
  label,
  value,
  tone
}: {
  label: string;
  value: string;
  tone: "default" | "success" | "warning" | "danger" | "accent";
}) {
  return (
    <Card className="workspace-stat-card border border-white/40 bg-white/80 shadow-xl shadow-slate-900/5 dark:border-white/10 dark:bg-slate-950/70">
      <CardContent className="gap-3 px-5 py-4">
        <div className="workspace-stat-card__header">
          <p className="workspace-kicker">{label}</p>
          <Chip color={tone} variant="soft">
            {value}
          </Chip>
        </div>
        <p className="workspace-stat-card__value">{value}</p>
      </CardContent>
    </Card>
  );
}
