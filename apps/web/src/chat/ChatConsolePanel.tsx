
import { useEffect, useMemo, useRef, useState } from "react";

import {
  applyPatchDocument,
  documentToJsonValue,
  normalizeA2uiDocument,
  parsePatchDocument,
  type A2uiDocument
} from "../a2ui";
import type {
  ChatRunStatusRecord,
  ChatRunTapeSnapshot,
  ChatSessionRecord,
  ChatStreamEventEnvelope,
  ChatStreamLine,
  JsonValue,
  ConsoleApiClient
} from "../consoleApi";
import { ChatRunDrawer } from "./ChatRunDrawer";
import { ChatTranscript } from "./ChatTranscript";
import {
  DEFAULT_APPROVAL_SCOPE,
  DEFAULT_APPROVAL_TTL_MS,
  MAX_RENDERED_TRANSCRIPT,
  asBoolean,
  asObject,
  asString,
  collectCanvasFrameUrls,
  emptyToUndefined,
  isAbortError,
  normalizePatchValue,
  parseInteger,
  prettifyEventType,
  retainTranscriptWindow,
  shortId,
  toErrorMessage
} from "./chatShared";
import type { ApprovalDraft, TranscriptEntry } from "./chatShared";

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
  const [sessionsBusy, setSessionsBusy] = useState(false);
  const [sessions, setSessions] = useState<ChatSessionRecord[]>([]);
  const [activeSessionId, setActiveSessionId] = useState("");
  const [sessionLabelDraft, setSessionLabelDraft] = useState("");
  const [newSessionLabel, setNewSessionLabel] = useState("");

  const [composerText, setComposerText] = useState("");
  const [allowSensitiveTools, setAllowSensitiveTools] = useState(false);
  const [streaming, setStreaming] = useState(false);
  const streamAbortRef = useRef<AbortController | null>(null);
  const runDetailsRequestSeqRef = useRef(0);

  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [runDrawerOpen, setRunDrawerOpen] = useState(false);
  const [runDrawerBusy, setRunDrawerBusy] = useState(false);
  const [runDrawerId, setRunDrawerId] = useState("");
  const [runStatus, setRunStatus] = useState<ChatRunStatusRecord | null>(null);
  const [runTape, setRunTape] = useState<ChatRunTapeSnapshot | null>(null);

  const [transcript, setTranscript] = useState<TranscriptEntry[]>([]);
  const transcriptBoxRef = useRef<HTMLDivElement | null>(null);
  const assistantEntryByRunRef = useRef<Map<string, string>>(new Map());
  const canvasEntrySetRef = useRef<Set<string>>(new Set());

  const [approvalDrafts, setApprovalDrafts] = useState<Record<string, ApprovalDraft>>({});

  const [a2uiDocuments, setA2uiDocuments] = useState<Record<string, A2uiDocument>>({});
  const a2uiDocumentsRef = useRef<Record<string, A2uiDocument>>({});

  const sortedSessions = useMemo(() => {
    return [...sessions].sort((left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms);
  }, [sessions]);

  const selectedSession = useMemo(() => {
    return sortedSessions.find((session) => session.session_id === activeSessionId) ?? null;
  }, [activeSessionId, sortedSessions]);

  const runIds = useMemo(() => {
    const values = new Set<string>();
    for (const entry of transcript) {
      if (typeof entry.run_id === "string" && entry.run_id.length > 0) {
        values.add(entry.run_id);
      }
    }
    return Array.from(values).reverse();
  }, [transcript]);

  const hiddenTranscriptItems = Math.max(0, transcript.length - MAX_RENDERED_TRANSCRIPT);
  const visibleTranscript = useMemo(() => {
    if (transcript.length <= MAX_RENDERED_TRANSCRIPT) {
      return transcript;
    }
    return transcript.slice(-MAX_RENDERED_TRANSCRIPT);
  }, [transcript]);

  useEffect(() => {
    void refreshSessions(true);
    return () => {
      streamAbortRef.current?.abort();
      streamAbortRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (selectedSession === null) {
      setSessionLabelDraft("");
      return;
    }
    setSessionLabelDraft(selectedSession.session_label ?? "");
  }, [selectedSession]);

  useEffect(() => {
    if (transcriptBoxRef.current === null) {
      return;
    }
    transcriptBoxRef.current.scrollTop = transcriptBoxRef.current.scrollHeight;
  }, [visibleTranscript.length]);

  useEffect(() => {
    if (!runDrawerOpen || runDrawerId.trim().length === 0) {
      return;
    }
    void loadRunDetails(runDrawerId.trim());
  }, [runDrawerOpen, runDrawerId]);

  async function refreshSessions(ensureSession: boolean): Promise<void> {
    setSessionsBusy(true);
    try {
      const response = await api.listChatSessions();
      const nextSessions = [...response.sessions].sort(
        (left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms
      );
      if (nextSessions.length === 0 && ensureSession) {
        const created = await api.resolveChatSession({
          session_label: emptyToUndefined(newSessionLabel)
        });
        setSessions([created.session]);
        setActiveSessionId(created.session.session_id);
        setNewSessionLabel("");
        setNotice("New chat session created.");
        return;
      }
      setSessions(nextSessions);
      if (nextSessions.length === 0) {
        setActiveSessionId("");
        return;
      }
      setActiveSessionId((previous) => {
        if (previous.length > 0 && nextSessions.some((session) => session.session_id === previous)) {
          return previous;
        }
        return nextSessions[0].session_id;
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function createSession(): Promise<void> {
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.resolveChatSession({
        session_label: emptyToUndefined(newSessionLabel)
      });
      setSessions((previous) => {
        const without = previous.filter((entry) => entry.session_id !== response.session.session_id);
        return [response.session, ...without];
      });
      setActiveSessionId(response.session.session_id);
      setNewSessionLabel("");
      setNotice("Chat session created.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function renameSession(): Promise<void> {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return;
    }
    if (sessionLabelDraft.trim().length === 0) {
      setError("Session label cannot be empty.");
      return;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.renameChatSession(activeSessionId, {
        session_label: sessionLabelDraft.trim()
      });
      setSessions((previous) => {
        return previous.map((entry) => {
          if (entry.session_id !== response.session.session_id) {
            return entry;
          }
          return response.session;
        });
      });
      setNotice("Session label updated.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function resetSession(): Promise<void> {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.resetChatSession(activeSessionId);
      setSessions((previous) => {
        return previous.map((entry) => {
          if (entry.session_id !== response.session.session_id) {
            return entry;
          }
          return response.session;
        });
      });
      clearTranscriptState();
      setNotice("Session reset applied. Local transcript cleared.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  function clearTranscriptState(): void {
    runDetailsRequestSeqRef.current += 1;
    assistantEntryByRunRef.current.clear();
    canvasEntrySetRef.current.clear();
    setTranscript([]);
    setActiveRunId(null);
    setRunDrawerBusy(false);
    setRunDrawerId("");
    setRunStatus(null);
    setRunTape(null);
    a2uiDocumentsRef.current = {};
    setA2uiDocuments({});
    setApprovalDrafts({});
  }

  async function sendMessage(event: React.FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (activeSessionId.trim().length === 0) {
      setError("Select or create a chat session before sending a message.");
      return;
    }
    const trimmed = composerText.trim();
    if (trimmed.length === 0) {
      setError("Message cannot be empty.");
      return;
    }
    if (streaming) {
      setError("A stream is already active. Cancel it first.");
      return;
    }

    setError(null);
    setNotice(null);
    setComposerText("");
    appendTranscriptEntry({
      id: `user-${Date.now()}`,
      kind: "user",
      created_at_unix_ms: Date.now(),
      session_id: activeSessionId,
      title: "You",
      text: trimmed
    });

    const controller = new AbortController();
    streamAbortRef.current = controller;
    setStreaming(true);
    try {
      await api.streamChatMessage(
        activeSessionId,
        {
          text: trimmed,
          allow_sensitive_tools: allowSensitiveTools,
          session_label: emptyToUndefined(sessionLabelDraft)
        },
        {
          signal: controller.signal,
          onLine: handleStreamLine
        }
      );
      await refreshSessions(false);
    } catch (error) {
      if (isAbortError(error)) {
        setNotice("Streaming canceled.");
      } else {
        setError(toErrorMessage(error));
      }
    } finally {
      if (streamAbortRef.current === controller) {
        streamAbortRef.current = null;
      }
      setStreaming(false);
    }
  }

  function cancelStreaming(): void {
    if (streamAbortRef.current !== null) {
      streamAbortRef.current.abort();
      streamAbortRef.current = null;
    }
  }

  function handleStreamLine(line: ChatStreamLine): void {
    if (line.type === "meta") {
      setActiveRunId(line.run_id);
      if (runDrawerId.trim().length === 0) {
        setRunDrawerId(line.run_id);
      }
      appendTranscriptEntry({
        id: `meta-${line.run_id}`,
        kind: "meta",
        created_at_unix_ms: Date.now(),
        run_id: line.run_id,
        session_id: line.session_id,
        title: "Run accepted",
        text: `Run ${line.run_id} attached to session ${line.session_id}.`
      });
      return;
    }

    if (line.type === "error") {
      appendTranscriptEntry({
        id: `error-${Date.now()}`,
        kind: "error",
        created_at_unix_ms: Date.now(),
        run_id: line.run_id,
        title: "Stream error",
        text: line.error
      });
      setError(line.error);
      return;
    }

    if (line.type === "complete") {
      appendTranscriptEntry({
        id: `complete-${Date.now()}`,
        kind: "complete",
        created_at_unix_ms: Date.now(),
        run_id: line.run_id,
        title: "Run complete",
        text: `Run status: ${line.status}`,
        status: line.status
      });
      setActiveRunId(line.run_id);
      setRunDrawerId((previous) => (previous.trim().length === 0 ? line.run_id : previous));
      return;
    }

    handleRunEvent(line.event);
  }

  function handleRunEvent(event: ChatStreamEventEnvelope): void {
    const runId = event.run_id;
    if (typeof runId === "string" && runId.length > 0) {
      setActiveRunId(runId);
    }

    if (event.event_type === "model_token") {
      const modelToken = asObject(event.model_token);
      const token = asString(modelToken?.token) ?? "";
      const isFinal = asBoolean(modelToken?.is_final) ?? false;
      if (token.length > 0 || isFinal) {
        appendAssistantToken(runId, token, isFinal);
      }
      return;
    }

    if (event.event_type === "status") {
      const status = asObject(event.status);
      const statusKind = asString(status?.kind) ?? "unknown";
      const message = asString(status?.message) ?? "";
      appendTranscriptEntry({
        id: `status-${Date.now()}`,
        kind: "status",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: `Status: ${statusKind}`,
        text: message,
        status: statusKind,
        payload: event
      });
      return;
    }

    if (event.event_type === "tool_approval_request") {
      const request = asObject(event.tool_approval_request);
      const approvalId = asString(request?.approval_id) ?? "";
      const proposalId = asString(request?.proposal_id) ?? "";
      const toolName = asString(request?.tool_name) ?? "tool";
      const summary = asString(request?.request_summary) ?? "Approval required.";
      if (approvalId.length > 0) {
        ensureApprovalDraft(approvalId);
      }
      appendTranscriptEntry({
        id: `approval-request-${approvalId}-${Date.now()}`,
        kind: "approval_request",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: `Approval request: ${toolName}`,
        text: summary,
        approval_id: approvalId,
        proposal_id: proposalId,
        tool_name: toolName,
        payload: request ?? event
      });
      return;
    }

    if (event.event_type === "tool_approval_response") {
      appendTranscriptEntry({
        id: `approval-response-${Date.now()}`,
        kind: "approval_response",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: "Approval response",
        payload: event.tool_approval_response ?? event
      });
      return;
    }

    if (event.event_type === "a2ui_update") {
      const update = asObject(event.a2ui_update);
      const surface = asString(update?.surface) ?? "chat";
      const patchValue = normalizePatchValue(update?.patch_json);
      if (patchValue !== null) {
        applyA2uiPatch(surface, patchValue);
      }
      appendTranscriptEntry({
        id: `a2ui-${Date.now()}`,
        kind: "a2ui",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: `A2UI update: ${surface}`,
        surface,
        payload: update ?? event
      });
      return;
    }

    if (event.event_type === "journal_event") {
      appendTranscriptEntry({
        id: `journal-${Date.now()}`,
        kind: "journal",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: "Journal event",
        payload: event.journal_event ?? event
      });
      return;
    }

    if (
      event.event_type === "tool_proposal" ||
      event.event_type === "tool_decision" ||
      event.event_type === "tool_result" ||
      event.event_type === "tool_attestation"
    ) {
      const payloadKey = event.event_type;
      const payloadValue = (event as Record<string, JsonValue>)[payloadKey];
      appendTranscriptEntry({
        id: `${event.event_type}-${Date.now()}`,
        kind: "tool",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: prettifyEventType(event.event_type),
        payload: payloadValue ?? event
      });
      const canvasCandidates = collectCanvasFrameUrls(payloadValue ?? event);
      for (const canvasUrl of canvasCandidates) {
        appendCanvasEntry(runId, canvasUrl);
      }
      return;
    }

    appendTranscriptEntry({
      id: `event-${Date.now()}`,
      kind: "event",
      created_at_unix_ms: Date.now(),
      run_id: runId,
      title: prettifyEventType(event.event_type),
      payload: event
    });
  }

  function ensureApprovalDraft(approvalId: string): void {
    setApprovalDrafts((previous) => {
      if (previous[approvalId] !== undefined) {
        return previous;
      }
      return {
        ...previous,
        [approvalId]: {
          scope: DEFAULT_APPROVAL_SCOPE,
          reason: "",
          ttl_ms: DEFAULT_APPROVAL_TTL_MS,
          busy: false
        }
      };
    });
  }

  function applyA2uiPatch(surface: string, patchValue: unknown): void {
    const currentDocument =
      a2uiDocumentsRef.current[surface] ??
      normalizeA2uiDocument({
        v: 1,
        surface,
        components: []
      });
    let nextDocument: A2uiDocument;
    try {
      const patch = parsePatchDocument(patchValue);
      const patchedValue = applyPatchDocument(documentToJsonValue(currentDocument), patch);
      nextDocument = normalizeA2uiDocument(patchedValue);
    } catch (error) {
      setError(`A2UI patch rejected for surface '${surface}': ${toErrorMessage(error)}`);
      return;
    }
    const nextDocuments = {
      ...a2uiDocumentsRef.current,
      [surface]: nextDocument
    };
    a2uiDocumentsRef.current = nextDocuments;
    setA2uiDocuments(nextDocuments);
  }

  function appendCanvasEntry(runId: string, canvasUrl: string): void {
    const key = `${runId}:${canvasUrl}`;
    if (canvasEntrySetRef.current.has(key)) {
      return;
    }
    canvasEntrySetRef.current.add(key);
    appendTranscriptEntry({
      id: `canvas-${Date.now()}`,
      kind: "canvas",
      created_at_unix_ms: Date.now(),
      run_id: runId,
      title: "Canvas",
      canvas_url: canvasUrl,
      text: canvasUrl
    });
  }

  function appendAssistantToken(runId: string, token: string, isFinal: boolean): void {
    setTranscript((previous) => {
      const mappedEntryId = assistantEntryByRunRef.current.get(runId);
      if (mappedEntryId !== undefined) {
        const index = previous.findIndex((entry) => entry.id === mappedEntryId);
        if (index >= 0) {
          const existing = previous[index];
          const nextEntry: TranscriptEntry = {
            ...existing,
            text: `${existing.text ?? ""}${token}`,
            is_final: Boolean(existing.is_final) || isFinal
          };
          const next = [...previous];
          next[index] = nextEntry;
          return retainTranscriptWindow(next);
        }
      }

      const entryId = `assistant-${runId}-${Date.now()}`;
      assistantEntryByRunRef.current.set(runId, entryId);
      const appended: TranscriptEntry = {
        id: entryId,
        kind: "assistant",
        created_at_unix_ms: Date.now(),
        run_id: runId,
        title: "Assistant",
        text: token,
        is_final: isFinal
      };
      const next = [
        ...previous,
        appended
      ];
      return retainTranscriptWindow(next);
    });
  }

  function appendTranscriptEntry(entry: TranscriptEntry): void {
    setTranscript((previous) => retainTranscriptWindow([...previous, entry]));
  }

  function updateApprovalDraft(
    approvalId: string,
    mutator: (draft: ApprovalDraft) => ApprovalDraft
  ): void {
    setApprovalDrafts((previous) => {
      const current =
        previous[approvalId] ??
        ({
          scope: DEFAULT_APPROVAL_SCOPE,
          reason: "",
          ttl_ms: DEFAULT_APPROVAL_TTL_MS,
          busy: false
        } satisfies ApprovalDraft);
      return {
        ...previous,
        [approvalId]: mutator(current)
      };
    });
  }

  async function decideInlineApproval(approvalId: string, approved: boolean): Promise<void> {
    const draft = approvalDrafts[approvalId];
    if (draft === undefined) {
      setError("Approval draft state is missing.");
      return;
    }

    updateApprovalDraft(approvalId, (current) => ({ ...current, busy: true }));
    setError(null);
    setNotice(null);
    try {
      const ttl = parseInteger(draft.ttl_ms);
      await api.decideApproval(approvalId, {
        approved,
        reason: emptyToUndefined(draft.reason),
        decision_scope: draft.scope,
        decision_scope_ttl_ms:
          draft.scope === "timeboxed" && ttl !== null && ttl > 0 ? ttl : undefined
      });
      setNotice(approved ? `Approval ${approvalId} allowed.` : `Approval ${approvalId} denied.`);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      updateApprovalDraft(approvalId, (current) => ({ ...current, busy: false }));
    }
  }

  async function loadRunDetails(runId: string): Promise<void> {
    const requestSeq = runDetailsRequestSeqRef.current + 1;
    runDetailsRequestSeqRef.current = requestSeq;
    setRunDrawerBusy(true);
    try {
      const params = new URLSearchParams();
      params.set("limit", "256");
      const [statusResponse, eventsResponse] = await Promise.all([
        api.chatRunStatus(runId),
        api.chatRunEvents(runId, params)
      ]);
      if (requestSeq !== runDetailsRequestSeqRef.current) {
        return;
      }
      setRunStatus(statusResponse.run);
      setRunTape(eventsResponse.tape);
    } catch (error) {
      if (requestSeq !== runDetailsRequestSeqRef.current) {
        return;
      }
      setError(toErrorMessage(error));
    } finally {
      if (requestSeq === runDetailsRequestSeqRef.current) {
        setRunDrawerBusy(false);
      }
    }
  }

  return (
    <main className="console-card chat-console-panel">
      <header className="console-card__header">
        <div>
          <h2>Chat Workspace</h2>
          <p className="console-copy">
            Streaming runs, inline approvals, A2UI surfaces, and canvas embeds in one operator-safe view.
          </p>
        </div>
        <div className="console-inline-actions">
          <button type="button" onClick={() => void refreshSessions(false)} disabled={sessionsBusy}>
            {sessionsBusy ? "Refreshing..." : "Refresh sessions"}
          </button>
          <button
            type="button"
            onClick={() => {
              if (activeRunId === null) {
                setError("No active run selected.");
                return;
              }
              setRunDrawerId(activeRunId);
              setRunDrawerOpen(true);
            }}
            disabled={activeRunId === null}
          >
            Run details
          </button>
        </div>
      </header>

      <div className="chat-layout">
        <aside className="chat-sessions" aria-label="Chat sessions">
          <h3>Sessions</h3>
          <div className="chat-session-create">
            <label>
              New label
              <input
                value={newSessionLabel}
                onChange={(event) => setNewSessionLabel(event.target.value)}
                placeholder="optional"
              />
            </label>
            <button type="button" onClick={() => void createSession()} disabled={sessionsBusy}>
              Create
            </button>
          </div>

          <div className="chat-session-edit">
            <label>
              Active label
              <input
                value={sessionLabelDraft}
                onChange={(event) => setSessionLabelDraft(event.target.value)}
                disabled={selectedSession === null || sessionsBusy}
              />
            </label>
            <div className="console-inline-actions">
              <button type="button" onClick={() => void renameSession()} disabled={selectedSession === null || sessionsBusy}>
                Rename
              </button>
              <button type="button" className="button--warn" onClick={() => void resetSession()} disabled={selectedSession === null || sessionsBusy}>
                Reset
              </button>
            </div>
          </div>

          <div className="chat-session-list" role="listbox" aria-label="Conversation sessions">
            {sortedSessions.length === 0 ? (
              <p className="chat-muted">No sessions yet.</p>
            ) : (
              sortedSessions.map((session) => {
                const active = session.session_id === activeSessionId;
                const label = session.session_label?.trim().length
                  ? session.session_label
                  : shortId(session.session_id);
                return (
                  <button
                    key={session.session_id}
                    type="button"
                    className={`chat-session-item${active ? " is-active" : ""}`}
                    onClick={() => setActiveSessionId(session.session_id)}
                    aria-selected={active}
                  >
                    <span>{label}</span>
                    <small>
                      Updated {new Date(session.updated_at_unix_ms).toLocaleTimeString()} · {shortId(session.session_id)}
                    </small>
                  </button>
                );
              })
            )}
          </div>
        </aside>

        <section className="chat-main" aria-label="Conversation stream">
          <header className="chat-main-header">
            <div>
              <h3>
                {selectedSession === null
                  ? "No active session"
                  : selectedSession.session_label?.trim().length
                    ? selectedSession.session_label
                    : shortId(selectedSession.session_id)}
              </h3>
              <p className="chat-muted">
                {activeRunId === null ? "No active run" : `Active run: ${activeRunId}`}
              </p>
            </div>
            <label className="console-checkbox-inline">
              <input
                type="checkbox"
                checked={allowSensitiveTools}
                onChange={(event) => setAllowSensitiveTools(event.target.checked)}
              />
              Allow sensitive tools for next run
            </label>
          </header>

          <ChatTranscript
            visibleTranscript={visibleTranscript}
            hiddenTranscriptItems={hiddenTranscriptItems}
            transcriptBoxRef={transcriptBoxRef}
            approvalDrafts={approvalDrafts}
            a2uiDocuments={a2uiDocuments}
            revealSensitiveValues={revealSensitiveValues}
            updateApprovalDraft={(approvalId, next) => {
              updateApprovalDraft(approvalId, () => next);
            }}
            decideInlineApproval={(approvalId, approved) => {
              void decideInlineApproval(approvalId, approved);
            }}
            openRunDetails={(runId) => {
              setRunDrawerId(runId);
              setRunDrawerOpen(true);
            }}
          />

          <form className="chat-composer" onSubmit={(event) => {
            void sendMessage(event);
          }}>
            <label>
              Message
              <textarea
                value={composerText}
                onChange={(event) => setComposerText(event.target.value)}
                rows={4}
                placeholder="Describe what you want the assistant to do"
              />
            </label>
            <div className="console-inline-actions">
              <button type="submit" disabled={streaming || activeSessionId.trim().length === 0}>
                {streaming ? "Streaming..." : "Send"}
              </button>
              <button type="button" className="button--warn" onClick={cancelStreaming} disabled={!streaming}>
                Cancel stream
              </button>
              <button
                type="button"
                onClick={() => {
                  clearTranscriptState();
                  setNotice("Local transcript cleared.");
                }}
              >
                Clear local transcript
              </button>
            </div>
          </form>
        </section>
      </div>

      <ChatRunDrawer
        open={runDrawerOpen}
        runIds={runIds}
        runDrawerId={runDrawerId}
        setRunDrawerId={setRunDrawerId}
        runDrawerBusy={runDrawerBusy}
        runStatus={runStatus}
        runTape={runTape}
        revealSensitiveValues={revealSensitiveValues}
        refreshRun={() => {
          if (runDrawerId.trim().length > 0) {
            void loadRunDetails(runDrawerId.trim());
          }
        }}
        close={() => setRunDrawerOpen(false)}
      />
    </main>
  );
}
