
import { useEffect, useMemo, useRef, useState } from "react";

import {
  A2uiRenderer,
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

const MAX_TRANSCRIPT_RETENTION = 800;
const MAX_RENDERED_TRANSCRIPT = 120;
const DEFAULT_APPROVAL_SCOPE = "once" as const;
const DEFAULT_APPROVAL_TTL_MS = "300000";
const SENSITIVE_KEY_PATTERN =
  /(secret|token|password|cookie|authorization|credential|api[-_]?key|private[-_]?key|vault[-_]?ref)/i;
const SENSITIVE_VALUE_PATTERN =
  /^(Bearer\s+|sk-[a-z0-9]|ghp_[A-Za-z0-9]|xox[baprs]-|AIza[0-9A-Za-z\-_]{20,})/i;

type ApprovalScope = "once" | "session" | "timeboxed";
type TranscriptEntryKind =
  | "meta"
  | "user"
  | "assistant"
  | "status"
  | "tool"
  | "approval_request"
  | "approval_response"
  | "a2ui"
  | "canvas"
  | "journal"
  | "error"
  | "complete"
  | "event";

interface TranscriptEntry {
  readonly id: string;
  readonly kind: TranscriptEntryKind;
  readonly created_at_unix_ms: number;
  readonly run_id?: string;
  readonly session_id?: string;
  readonly title: string;
  readonly text?: string;
  readonly payload?: JsonValue;
  readonly approval_id?: string;
  readonly proposal_id?: string;
  readonly tool_name?: string;
  readonly surface?: string;
  readonly canvas_url?: string;
  readonly status?: string;
  readonly is_final?: boolean;
}

interface ApprovalDraft {
  readonly scope: ApprovalScope;
  readonly reason: string;
  readonly ttl_ms: string;
  readonly busy: boolean;
}

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
    assistantEntryByRunRef.current.clear();
    canvasEntrySetRef.current.clear();
    setTranscript([]);
    setActiveRunId(null);
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
    setRunDrawerBusy(true);
    try {
      const params = new URLSearchParams();
      params.set("limit", "256");
      const [statusResponse, eventsResponse] = await Promise.all([
        api.chatRunStatus(runId),
        api.chatRunEvents(runId, params)
      ]);
      setRunStatus(statusResponse.run);
      setRunTape(eventsResponse.tape);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setRunDrawerBusy(false);
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

          {hiddenTranscriptItems > 0 && (
            <p className="chat-muted">
              Showing latest {MAX_RENDERED_TRANSCRIPT} items. {hiddenTranscriptItems} older items are retained but not rendered.
            </p>
          )}

          <div className="chat-transcript" ref={transcriptBoxRef} role="log" aria-live="polite">
            {visibleTranscript.length === 0 ? (
              <p className="chat-muted">Send a message to start streaming output.</p>
            ) : (
              visibleTranscript.map((entry) => (
                <article key={entry.id} className={`chat-entry chat-entry--${entry.kind}`}>
                  <header className="chat-entry-header">
                    <strong>{entry.title}</strong>
                    <span>{new Date(entry.created_at_unix_ms).toLocaleTimeString()}</span>
                  </header>
                  {entry.text !== undefined && <p className="chat-entry-text">{entry.text}</p>}

                  {entry.kind === "approval_request" && entry.approval_id !== undefined && (
                    <ApprovalRequestControls
                      approvalId={entry.approval_id}
                      draft={approvalDrafts[entry.approval_id]}
                      onDraftChange={(next) => {
                        updateApprovalDraft(entry.approval_id as string, () => next);
                      }}
                      onDecision={(approved) => {
                        void decideInlineApproval(entry.approval_id as string, approved);
                      }}
                    />
                  )}

                  {entry.kind === "a2ui" && entry.surface !== undefined && a2uiDocuments[entry.surface] !== undefined && (
                    <div className="chat-a2ui-shell">
                      <A2uiRenderer document={a2uiDocuments[entry.surface]} />
                    </div>
                  )}

                  {entry.kind === "canvas" && entry.canvas_url !== undefined && (
                    <iframe
                      className="chat-canvas-frame"
                      title={`Canvas ${entry.run_id ?? ""}`}
                      src={entry.canvas_url}
                      sandbox="allow-scripts allow-same-origin"
                      loading="lazy"
                      referrerPolicy="no-referrer"
                    />
                  )}

                  {entry.payload !== undefined && entry.kind !== "assistant" && entry.kind !== "user" && (
                    <pre>{toPrettyJson(entry.payload, revealSensitiveValues)}</pre>
                  )}

                  {entry.run_id !== undefined && (
                    <div className="chat-entry-actions">
                      <button
                        type="button"
                        onClick={() => {
                          setRunDrawerId(entry.run_id as string);
                          setRunDrawerOpen(true);
                        }}
                      >
                        Open run details
                      </button>
                    </div>
                  )}
                </article>
              ))
            )}
          </div>

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

      {runDrawerOpen && (
        <aside className="chat-run-drawer" aria-label="Run details drawer">
          <header className="chat-run-drawer__header">
            <h3>Run details</h3>
            <div className="console-inline-actions">
              <select
                value={runDrawerId}
                onChange={(event) => setRunDrawerId(event.target.value)}
              >
                <option value="">Select run</option>
                {runIds.map((runId) => (
                  <option key={runId} value={runId}>{runId}</option>
                ))}
              </select>
              <button
                type="button"
                onClick={() => {
                  if (runDrawerId.trim().length > 0) {
                    void loadRunDetails(runDrawerId.trim());
                  }
                }}
                disabled={runDrawerId.trim().length === 0 || runDrawerBusy}
              >
                {runDrawerBusy ? "Loading..." : "Refresh run"}
              </button>
              <button type="button" onClick={() => setRunDrawerOpen(false)}>Close</button>
            </div>
          </header>

          {runDrawerId.trim().length === 0 ? (
            <p className="chat-muted">Select a run to inspect status and tape events.</p>
          ) : (
            <>
              {runStatus === null ? (
                <p className="chat-muted">No run status loaded yet.</p>
              ) : (
                <section className="console-subpanel">
                  <h4>Status</h4>
                  <div className="console-grid-3">
                    <p><strong>State:</strong> {runStatus.state}</p>
                    <p><strong>Prompt tokens:</strong> {runStatus.prompt_tokens}</p>
                    <p><strong>Completion tokens:</strong> {runStatus.completion_tokens}</p>
                    <p><strong>Total tokens:</strong> {runStatus.total_tokens}</p>
                    <p><strong>Tape events:</strong> {runStatus.tape_events}</p>
                    <p><strong>Updated:</strong> {new Date(runStatus.updated_at_unix_ms).toLocaleString()}</p>
                  </div>
                  {runStatus.last_error !== undefined && runStatus.last_error.length > 0 && (
                    <p className="console-banner console-banner--error">{runStatus.last_error}</p>
                  )}
                </section>
              )}

              {runTape === null ? (
                <p className="chat-muted">No tape snapshot loaded.</p>
              ) : (
                <section className="console-subpanel">
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
      )}
    </main>
  );
}

interface ApprovalRequestControlsProps {
  readonly approvalId: string;
  readonly draft: ApprovalDraft | undefined;
  readonly onDraftChange: (next: ApprovalDraft) => void;
  readonly onDecision: (approved: boolean) => void;
}

function ApprovalRequestControls({
  approvalId,
  draft,
  onDraftChange,
  onDecision
}: ApprovalRequestControlsProps) {
  const effectiveDraft =
    draft ??
    ({
      scope: DEFAULT_APPROVAL_SCOPE,
      reason: "",
      ttl_ms: DEFAULT_APPROVAL_TTL_MS,
      busy: false
    } satisfies ApprovalDraft);

  return (
    <section className="chat-approval-controls" aria-label={`Approval controls ${approvalId}`}>
      <div className="console-grid-4">
        <label>
          Scope
          <select
            value={effectiveDraft.scope}
            onChange={(event) => {
              const scope = normalizeScope(event.target.value);
              onDraftChange({
                ...effectiveDraft,
                scope
              });
            }}
            disabled={effectiveDraft.busy}
          >
            <option value="once">once</option>
            <option value="session">session</option>
            <option value="timeboxed">timeboxed</option>
          </select>
        </label>
        <label>
          TTL ms
          <input
            value={effectiveDraft.ttl_ms}
            disabled={effectiveDraft.scope !== "timeboxed" || effectiveDraft.busy}
            onChange={(event) => {
              onDraftChange({
                ...effectiveDraft,
                ttl_ms: event.target.value
              });
            }}
          />
        </label>
        <label>
          Reason
          <input
            value={effectiveDraft.reason}
            onChange={(event) => {
              onDraftChange({
                ...effectiveDraft,
                reason: event.target.value
              });
            }}
            disabled={effectiveDraft.busy}
          />
        </label>
        <div className="console-inline-actions">
          <button type="button" onClick={() => onDecision(true)} disabled={effectiveDraft.busy}>
            Approve
          </button>
          <button type="button" className="button--warn" onClick={() => onDecision(false)} disabled={effectiveDraft.busy}>
            Deny
          </button>
        </div>
      </div>
    </section>
  );
}

function retainTranscriptWindow(values: TranscriptEntry[]): TranscriptEntry[] {
  if (values.length <= MAX_TRANSCRIPT_RETENTION) {
    return values;
  }
  return values.slice(values.length - MAX_TRANSCRIPT_RETENTION);
}

function collectCanvasFrameUrls(value: JsonValue): string[] {
  const pending: JsonValue[] = [value];
  const urls = new Set<string>();
  let inspected = 0;
  while (pending.length > 0 && inspected < 256) {
    const current = pending.pop() as JsonValue;
    inspected += 1;
    if (typeof current === "string") {
      const candidate = normalizeCanvasFrameUrl(current);
      if (candidate !== null) {
        urls.add(candidate);
      }
      continue;
    }
    if (Array.isArray(current)) {
      for (const item of current) {
        pending.push(item);
      }
      continue;
    }
    if (isJsonObject(current)) {
      for (const valueItem of Object.values(current)) {
        pending.push(valueItem);
      }
    }
  }
  return Array.from(urls);
}

function normalizeCanvasFrameUrl(raw: string): string | null {
  if (!raw.includes("/canvas/v1/frame/")) {
    return null;
  }
  let parsed: URL;
  try {
    parsed = new URL(raw, window.location.origin);
  } catch {
    return null;
  }
  if (parsed.origin !== window.location.origin) {
    return null;
  }
  if (!parsed.pathname.startsWith("/canvas/v1/frame/")) {
    return null;
  }
  if (!parsed.searchParams.has("token")) {
    return null;
  }
  return parsed.toString();
}

function parseTapePayload(payload: string): JsonValue {
  try {
    return JSON.parse(payload) as JsonValue;
  } catch {
    return payload;
  }
}

function normalizePatchValue(value: unknown): JsonValue | null {
  if (isJsonValue(value)) {
    return value;
  }
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value) as unknown;
      return isJsonValue(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }
  return null;
}

function shortId(value: string): string {
  if (value.length <= 16) {
    return value;
  }
  return `${value.slice(0, 8)}…${value.slice(-6)}`;
}

function prettifyEventType(value: string): string {
  const normalized = value.replace(/_/g, " ").trim();
  if (normalized.length === 0) {
    return "Event";
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function normalizeScope(value: string): ApprovalScope {
  if (value === "session") {
    return "session";
  }
  if (value === "timeboxed") {
    return "timeboxed";
  }
  return "once";
}

function parseInteger(raw: string): number | null {
  const trimmed = raw.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function emptyToUndefined(raw: string): string | undefined {
  const trimmed = raw.trim();
  return trimmed.length === 0 ? undefined : trimmed;
}

function isAbortError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "AbortError";
  }
  if (error instanceof Error) {
    return error.name === "AbortError";
  }
  return false;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected failure.";
}

function asObject(value: unknown): Record<string, JsonValue> | null {
  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, JsonValue>;
  }
  return null;
}

function asString(value: unknown): string | null {
  if (typeof value === "string") {
    return value;
  }
  return null;
}

function asBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") {
    return value;
  }
  return null;
}

function isJsonObject(value: JsonValue): value is { [key: string]: JsonValue } {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isJsonValue(value: unknown): value is JsonValue {
  if (value === null) {
    return true;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return true;
  }
  if (Array.isArray(value)) {
    return value.every((entry) => isJsonValue(entry));
  }
  if (typeof value === "object") {
    return Object.values(value as Record<string, unknown>).every((entry) => isJsonValue(entry));
  }
  return false;
}

function redactValue(value: JsonValue, revealSensitive: boolean): JsonValue {
  if (revealSensitive) {
    return value;
  }
  if (typeof value === "string") {
    return SENSITIVE_VALUE_PATTERN.test(value) ? "[redacted]" : value;
  }
  if (Array.isArray(value)) {
    return value.map((entry) => redactValue(entry, false));
  }
  if (isJsonObject(value)) {
    const sanitized: { [key: string]: JsonValue } = {};
    for (const [key, item] of Object.entries(value)) {
      sanitized[key] = SENSITIVE_KEY_PATTERN.test(key) ? "[redacted]" : redactValue(item, false);
    }
    return sanitized;
  }
  return value;
}

function toPrettyJson(value: JsonValue, revealSensitive: boolean): string {
  return JSON.stringify(redactValue(value, revealSensitive), null, 2);
}
