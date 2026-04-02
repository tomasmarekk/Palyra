import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import type {
  ChatPinRecord,
  ChatQueuedInputRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
  JsonValue,
  RecallPreviewEnvelope,
} from "../consoleApi";
import { type DetailPanelState, type TranscriptSearchMatch } from "./ChatInspectorColumn";
import { ChatConsoleWorkspaceView } from "./ChatConsoleWorkspaceView";
import {
  buildDetailFromLiveEntry,
  buildDetailFromSearchMatch,
  buildDetailFromTranscriptRecord,
  downloadTextFile,
  uploadComposerAttachments,
} from "./chatConsoleUtils";
import {
  CHAT_SLASH_COMMANDS,
  buildContextBudgetSummary,
  buildSessionLineageHint,
  describeBranchState,
  emptyToUndefined,
  parseSlashCommand,
  prettifyEventType,
  shortId,
  toErrorMessage,
  type ComposerAttachment,
  type TranscriptEntry,
} from "./chatShared";
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
  const sessionSwitchRef = useRef<string>("");
  const transcriptRequestSeqRef = useRef(0);
  const transcriptSearchSeqRef = useRef(0);
  const recallPreviewRequestSeqRef = useRef(0);

  const [runActionBusy, setRunActionBusy] = useState(false);
  const [commandBusy, setCommandBusy] = useState<string | null>(null);
  const [transcriptBusy, setTranscriptBusy] = useState(false);
  const [transcriptRecords, setTranscriptRecords] = useState<ChatTranscriptRecord[]>([]);
  const [sessionPins, setSessionPins] = useState<ChatPinRecord[]>([]);
  const [queuedInputs, setQueuedInputs] = useState<ChatQueuedInputRecord[]>([]);
  const [transcriptSearchQuery, setTranscriptSearchQuery] = useState("");
  const [transcriptSearchBusy, setTranscriptSearchBusy] = useState(false);
  const [transcriptSearchResults, setTranscriptSearchResults] = useState<TranscriptSearchMatch[]>(
    [],
  );
  const [detailPanel, setDetailPanel] = useState<DetailPanelState | null>(null);
  const [attachments, setAttachments] = useState<ComposerAttachment[]>([]);
  const [attachmentBusy, setAttachmentBusy] = useState(false);
  const [exportBusy, setExportBusy] = useState<"json" | "markdown" | null>(null);
  const [recallPreviewBusy, setRecallPreviewBusy] = useState(false);
  const [recallPreview, setRecallPreview] = useState<RecallPreviewEnvelope | null>(null);
  const [recallPreviewQuery, setRecallPreviewQuery] = useState("");
  const attachmentInputRef = useRef<HTMLInputElement | null>(null);

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
    appendLocalEntry,
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
  const toolPayloadCount = useMemo(
    () => visibleTranscript.filter((entry) => entry.payload !== undefined).length,
    [visibleTranscript],
  );
  const recentTranscriptRecords = useMemo(
    () => [...transcriptRecords].slice(-8).reverse(),
    [transcriptRecords],
  );
  const deferredComposerText = useDeferredValue(composerText);
  const deferredSearchQuery = useDeferredValue(transcriptSearchQuery);
  const deferredRecallQuery = useDeferredValue(composerText);
  const parsedSlashCommand = useMemo(
    () => parseSlashCommand(deferredComposerText),
    [deferredComposerText],
  );
  const showSlashPalette = deferredComposerText.trim().startsWith("/");
  const slashQuery = useMemo(() => {
    if (!showSlashPalette) {
      return "";
    }
    return deferredComposerText.trim().slice(1).trim().split(/\s+/, 1)[0]?.toLowerCase() ?? "";
  }, [deferredComposerText, showSlashPalette]);
  const slashCommandMatches = useMemo(
    () =>
      slashQuery.length === 0 || slashQuery === "help"
        ? CHAT_SLASH_COMMANDS
        : CHAT_SLASH_COMMANDS.filter((command) => command.name.includes(slashQuery)),
    [slashQuery],
  );
  const selectedSessionLineage = useMemo(
    () => buildSessionLineageHint(sessions.selectedSession),
    [sessions.selectedSession],
  );
  const contextBudget = useMemo(
    () =>
      buildContextBudgetSummary({
        baseline_tokens: Math.max(
          sessions.selectedSession?.total_tokens ?? 0,
          runStatus?.total_tokens ?? 0,
        ),
        draft_text: composerText,
        attachments,
      }),
    [attachments, composerText, runStatus?.total_tokens, sessions.selectedSession?.total_tokens],
  );
  const recallPreviewStale = useMemo(() => {
    const trimmed = composerText.trim();
    if (trimmed.length === 0 || trimmed.startsWith("/")) {
      return false;
    }
    return recallPreview !== null && recallPreviewQuery !== trimmed;
  }, [composerText, recallPreview, recallPreviewQuery]);

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

  const refreshSessionTranscript = useCallback(async () => {
    const sessionId = sessions.activeSessionId.trim();
    transcriptRequestSeqRef.current += 1;
    const requestSeq = transcriptRequestSeqRef.current;

    if (sessionId.length === 0) {
      setTranscriptRecords([]);
      setSessionPins([]);
      setQueuedInputs([]);
      return;
    }

    setTranscriptBusy(true);
    try {
      const response = await api.getSessionTranscript(sessionId);
      if (requestSeq !== transcriptRequestSeqRef.current) {
        return;
      }
      sessions.upsertSession(response.session);
      setTranscriptRecords(response.records);
      setSessionPins(response.pins);
      setQueuedInputs(response.queued_inputs);
    } catch (error) {
      if (requestSeq === transcriptRequestSeqRef.current) {
        setError(toErrorMessage(error));
      }
    } finally {
      if (requestSeq === transcriptRequestSeqRef.current) {
        setTranscriptBusy(false);
      }
    }
  }, [api, sessions.activeSessionId, sessions.upsertSession, setError]);

  const resetRecallPreview = useCallback(() => {
    recallPreviewRequestSeqRef.current += 1;
    setRecallPreviewBusy(false);
    setRecallPreview(null);
    setRecallPreviewQuery("");
  }, []);

  const loadRecallPreview = useCallback(
    async (
      query: string,
      options: { reportError?: boolean } = {},
    ): Promise<RecallPreviewEnvelope | null> => {
      const trimmed = query.trim();
      const sessionId = sessions.activeSessionId.trim();
      if (trimmed.length === 0 || trimmed.startsWith("/") || sessionId.length === 0) {
        resetRecallPreview();
        return null;
      }

      recallPreviewRequestSeqRef.current += 1;
      const requestSeq = recallPreviewRequestSeqRef.current;
      setRecallPreviewBusy(true);
      try {
        const response = await api.previewRecall({
          query: trimmed,
          channel: emptyToUndefined(sessions.selectedSession?.channel ?? ""),
          session_id: sessionId,
          memory_top_k: 4,
          workspace_top_k: 4,
        });
        if (requestSeq !== recallPreviewRequestSeqRef.current) {
          return null;
        }
        setRecallPreview(response);
        setRecallPreviewQuery(trimmed);
        return response;
      } catch (error) {
        if (requestSeq === recallPreviewRequestSeqRef.current && options.reportError !== false) {
          setError(toErrorMessage(error));
        }
        return null;
      } finally {
        if (requestSeq === recallPreviewRequestSeqRef.current) {
          setRecallPreviewBusy(false);
        }
      }
    },
    [api, resetRecallPreview, sessions.activeSessionId, sessions.selectedSession?.channel, setError],
  );

  const ensureRecallPreviewForCurrentDraft = useCallback(async (): Promise<RecallPreviewEnvelope | null> => {
    const trimmed = composerText.trim();
    if (trimmed.length === 0 || trimmed.startsWith("/")) {
      return null;
    }
    if (recallPreview !== null && recallPreviewQuery === trimmed) {
      return recallPreview;
    }
    return loadRecallPreview(trimmed, { reportError: true });
  }, [composerText, loadRecallPreview, recallPreview, recallPreviewQuery]);

  useEffect(() => {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      sessionSwitchRef.current = "";
      setTranscriptRecords([]);
      setSessionPins([]);
      setQueuedInputs([]);
      setTranscriptSearchResults([]);
      setDetailPanel(null);
      setAttachments([]);
      resetRecallPreview();
      return;
    }

    if (sessionSwitchRef.current.length > 0 && sessionSwitchRef.current !== sessionId) {
      clearTranscriptState();
      setDetailPanel(null);
      setTranscriptSearchResults([]);
      setAttachments([]);
      resetRecallPreview();
    }
    sessionSwitchRef.current = sessionId;
    void refreshSessionTranscript();
  }, [clearTranscriptState, refreshSessionTranscript, resetRecallPreview, sessions.activeSessionId]);

  useEffect(() => {
    const sessionId = sessions.activeSessionId.trim();
    const trimmed = deferredRecallQuery.trim();
    if (sessionId.length === 0 || trimmed.length === 0 || trimmed.startsWith("/")) {
      resetRecallPreview();
      return;
    }

    const timeoutHandle = window.setTimeout(() => {
      void loadRecallPreview(trimmed, { reportError: false });
    }, 350);

    return () => {
      window.clearTimeout(timeoutHandle);
    };
  }, [deferredRecallQuery, loadRecallPreview, resetRecallPreview, sessions.activeSessionId]);

  async function resetSessionAndTranscript(): Promise<void> {
    const resetApplied = await sessions.resetSession();
    if (!resetApplied) {
      return;
    }
    clearTranscriptState();
    setDetailPanel(null);
    setTranscriptSearchResults([]);
    setAttachments([]);
    void refreshSessionTranscript();
    setNotice("Session reset applied. Local transcript cleared.");
  }

  async function archiveSessionAndTranscript(): Promise<void> {
    const archived = await sessions.archiveSession();
    if (!archived) {
      return;
    }
    clearTranscriptState();
    setDetailPanel(null);
    setTranscriptSearchResults([]);
    setAttachments([]);
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
      void refreshSessionTranscript();
    } catch (error) {
      setError(error instanceof Error ? error.message : "Unexpected failure.");
    } finally {
      setRunActionBusy(false);
    }
  }

  async function handleComposerSubmit(): Promise<void> {
    const command = parseSlashCommand(composerText);
    if (command !== null) {
      await executeSlashCommand(command);
      return;
    }

    const effectiveRecallPreview = await ensureRecallPreviewForCurrentDraft();

    const didSend = await sendMessage(
      async () => {
        await Promise.all([sessions.refreshSessions(false), refreshSessionTranscript()]);
      },
      {
        attachments: attachments.map((attachment) => ({ artifact_id: attachment.artifact_id })),
        attachment_summaries: attachments.map((attachment) => ({
          id: attachment.local_id,
          filename: attachment.filename,
          kind: attachment.kind,
          size_bytes: attachment.size_bytes,
          budget_tokens: attachment.budget_tokens,
          preview_url: attachment.preview_url,
        })),
        parameter_delta: effectiveRecallPreview?.parameter_delta,
      },
    );

    if (didSend) {
      setAttachments([]);
    }
  }

  async function executeSlashCommand(command: NonNullable<ReturnType<typeof parseSlashCommand>>) {
    if (commandBusy !== null) {
      setError("Another chat command is already running.");
      return;
    }

    switch (command.name) {
      case "help":
        setComposerText("/");
        setNotice("Slash command help is open in the composer.");
        return;
      case "new":
        await createNewSession(command.args);
        return;
      case "reset":
        await resetSessionAndTranscript();
        return;
      case "retry":
        await retryLatestTurn();
        return;
      case "branch":
        await branchCurrentSession(command.args);
        return;
      case "queue":
        if (command.args.length === 0) {
          setError("Provide queued text after /queue.");
          return;
        }
        await queueFollowUpText(command.args);
        return;
      case "history":
        sessions.setSearchQuery(command.args);
        setNotice(
          command.args.trim().length > 0
            ? `Session history filtered by "${command.args.trim()}".`
            : "Session history filter cleared.",
        );
        return;
      case "resume":
        await resumeSession(command.args);
        return;
      case "attach":
        if (sessions.activeSessionId.trim().length === 0) {
          setError("Select or create a session before attaching files.");
          return;
        }
        attachmentInputRef.current?.click();
        setNotice("Attachment picker opened for the active session.");
        return;
      case "usage":
        setNotice(
          `Estimated context ${contextBudget.label}; branch ${describeBranchState(sessions.selectedSession?.branch_state ?? "missing")}; ${transcriptRecords.length} persisted transcript record${transcriptRecords.length === 1 ? "" : "s"}.`,
        );
        return;
      case "compact":
        setNotice(
          "Compaction lands in a later phase. In Phase 2, use retry, branch, search, or export to keep long sessions manageable.",
        );
        return;
      case "search":
        if (command.args.length === 0) {
          setError("Provide a search term after /search.");
          return;
        }
        setTranscriptSearchQuery(command.args);
        await searchTranscript(command.args);
        return;
      case "export":
        await exportTranscript(
          command.args.trim().toLowerCase() === "markdown" ||
            command.args.trim().toLowerCase() === "md"
            ? "markdown"
            : "json",
        );
        return;
      default:
        setError("Unsupported slash command.");
    }
  }

  async function createNewSession(requestedLabel?: string): Promise<void> {
    setError(null);
    setNotice(null);
    const createdSessionId = await sessions.createSessionWithLabel(requestedLabel);
    if (createdSessionId === null) {
      return;
    }
    clearTranscriptState();
    setDetailPanel(null);
    setTranscriptSearchResults([]);
    setAttachments([]);
    setComposerText("");
    setNotice(
      requestedLabel !== undefined && requestedLabel.trim().length > 0
        ? `Created a fresh session: ${requestedLabel.trim()}.`
        : "Created a fresh session.",
    );
  }

  async function resumeSession(rawTarget: string): Promise<void> {
    const target = rawTarget.trim();
    if (target.length === 0) {
      setError("Usage: /resume <session-id-or-key>");
      return;
    }
    const matchedSession =
      sessions.sortedSessions.find((session) => session.session_id === target) ??
      sessions.sortedSessions.find((session) => session.session_key === target) ??
      sessions.sortedSessions.find(
        (session) => session.title.toLowerCase() === target.toLowerCase(),
      ) ??
      null;
    if (matchedSession === null) {
      setError(`No loaded session matches "${target}". Use /history first if needed.`);
      return;
    }
    sessions.setActiveSessionId(matchedSession.session_id);
    setComposerText("");
    setNotice(`Resumed session ${matchedSession.title}.`);
  }

  async function retryLatestTurn(): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before retrying.");
      return;
    }

    setCommandBusy("retry");
    setError(null);
    setNotice(null);
    try {
      const response = await api.prepareRetry(sessionId);
      const didSend = await sendMessage(
        async () => {
          await Promise.all([sessions.refreshSessions(false), refreshSessionTranscript()]);
        },
        {
          text: response.text,
          origin_kind: response.origin_kind,
          origin_run_id: response.origin_run_id,
          parameter_delta: response.parameter_delta,
        },
      );
      if (didSend) {
        appendLocalEntry({
          kind: "status",
          session_id: sessionId,
          run_id: response.origin_run_id,
          title: "Retry requested",
          text: `Replayed the latest user turn from ${shortId(response.origin_run_id)}.`,
          payload: response.parameter_delta,
        });
      }
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }

  async function branchCurrentSession(requestedLabel?: string): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before branching.");
      return;
    }

    setCommandBusy("branch");
    setError(null);
    setNotice(null);
    try {
      const response = await api.branchSession(sessionId, {
        session_label: emptyToUndefined(requestedLabel ?? ""),
      });
      sessions.upsertSession(response.session, { select: true });
      clearTranscriptState();
      setDetailPanel(null);
      setAttachments([]);
      setComposerText("");
      await Promise.all([sessions.refreshSessions(false), refreshSessionTranscript()]);
      setNotice(
        `Branch ready: ${response.session.title} from run ${shortId(response.source_run_id)}.`,
      );
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }

  async function queueFollowUpText(text: string): Promise<void> {
    const targetRunId = actionableRunId;
    if (targetRunId === null || targetRunId.trim().length === 0) {
      setError("No active run is available for queued follow-up.");
      return;
    }

    const trimmed = text.trim();
    if (trimmed.length === 0) {
      setError("Queued follow-up cannot be empty.");
      return;
    }

    setCommandBusy("queue");
    setError(null);
    setNotice(null);
    try {
      const response = await api.queueFollowUp(targetRunId, { text: trimmed });
      appendLocalEntry({
        kind: "status",
        run_id: targetRunId,
        session_id: sessions.activeSessionId,
        title: "Queued follow-up",
        text: `Queued input ${shortId(response.queued_input.queued_input_id)} for ${shortId(targetRunId)}.`,
        payload: response.queued_input as unknown as JsonValue,
        status: "queued",
      });
      setComposerText("");
      await refreshSessionTranscript();
      setNotice("Queued follow-up stored.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }

  async function searchTranscript(query = transcriptSearchQuery): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    const trimmed = query.trim();
    if (sessionId.length === 0) {
      setError("Select a session before searching the transcript.");
      return;
    }
    if (trimmed.length === 0) {
      setTranscriptSearchResults([]);
      return;
    }

    transcriptSearchSeqRef.current += 1;
    const requestSeq = transcriptSearchSeqRef.current;
    setTranscriptSearchBusy(true);
    setError(null);
    try {
      const response = await api.searchSessionTranscript(sessionId, trimmed);
      if (requestSeq !== transcriptSearchSeqRef.current) {
        return;
      }
      sessions.upsertSession(response.session);
      setTranscriptSearchResults(response.matches);
    } catch (error) {
      if (requestSeq === transcriptSearchSeqRef.current) {
        setError(toErrorMessage(error));
      }
    } finally {
      if (requestSeq === transcriptSearchSeqRef.current) {
        setTranscriptSearchBusy(false);
      }
    }
  }

  async function exportTranscript(format: "json" | "markdown"): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before exporting.");
      return;
    }

    setExportBusy(format);
    setError(null);
    try {
      const response = await api.exportSessionTranscript(sessionId, format);
      const extension = format === "json" ? "json" : "md";
      const mimeType = format === "json" ? "application/json" : "text/markdown";
      const content =
        typeof response.content === "string"
          ? response.content
          : JSON.stringify(response.content, null, 2);
      downloadTextFile(
        `chat-${sessions.selectedSession?.session_label ?? shortId(sessionId)}.${extension}`,
        content,
        mimeType,
      );
      setNotice(`Transcript export ready: ${format}.`);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setExportBusy(null);
    }
  }

  async function pinTranscriptRecord(record: ChatTranscriptRecord): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before pinning transcript events.");
      return;
    }

    setCommandBusy("pin");
    try {
      await api.createSessionPin(sessionId, {
        run_id: record.run_id,
        tape_seq: record.seq,
        title: `${prettifyEventType(record.event_type)} #${record.seq}`,
        note: `Pinned from ${record.origin_kind} at ${new Date(record.created_at_unix_ms).toLocaleString()}.`,
      });
      await refreshSessionTranscript();
      setNotice(`Pinned event #${record.seq}.`);
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }

  async function deletePin(pinId: string): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before deleting pins.");
      return;
    }

    setCommandBusy("delete-pin");
    try {
      await api.deleteSessionPin(sessionId, pinId);
      await refreshSessionTranscript();
      setNotice("Pin deleted.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }

  async function handleAttachmentFiles(files: readonly File[]): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before uploading attachments.");
      return;
    }
    if (files.length === 0) {
      return;
    }

    setAttachmentBusy(true);
    setError(null);
    setNotice(null);
    try {
      const nextAttachments = await uploadComposerAttachments(api, sessionId, files);
      setAttachments((previous) => [...previous, ...nextAttachments]);
      setNotice(
        `${nextAttachments.length} attachment${nextAttachments.length === 1 ? "" : "s"} ready for the next message.`,
      );
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setAttachmentBusy(false);
      if (attachmentInputRef.current !== null) {
        attachmentInputRef.current.value = "";
      }
    }
  }

  function inspectLiveEntry(entry: TranscriptEntry): void {
    setDetailPanel(buildDetailFromLiveEntry(entry));
  }
  function inspectTranscriptRecord(record: ChatTranscriptRecord): void {
    setDetailPanel(buildDetailFromTranscriptRecord(record));
  }
  function inspectSearchMatch(match: TranscriptSearchMatch): void {
    const matchingRecord = transcriptRecords.find(
      (record) => record.run_id === match.run_id && record.seq === match.seq,
    );
    if (matchingRecord !== undefined) {
      inspectTranscriptRecord(matchingRecord);
      return;
    }
    setDetailPanel(buildDetailFromSearchMatch(match));
  }

  return (
    <main className="workspace-page chat-workspace">
      <input
        ref={attachmentInputRef}
        hidden
        multiple
        type="file"
        onChange={(event) => {
          void handleAttachmentFiles(Array.from(event.currentTarget.files ?? []));
        }}
      />
      <ChatConsoleWorkspaceView
        allowSensitiveTools={allowSensitiveTools}
        canAbortRun={actionableRunId !== null}
        canInspectRun={(activeRunId ?? runIds[0] ?? null) !== null}
        composerProps={{
          composerText,
          setComposerText,
          streaming,
          activeSessionId: sessions.activeSessionId,
          attachments,
          attachmentBusy,
          canQueueFollowUp: actionableRunId !== null,
          submitMessage: () => {
            void handleComposerSubmit();
          },
          retryLast: () => {
            void retryLatestTurn();
          },
          branchSession: () => {
            void branchCurrentSession();
          },
          queueFollowUp: () => {
            void queueFollowUpText(composerText);
          },
          cancelStreaming,
          clearTranscript: () => {
            clearTranscriptState();
            setDetailPanel(null);
            setAttachments([]);
            setNotice("Local transcript cleared.");
          },
          openAttachmentPicker: () => attachmentInputRef.current?.click(),
          removeAttachment: (localId) => {
            setAttachments((previous) =>
              previous.filter((attachment) => attachment.local_id !== localId),
            );
          },
          attachFiles: (files) => {
            void handleAttachmentFiles(files);
          },
          showSlashPalette,
          parsedSlashCommand,
          slashCommandMatches,
          useSlashCommand: (command) => setComposerText(command.example),
          contextBudget,
          recallPreview,
          recallPreviewBusy,
          recallPreviewStale,
          refreshRecallPreview: () => {
            void loadRecallPreview(composerText, { reportError: true });
          },
        }}
        contextBudget={contextBudget}
        inspectorProps={{
          pendingApprovalCount,
          a2uiSurfaces,
          runIds,
          selectedSession: sessions.selectedSession,
          selectedSessionLineage,
          contextBudgetEstimatedTokens: contextBudget.estimated_total_tokens,
          transcriptBusy,
          transcriptSearchQuery,
          setTranscriptSearchQuery,
          transcriptSearchBusy,
          canSearchTranscript: deferredSearchQuery.trim().length > 0,
          pinnedRecordKeys: new Set(sessionPins.map((pin) => `${pin.run_id}:${pin.tape_seq}`)),
          searchResults: transcriptSearchResults,
          searchTranscript: () => {
            void searchTranscript();
          },
          inspectSearchMatch,
          exportBusy,
          exportTranscript: (format) => {
            void exportTranscript(format);
          },
          recentTranscriptRecords,
          inspectTranscriptRecord,
          pinTranscriptRecord: (record) => {
            void pinTranscriptRecord(record);
          },
          sessionPins,
          deletePin: (pinId) => {
            void deletePin(pinId);
          },
          queuedInputs,
          detailPanel,
          revealSensitiveValues,
          inspectorVisible,
          runDrawerId,
          setRunDrawerId,
          runDrawerBusy,
          runStatus,
          runTape,
          refreshRunDetails,
          closeRunDrawer,
        }}
        onAbortRun={() => {
          void abortCurrentRun();
        }}
        onOpenRunDetails={() => {
          const targetRunId = activeRunId ?? runIds[0] ?? null;
          if (targetRunId === null) {
            setError("No run is available for inspection.");
            return;
          }
          openRunDetails(targetRunId);
        }}
        onRefresh={() => {
          void Promise.all([sessions.refreshSessions(false), refreshSessionTranscript()]);
        }}
        onSetAllowSensitiveTools={setAllowSensitiveTools}
        pendingApprovalCount={pendingApprovalCount}
        runActionBusy={runActionBusy}
        selectedSessionBranchState={describeBranchState(
          sessions.selectedSession?.branch_state ?? "missing",
        )}
        selectedSessionLineage={selectedSessionLineage}
        selectedSessionTitle={
          sessions.selectedSession?.title ??
          (sessions.selectedSession
            ? shortId(sessions.selectedSession.session_id)
            : "Operator workspace")
        }
        sessionsBusy={sessions.sessionsBusy}
        sessionsSidebarProps={{
          sessionsBusy: sessions.sessionsBusy,
          newSessionLabel: sessions.newSessionLabel,
          setNewSessionLabel: sessions.setNewSessionLabel,
          searchQuery: sessions.searchQuery,
          setSearchQuery: sessions.setSearchQuery,
          includeArchived: sessions.includeArchived,
          setIncludeArchived: sessions.setIncludeArchived,
          sessionLabelDraft: sessions.sessionLabelDraft,
          setSessionLabelDraft: sessions.setSessionLabelDraft,
          selectedSession: sessions.selectedSession,
          sortedSessions: sessions.sortedSessions,
          activeSessionId: sessions.activeSessionId,
          setActiveSessionId: sessions.setActiveSessionId,
          createSession: () => {
            void sessions.createSession();
          },
          renameSession: () => {
            void sessions.renameSession();
          },
          resetSession: () => {
            void resetSessionAndTranscript();
          },
          archiveSession: () => {
            void archiveSessionAndTranscript();
          },
        }}
        streaming={streaming}
        toolPayloadCount={toolPayloadCount}
        transcriptBusy={transcriptBusy}
        transcriptProps={{
          visibleTranscript,
          hiddenTranscriptItems,
          transcriptBoxRef,
          approvalDrafts,
          a2uiDocuments,
          selectedDetailId: detailPanel?.id ?? null,
          updateApprovalDraft: updateApprovalDraftValue,
          decideInlineApproval: (approvalId, approved) => {
            void decideInlineApproval(approvalId, approved);
          },
          openRunDetails,
          inspectPayload: inspectLiveEntry,
        }}
      />
    </main>
  );
}
