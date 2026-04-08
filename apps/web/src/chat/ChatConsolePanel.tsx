import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type {
  ChatAttachmentRecord,
  ChatBackgroundTaskRecord,
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatDelegationCatalog,
  MediaDerivedArtifactRecord,
  ChatPinRecord,
  ChatQueuedInputRecord,
  ChatRunStatusRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
} from "../consoleApi";
import { type DetailPanelState, type TranscriptSearchMatch } from "./ChatInspectorColumn";
import { ChatConsoleWorkspaceView } from "./ChatConsoleWorkspaceView";
import {
  inspectCheckpointAction,
  inspectCompactionAction,
  restoreCheckpointAction,
  runCompactionFlowAction,
} from "./chatPhase4Actions";
import {
  inspectBackgroundTaskDetail,
  inspectDerivedArtifactDetail,
  inspectLiveEntryDetail,
  inspectSearchMatchDetail,
  inspectTranscriptRecordDetail,
  runBackgroundTaskLifecycleAction,
  runDerivedArtifactLifecycleAction,
  useChatAttachmentUploadHandler,
} from "./chatInspectorActions";
import {
  CHAT_SLASH_COMMANDS,
  buildContextBudgetSummary,
  buildSessionLineageHint,
  describeBranchState,
  parseSlashCommand,
  parseCompactCommandMode,
  shortId,
  toErrorMessage,
  type ComposerAttachment,
} from "./chatShared";
import {
  abortCurrentRunAction,
  branchCurrentSessionAction,
  createNewSessionAction,
  delegateWorkAction,
  deletePinAction,
  exportTranscriptAction,
  pinTranscriptRecordAction,
  queueFollowUpTextAction,
  resumeSessionAction,
  retryLatestTurnAction,
} from "./chatSessionActions";
import { useContextReferencePreview } from "./useContextReferencePreview";
import { useRecallPreview } from "./useRecallPreview";
import { useChatRunStream } from "./useChatRunStream";
import { useChatSessions } from "./useChatSessions";

interface ChatConsolePanelProps {
  readonly api: ConsoleApiClient;
  readonly revealSensitiveValues: boolean;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly openBrowserSessionWorkbench: (sessionId: string) => void;
}

export function ChatConsolePanel({
  api,
  revealSensitiveValues,
  setError,
  setNotice,
  openBrowserSessionWorkbench,
}: ChatConsolePanelProps) {
  const [searchParams] = useSearchParams();
  const preferredSessionId = searchParams.get("sessionId");
  const preferredRunId = searchParams.get("runId");
  const deepLinkedRunRef = useRef<string | null>(null);
  const sessionSwitchRef = useRef<string>("");
  const transcriptRequestSeqRef = useRef(0);
  const transcriptSearchSeqRef = useRef(0);
  const [runActionBusy, setRunActionBusy] = useState(false);
  const [commandBusy, setCommandBusy] = useState<string | null>(null);
  const [transcriptBusy, setTranscriptBusy] = useState(false);
  const [transcriptRecords, setTranscriptRecords] = useState<ChatTranscriptRecord[]>([]);
  const [sessionAttachments, setSessionAttachments] = useState<ChatAttachmentRecord[]>([]);
  const [sessionDerivedArtifacts, setSessionDerivedArtifacts] = useState<
    MediaDerivedArtifactRecord[]
  >([]);
  const [sessionRuns, setSessionRuns] = useState<ChatRunStatusRecord[]>([]);
  const [sessionPins, setSessionPins] = useState<ChatPinRecord[]>([]);
  const [compactions, setCompactions] = useState<ChatCompactionArtifactRecord[]>([]);
  const [checkpoints, setCheckpoints] = useState<ChatCheckpointRecord[]>([]);
  const [queuedInputs, setQueuedInputs] = useState<ChatQueuedInputRecord[]>([]);
  const [backgroundTasks, setBackgroundTasks] = useState<ChatBackgroundTaskRecord[]>([]);
  const [delegationCatalog, setDelegationCatalog] = useState<ChatDelegationCatalog | null>(null);
  const [transcriptSearchQuery, setTranscriptSearchQuery] = useState("");
  const [transcriptSearchBusy, setTranscriptSearchBusy] = useState(false);
  const [transcriptSearchResults, setTranscriptSearchResults] = useState<TranscriptSearchMatch[]>(
    [],
  );
  const [detailPanel, setDetailPanel] = useState<DetailPanelState | null>(null);
  const [attachments, setAttachments] = useState<ComposerAttachment[]>([]);
  const [attachmentBusy, setAttachmentBusy] = useState(false);
  const [exportBusy, setExportBusy] = useState<"json" | "markdown" | null>(null);
  const [phase4BusyKey, setPhase4BusyKey] = useState<string | null>(null);
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
    runLineage,
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
  const knownRunIds = useMemo(() => {
    const ordered = new Set<string>();
    for (const runId of runIds) {
      ordered.add(runId);
    }
    for (const run of [...sessionRuns].reverse()) {
      ordered.add(run.run_id);
    }
    return Array.from(ordered);
  }, [runIds, sessionRuns]);
  const inspectorVisible = runDrawerOpen || knownRunIds.length > 0;
  const actionableRunId =
    activeRunId ??
    (runDrawerId.trim().length > 0 ? runDrawerId.trim() : null) ??
    knownRunIds[0] ??
    null;
  const toolPayloadCount = useMemo(() => {
    return visibleTranscript.filter((entry) => entry.payload !== undefined).length;
  }, [visibleTranscript]);
  const recentTranscriptRecords = [...transcriptRecords].slice(-8).reverse();
  const deferredComposerText = useDeferredValue(composerText);
  const deferredSearchQuery = useDeferredValue(transcriptSearchQuery);
  const parsedSlashCommand = parseSlashCommand(deferredComposerText);
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
  const attachSelectedFiles = useChatAttachmentUploadHandler({
    api,
    sessionId: sessions.activeSessionId.trim(),
    attachmentInputRef,
    setAttachments,
    setAttachmentBusy,
    setError,
    setNotice,
  });
  const {
    recallPreview,
    recallPreviewBusy,
    recallPreviewStale,
    loadRecallPreview,
    ensureRecallPreviewForCurrentDraft,
    resetRecallPreview,
  } = useRecallPreview({
    api,
    activeSessionId: sessions.activeSessionId,
    composerText,
    selectedChannel: sessions.selectedSession?.channel,
    setError,
  });
  const {
    contextReferencePreview,
    contextReferencePreviewBusy,
    contextReferencePreviewStale,
    loadContextReferencePreview,
    ensureContextReferencePreviewForCurrentDraft,
    resetContextReferencePreview,
    removeContextReference,
  } = useContextReferencePreview({
    api,
    activeSessionId: sessions.activeSessionId,
    composerText,
    setComposerText,
    setError,
  });
  const contextBudget = useMemo(
    () =>
      buildContextBudgetSummary({
        baseline_tokens: Math.max(
          sessions.selectedSession?.total_tokens ?? 0,
          runStatus?.total_tokens ?? 0,
        ),
        draft_text: composerText,
        attachments,
        reference_tokens:
          contextReferencePreview !== null && !contextReferencePreviewStale
            ? contextReferencePreview.total_estimated_tokens
            : 0,
      }),
    [
      attachments,
      composerText,
      contextReferencePreview,
      contextReferencePreviewStale,
      runStatus?.total_tokens,
      sessions.selectedSession?.total_tokens,
    ],
  );

  useEffect(() => {
    void sessions.refreshSessions(true);
    void api
      .getDelegationCatalog()
      .then((response) => {
        setDelegationCatalog(response.catalog);
      })
      .catch((error) => {
        setError(toErrorMessage(error));
      });
    return () => {
      dispose();
    };
  }, [api, dispose, sessions.refreshSessions, setError]);

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
      setSessionAttachments([]);
      setSessionDerivedArtifacts([]);
      setSessionRuns([]);
      setSessionPins([]);
      setCompactions([]);
      setCheckpoints([]);
      setQueuedInputs([]);
      setBackgroundTasks([]);
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
      setSessionAttachments(response.attachments);
      setSessionDerivedArtifacts(response.derived_artifacts);
      setSessionRuns(response.runs);
      setSessionPins(response.pins);
      setCompactions(response.compactions);
      setCheckpoints(response.checkpoints);
      setQueuedInputs(response.queued_inputs);
      setBackgroundTasks(response.background_tasks);
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

  useEffect(() => {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      sessionSwitchRef.current = "";
      setTranscriptRecords([]);
      setSessionAttachments([]);
      setSessionDerivedArtifacts([]);
      setSessionRuns([]);
      setSessionPins([]);
      setCompactions([]);
      setCheckpoints([]);
      setQueuedInputs([]);
      setBackgroundTasks([]);
      setTranscriptSearchResults([]);
      setDetailPanel(null);
      setAttachments([]);
      setPhase4BusyKey(null);
      resetRecallPreview();
      resetContextReferencePreview();
      return;
    }

    if (sessionSwitchRef.current.length > 0 && sessionSwitchRef.current !== sessionId) {
      clearTranscriptState();
      setDetailPanel(null);
      setTranscriptSearchResults([]);
      setAttachments([]);
      setSessionAttachments([]);
      setSessionDerivedArtifacts([]);
      setPhase4BusyKey(null);
      resetRecallPreview();
      resetContextReferencePreview();
    }
    sessionSwitchRef.current = sessionId;
    void refreshSessionTranscript();
  }, [
    clearTranscriptState,
    refreshSessionTranscript,
    resetContextReferencePreview,
    resetRecallPreview,
    sessions.activeSessionId,
  ]);

  async function resetSessionAndTranscript(): Promise<void> {
    const resetApplied = await sessions.resetSession();
    if (!resetApplied) {
      return;
    }
    clearTranscriptState();
    setDetailPanel(null);
    setTranscriptSearchResults([]);
    setAttachments([]);
    setSessionAttachments([]);
    setSessionDerivedArtifacts([]);
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
    setSessionAttachments([]);
    setSessionDerivedArtifacts([]);
    setNotice("Session archived. Local transcript cleared.");
  }

  async function abortCurrentRun(): Promise<void> {
    await abortCurrentRunAction({
      api,
      targetRunId: actionableRunId,
      runDrawerOpen,
      runDrawerId,
      refreshRunDetails,
      refreshSessions: sessions.refreshSessions,
      refreshSessionTranscript,
      setRunActionBusy,
      setError,
      setNotice,
    });
  }

  async function handleComposerSubmit(): Promise<void> {
    const command = parseSlashCommand(composerText);
    if (command !== null) {
      await executeSlashCommand(command);
      return;
    }

    const effectiveContextReferencePreview = await ensureContextReferencePreviewForCurrentDraft();
    if (effectiveContextReferencePreview?.errors.length) {
      setError(effectiveContextReferencePreview.errors[0]?.message ?? "Invalid context reference.");
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
          id: attachment.artifact_id,
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
      case "delegate":
        await delegateWorkAction({
          api,
          sessionId: sessions.activeSessionId.trim(),
          raw: command.args,
          delegationCatalog,
          upsertSession: sessions.upsertSession,
          refreshSessionTranscript,
          appendLocalEntry,
          setComposerText,
          setCommandBusy,
          setError,
          setNotice,
        });
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
        await runCompactionFlow(parseCompactCommandMode(command.args));
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

  async function runCompactionFlow(mode: "preview" | "apply"): Promise<void> {
    const sessionId = sessions.activeSessionId.trim();
    if (sessionId.length === 0) {
      setError("Select a session before compacting.");
      return;
    }

    setCommandBusy(mode === "apply" ? "compact-apply" : "compact-preview");
    setError(null);
    setNotice(null);
    try {
      await runCompactionFlowAction({
        mode,
        api,
        sessionId,
        upsertSession: sessions.upsertSession,
        refreshSessionTranscript,
        setDetailPanel,
        appendLocalEntry,
        setNotice,
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setCommandBusy(null);
    }
  }
  async function createNewSession(requestedLabel?: string): Promise<void> {
    await createNewSessionAction({
      requestedLabel,
      createSessionWithLabel: sessions.createSessionWithLabel,
      clearTranscriptState,
      setDetailPanel,
      setTranscriptSearchResults,
      setAttachments,
      setComposerText,
      setError,
      setNotice,
    });
  }

  async function resumeSession(rawTarget: string): Promise<void> {
    resumeSessionAction({
      rawTarget,
      sortedSessions: sessions.sortedSessions,
      setActiveSessionId: sessions.setActiveSessionId,
      setComposerText,
      setError,
      setNotice,
    });
  }

  async function retryLatestTurn(): Promise<void> {
    await retryLatestTurnAction({
      api,
      sessionId: sessions.activeSessionId.trim(),
      refreshSessions: sessions.refreshSessions,
      refreshSessionTranscript,
      sendMessage,
      appendLocalEntry,
      setCommandBusy,
      setError,
      setNotice,
    });
  }

  async function branchCurrentSession(requestedLabel?: string): Promise<void> {
    await branchCurrentSessionAction({
      api,
      sessionId: sessions.activeSessionId.trim(),
      requestedLabel,
      upsertSession: sessions.upsertSession,
      clearTranscriptState,
      setDetailPanel,
      setAttachments,
      setComposerText,
      refreshSessions: sessions.refreshSessions,
      refreshSessionTranscript,
      setCommandBusy,
      setError,
      setNotice,
    });
  }

  async function queueFollowUpText(text: string): Promise<void> {
    await queueFollowUpTextAction({
      api,
      targetRunId: actionableRunId,
      text,
      sessionId: sessions.activeSessionId,
      appendLocalEntry,
      refreshSessionTranscript,
      setComposerText,
      setCommandBusy,
      setError,
      setNotice,
    });
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
    await exportTranscriptAction({
      api,
      sessionId: sessions.activeSessionId.trim(),
      sessionLabel: sessions.selectedSession?.session_label,
      format,
      setExportBusy,
      setError,
      setNotice,
    });
  }

  async function pinTranscriptRecord(record: ChatTranscriptRecord): Promise<void> {
    await pinTranscriptRecordAction({
      api,
      sessionId: sessions.activeSessionId.trim(),
      record,
      refreshSessionTranscript,
      setCommandBusy,
      setError,
      setNotice,
    });
  }

  async function deletePin(pinId: string): Promise<void> {
    await deletePinAction({
      api,
      sessionId: sessions.activeSessionId.trim(),
      pinId,
      refreshSessionTranscript,
      setCommandBusy,
      setError,
      setNotice,
    });
  }
  async function inspectCompaction(artifactId: string): Promise<void> {
    setPhase4BusyKey(`inspect-compaction:${artifactId}`);
    setError(null);
    try {
      await inspectCompactionAction({
        api,
        artifactId,
        upsertSession: sessions.upsertSession,
        setDetailPanel,
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setPhase4BusyKey(null);
    }
  }

  async function inspectCheckpoint(checkpointId: string): Promise<void> {
    setPhase4BusyKey(`inspect-checkpoint:${checkpointId}`);
    setError(null);
    try {
      await inspectCheckpointAction({
        api,
        checkpointId,
        upsertSession: sessions.upsertSession,
        setDetailPanel,
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setPhase4BusyKey(null);
    }
  }

  async function restoreCheckpoint(checkpointId: string): Promise<void> {
    setPhase4BusyKey(`restore-checkpoint:${checkpointId}`);
    setError(null);
    setNotice(null);
    try {
      await restoreCheckpointAction({
        api,
        checkpointId,
        selectedSession: sessions.selectedSession,
        upsertSession: sessions.upsertSession,
        clearTranscriptState,
        setAttachments,
        refreshSessions: sessions.refreshSessions,
        refreshSessionTranscript,
        setDetailPanel,
        setNotice,
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setPhase4BusyKey(null);
    }
  }

  return (
    <main className="workspace-page chat-workspace">
      <input
        ref={attachmentInputRef}
        hidden
        multiple
        type="file"
        onChange={(event) => {
          attachSelectedFiles(Array.from(event.currentTarget.files ?? []));
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
            attachSelectedFiles(files);
          },
          showSlashPalette,
          parsedSlashCommand,
          slashCommandMatches,
          useSlashCommand: (command) => setComposerText(command.example),
          contextBudget,
          contextReferencePreview,
          contextReferencePreviewBusy,
          contextReferencePreviewStale,
          refreshContextReferencePreview: () => {
            void loadContextReferencePreview(composerText, { reportError: true });
          },
          removeContextReference,
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
          runIds: knownRunIds,
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
          inspectSearchMatch: (match) => {
            inspectSearchMatchDetail({
              match,
              transcriptRecords,
              setDetailPanel,
            });
          },
          exportBusy,
          exportTranscript: (format) => {
            void exportTranscript(format);
          },
          recentTranscriptRecords,
          inspectTranscriptRecord: (record) => {
            inspectTranscriptRecordDetail(record, setDetailPanel);
          },
          pinTranscriptRecord: (record) => {
            void pinTranscriptRecord(record);
          },
          sessionPins,
          deletePin: (pinId) => {
            void deletePin(pinId);
          },
          compactions,
          inspectCompaction: (artifactId) => {
            void inspectCompaction(artifactId);
          },
          checkpoints,
          inspectCheckpoint: (checkpointId) => {
            void inspectCheckpoint(checkpointId);
          },
          restoreCheckpoint: (checkpointId) => {
            void restoreCheckpoint(checkpointId);
          },
          queuedInputs,
          backgroundTasks,
          inspectBackgroundTask: (taskId) => {
            void inspectBackgroundTaskDetail({
              api,
              taskId,
              setDetailPanel,
              setError,
              setPhase4BusyKey,
            });
          },
          runBackgroundTaskAction: (taskId, action) => {
            void runBackgroundTaskLifecycleAction({
              api,
              taskId,
              action,
              refreshSessionTranscript,
              setError,
              setNotice,
              setPhase4BusyKey,
            });
          },
          detailPanel,
          revealSensitiveValues,
          inspectorVisible,
          openRunDetails,
          phase4BusyKey,
          runDrawerId,
          setRunDrawerId,
          runDrawerBusy,
          runStatus,
          runTape,
          runLineage,
          refreshRunDetails,
          closeRunDrawer,
          openBrowserSessionWorkbench,
        }}
        onAbortRun={() => {
          void abortCurrentRun();
        }}
        onOpenRunDetails={() => {
          const targetRunId = activeRunId ?? knownRunIds[0] ?? null;
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
          sessionAttachments,
          sessionDerivedArtifacts,
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
          inspectPayload: (entry) => {
            inspectLiveEntryDetail(entry, setDetailPanel);
          },
          inspectDerivedArtifact: (derivedArtifactId) => {
            inspectDerivedArtifactDetail({
              derivedArtifactId,
              sessionDerivedArtifacts,
              sessionAttachments,
              setDetailPanel,
              setError,
            });
          },
          runDerivedArtifactAction: (derivedArtifactId, action) => {
            void runDerivedArtifactLifecycleAction({
              api,
              derivedArtifactId,
              action,
              refreshSessionTranscript,
              setError,
              setNotice,
              setPhase4BusyKey,
            });
          },
        }}
      />
    </main>
  );
}
