import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import type {
  ChatAttachmentRecord,
  ChatBackgroundTaskRecord,
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
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
  deleteChatPin,
  exportChatTranscript,
  inspectCheckpointDetails,
  inspectCompactionDetails,
  pinChatTranscriptRecord,
  restoreChatCheckpoint,
  runChatCompactionFlow,
  searchChatTranscript,
} from "./chatConsoleOperations";
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
  buildSessionLineageHint,
  describeBranchState,
  toErrorMessage,
  type ComposerAttachment,
} from "./chatShared";
import {
  createUndoCheckpoint,
  executeChatSlashCommand,
  interruptAndMaybeRedirect,
} from "./chatSlashActions";
import {
  branchCurrentSessionAction,
  createNewSessionAction,
  delegateWorkAction,
  queueFollowUpTextAction,
  resumeSessionAction,
  retryLatestTurnAction,
} from "./chatSessionActions";
import { useContextReferencePreview } from "./useContextReferencePreview";
import { useRecallPreview } from "./useRecallPreview";
import { useChatContextBudget } from "./useChatContextBudget";
import { useChatRunStream } from "./useChatRunStream";
import { useChatSessions } from "./useChatSessions";
import { useChatSlashPalette } from "./useChatSlashPalette";
import { usePhase4DeepLinks } from "./usePhase4DeepLinks";
import { useChatObjectives } from "./useChatObjectives";
import { useChatPanelBootstrap } from "./useChatPanelBootstrap";
import {
  buildSessionsSidebarProps,
  describeSelectedSessionTitle,
} from "./chatWorkspaceSessionBindings";
import type { UxTelemetryEvent } from "../console/contracts";
import { parseConsoleHandoff } from "../console/contracts";
import type { Section } from "../console/sectionMetadata";
import { buildObjectiveOverviewHref } from "../console/objectiveLinks";
import { readString } from "../console/shared";
interface ChatConsolePanelProps {
  readonly api: ConsoleApiClient;
  readonly emitUxEvent: (
    event: Omit<UxTelemetryEvent, "surface" | "locale" | "mode">,
  ) => Promise<void>;
  readonly revealSensitiveValues: boolean;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly setConsoleSection: (section: Section) => void;
  readonly openBrowserSessionWorkbench: (sessionId: string) => void;
}

export function ChatConsolePanel({
  api,
  emitUxEvent,
  revealSensitiveValues,
  setError,
  setNotice,
  setConsoleSection,
  openBrowserSessionWorkbench,
}: ChatConsolePanelProps) {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const preferredSessionId = searchParams.get("sessionId");
  const preferredRunId = searchParams.get("runId");
  const preferredCompactionId = searchParams.get("compactionId");
  const preferredCheckpointId = searchParams.get("checkpointId");
  const preferredObjectiveId = searchParams.get("objectiveId");
  const sessionSwitchRef = useRef<string>("");
  const handoffTelemetryRef = useRef<string>("");
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
    onSessionActivated: async (sessionId) => {
      await emitUxEvent({
        name: "ux.session.resumed",
        section: "chat",
        sessionId,
        summary: "Resumed chat session.",
      });
    },
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
    onPromptSubmitted: async (sessionId) => {
      await emitUxEvent({
        name: "ux.chat.prompt_submitted",
        section: "chat",
        sessionId,
        summary: "Submitted a chat prompt.",
      });
    },
    onRunInspected: async (runId) => {
      await emitUxEvent({
        name: "ux.run.inspected",
        section: "chat",
        runId,
        summary: "Opened run inspector.",
      });
    },
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
  const deferredSearchQuery = useDeferredValue(transcriptSearchQuery);
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
    objectives,
    refreshObjectives,
    selectedObjective,
    selectedObjectiveFocus,
    selectedObjectiveLabel,
  } = useChatObjectives({
    api,
    preferredObjectiveId,
    selectedSession:
      sessions.selectedSession === null
        ? null
        : {
            session_id: sessions.selectedSession.session_id,
            session_key: sessions.selectedSession.session_key ?? undefined,
            session_label: sessions.selectedSession.session_label ?? undefined,
          },
  });

  useEffect(() => {
    const signature = searchParams.toString();
    if (signature.length === 0 || handoffTelemetryRef.current === signature) {
      return;
    }
    const handoff = parseConsoleHandoff(searchParams);
    if (
      handoff.sessionId === undefined &&
      handoff.runId === undefined &&
      handoff.objectiveId === undefined &&
      handoff.canvasId === undefined &&
      handoff.intent === undefined &&
      handoff.source === undefined
    ) {
      return;
    }
    handoffTelemetryRef.current = signature;
    void emitUxEvent({
      name: "ux.handoff.opened",
      section: handoff.section === "home" ? "overview" : (handoff.section ?? "chat"),
      sessionId: handoff.sessionId,
      runId: handoff.runId,
      objectiveId: handoff.objectiveId,
      canvasId: handoff.canvasId,
      intent: handoff.intent,
      summary: "Opened a scoped handoff.",
    });
  }, [emitUxEvent, searchParams]);
  const delegationCatalog = useChatPanelBootstrap({
    api,
    dispose,
    refreshObjectives,
    refreshSessions: sessions.refreshSessions,
    setError,
  });
  const {
    authProfiles,
    browserProfiles,
    browserSessions,
    parsedSlashCommand,
    showSlashPalette,
    slashCommandMatches,
    slashSuggestions,
    selectedSlashSuggestionIndex,
    setSelectedSlashSuggestionIndex,
    dismissSlashPalette,
    applySlashSuggestion,
    updateComposerDraft,
    refreshSlashEntityCatalogs,
    uxMetrics,
    recordUxMetric,
  } = useChatSlashPalette({
    api,
    composerText,
    setComposerText,
    sessions: sessions.sortedSessions,
    objectives,
    checkpoints,
    delegationCatalog,
    streaming,
    setError,
  });
  usePhase4DeepLinks({
    activeSessionId: sessions.activeSessionId,
    preferredSessionId,
    preferredRunId,
    preferredCompactionId,
    preferredCheckpointId,
    openRunDetails,
    inspectCompaction,
    inspectCheckpoint,
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
  const contextBudget = useChatContextBudget({
    attachments,
    composerText,
    contextReferencePreview,
    contextReferencePreviewStale,
    runTotalTokens: runStatus?.total_tokens ?? 0,
    sessionTotalTokens: sessions.selectedSession?.total_tokens ?? 0,
  });

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

  async function handleComposerSubmit(): Promise<void> {
    const command = parsedSlashCommand;
    if (command !== null) {
      await executeSlashCommand(command);
      return;
    }

    const effectiveContextReferencePreview = await ensureContextReferencePreviewForCurrentDraft();
    if (effectiveContextReferencePreview?.errors.length) {
      setError(effectiveContextReferencePreview.errors[0]?.message ?? "Invalid context reference.");
      recordUxMetric("errors");
      return;
    }
    const effectiveRecallPreview = await ensureRecallPreviewForCurrentDraft();
    await createUndoCheckpoint({
      api,
      activeSessionId: sessions.activeSessionId,
      transcriptRecordCount: transcriptRecords.length,
      sessionRunCount: sessionRuns.length,
      source: "send",
      setNotice,
      recordUxMetric,
    });

    const didSend = await sendMessage(
      async () => {
        await Promise.all([
          sessions.refreshSessions(false),
          refreshSessionTranscript(),
          refreshSlashEntityCatalogs(),
        ]);
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

  async function interruptCurrentRun(raw: string): Promise<void> {
    await interruptAndMaybeRedirect({
      api,
      actionableRunId,
      raw,
      runDrawerOpen,
      runDrawerId,
      cancelStreaming,
      refreshRunDetails: async () => {
        refreshRunDetails();
      },
      refreshSessions: async (preserveSelection = false) => {
        await sessions.refreshSessions(preserveSelection);
      },
      refreshSessionTranscript,
      refreshSlashEntityCatalogs,
      setRunActionBusy,
      setError,
      setNotice,
      recordUxMetric,
      createUndoCheckpointBeforeRedirect: () =>
        createUndoCheckpoint({
          api,
          activeSessionId: sessions.activeSessionId,
          transcriptRecordCount: transcriptRecords.length,
          sessionRunCount: sessionRuns.length,
          source: "redirect",
          setNotice,
          recordUxMetric,
        }),
      clearComposerDraft: () => updateComposerDraft(""),
      sendRedirectPrompt: async (redirectText, metadata) => {
        const didSend = await sendMessage(
          () =>
            Promise.all([
              sessions.refreshSessions(false),
              refreshSessionTranscript(),
              refreshSlashEntityCatalogs(),
            ]).then(() => undefined),
          { text: redirectText },
        );
        if (didSend) {
          appendLocalEntry({
            kind: "status",
            session_id: sessions.activeSessionId,
            run_id: metadata.runId,
            title: "Interrupt redirect",
            text: `Requested ${metadata.mode} interrupt and redirected the next prompt into a fresh run.`,
          });
        }
      },
    });
  }

  async function executeSlashCommand(command: NonNullable<typeof parsedSlashCommand>) {
    await executeChatSlashCommand({
      command,
      commandBusy,
      api,
      activeSessionId: sessions.activeSessionId,
      checkpoints,
      objectives,
      selectedObjective,
      authProfiles,
      browserProfiles,
      browserSessions,
      usageSummary: `Estimated context ${contextBudget.label}; branch ${describeBranchState(sessions.selectedSession?.branch_state ?? "missing")}; ${transcriptRecords.length} persisted transcript record${transcriptRecords.length === 1 ? "" : "s"}.`,
      openAttachmentPicker: () => attachmentInputRef.current?.click(),
      setSearchQuery: sessions.setSearchQuery,
      setError,
      setNotice,
      setCommandBusy,
      setConsoleSection,
      recordUxMetric,
      updateComposerDraft,
      navigateToObjective: (objectiveId) => {
        void navigate(buildObjectiveOverviewHref(objectiveId));
      },
      inspectCheckpoint,
      restoreCheckpoint,
      onInterrupt: interruptCurrentRun,
      onCreateSession: createNewSession,
      onResetSession: resetSessionAndTranscript,
      onRetry: retryLatestTurn,
      onBranchSession: branchCurrentSession,
      onQueueFollowUp: queueFollowUpText,
      onDelegate: async (raw) => {
        await delegateWorkAction({
          api,
          sessionId: sessions.activeSessionId.trim(),
          raw,
          delegationCatalog,
          upsertSession: sessions.upsertSession,
          refreshSessionTranscript,
          appendLocalEntry,
          setComposerText: updateComposerDraft,
          setCommandBusy,
          setError,
          setNotice,
        });
      },
      onResumeSession: resumeSession,
      onRunCompactionFlow: runCompactionFlow,
      onSearchTranscript: async (query) => {
        setTranscriptSearchQuery(query);
        await searchTranscript(query);
      },
      onExportTranscript: exportTranscript,
      refreshSessionTranscript,
      openBrowserSessionWorkbench,
    });
  }

  async function runCompactionFlow(mode: "preview" | "apply"): Promise<void> {
    await runChatCompactionFlow({
      api,
      activeSessionId: sessions.activeSessionId,
      mode,
      upsertSession: sessions.upsertSession,
      refreshSessionTranscript,
      setDetailPanel,
      appendLocalEntry,
      setCommandBusy,
      setError,
      setNotice,
    });
  }

  async function createNewSession(requestedLabel?: string): Promise<void> {
    await createNewSessionAction({
      requestedLabel,
      createSessionWithLabel: sessions.createSessionWithLabel,
      clearTranscriptState,
      setDetailPanel,
      setTranscriptSearchResults,
      setAttachments,
      setComposerText: updateComposerDraft,
      setError,
      setNotice,
    });
  }

  async function resumeSession(rawTarget: string): Promise<void> {
    resumeSessionAction({
      rawTarget,
      sortedSessions: sessions.sortedSessions,
      setActiveSessionId: sessions.setActiveSessionId,
      setComposerText: updateComposerDraft,
      setError,
      setNotice,
    });
  }

  async function retryLatestTurn(): Promise<void> {
    await createUndoCheckpoint({
      api,
      activeSessionId: sessions.activeSessionId,
      transcriptRecordCount: transcriptRecords.length,
      sessionRunCount: sessionRuns.length,
      source: "retry",
      setNotice,
      recordUxMetric,
    });
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
      setComposerText: updateComposerDraft,
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
      setComposerText: updateComposerDraft,
      setCommandBusy,
      setError,
      setNotice,
    });
  }
  async function searchTranscript(query = transcriptSearchQuery): Promise<void> {
    transcriptSearchSeqRef.current += 1;
    const requestSeq = transcriptSearchSeqRef.current;
    await searchChatTranscript({
      api,
      activeSessionId: sessions.activeSessionId,
      query,
      transcriptSearchRequestSeq: requestSeq,
      getCurrentTranscriptSearchSeq: () => transcriptSearchSeqRef.current,
      upsertSession: sessions.upsertSession,
      setTranscriptSearchResults,
      setTranscriptSearchBusy,
      setError,
    });
  }
  async function exportTranscript(format: "json" | "markdown"): Promise<void> {
    await exportChatTranscript({
      api,
      activeSessionId: sessions.activeSessionId,
      sessionLabel: sessions.selectedSession?.session_label,
      format,
      setExportBusy,
      setError,
      setNotice,
    });
  }
  async function pinTranscriptRecord(record: ChatTranscriptRecord): Promise<void> {
    await pinChatTranscriptRecord({
      api,
      activeSessionId: sessions.activeSessionId,
      record,
      refreshSessionTranscript,
      setCommandBusy,
      setError,
      setNotice,
    });
  }
  async function deletePin(pinId: string): Promise<void> {
    await deleteChatPin({
      api,
      activeSessionId: sessions.activeSessionId,
      pinId,
      refreshSessionTranscript,
      setCommandBusy,
      setError,
      setNotice,
    });
  }
  async function inspectCompaction(artifactId: string): Promise<void> {
    await inspectCompactionDetails({
      api,
      artifactId,
      upsertSession: sessions.upsertSession,
      setDetailPanel,
      setPhase4BusyKey,
      setError,
    });
  }
  async function inspectCheckpoint(checkpointId: string): Promise<void> {
    await inspectCheckpointDetails({
      api,
      checkpointId,
      upsertSession: sessions.upsertSession,
      setDetailPanel,
      setPhase4BusyKey,
      setError,
    });
  }
  async function restoreCheckpoint(
    checkpointId: string,
    options?: { source?: "undo" | "checkpoint" | "inspector" },
  ): Promise<void> {
    await restoreChatCheckpoint({
      api,
      checkpointId,
      checkpoints,
      actionableRunId,
      visibleTranscript,
      selectedSession: sessions.selectedSession,
      clearTranscriptState,
      setAttachments,
      refreshSessions: async (preserveSelection = false) => {
        await sessions.refreshSessions(preserveSelection);
      },
      refreshSessionTranscript,
      setDetailPanel,
      setPhase4BusyKey,
      setError,
      setNotice,
      upsertSession: sessions.upsertSession,
      source: options?.source,
    });
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
          setComposerText: updateComposerDraft,
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
          slashSuggestions,
          selectedSlashSuggestionIndex,
          setSelectedSlashSuggestionIndex,
          dismissSlashPalette,
          acceptSlashSuggestion: (replacement, acceptedWithKeyboard) => {
            applySlashSuggestion(replacement, acceptedWithKeyboard);
          },
          uxMetrics,
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
            void restoreCheckpoint(checkpointId, { source: "inspector" });
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
          void interruptCurrentRun("");
        }}
        onOpenObjective={
          selectedObjective === null
            ? null
            : () => {
                const objectiveId = readString(selectedObjective, "objective_id");
                if (objectiveId === null) {
                  return;
                }
                void navigate(buildObjectiveOverviewHref(objectiveId));
              }
        }
        onOpenRunDetails={() => {
          const targetRunId = activeRunId ?? knownRunIds[0] ?? null;
          if (targetRunId === null) {
            setError("No run is available for inspection.");
            return;
          }
          openRunDetails(targetRunId);
        }}
        onRefresh={() => {
          void Promise.all([
            sessions.refreshSessions(false),
            refreshSessionTranscript(),
            refreshObjectives(),
            refreshSlashEntityCatalogs(),
          ]);
        }}
        onSetAllowSensitiveTools={setAllowSensitiveTools}
        pendingApprovalCount={pendingApprovalCount}
        runActionBusy={runActionBusy}
        selectedObjectiveFocus={selectedObjectiveFocus}
        selectedObjectiveLabel={selectedObjectiveLabel}
        selectedSessionBranchState={describeBranchState(
          sessions.selectedSession?.branch_state ?? "missing",
        )}
        selectedSessionLineage={selectedSessionLineage}
        selectedSessionTitle={describeSelectedSessionTitle(sessions.selectedSession)}
        sessionsBusy={sessions.sessionsBusy}
        sessionsSidebarProps={buildSessionsSidebarProps({
          sessions,
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
        })}
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
