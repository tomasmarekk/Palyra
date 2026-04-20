import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import type {
  ChatAttachmentRecord,
  ChatBackgroundTaskRecord,
  ChatCheckpointRecord,
  ChatCompactionArtifactRecord,
  ChatPinRecord,
  ChatQueuedInputRecord,
  ChatRunStatusRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
  MediaDerivedArtifactRecord,
} from "../consoleApi";
import { type DetailPanelState, type TranscriptSearchMatch } from "./ChatInspectorColumn";
import type { RunDrawerTab } from "./ChatRunDrawer";
import { ChatCanvasWorkspaceView } from "./ChatCanvasWorkspaceView";
import { ChatConsoleWorkspaceView } from "./ChatConsoleWorkspaceView";
import {
  deleteChatPin,
  inspectCheckpointDetails,
  inspectCompactionDetails,
  pinChatTranscriptRecord,
} from "./chatConsoleOperations";
import { useChatAttachmentUploadHandler } from "./chatInspectorActions";
import { buildInspectorProps, buildTranscriptProps } from "./chatConsolePanelProps";
import { createChatConsoleSessionHandlers } from "./chatConsoleSessionHandlers";
import { describeBranchState, toErrorMessage, type ComposerAttachment } from "./chatShared";
import { emitPromptSubmitted, emitRunInspected, emitSessionResumed } from "./chatConsoleTelemetry";
import {
  createUndoCheckpoint,
  executeChatSlashCommand,
  interruptAndMaybeRedirect,
} from "./chatSlashActions";
import { delegateWorkAction, submitComposerTurnAction } from "./chatSessionActions";
import {
  approveProjectContextEntryAction,
  disableProjectContextEntryAction,
  enableProjectContextEntryAction,
  refreshProjectContextAction,
  scaffoldProjectContextAction,
} from "./chatProjectContextActions";
import { useContextReferencePreview } from "./useContextReferencePreview";
import { useProjectContextPreview } from "./useProjectContextPreview";
import { useRecallPreview } from "./useRecallPreview";
import { useChatContextBudget } from "./useChatContextBudget";
import { useChatRunStream } from "./useChatRunStream";
import { useChatSessions } from "./useChatSessions";
import { useChatSlashPalette } from "./useChatSlashPalette";
import { useChatDeepLinks } from "./useChatDeepLinks";
import { useChatObjectives } from "./useChatObjectives";
import { useChatPanelViewState } from "./useChatPanelViewState";
import { useChatPanelBootstrap } from "./useChatPanelBootstrap";
import { useChatWorkspaceRollbackHandlers } from "./useChatWorkspaceRollbackHandlers";
import {
  buildWorkspaceHeaderSessionState,
  buildSessionsSidebarProps,
  describeSelectedSessionTitle,
} from "./chatWorkspaceSessionBindings";
import { FIRST_SUCCESS_PROMPTS } from "./starterPrompts";
import { useChatCanvasSurfaceActions } from "./useChatCanvasSurfaceActions";
import { useSessionCanvases } from "./useSessionCanvases";
import { useStarterPromptGuidance } from "./useStarterPromptGuidance";
import { useStarterPromptHandoff } from "./useStarterPromptHandoff";
import type { UxTelemetryEvent } from "../console/contracts";
import { parseConsoleHandoff } from "../console/contracts";
import { getSectionPath } from "../console/navigation";
import type { Section } from "../console/sectionMetadata";
import { buildObjectiveOverviewHref } from "../console/objectiveLinks";
import { readString } from "../console/shared";
interface ChatConsolePanelProps {
  readonly api: ConsoleApiClient;
  readonly emitUxEvent: (
    event: Omit<UxTelemetryEvent, "surface" | "locale" | "mode">,
  ) => Promise<void>;
  readonly surface?: "chat" | "canvas";
  readonly revealSensitiveValues: boolean;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly setConsoleSection: (section: Section) => void;
  readonly openBrowserSessionWorkbench: (sessionId: string) => void;
}
export function ChatConsolePanel({
  api,
  emitUxEvent,
  surface = "chat",
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
  const preferredCanvasId = searchParams.get("canvasId");
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
  const [sessionMaintenanceBusyKey, setSessionMaintenanceBusyKey] = useState<string | null>(null);
  const [runDrawerTab, setRunDrawerTab] = useState<RunDrawerTab>("status");
  const attachmentInputRef = useRef<HTMLInputElement | null>(null);
  const sessionSearchInputRef = useRef<HTMLInputElement | null>(null);
  const handleSessionActivated = useCallback(
    (sessionId: string) => emitSessionResumed(emitUxEvent, sessionId),
    [emitUxEvent],
  );
  const sessions = useChatSessions({
    api,
    onSessionActivated: handleSessionActivated,
    setError,
    setNotice,
    preferredSessionId,
  });
  const starterPromptGuidance = useStarterPromptGuidance();
  const handlePromptSubmitted = useCallback(
    (sessionId: string) => {
      starterPromptGuidance.markFirstSuccessCompleted();
      return emitPromptSubmitted(emitUxEvent, sessionId);
    },
    [emitUxEvent, starterPromptGuidance],
  );
  const handleRunInspected = useCallback(
    (runId: string) => emitRunInspected(emitUxEvent, runId),
    [emitUxEvent],
  );
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
    onPromptSubmitted: handlePromptSubmitted,
    onRunInspected: handleRunInspected,
    sessionLabelDraft: sessions.sessionLabelDraft,
    setError,
    setNotice,
  });
  const openRunDetailsPanel = useCallback(
    (runId: string, tab: RunDrawerTab = "status"): void => {
      setRunDrawerTab(tab);
      openRunDetails(runId);
    },
    [openRunDetails],
  );
  const {
    filteredTranscript,
    filteredHiddenTranscriptItems,
    sessionQuickControlHeaderProps,
    sessionQuickControlPanelProps,
    pendingApprovalCount,
    a2uiSurfaces,
    knownRunIds,
    inspectorVisible,
    actionableRunId,
    toolPayloadCount,
    recentTranscriptRecords,
    deferredSearchQuery,
    selectedSessionLineage,
  } = useChatPanelViewState({
    api,
    selectedSession: sessions.selectedSession,
    upsertSession: sessions.upsertSession,
    visibleTranscript,
    hiddenTranscriptItems,
    a2uiDocuments,
    runIds,
    sessionRuns,
    runDrawerOpen,
    activeRunId,
    runDrawerId,
    transcriptSearchQuery,
    transcriptRecords,
    setError,
    setNotice,
  });
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
  useStarterPromptHandoff({
    activeSessionId: sessions.activeSessionId,
    setNotice,
    updateComposerDraft,
  });
  const inspectCompaction = (artifactId: string) =>
    inspectCompactionDetails({
      api,
      artifactId,
      upsertSession: sessions.upsertSession,
      setDetailPanel,
      setSessionMaintenanceBusyKey,
      setError,
    });
  const inspectCheckpoint = (checkpointId: string) =>
    inspectCheckpointDetails({
      api,
      checkpointId,
      upsertSession: sessions.upsertSession,
      setDetailPanel,
      setSessionMaintenanceBusyKey,
      setError,
    });
  useChatDeepLinks({
    activeSessionId: sessions.activeSessionId,
    preferredSessionId,
    preferredRunId,
    preferredCompactionId,
    preferredCheckpointId,
    openRunDetails: openRunDetailsPanel,
    inspectCompaction,
    inspectCheckpoint,
  });
  const {
    projectContextPreview,
    projectContextPreviewBusy,
    projectContextPreviewStale,
    projectContextPromptPreview,
    ensureProjectContextPreviewForCurrentDraft,
    loadProjectContextPreview,
  } = useProjectContextPreview({
    api,
    activeSessionId: sessions.activeSessionId,
    composerText,
    setError,
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
    projectContextPreview,
    projectContextPreviewStale,
    contextReferencePreview,
    contextReferencePreviewStale,
    runTotalTokens: runStatus?.total_tokens ?? 0,
    sessionTotalTokens: sessions.selectedSession?.total_tokens ?? 0,
  });
  const refreshSessionTranscript = useCallback(
    async (sessionIdOverride?: string) => {
      const sessionId = (sessionIdOverride ?? sessions.activeSessionId).trim();
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
    },
    [api, sessions.activeSessionId, sessions.upsertSession, setError],
  );
  const sessionCanvases = useSessionCanvases({
    api,
    activeSessionId: sessions.activeSessionId,
    preferredCanvasId,
    setError,
    onCanvasRestored: async (response) => {
      sessions.upsertSession(response.session);
      await Promise.all([sessions.refreshSessions(false), refreshSessionTranscript()]);
      setNotice(
        `Restored canvas state v${response.restored_from_state_version} into v${response.canvas.state_version}.`,
      );
    },
  });
  const {
    openConversationSurface,
    openCanvasSourceRun,
    setCanvasSurfaceSessionId,
    selectCanvasSurfaceCanvas,
    openCanvasSurfaceFromUrl,
    toggleCanvasPinFromUrl,
    reopenLastCanvas,
  } = useChatCanvasSurfaceActions({
    navigate,
    surface,
    activeSessionId: sessions.activeSessionId,
    activeRunId,
    knownRunIds,
    sessionSearchInputRef,
    selectedCanvas: sessionCanvases.selectedCanvas,
    canvases: sessionCanvases.canvases,
    pinnedCanvasId: sessionCanvases.pinnedCanvasId,
    selectedCanvasId: sessionCanvases.selectedCanvasId,
    setActiveSessionId: sessions.setActiveSessionId,
    selectCanvas: sessionCanvases.selectCanvas,
    togglePinnedCanvasById: sessionCanvases.togglePinnedCanvasById,
    setConsoleSection,
    openRunDetails: openRunDetailsPanel,
    setError,
    setNotice,
  });
  const openMemorySection = useCallback(() => setConsoleSection("memory"), [setConsoleSection]);
  const openSupportSection = useCallback(() => setConsoleSection("support"), [setConsoleSection]);
  const { handleWorkspaceRestore, openRollbackInspector, previewRollbackDiff } =
    useChatWorkspaceRollbackHandlers({
      api,
      actionableRunId,
      runDrawerId,
      sessionRuns,
      knownRunIds,
      selectedLastRunId: sessions.selectedSession?.last_run_id,
      pinnedCanvasId: sessionCanvases.pinnedCanvasId,
      canvases: sessionCanvases.canvases,
      upsertSession: sessions.upsertSession,
      clearTranscriptState,
      setAttachments,
      setDetailPanel,
      refreshSessions: () => sessions.refreshSessions(false),
      refreshSessionTranscript,
      refreshSessionCanvases: sessionCanvases.refreshSessionCanvases,
      appendLocalEntry,
      openRunDetails: openRunDetailsPanel,
      setError,
      setNotice,
    });
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
      setSessionMaintenanceBusyKey(null);
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
      setSessionMaintenanceBusyKey(null);
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
  const {
    resetSessionAndTranscript,
    archiveSessionAndTranscript,
    runCompactionFlow,
    createNewSession,
    resumeSession,
    retryLatestTurn,
    branchCurrentSession,
    queueFollowUpText,
    searchTranscript,
    exportTranscript,
    restoreCheckpoint,
  } = createChatConsoleSessionHandlers({
    api,
    activeSessionId: sessions.activeSessionId,
    selectedSession: sessions.selectedSession,
    sortedSessions: sessions.sortedSessions,
    createSessionWithLabel: sessions.createSessionWithLabel,
    setActiveSessionId: sessions.setActiveSessionId,
    upsertSession: sessions.upsertSession,
    refreshSessions: sessions.refreshSessions,
    resetSession: sessions.resetSession,
    archiveSession: sessions.archiveSession,
    clearTranscriptState,
    setDetailPanel,
    setTranscriptSearchResults,
    setTranscriptSearchBusy,
    setAttachments,
    setSessionAttachments: () => setSessionAttachments([]),
    setSessionDerivedArtifacts: () => setSessionDerivedArtifacts([]),
    setComposerText: updateComposerDraft,
    setExportBusy,
    setCommandBusy,
    setSessionMaintenanceBusyKey,
    setError,
    setNotice,
    appendLocalEntry,
    sendMessage,
    actionableRunId,
    checkpoints,
    visibleTranscript,
    transcriptRecordsLength: transcriptRecords.length,
    sessionRunsLength: sessionRuns.length,
    transcriptSearchQuery,
    nextTranscriptSearchRequestSeq: () => {
      transcriptSearchSeqRef.current += 1;
      return transcriptSearchSeqRef.current;
    },
    getCurrentTranscriptSearchSeq: () => transcriptSearchSeqRef.current,
    refreshSessionTranscript,
    recordUxMetric,
  });
  async function handleComposerSubmit(): Promise<void> {
    await submitComposerTurnAction({
      parsedSlashCommand,
      executeSlashCommand,
      ensureProjectContextPreviewForCurrentDraft,
      ensureContextReferencePreviewForCurrentDraft,
      ensureRecallPreviewForCurrentDraft,
      createUndoCheckpoint: async () => {
        await createUndoCheckpoint({
          api,
          activeSessionId: sessions.activeSessionId,
          transcriptRecordCount: transcriptRecords.length,
          sessionRunCount: sessionRuns.length,
          source: "send",
          setNotice,
          recordUxMetric,
        });
      },
      sendMessage,
      refreshSessions: () => sessions.refreshSessions(false),
      refreshSessionTranscript,
      refreshSlashEntityCatalogs,
      attachments,
      setAttachments,
      setError,
      recordUxMetric,
    });
  }
  async function interruptCurrentRun(raw: string): Promise<void> {
    await interruptAndMaybeRedirect({
      api,
      actionableRunId,
      raw,
      runDrawerOpen,
      runDrawerId,
      cancelStreaming,
      refreshRunDetails: async () => refreshRunDetails(),
      refreshSessions: async (preserveSelection = false) =>
        sessions.refreshSessions(preserveSelection),
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
        if (!didSend) {
          return;
        }
        appendLocalEntry({
          kind: "status",
          session_id: sessions.activeSessionId,
          run_id: metadata.runId,
          title: "Interrupt redirect",
          text: `Requested ${metadata.mode} interrupt and redirected the next prompt into a fresh run.`,
        });
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
      navigateToObjective: (objectiveId) => void navigate(buildObjectiveOverviewHref(objectiveId)),
      inspectCheckpoint,
      restoreCheckpoint,
      onInterrupt: interruptCurrentRun,
      onCreateSession: createNewSession,
      onRenameSession: async (requestedLabel) => sessions.renameSession(requestedLabel),
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
      onOpenRollback: openRollbackInspector,
      onPreviewRollbackDiff: previewRollbackDiff,
      onSearchTranscript: async (query) => {
        setTranscriptSearchQuery(query);
        await searchTranscript(query);
      },
      onExportTranscript: exportTranscript,
      refreshSessionTranscript,
      openBrowserSessionWorkbench,
    });
  }
  const projectContextActionArgs = {
    api,
    sessionId: sessions.activeSessionId,
    selectedSession: sessions.selectedSession,
    composerText,
    loadProjectContextPreview,
    upsertSession: sessions.upsertSession,
    setError,
    setNotice,
    setSessionMaintenanceBusyKey,
  };
  const sessionsSidebarProps = buildSessionsSidebarProps({
    sessions:
      surface === "canvas"
        ? { ...sessions, setActiveSessionId: setCanvasSurfaceSessionId }
        : sessions,
    createSession: () => void sessions.createSession(),
    renameSession: () => void sessions.renameSession(),
    resetSession: () => void resetSessionAndTranscript(),
    archiveSession: () => void archiveSessionAndTranscript(),
  });
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
      {surface === "canvas" ? (
        <ChatCanvasWorkspaceView
          canvases={sessionCanvases.canvases}
          canvasesBusy={sessionCanvases.canvasesBusy}
          canvasDetailBusy={sessionCanvases.canvasDetailBusy}
          pinnedCanvasId={sessionCanvases.pinnedCanvasId}
          restoringStateVersion={sessionCanvases.restoringStateVersion}
          runtimeFrameUrl={sessionCanvases.runtimeFrameUrl}
          selectedCanvas={sessionCanvases.selectedCanvas}
          selectedCanvasId={sessionCanvases.selectedCanvasId}
          {...buildWorkspaceHeaderSessionState(sessions.selectedSession)}
          selectedSessionLineage={selectedSessionLineage}
          selectedSessionTitle={describeSelectedSessionTitle(sessions.selectedSession)}
          sessionsBusy={sessions.sessionsBusy}
          sessionsSidebarProps={{ ...sessionsSidebarProps, searchInputRef: sessionSearchInputRef }}
          onOpenConversation={() => openConversationSurface()}
          onOpenSourceRun={openCanvasSourceRun}
          onRefresh={() =>
            void Promise.all([
              sessions.refreshSessions(false),
              refreshSessionTranscript(),
              refreshObjectives(),
              refreshSlashEntityCatalogs(),
              sessionCanvases.refreshSessionCanvases(),
            ])
          }
          onRestoreCanvas={(stateVersion) => void sessionCanvases.restoreCanvasState(stateVersion)}
          onSelectCanvas={selectCanvasSurfaceCanvas}
          onTogglePinnedCanvas={sessionCanvases.togglePinnedCanvas}
        />
      ) : (
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
            submitMessage: () => void handleComposerSubmit(),
            retryLast: () => void retryLatestTurn(),
            branchSession: () => void branchCurrentSession(),
            queueFollowUp: () => void queueFollowUpText(composerText),
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
            attachFiles: attachSelectedFiles,
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
            projectContextPreview,
            projectContextPreviewBusy,
            projectContextPreviewStale,
            projectContextPromptPreview,
            refreshProjectContextPreview: () =>
              void loadProjectContextPreview(composerText, { reportError: true }),
            contextReferencePreview,
            contextReferencePreviewBusy,
            contextReferencePreviewStale,
            refreshContextReferencePreview: () =>
              void loadContextReferencePreview(composerText, { reportError: true }),
            removeContextReference,
            recallPreview,
            recallPreviewBusy,
            recallPreviewStale,
            refreshRecallPreview: () => void loadRecallPreview(composerText, { reportError: true }),
          }}
          contextBudget={contextBudget}
          inspectorProps={buildInspectorProps({
            api,
            pendingApprovalCount,
            a2uiSurfaces,
            runIds: knownRunIds,
            selectedSession: sessions.selectedSession,
            selectedSessionLineage,
            sessionQuickControlPanelProps,
            contextBudgetEstimatedTokens: contextBudget.estimated_total_tokens,
            projectContextBusy: projectContextPreviewBusy,
            refreshProjectContext: () => refreshProjectContextAction(projectContextActionArgs),
            disableProjectContextEntry: (entryId) => {
              void disableProjectContextEntryAction({ ...projectContextActionArgs, entryId });
            },
            enableProjectContextEntry: (entryId) => {
              void enableProjectContextEntryAction({ ...projectContextActionArgs, entryId });
            },
            approveProjectContextEntry: (entryId) => {
              void approveProjectContextEntryAction({ ...projectContextActionArgs, entryId });
            },
            scaffoldProjectContext: () =>
              void scaffoldProjectContextAction(projectContextActionArgs),
            transcriptBusy,
            transcriptSearchQuery,
            setTranscriptSearchQuery,
            transcriptSearchBusy,
            canSearchTranscript: deferredSearchQuery.trim().length > 0,
            sessionPins,
            searchResults: transcriptSearchResults,
            searchTranscript,
            exportBusy,
            exportTranscript,
            recentTranscriptRecords,
            pinTranscriptRecord: async (record) =>
              pinChatTranscriptRecord({
                api,
                activeSessionId: sessions.activeSessionId,
                record,
                refreshSessionTranscript,
                setCommandBusy,
                setError,
                setNotice,
              }),
            deletePin: async (pinId) =>
              deleteChatPin({
                api,
                activeSessionId: sessions.activeSessionId,
                pinId,
                refreshSessionTranscript,
                setCommandBusy,
                setError,
                setNotice,
              }),
            compactions,
            checkpoints,
            queuedInputs,
            backgroundTasks,
            detailPanel,
            revealSensitiveValues,
            inspectorVisible,
            openRunDetails: openRunDetailsPanel,
            sessionMaintenanceBusyKey,
            runDrawerId,
            setRunDrawerId,
            runDrawerBusy,
            runStatus,
            runTape,
            runLineage,
            runDrawerTab,
            setRunDrawerTab,
            setError,
            setNotice,
            onWorkspaceRestore: handleWorkspaceRestore,
            openMemorySection,
            openSupportSection,
            openCanvasSurface: openCanvasSurfaceFromUrl,
            refreshRunDetails,
            closeRunDrawer,
            openBrowserSessionWorkbench,
            transcriptRecords,
            inspectCompaction,
            inspectCheckpoint,
            restoreCheckpoint,
            refreshSessionTranscript,
            setDetailPanel,
            setSessionMaintenanceBusyKey,
          })}
          onAbortRun={() => void interruptCurrentRun("")}
          onOpenObjective={
            selectedObjective === null
              ? null
              : () => {
                  const objectiveId = readString(selectedObjective, "objective_id");
                  if (objectiveId !== null) {
                    void navigate(buildObjectiveOverviewHref(objectiveId));
                  }
                }
          }
          onOpenRunDetails={() => {
            const targetRunId = activeRunId ?? knownRunIds[0] ?? null;
            if (targetRunId === null) {
              setError("No run is available for inspection.");
              return;
            }
            openRunDetailsPanel(targetRunId);
          }}
          onRefresh={() =>
            void Promise.all([
              sessions.refreshSessions(false),
              refreshSessionTranscript(),
              refreshObjectives(),
              refreshSlashEntityCatalogs(),
            ])
          }
          onSetAllowSensitiveTools={setAllowSensitiveTools}
          onHideStarterPrompts={starterPromptGuidance.hideStarterPrompts}
          onShowStarterPrompts={starterPromptGuidance.showStarterPrompts}
          onUseStarterPrompt={updateComposerDraft}
          pendingApprovalCount={pendingApprovalCount}
          runActionBusy={runActionBusy}
          selectedObjectiveFocus={selectedObjectiveFocus}
          selectedObjectiveLabel={selectedObjectiveLabel}
          sessionQuickControlHeaderProps={sessionQuickControlHeaderProps}
          {...buildWorkspaceHeaderSessionState(sessions.selectedSession)}
          selectedSessionLineage={selectedSessionLineage}
          selectedSessionTitle={describeSelectedSessionTitle(sessions.selectedSession)}
          sessionsBusy={sessions.sessionsBusy}
          sessionsSidebarProps={{ ...sessionsSidebarProps, searchInputRef: sessionSearchInputRef }}
          showStarterPrompts={
            !starterPromptGuidance.firstSuccessCompleted &&
            !starterPromptGuidance.starterPromptsHidden &&
            transcriptRecords.length === 0 &&
            composerText.trim().length === 0
          }
          starterPromptsHidden={starterPromptGuidance.starterPromptsHidden}
          starterPromptHint="Use a starter prompt to confirm the control plane is responsive before branching into a real task."
          starterPrompts={FIRST_SUCCESS_PROMPTS}
          streaming={streaming}
          toolPayloadCount={toolPayloadCount}
          transcriptBusy={transcriptBusy}
          transcriptProps={buildTranscriptProps({
            api,
            visibleTranscript: filteredTranscript,
            sessionAttachments,
            sessionDerivedArtifacts,
            hiddenTranscriptItems: filteredHiddenTranscriptItems,
            transcriptBoxRef,
            approvalDrafts,
            a2uiDocuments,
            selectedDetailId: detailPanel?.id ?? null,
            updateApprovalDraft: updateApprovalDraftValue,
            decideInlineApproval: async (approvalId, approved) =>
              await decideInlineApproval(approvalId, approved),
            openToolPermissions: (toolName) => {
              setConsoleSection("approvals");
              void navigate(
                `${getSectionPath("approvals")}?${new URLSearchParams([["tool", toolName]]).toString()}`,
              );
            },
            openRunDetails: openRunDetailsPanel,
            openCanvasSurface: openCanvasSurfaceFromUrl,
            togglePinnedCanvas: toggleCanvasPinFromUrl,
            reopenLastCanvas,
            canReopenLastCanvas:
              sessionCanvases.pinnedCanvasId !== null || sessionCanvases.canvases.length > 0,
            pinnedCanvasId: sessionCanvases.pinnedCanvasId,
            refreshSessionTranscript,
            setDetailPanel,
            setError,
            setNotice,
            setSessionMaintenanceBusyKey,
          })}
        />
      )}
    </main>
  );
}
