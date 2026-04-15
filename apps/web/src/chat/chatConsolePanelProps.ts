import type { ComponentProps } from "react";

import type { ConsoleApiClient, ChatTranscriptRecord } from "../consoleApi";

import type { DetailPanelState } from "./ChatInspectorColumn";
import type { ChatConsoleWorkspaceView } from "./ChatConsoleWorkspaceView";
import {
  inspectBackgroundTaskDetail,
  inspectDerivedArtifactDetail,
  inspectLiveEntryDetail,
  inspectSearchMatchDetail,
  inspectTranscriptRecordDetail,
  runBackgroundTaskLifecycleAction,
  runDerivedArtifactLifecycleAction,
} from "./chatInspectorActions";

type WorkspaceViewProps = ComponentProps<typeof ChatConsoleWorkspaceView>;
type InspectorProps = WorkspaceViewProps["inspectorProps"];
type TranscriptProps = WorkspaceViewProps["transcriptProps"];

interface BuildInspectorPropsArgs {
  readonly api: ConsoleApiClient;
  readonly pendingApprovalCount: InspectorProps["pendingApprovalCount"];
  readonly a2uiSurfaces: InspectorProps["a2uiSurfaces"];
  readonly runIds: InspectorProps["runIds"];
  readonly selectedSession: InspectorProps["selectedSession"];
  readonly selectedSessionLineage: InspectorProps["selectedSessionLineage"];
  readonly sessionQuickControlPanelProps: InspectorProps["sessionQuickControlPanelProps"];
  readonly contextBudgetEstimatedTokens: InspectorProps["contextBudgetEstimatedTokens"];
  readonly projectContextBusy: InspectorProps["projectContextBusy"];
  readonly refreshProjectContext: InspectorProps["refreshProjectContext"];
  readonly disableProjectContextEntry: InspectorProps["disableProjectContextEntry"];
  readonly enableProjectContextEntry: InspectorProps["enableProjectContextEntry"];
  readonly approveProjectContextEntry: InspectorProps["approveProjectContextEntry"];
  readonly scaffoldProjectContext: InspectorProps["scaffoldProjectContext"];
  readonly transcriptBusy: InspectorProps["transcriptBusy"];
  readonly transcriptSearchQuery: InspectorProps["transcriptSearchQuery"];
  readonly setTranscriptSearchQuery: InspectorProps["setTranscriptSearchQuery"];
  readonly transcriptSearchBusy: InspectorProps["transcriptSearchBusy"];
  readonly canSearchTranscript: InspectorProps["canSearchTranscript"];
  readonly sessionPins: InspectorProps["sessionPins"];
  readonly searchResults: InspectorProps["searchResults"];
  readonly searchTranscript: InspectorProps["searchTranscript"];
  readonly exportBusy: InspectorProps["exportBusy"];
  readonly exportTranscript: InspectorProps["exportTranscript"];
  readonly recentTranscriptRecords: InspectorProps["recentTranscriptRecords"];
  readonly pinTranscriptRecord: InspectorProps["pinTranscriptRecord"];
  readonly deletePin: InspectorProps["deletePin"];
  readonly compactions: InspectorProps["compactions"];
  readonly checkpoints: InspectorProps["checkpoints"];
  readonly queuedInputs: InspectorProps["queuedInputs"];
  readonly backgroundTasks: InspectorProps["backgroundTasks"];
  readonly detailPanel: InspectorProps["detailPanel"];
  readonly revealSensitiveValues: InspectorProps["revealSensitiveValues"];
  readonly inspectorVisible: InspectorProps["inspectorVisible"];
  readonly openRunDetails: InspectorProps["openRunDetails"];
  readonly phase4BusyKey: InspectorProps["phase4BusyKey"];
  readonly runDrawerId: InspectorProps["runDrawerId"];
  readonly setRunDrawerId: InspectorProps["setRunDrawerId"];
  readonly runDrawerBusy: InspectorProps["runDrawerBusy"];
  readonly runStatus: InspectorProps["runStatus"];
  readonly runTape: InspectorProps["runTape"];
  readonly runLineage: InspectorProps["runLineage"];
  readonly runDrawerTab: InspectorProps["runDrawerTab"];
  readonly setRunDrawerTab: InspectorProps["setRunDrawerTab"];
  readonly refreshRunDetails: InspectorProps["refreshRunDetails"];
  readonly closeRunDrawer: InspectorProps["closeRunDrawer"];
  readonly openBrowserSessionWorkbench: InspectorProps["openBrowserSessionWorkbench"];
  readonly onWorkspaceRestore: InspectorProps["onWorkspaceRestore"];
  readonly openMemorySection: InspectorProps["openMemorySection"];
  readonly openSupportSection: InspectorProps["openSupportSection"];
  readonly transcriptRecords: ChatTranscriptRecord[];
  readonly inspectCompaction: (artifactId: string) => void;
  readonly inspectCheckpoint: (checkpointId: string) => void;
  readonly restoreCheckpoint: (
    checkpointId: string,
    options?: { source?: "undo" | "checkpoint" | "inspector" },
  ) => Promise<void>;
  readonly refreshSessionTranscript: () => Promise<void>;
  readonly setDetailPanel: (next: DetailPanelState | null) => void;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly setPhase4BusyKey: (next: string | null) => void;
}

export function buildInspectorProps({
  api,
  pendingApprovalCount,
  a2uiSurfaces,
  runIds,
  selectedSession,
  selectedSessionLineage,
  sessionQuickControlPanelProps,
  contextBudgetEstimatedTokens,
  projectContextBusy,
  refreshProjectContext,
  disableProjectContextEntry,
  enableProjectContextEntry,
  approveProjectContextEntry,
  scaffoldProjectContext,
  transcriptBusy,
  transcriptSearchQuery,
  setTranscriptSearchQuery,
  transcriptSearchBusy,
  canSearchTranscript,
  sessionPins,
  searchResults,
  searchTranscript,
  exportBusy,
  exportTranscript,
  recentTranscriptRecords,
  pinTranscriptRecord,
  deletePin,
  compactions,
  checkpoints,
  queuedInputs,
  backgroundTasks,
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
  runDrawerTab,
  setRunDrawerTab,
  refreshRunDetails,
  closeRunDrawer,
  openBrowserSessionWorkbench,
  onWorkspaceRestore,
  openMemorySection,
  openSupportSection,
  transcriptRecords,
  inspectCompaction,
  inspectCheckpoint,
  restoreCheckpoint,
  refreshSessionTranscript,
  setDetailPanel,
  setError,
  setNotice,
  setPhase4BusyKey,
}: BuildInspectorPropsArgs): InspectorProps {
  return {
    pendingApprovalCount,
    a2uiSurfaces,
    runIds,
    selectedSession,
    selectedSessionLineage,
    sessionQuickControlPanelProps,
    contextBudgetEstimatedTokens,
    projectContextBusy,
    refreshProjectContext,
    disableProjectContextEntry,
    enableProjectContextEntry,
    approveProjectContextEntry,
    scaffoldProjectContext,
    transcriptBusy,
    transcriptSearchQuery,
    setTranscriptSearchQuery,
    transcriptSearchBusy,
    canSearchTranscript,
    pinnedRecordKeys: new Set(sessionPins.map((pin) => `${pin.run_id}:${pin.tape_seq}`)),
    searchResults,
    searchTranscript,
    inspectSearchMatch: (match) => {
      inspectSearchMatchDetail({
        match,
        transcriptRecords,
        setDetailPanel,
      });
    },
    exportBusy,
    exportTranscript,
    recentTranscriptRecords,
    inspectTranscriptRecord: (record) => {
      inspectTranscriptRecordDetail(record, setDetailPanel);
    },
    pinTranscriptRecord,
    sessionPins,
    deletePin,
    compactions,
    inspectCompaction,
    checkpoints,
    inspectCheckpoint,
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
    runDrawerTab,
    setRunDrawerTab,
    api,
    setError,
    setNotice,
    onWorkspaceRestore,
    openMemorySection,
    openSupportSection,
    refreshRunDetails,
    closeRunDrawer,
    openBrowserSessionWorkbench,
  };
}

interface BuildTranscriptPropsArgs {
  readonly api: ConsoleApiClient;
  readonly visibleTranscript: TranscriptProps["visibleTranscript"];
  readonly sessionAttachments: TranscriptProps["sessionAttachments"];
  readonly sessionDerivedArtifacts: TranscriptProps["sessionDerivedArtifacts"];
  readonly hiddenTranscriptItems: TranscriptProps["hiddenTranscriptItems"];
  readonly transcriptBoxRef: TranscriptProps["transcriptBoxRef"];
  readonly approvalDrafts: TranscriptProps["approvalDrafts"];
  readonly a2uiDocuments: TranscriptProps["a2uiDocuments"];
  readonly selectedDetailId: TranscriptProps["selectedDetailId"];
  readonly updateApprovalDraft: TranscriptProps["updateApprovalDraft"];
  readonly decideInlineApproval: TranscriptProps["decideInlineApproval"];
  readonly openToolPermissions?: TranscriptProps["openToolPermissions"];
  readonly openRunDetails: TranscriptProps["openRunDetails"];
  readonly refreshSessionTranscript: () => Promise<void>;
  readonly setDetailPanel: (next: DetailPanelState | null) => void;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly setPhase4BusyKey: (next: string | null) => void;
}

export function buildTranscriptProps({
  api,
  visibleTranscript,
  sessionAttachments,
  sessionDerivedArtifacts,
  hiddenTranscriptItems,
  transcriptBoxRef,
  approvalDrafts,
  a2uiDocuments,
  selectedDetailId,
  updateApprovalDraft,
  decideInlineApproval,
  openToolPermissions,
  openRunDetails,
  refreshSessionTranscript,
  setDetailPanel,
  setError,
  setNotice,
  setPhase4BusyKey,
}: BuildTranscriptPropsArgs): TranscriptProps {
  return {
    visibleTranscript,
    sessionAttachments,
    sessionDerivedArtifacts,
    hiddenTranscriptItems,
    transcriptBoxRef,
    approvalDrafts,
    a2uiDocuments,
    selectedDetailId,
    updateApprovalDraft,
    decideInlineApproval,
    openToolPermissions,
    openRunDetails,
    inspectPayload: (entry) => {
      inspectLiveEntryDetail(entry, setDetailPanel);
    },
    inspectDerivedArtifact: (derivedArtifactId) => {
      inspectDerivedArtifactDetail({
        derivedArtifactId,
        sessionDerivedArtifacts: sessionDerivedArtifacts ?? [],
        sessionAttachments: sessionAttachments ?? [],
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
  };
}
