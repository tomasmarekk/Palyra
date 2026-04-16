import { useCallback } from "react";
import type { Dispatch, SetStateAction } from "react";

import type {
  ChatRunStatusRecord,
  ConsoleApiClient,
  SessionCanvasSummary,
  WorkspaceRestoreResponseEnvelope,
} from "../consoleApi";

import type { DetailPanelState } from "./ChatInspectorColumn";
import type { RunDrawerTab } from "./ChatRunDrawer";
import type { ComposerAttachment, TranscriptEntry } from "./chatShared";
import {
  openWorkspaceRollbackInspectorAction,
  previewWorkspaceRollbackDiffAction,
  reconcileWorkspaceRestoreAction,
} from "./chatWorkspaceRollbackActions";

type AppendLocalEntry = (entry: Omit<TranscriptEntry, "id" | "created_at_unix_ms">) => void;

type UseChatWorkspaceRollbackHandlersArgs = {
  readonly api: ConsoleApiClient;
  readonly actionableRunId: string | null;
  readonly runDrawerId: string;
  readonly sessionRuns: readonly ChatRunStatusRecord[];
  readonly knownRunIds: readonly string[];
  readonly selectedLastRunId?: string;
  readonly pinnedCanvasId: string | null;
  readonly canvases: readonly SessionCanvasSummary[];
  readonly upsertSession: (
    session: WorkspaceRestoreResponseEnvelope["session"],
    options?: { select?: boolean },
  ) => void;
  readonly clearTranscriptState: () => void;
  readonly setAttachments: Dispatch<SetStateAction<ComposerAttachment[]>>;
  readonly setDetailPanel: (next: DetailPanelState | null) => void;
  readonly refreshSessions: () => Promise<void>;
  readonly refreshSessionTranscript: (sessionIdOverride?: string) => Promise<void>;
  readonly refreshSessionCanvases: (sessionIdOverride?: string) => Promise<void>;
  readonly appendLocalEntry: AppendLocalEntry;
  readonly openRunDetails: (runId: string, tab?: RunDrawerTab) => void;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
};

type UseChatWorkspaceRollbackHandlersResult = {
  readonly handleWorkspaceRestore: (response: WorkspaceRestoreResponseEnvelope) => Promise<void>;
  readonly openRollbackInspector: (rawTarget?: string) => Promise<void>;
  readonly previewRollbackDiff: (rawTarget: string) => Promise<void>;
};

export function useChatWorkspaceRollbackHandlers({
  api,
  actionableRunId,
  runDrawerId,
  sessionRuns,
  knownRunIds,
  selectedLastRunId,
  pinnedCanvasId,
  canvases,
  upsertSession,
  clearTranscriptState,
  setAttachments,
  setDetailPanel,
  refreshSessions,
  refreshSessionTranscript,
  refreshSessionCanvases,
  appendLocalEntry,
  openRunDetails,
  setError,
  setNotice,
}: UseChatWorkspaceRollbackHandlersArgs): UseChatWorkspaceRollbackHandlersResult {
  const handleWorkspaceRestore = useCallback(
    async (response: WorkspaceRestoreResponseEnvelope): Promise<void> => {
      await reconcileWorkspaceRestoreAction({
        response,
        upsertSession,
        clearTranscriptState,
        setAttachments,
        setDetailPanel,
        refreshSessions,
        refreshSessionTranscript,
        appendLocalEntry,
        openRunDetails,
        setNotice: (next) => {
          if (next === null || (pinnedCanvasId === null && canvases.length === 0)) {
            setNotice(next);
            return;
          }
          setNotice(`${next} Reopen the canvas surface if you need to reconcile visual state.`);
        },
      });
      await refreshSessionCanvases(response.session.session_id);
    },
    [
      appendLocalEntry,
      canvases.length,
      clearTranscriptState,
      openRunDetails,
      pinnedCanvasId,
      refreshSessionCanvases,
      refreshSessionTranscript,
      refreshSessions,
      setAttachments,
      setDetailPanel,
      setNotice,
      upsertSession,
    ],
  );

  const openRollbackInspector = useCallback(
    async (rawTarget = ""): Promise<void> => {
      await openWorkspaceRollbackInspectorAction({
        rawTarget,
        actionableRunId,
        sessionRuns,
        selectedLastRunId,
        knownRunIds,
        setDetailPanel,
        openRunDetails,
        setError,
        setNotice,
      });
    },
    [
      actionableRunId,
      knownRunIds,
      openRunDetails,
      selectedLastRunId,
      sessionRuns,
      setDetailPanel,
      setError,
      setNotice,
    ],
  );

  const previewRollbackDiff = useCallback(
    async (rawTarget: string): Promise<void> => {
      await previewWorkspaceRollbackDiffAction({
        api,
        rawTarget,
        actionableRunId,
        runDrawerId,
        sessionRuns,
        selectedLastRunId,
        knownRunIds,
        setDetailPanel,
        openRunDetails,
        setError,
        setNotice,
      });
    },
    [
      actionableRunId,
      api,
      knownRunIds,
      openRunDetails,
      runDrawerId,
      selectedLastRunId,
      sessionRuns,
      setDetailPanel,
      setError,
      setNotice,
    ],
  );

  return {
    handleWorkspaceRestore,
    openRollbackInspector,
    previewRollbackDiff,
  };
}
