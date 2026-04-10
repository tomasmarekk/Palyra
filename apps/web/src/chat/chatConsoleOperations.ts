import type { Dispatch, SetStateAction } from "react";

import type {
  ChatCheckpointRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
  SessionCatalogRecord,
} from "../consoleApi";

import type { DetailPanelState, TranscriptSearchMatch } from "./ChatInspectorColumn";
import {
  inspectCheckpointAction,
  inspectCompactionAction,
  restoreCheckpointAction,
  runCompactionFlowAction,
} from "./chatPhase4Actions";
import {
  deletePinAction,
  exportTranscriptAction,
  pinTranscriptRecordAction,
} from "./chatSessionActions";
import { checkpointHasTag } from "./chatCommandSuggestions";
import { toErrorMessage, type ComposerAttachment, type TranscriptEntry } from "./chatShared";

export async function runChatCompactionFlow(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  mode: "preview" | "apply";
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  refreshSessionTranscript: () => Promise<void>;
  setDetailPanel: (next: DetailPanelState | null) => void;
  appendLocalEntry: (entry: Omit<TranscriptEntry, "id" | "created_at_unix_ms">) => void;
  setCommandBusy: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  const {
    api,
    activeSessionId,
    mode,
    upsertSession,
    refreshSessionTranscript,
    setDetailPanel,
    appendLocalEntry,
    setCommandBusy,
    setError,
    setNotice,
  } = args;
  const sessionId = activeSessionId.trim();
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
      upsertSession,
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

export async function searchChatTranscript(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  query: string;
  transcriptSearchRequestSeq: number;
  getCurrentTranscriptSearchSeq: () => number;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  setTranscriptSearchResults: (results: TranscriptSearchMatch[]) => void;
  setTranscriptSearchBusy: (next: boolean) => void;
  setError: (next: string | null) => void;
}): Promise<void> {
  const {
    api,
    activeSessionId,
    query,
    transcriptSearchRequestSeq,
    getCurrentTranscriptSearchSeq,
    upsertSession,
    setTranscriptSearchResults,
    setTranscriptSearchBusy,
    setError,
  } = args;
  const sessionId = activeSessionId.trim();
  const trimmed = query.trim();
  if (sessionId.length === 0) {
    setError("Select a session before searching the transcript.");
    return;
  }
  if (trimmed.length === 0) {
    setTranscriptSearchResults([]);
    return;
  }

  setTranscriptSearchBusy(true);
  setError(null);
  try {
    const response = await api.searchSessionTranscript(sessionId, trimmed);
    if (transcriptSearchRequestSeq !== getCurrentTranscriptSearchSeq()) {
      return;
    }
    upsertSession(response.session);
    setTranscriptSearchResults(response.matches);
  } catch (error) {
    if (transcriptSearchRequestSeq === getCurrentTranscriptSearchSeq()) {
      setError(toErrorMessage(error));
    }
  } finally {
    if (transcriptSearchRequestSeq === getCurrentTranscriptSearchSeq()) {
      setTranscriptSearchBusy(false);
    }
  }
}

export async function exportChatTranscript(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  sessionLabel: string | null | undefined;
  format: "json" | "markdown";
  setExportBusy: (next: "json" | "markdown" | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  await exportTranscriptAction({
    api: args.api,
    sessionId: args.activeSessionId.trim(),
    sessionLabel: args.sessionLabel ?? undefined,
    format: args.format,
    setExportBusy: args.setExportBusy,
    setError: args.setError,
    setNotice: args.setNotice,
  });
}

export async function pinChatTranscriptRecord(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  record: ChatTranscriptRecord;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  await pinTranscriptRecordAction({
    api: args.api,
    sessionId: args.activeSessionId.trim(),
    record: args.record,
    refreshSessionTranscript: args.refreshSessionTranscript,
    setCommandBusy: args.setCommandBusy,
    setError: args.setError,
    setNotice: args.setNotice,
  });
}

export async function deleteChatPin(args: {
  api: ConsoleApiClient;
  activeSessionId: string;
  pinId: string;
  refreshSessionTranscript: () => Promise<void>;
  setCommandBusy: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): Promise<void> {
  await deletePinAction({
    api: args.api,
    sessionId: args.activeSessionId.trim(),
    pinId: args.pinId,
    refreshSessionTranscript: args.refreshSessionTranscript,
    setCommandBusy: args.setCommandBusy,
    setError: args.setError,
    setNotice: args.setNotice,
  });
}

export async function inspectCompactionDetails(args: {
  api: ConsoleApiClient;
  artifactId: string;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  setDetailPanel: (next: DetailPanelState | null) => void;
  setPhase4BusyKey: (next: string | null) => void;
  setError: (next: string | null) => void;
}): Promise<void> {
  const { api, artifactId, upsertSession, setDetailPanel, setPhase4BusyKey, setError } = args;
  setPhase4BusyKey(`inspect-compaction:${artifactId}`);
  setError(null);
  try {
    await inspectCompactionAction({
      api,
      artifactId,
      upsertSession,
      setDetailPanel,
    });
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setPhase4BusyKey(null);
  }
}

export async function inspectCheckpointDetails(args: {
  api: ConsoleApiClient;
  checkpointId: string;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  setDetailPanel: (next: DetailPanelState | null) => void;
  setPhase4BusyKey: (next: string | null) => void;
  setError: (next: string | null) => void;
}): Promise<void> {
  const { api, checkpointId, upsertSession, setDetailPanel, setPhase4BusyKey, setError } = args;
  setPhase4BusyKey(`inspect-checkpoint:${checkpointId}`);
  setError(null);
  try {
    await inspectCheckpointAction({
      api,
      checkpointId,
      upsertSession,
      setDetailPanel,
    });
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setPhase4BusyKey(null);
  }
}

export async function restoreChatCheckpoint(args: {
  api: ConsoleApiClient;
  checkpointId: string;
  checkpoints: readonly ChatCheckpointRecord[];
  actionableRunId: string | null;
  visibleTranscript: readonly { run_id?: string; kind: string }[];
  selectedSession: SessionCatalogRecord | null;
  clearTranscriptState: () => void;
  setAttachments: Dispatch<SetStateAction<ComposerAttachment[]>>;
  refreshSessions: (preserveSelection?: boolean) => Promise<void>;
  refreshSessionTranscript: () => Promise<void>;
  setDetailPanel: (next: DetailPanelState | null) => void;
  setPhase4BusyKey: (next: string | null) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  source?: "undo" | "checkpoint" | "inspector";
}): Promise<void> {
  const {
    api,
    checkpointId,
    checkpoints,
    actionableRunId,
    visibleTranscript,
    selectedSession,
    clearTranscriptState,
    setAttachments,
    refreshSessions,
    refreshSessionTranscript,
    setDetailPanel,
    setPhase4BusyKey,
    setError,
    setNotice,
    upsertSession,
    source,
  } = args;
  setPhase4BusyKey(`restore-checkpoint:${checkpointId}`);
  setError(null);
  setNotice(null);
  try {
    const checkpoint =
      checkpoints.find((candidate) => candidate.checkpoint_id === checkpointId) ?? null;
    const latestRunHadSideEffects =
      actionableRunId !== null &&
      visibleTranscript.some(
        (entry) =>
          entry.run_id === actionableRunId &&
          (entry.kind === "tool" || entry.kind === "approval_request"),
      );
    await restoreCheckpointAction({
      api,
      checkpointId,
      selectedSession,
      upsertSession,
      clearTranscriptState,
      setAttachments,
      refreshSessions,
      refreshSessionTranscript,
      setDetailPanel,
      setNotice: (message) => {
        if (source === "undo") {
          const qualifier =
            latestRunHadSideEffects ||
            (checkpoint !== null && !checkpointHasTag(checkpoint, "undo_safe"))
              ? " Session history was restored, but any external side effects that already happened remain unchanged."
              : " Last conversational state restored from a safe checkpoint.";
          setNotice(`${message ?? "Checkpoint restored."}${qualifier}`);
          return;
        }
        setNotice(message);
      },
    });
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setPhase4BusyKey(null);
  }
}
