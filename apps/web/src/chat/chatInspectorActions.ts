import { useCallback, type Dispatch, type RefObject, type SetStateAction } from "react";

import type {
  ChatAttachmentRecord,
  ChatTranscriptRecord,
  ConsoleApiClient,
  MediaDerivedArtifactRecord,
} from "../consoleApi";
import type { DetailPanelState, TranscriptSearchMatch } from "./ChatInspectorColumn";
import {
  buildDetailFromDerivedArtifact,
  buildDetailFromLiveEntry,
  buildDetailFromSearchMatch,
  buildDetailFromTranscriptRecord,
} from "./chatConsoleUtils";
import {
  inspectBackgroundTaskAction,
  runBackgroundTaskActionRequest,
} from "./chatTranscriptMaintenanceActions";
import { toErrorMessage, type ComposerAttachment, type TranscriptEntry } from "./chatShared";
import { handleAttachmentFilesAction } from "./chatSessionActions";

function setBusyKey(
  setSessionMaintenanceBusyKey: ((next: string | null) => void) | undefined,
  next: string | null,
): void {
  setSessionMaintenanceBusyKey?.(next);
}

export function inspectLiveEntryDetail(
  entry: TranscriptEntry,
  setDetailPanel: (next: DetailPanelState | null) => void,
): void {
  setDetailPanel(buildDetailFromLiveEntry(entry));
}

export function inspectTranscriptRecordDetail(
  record: ChatTranscriptRecord,
  setDetailPanel: (next: DetailPanelState | null) => void,
): void {
  setDetailPanel(buildDetailFromTranscriptRecord(record));
}

export function inspectSearchMatchDetail({
  match,
  transcriptRecords,
  setDetailPanel,
}: {
  match: TranscriptSearchMatch;
  transcriptRecords: readonly ChatTranscriptRecord[];
  setDetailPanel: (next: DetailPanelState | null) => void;
}): void {
  const matchingRecord = transcriptRecords.find(
    (record) => record.run_id === match.run_id && record.seq === match.seq,
  );
  if (matchingRecord !== undefined) {
    setDetailPanel(buildDetailFromTranscriptRecord(matchingRecord));
    return;
  }
  setDetailPanel(buildDetailFromSearchMatch(match));
}

export function inspectDerivedArtifactDetail({
  derivedArtifactId,
  sessionDerivedArtifacts,
  sessionAttachments,
  setDetailPanel,
  setError,
}: {
  derivedArtifactId: string;
  sessionDerivedArtifacts: readonly MediaDerivedArtifactRecord[];
  sessionAttachments: readonly ChatAttachmentRecord[];
  setDetailPanel: (next: DetailPanelState | null) => void;
  setError: (next: string | null) => void;
}): void {
  const derivedArtifact = sessionDerivedArtifacts.find(
    (record) => record.derived_artifact_id === derivedArtifactId,
  );
  if (derivedArtifact === undefined) {
    setError("Derived artifact is no longer available.");
    return;
  }
  const attachment = sessionAttachments.find(
    (record) => record.artifact_id === derivedArtifact.source_artifact_id,
  );
  setDetailPanel(buildDetailFromDerivedArtifact(derivedArtifact, attachment));
}

export async function runDerivedArtifactLifecycleAction({
  api,
  derivedArtifactId,
  action,
  refreshSessionTranscript,
  setError,
  setNotice,
  setSessionMaintenanceBusyKey,
}: {
  api: ConsoleApiClient;
  derivedArtifactId: string;
  action: "recompute" | "quarantine" | "release" | "purge";
  refreshSessionTranscript: () => Promise<void>;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  setSessionMaintenanceBusyKey?: (next: string | null) => void;
}): Promise<void> {
  setBusyKey(setSessionMaintenanceBusyKey, `derived:${action}:${derivedArtifactId}`);
  setError(null);
  setNotice(null);
  try {
    switch (action) {
      case "recompute":
        await api.recomputeDerivedArtifact(derivedArtifactId);
        break;
      case "quarantine":
        await api.quarantineDerivedArtifact(derivedArtifactId, {
          reason: "Quarantined from chat session surface.",
        });
        break;
      case "release":
        await api.releaseDerivedArtifact(derivedArtifactId);
        break;
      case "purge":
        await api.purgeDerivedArtifact(derivedArtifactId);
        break;
    }
    await refreshSessionTranscript();
    setNotice(`Derived artifact action applied: ${action}.`);
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setBusyKey(setSessionMaintenanceBusyKey, null);
  }
}

export async function inspectBackgroundTaskDetail({
  api,
  taskId,
  setDetailPanel,
  setError,
  setSessionMaintenanceBusyKey,
}: {
  api: ConsoleApiClient;
  taskId: string;
  setDetailPanel: (next: DetailPanelState | null) => void;
  setError: (next: string | null) => void;
  setSessionMaintenanceBusyKey?: (next: string | null) => void;
}): Promise<void> {
  setBusyKey(setSessionMaintenanceBusyKey, `inspect-background-task:${taskId}`);
  setError(null);
  try {
    await inspectBackgroundTaskAction({
      api,
      taskId,
      setDetailPanel,
    });
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setBusyKey(setSessionMaintenanceBusyKey, null);
  }
}

export async function runBackgroundTaskLifecycleAction({
  api,
  taskId,
  action,
  refreshSessionTranscript,
  setError,
  setNotice,
  setSessionMaintenanceBusyKey,
}: {
  api: ConsoleApiClient;
  taskId: string;
  action: "pause" | "resume" | "retry" | "cancel";
  refreshSessionTranscript: () => Promise<void>;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  setSessionMaintenanceBusyKey?: (next: string | null) => void;
}): Promise<void> {
  setBusyKey(setSessionMaintenanceBusyKey, `background-${action}:${taskId}`);
  setError(null);
  setNotice(null);
  try {
    await runBackgroundTaskActionRequest({
      api,
      taskId,
      action,
      refreshSessionTranscript,
      setNotice,
    });
  } catch (error) {
    setError(toErrorMessage(error));
  } finally {
    setBusyKey(setSessionMaintenanceBusyKey, null);
  }
}

export async function attachChatFiles({
  api,
  sessionId,
  files,
  setAttachments,
  setAttachmentBusy,
  setError,
  setNotice,
  clearAttachmentInput,
}: {
  api: ConsoleApiClient;
  sessionId: string;
  files: readonly File[];
  setAttachments: Dispatch<SetStateAction<ComposerAttachment[]>>;
  setAttachmentBusy: (next: boolean) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  clearAttachmentInput: () => void;
}): Promise<void> {
  await handleAttachmentFilesAction({
    api,
    sessionId,
    files,
    setAttachments,
    setAttachmentBusy,
    setError,
    setNotice,
    clearAttachmentInput,
  });
}

export function useChatAttachmentUploadHandler({
  api,
  sessionId,
  attachmentInputRef,
  setAttachments,
  setAttachmentBusy,
  setError,
  setNotice,
}: {
  api: ConsoleApiClient;
  sessionId: string;
  attachmentInputRef: RefObject<HTMLInputElement | null>;
  setAttachments: Dispatch<SetStateAction<ComposerAttachment[]>>;
  setAttachmentBusy: (next: boolean) => void;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
}): (files: readonly File[]) => void {
  return useCallback(
    (files: readonly File[]) => {
      void attachChatFiles({
        api,
        sessionId,
        files,
        setAttachments,
        setAttachmentBusy,
        setError,
        setNotice,
        clearAttachmentInput: () => {
          if (attachmentInputRef.current !== null) {
            attachmentInputRef.current.value = "";
          }
        },
      });
    },
    [api, attachmentInputRef, sessionId, setAttachments, setAttachmentBusy, setError, setNotice],
  );
}
