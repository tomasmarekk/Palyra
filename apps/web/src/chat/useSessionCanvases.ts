import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  ConsoleApiClient,
  SessionCanvasDetailEnvelope,
  SessionCanvasRestoreEnvelope,
  SessionCanvasSummary,
} from "../consoleApi";
import { toErrorMessage } from "./chatShared";
import {
  buildCanvasRuntimeFrameUrl,
  readSessionCanvasPreference,
  rememberSessionCanvasPreference,
  resolvePreferredSessionCanvasId,
  togglePinnedSessionCanvasPreference,
} from "./sessionCanvasState";

type UseSessionCanvasesArgs = {
  readonly api: ConsoleApiClient;
  readonly activeSessionId: string;
  readonly preferredCanvasId?: string | null;
  readonly setError: (next: string | null) => void;
  readonly onCanvasRestored?: (response: SessionCanvasRestoreEnvelope) => Promise<void> | void;
};

type UseSessionCanvasesResult = {
  readonly canvases: SessionCanvasSummary[];
  readonly canvasesBusy: boolean;
  readonly canvasDetailBusy: boolean;
  readonly selectedCanvasId: string | null;
  readonly selectedCanvas: SessionCanvasDetailEnvelope | null;
  readonly pinnedCanvasId: string | null;
  readonly restoringStateVersion: number | null;
  readonly runtimeFrameUrl: string | null;
  readonly refreshSessionCanvases: (sessionIdOverride?: string) => Promise<void>;
  readonly selectCanvas: (canvasId: string) => void;
  readonly togglePinnedCanvas: () => void;
  readonly togglePinnedCanvasById: (canvasId: string) => void;
  readonly restoreCanvasState: (stateVersion: number) => Promise<void>;
};

export function useSessionCanvases({
  api,
  activeSessionId,
  preferredCanvasId,
  setError,
  onCanvasRestored,
}: UseSessionCanvasesArgs): UseSessionCanvasesResult {
  const listRequestSeqRef = useRef(0);
  const detailRequestSeqRef = useRef(0);
  const selectedCanvasIdRef = useRef<string | null>(null);
  const [canvases, setCanvases] = useState<SessionCanvasSummary[]>([]);
  const [canvasesBusy, setCanvasesBusy] = useState(false);
  const [canvasDetailBusy, setCanvasDetailBusy] = useState(false);
  const [selectedCanvasId, setSelectedCanvasId] = useState<string | null>(null);
  const [selectedCanvas, setSelectedCanvas] = useState<SessionCanvasDetailEnvelope | null>(null);
  const [pinnedCanvasId, setPinnedCanvasId] = useState<string | null>(null);
  const [restoringStateVersion, setRestoringStateVersion] = useState<number | null>(null);

  useEffect(() => {
    selectedCanvasIdRef.current = selectedCanvasId;
  }, [selectedCanvasId]);

  const refreshSessionCanvases = useCallback(
    async (sessionIdOverride?: string) => {
      const sessionId = (sessionIdOverride ?? activeSessionId).trim();
      listRequestSeqRef.current += 1;
      const requestSeq = listRequestSeqRef.current;

      if (sessionId.length === 0) {
        setCanvases([]);
        setSelectedCanvasId(null);
        setSelectedCanvas(null);
        setPinnedCanvasId(null);
        return;
      }

      setCanvasesBusy(true);
      try {
        const response = await api.listSessionCanvases(sessionId);
        if (requestSeq !== listRequestSeqRef.current) {
          return;
        }
        const sortedCanvases = [...response.canvases].sort(compareSessionCanvases);
        const preference = readSessionCanvasPreference(sessionId);
        setCanvases(sortedCanvases);
        setPinnedCanvasId(preference.pinnedCanvasId ?? null);
        setSelectedCanvasId(
          resolvePreferredSessionCanvasId(sortedCanvases, preference, [
            preferredCanvasId,
            selectedCanvasIdRef.current,
          ]),
        );
      } catch (error) {
        if (requestSeq === listRequestSeqRef.current) {
          setError(toErrorMessage(error));
        }
      } finally {
        if (requestSeq === listRequestSeqRef.current) {
          setCanvasesBusy(false);
        }
      }
    },
    [activeSessionId, api, preferredCanvasId, setError],
  );

  useEffect(() => {
    void refreshSessionCanvases();
  }, [refreshSessionCanvases]);

  useEffect(() => {
    const normalizedPreferredCanvasId = preferredCanvasId?.trim();
    if (
      normalizedPreferredCanvasId === undefined ||
      normalizedPreferredCanvasId.length === 0 ||
      !canvases.some((canvas) => canvas.canvas_id === normalizedPreferredCanvasId) ||
      normalizedPreferredCanvasId === selectedCanvasIdRef.current
    ) {
      return;
    }
    setSelectedCanvasId(normalizedPreferredCanvasId);
  }, [canvases, preferredCanvasId]);

  useEffect(() => {
    const sessionId = activeSessionId.trim();
    const canvasId = selectedCanvasId?.trim() ?? "";
    detailRequestSeqRef.current += 1;
    const requestSeq = detailRequestSeqRef.current;

    if (sessionId.length === 0 || canvasId.length === 0) {
      setSelectedCanvas(null);
      setCanvasDetailBusy(false);
      return;
    }

    setCanvasDetailBusy(true);
    void api
      .getSessionCanvas(sessionId, canvasId)
      .then((response) => {
        if (requestSeq !== detailRequestSeqRef.current) {
          return;
        }
        rememberSessionCanvasPreference(sessionId, canvasId);
        setSelectedCanvas(response);
        setPinnedCanvasId(readSessionCanvasPreference(sessionId).pinnedCanvasId ?? null);
      })
      .catch((error) => {
        if (requestSeq === detailRequestSeqRef.current) {
          setError(toErrorMessage(error));
        }
      })
      .finally(() => {
        if (requestSeq === detailRequestSeqRef.current) {
          setCanvasDetailBusy(false);
        }
      });
  }, [activeSessionId, api, selectedCanvasId, setError]);

  const selectCanvas = useCallback(
    (canvasId: string) => {
      const sessionId = activeSessionId.trim();
      const normalizedCanvasId = canvasId.trim();
      if (sessionId.length === 0 || normalizedCanvasId.length === 0) {
        return;
      }
      rememberSessionCanvasPreference(sessionId, normalizedCanvasId);
      setPinnedCanvasId(readSessionCanvasPreference(sessionId).pinnedCanvasId ?? null);
      setSelectedCanvasId(normalizedCanvasId);
    },
    [activeSessionId],
  );

  const togglePinnedCanvasById = useCallback(
    (canvasId: string) => {
      const sessionId = activeSessionId.trim();
      const normalizedCanvasId = canvasId.trim();
      if (sessionId.length === 0 || normalizedCanvasId.length === 0) {
        return;
      }
      const next = togglePinnedSessionCanvasPreference(sessionId, normalizedCanvasId);
      setPinnedCanvasId(next.pinnedCanvasId ?? null);
      setSelectedCanvasId(normalizedCanvasId);
    },
    [activeSessionId],
  );

  const togglePinnedCanvas = useCallback(() => {
    const sessionId = activeSessionId.trim();
    const canvasId = selectedCanvasIdRef.current?.trim() ?? "";
    if (sessionId.length === 0 || canvasId.length === 0) {
      return;
    }
    togglePinnedCanvasById(canvasId);
  }, [activeSessionId, togglePinnedCanvasById]);

  const restoreCanvasState = useCallback(
    async (stateVersion: number) => {
      const sessionId = activeSessionId.trim();
      const canvasId = selectedCanvasIdRef.current?.trim() ?? "";
      if (sessionId.length === 0 || canvasId.length === 0) {
        return;
      }
      setRestoringStateVersion(stateVersion);
      try {
        const response = await api.restoreSessionCanvas(sessionId, canvasId, {
          state_version: stateVersion,
        });
        rememberSessionCanvasPreference(sessionId, canvasId);
        setPinnedCanvasId(readSessionCanvasPreference(sessionId).pinnedCanvasId ?? null);
        setSelectedCanvas(response);
        setCanvases((previous) =>
          [
            ...previous.filter((canvas) => canvas.canvas_id !== response.canvas.canvas_id),
            response.canvas,
          ].sort(compareSessionCanvases),
        );
        await onCanvasRestored?.(response);
      } catch (error) {
        setError(toErrorMessage(error));
      } finally {
        setRestoringStateVersion((current) => (current === stateVersion ? null : current));
      }
    },
    [activeSessionId, api, onCanvasRestored, setError],
  );

  const runtimeFrameUrl = useMemo(
    () => buildCanvasRuntimeFrameUrl(selectedCanvas?.runtime ?? null),
    [selectedCanvas],
  );

  return {
    canvases,
    canvasesBusy,
    canvasDetailBusy,
    selectedCanvasId,
    selectedCanvas,
    pinnedCanvasId,
    restoringStateVersion,
    runtimeFrameUrl,
    refreshSessionCanvases,
    selectCanvas,
    togglePinnedCanvas,
    togglePinnedCanvasById,
    restoreCanvasState,
  };
}

function compareSessionCanvases(left: SessionCanvasSummary, right: SessionCanvasSummary): number {
  if (left.updated_at_unix_ms !== right.updated_at_unix_ms) {
    return right.updated_at_unix_ms - left.updated_at_unix_ms;
  }
  if (left.created_at_unix_ms !== right.created_at_unix_ms) {
    return right.created_at_unix_ms - left.created_at_unix_ms;
  }
  return left.canvas_id.localeCompare(right.canvas_id);
}
