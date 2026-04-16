import { useCallback, useEffect } from "react";
import type { NavigateFunction } from "react-router-dom";

import type {
  SessionCanvasDetailEnvelope,
  SessionCanvasSummary,
} from "../consoleApi";
import { buildConsoleHandoffHref } from "../console/contracts";
import { getSectionPath } from "../console/navigation";
import type { Section } from "../console/sectionMetadata";

import type { RunDrawerTab } from "./ChatRunDrawer";
import { buildChatCanvasHref, extractCanvasIdFromFrameUrl } from "./sessionCanvasState";

type UseChatCanvasSurfaceActionsArgs = {
  readonly navigate: NavigateFunction;
  readonly surface: "chat" | "canvas";
  readonly activeSessionId: string;
  readonly activeRunId: string | null;
  readonly knownRunIds: readonly string[];
  readonly sessionSearchInputRef: React.RefObject<HTMLInputElement | null>;
  readonly selectedCanvas: SessionCanvasDetailEnvelope | null;
  readonly canvases: readonly SessionCanvasSummary[];
  readonly pinnedCanvasId: string | null;
  readonly selectedCanvasId: string | null;
  readonly setActiveSessionId: (sessionId: string) => void;
  readonly selectCanvas: (canvasId: string) => void;
  readonly togglePinnedCanvasById: (canvasId: string) => void;
  readonly setConsoleSection: (section: Section) => void;
  readonly openRunDetails: (runId: string, tab?: RunDrawerTab) => void;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
};

type UseChatCanvasSurfaceActionsResult = {
  readonly openConversationSurface: (runId?: string | null) => void;
  readonly openCanvasSourceRun: () => void;
  readonly setCanvasSurfaceSessionId: (sessionId: string) => void;
  readonly selectCanvasSurfaceCanvas: (canvasId: string) => void;
  readonly openCanvasSurfaceFromUrl: (canvasUrl: string, runId?: string) => void;
  readonly toggleCanvasPinFromUrl: (canvasUrl: string) => void;
  readonly reopenLastCanvas: () => void;
};

export function useChatCanvasSurfaceActions({
  navigate,
  surface,
  activeSessionId,
  activeRunId,
  knownRunIds,
  sessionSearchInputRef,
  selectedCanvas,
  canvases,
  pinnedCanvasId,
  selectedCanvasId,
  setActiveSessionId,
  selectCanvas,
  togglePinnedCanvasById,
  setConsoleSection,
  openRunDetails,
  setError,
  setNotice,
}: UseChatCanvasSurfaceActionsArgs): UseChatCanvasSurfaceActionsResult {
  const openSelectedCanvasSurface = useCallback(
    (canvasId?: string | null, sessionIdOverride?: string) => {
      const sessionId = (sessionIdOverride ?? activeSessionId).trim();
      const normalizedCanvasId = canvasId?.trim() ?? "";
      void navigate(
        buildChatCanvasHref({
          sessionId: sessionId.length > 0 ? sessionId : undefined,
          canvasId: normalizedCanvasId.length > 0 ? normalizedCanvasId : undefined,
        }),
      );
    },
    [activeSessionId, navigate],
  );

  const openConversationSurface = useCallback(
    (runId?: string | null) => {
      const sessionId = activeSessionId.trim();
      const normalizedRunId = runId?.trim() ?? "";
      void navigate(
        buildConsoleHandoffHref({
          section: "chat",
          sessionId: sessionId.length > 0 ? sessionId : undefined,
          runId: normalizedRunId.length > 0 ? normalizedRunId : undefined,
          intent: normalizedRunId.length > 0 ? "inspect-run" : "resume-session",
        }),
      );
    },
    [activeSessionId, navigate],
  );

  const openCanvasSourceRun = useCallback(() => {
    const sourceRunId = selectedCanvas?.canvas.reference.source_run_id?.trim();
    if (!sourceRunId) {
      setError("Selected canvas is not linked to a source run.");
      return;
    }
    openConversationSurface(sourceRunId);
  }, [openConversationSurface, selectedCanvas, setError]);

  const setCanvasSurfaceSessionId = useCallback(
    (sessionId: string) => {
      setActiveSessionId(sessionId);
      openSelectedCanvasSurface(undefined, sessionId);
    },
    [openSelectedCanvasSurface, setActiveSessionId],
  );

  const selectCanvasSurfaceCanvas = useCallback(
    (canvasId: string) => {
      selectCanvas(canvasId);
      openSelectedCanvasSurface(canvasId);
    },
    [openSelectedCanvasSurface, selectCanvas],
  );

  const focusSessionSearch = useCallback(() => {
    sessionSearchInputRef.current?.focus();
    sessionSearchInputRef.current?.select();
  }, [sessionSearchInputRef]);

  const openCurrentRunInspector = useCallback(
    (tab: RunDrawerTab = "status") => {
      const targetRunId = activeRunId ?? knownRunIds[0] ?? null;
      if (targetRunId === null) {
        setError("No run is available for inspection.");
        return;
      }
      openRunDetails(targetRunId, tab);
    },
    [activeRunId, knownRunIds, openRunDetails, setError],
  );

  const openCanvasSurfaceFromUrl = useCallback(
    (canvasUrl: string, runId?: string) => {
      const canvasId = extractCanvasIdFromFrameUrl(canvasUrl);
      if (canvasId === null) {
        setError("This output does not expose a reusable canvas target.");
        return;
      }
      selectCanvas(canvasId);
      const sessionId = activeSessionId.trim();
      const normalizedRunId = runId?.trim() ?? "";
      void navigate(
        buildChatCanvasHref({
          sessionId: sessionId.length > 0 ? sessionId : undefined,
          canvasId,
          runId: normalizedRunId.length > 0 ? normalizedRunId : undefined,
        }),
      );
    },
    [activeSessionId, navigate, selectCanvas, setError],
  );

  const toggleCanvasPinFromUrl = useCallback(
    (canvasUrl: string) => {
      const canvasId = extractCanvasIdFromFrameUrl(canvasUrl);
      if (canvasId === null) {
        setError("This output does not expose a reusable canvas target.");
        return;
      }
      togglePinnedCanvasById(canvasId);
      setNotice(
        pinnedCanvasId === canvasId
          ? "Canvas unpinned."
          : "Canvas pinned for consistent reopen across session resumes.",
      );
    },
    [pinnedCanvasId, setError, setNotice, togglePinnedCanvasById],
  );

  const reopenLastCanvas = useCallback(() => {
    const targetCanvasId = pinnedCanvasId ?? selectedCanvasId ?? canvases[0]?.canvas_id ?? null;
    if (targetCanvasId === null) {
      setError("No canvas is available to reopen for this session.");
      return;
    }
    openSelectedCanvasSurface(targetCanvasId);
  }, [canvases, openSelectedCanvasSurface, pinnedCanvasId, selectedCanvasId, setError]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (
        event.defaultPrevented ||
        !event.altKey ||
        event.ctrlKey ||
        event.metaKey ||
        event.shiftKey ||
        isEditableShortcutTarget(event.target)
      ) {
        return;
      }

      switch (event.key.toLowerCase()) {
        case "s":
          event.preventDefault();
          focusSessionSearch();
          return;
        case "r":
          event.preventDefault();
          openCurrentRunInspector();
          return;
        case "w":
          event.preventDefault();
          openCurrentRunInspector("workspace");
          return;
        case "a":
          event.preventDefault();
          setConsoleSection("approvals");
          void navigate(getSectionPath("approvals"));
          return;
        case "c":
          event.preventDefault();
          if (surface === "canvas") {
            openConversationSurface();
            return;
          }
          if (pinnedCanvasId === null && canvases.length === 0) {
            setNotice("No canvas is available for the current session yet.");
            return;
          }
          reopenLastCanvas();
          return;
        default:
          return;
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [
    canvases,
    focusSessionSearch,
    navigate,
    openConversationSurface,
    openCurrentRunInspector,
    pinnedCanvasId,
    reopenLastCanvas,
    setConsoleSection,
    setNotice,
    surface,
  ]);

  return {
    openConversationSurface,
    openCanvasSourceRun,
    setCanvasSurfaceSessionId,
    selectCanvasSurfaceCanvas,
    openCanvasSurfaceFromUrl,
    toggleCanvasPinFromUrl,
    reopenLastCanvas,
  };
}

function isEditableShortcutTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) {
    return false;
  }
  return (
    target.isContentEditable ||
    target instanceof HTMLInputElement ||
    target instanceof HTMLTextAreaElement ||
    target instanceof HTMLSelectElement
  );
}
