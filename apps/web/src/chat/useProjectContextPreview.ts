import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

import type { ConsoleApiClient, ProjectContextPreviewEnvelope } from "../consoleApi";

import { toErrorMessage } from "./chatShared";

type UseProjectContextPreviewArgs = {
  api: ConsoleApiClient;
  activeSessionId: string;
  composerText: string;
  setError: (next: string | null) => void;
};

type UseProjectContextPreviewResult = {
  projectContextPreview: ProjectContextPreviewEnvelope | null;
  projectContextPreviewBusy: boolean;
  projectContextPreviewStale: boolean;
  projectContextPromptPreview: string | null;
  loadProjectContextPreview: (
    text: string,
    options?: { reportError?: boolean },
  ) => Promise<ProjectContextPreviewEnvelope | null>;
  ensureProjectContextPreviewForCurrentDraft: () => Promise<ProjectContextPreviewEnvelope | null>;
  resetProjectContextPreview: () => void;
};

const PROJECT_CONTEXT_DISCOVERY_PATTERN = /@(?:file|folder|diff|staged)\b/i;

function buildProjectContextDraftKey(text: string): string {
  const trimmed = text.trim();
  return PROJECT_CONTEXT_DISCOVERY_PATTERN.test(trimmed) ? trimmed : "";
}

export function useProjectContextPreview({
  api,
  activeSessionId,
  composerText,
  setError,
}: UseProjectContextPreviewArgs): UseProjectContextPreviewResult {
  const projectContextPreviewRequestSeqRef = useRef(0);
  const [projectContextPreviewBusy, setProjectContextPreviewBusy] = useState(false);
  const [projectContextPreview, setProjectContextPreview] = useState<ProjectContextPreviewEnvelope | null>(
    null,
  );
  const [projectContextPromptPreview, setProjectContextPromptPreview] = useState<string | null>(null);
  const [projectContextPreviewQuery, setProjectContextPreviewQuery] = useState("");
  const deferredComposerText = useDeferredValue(composerText);

  const projectContextPreviewStale = useMemo(() => {
    if (activeSessionId.trim().length === 0 || projectContextPreview === null) {
      return false;
    }
    return projectContextPreviewQuery !== buildProjectContextDraftKey(composerText);
  }, [activeSessionId, composerText, projectContextPreview, projectContextPreviewQuery]);

  const resetProjectContextPreview = useCallback(() => {
    projectContextPreviewRequestSeqRef.current += 1;
    setProjectContextPreviewBusy(false);
    setProjectContextPreview(null);
    setProjectContextPromptPreview(null);
    setProjectContextPreviewQuery("");
  }, []);

  const loadProjectContextPreview = useCallback(
    async (
      text: string,
      options: { reportError?: boolean } = {},
    ): Promise<ProjectContextPreviewEnvelope | null> => {
      const sessionId = activeSessionId.trim();
      if (sessionId.length === 0) {
        resetProjectContextPreview();
        return null;
      }
      const query = buildProjectContextDraftKey(text);
      projectContextPreviewRequestSeqRef.current += 1;
      const requestSeq = projectContextPreviewRequestSeqRef.current;
      setProjectContextPreviewBusy(true);
      try {
        const response = await api.previewChatProjectContext(
          sessionId,
          query.length > 0 ? { text: query } : {},
        );
        if (requestSeq !== projectContextPreviewRequestSeqRef.current) {
          return null;
        }
        setProjectContextPreview(response.preview);
        setProjectContextPromptPreview(response.prompt_preview ?? null);
        setProjectContextPreviewQuery(query);
        return response.preview;
      } catch (error) {
        if (
          requestSeq === projectContextPreviewRequestSeqRef.current &&
          options.reportError !== false
        ) {
          setError(toErrorMessage(error));
        }
        return null;
      } finally {
        if (requestSeq === projectContextPreviewRequestSeqRef.current) {
          setProjectContextPreviewBusy(false);
        }
      }
    },
    [activeSessionId, api, resetProjectContextPreview, setError],
  );

  const ensureProjectContextPreviewForCurrentDraft = useCallback(async () => {
    const query = buildProjectContextDraftKey(composerText);
    if (
      projectContextPreview !== null &&
      projectContextPreviewQuery === query &&
      activeSessionId.trim().length > 0
    ) {
      return projectContextPreview;
    }
    return loadProjectContextPreview(composerText, { reportError: true });
  }, [
    activeSessionId,
    composerText,
    loadProjectContextPreview,
    projectContextPreview,
    projectContextPreviewQuery,
  ]);

  useEffect(() => {
    if (activeSessionId.trim().length === 0) {
      resetProjectContextPreview();
      return;
    }
    const timeoutHandle = window.setTimeout(() => {
      void loadProjectContextPreview(deferredComposerText, { reportError: false });
    }, 250);
    return () => {
      window.clearTimeout(timeoutHandle);
    };
  }, [activeSessionId, deferredComposerText, loadProjectContextPreview, resetProjectContextPreview]);

  return {
    projectContextPreview,
    projectContextPreviewBusy,
    projectContextPreviewStale,
    projectContextPromptPreview,
    loadProjectContextPreview,
    ensureProjectContextPreviewForCurrentDraft,
    resetProjectContextPreview,
  };
}
