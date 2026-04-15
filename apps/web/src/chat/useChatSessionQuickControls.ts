import type { ComponentProps } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";

import type {
  AgentRecord,
  ConsoleApiClient,
  SessionCatalogQuickControlsRecord,
  SessionCatalogRecord,
} from "../consoleApi";

import type {
  ChatSessionQuickControlHeader,
  ChatSessionQuickControlPanel,
} from "./ChatSessionQuickControls";
import { toErrorMessage, type TranscriptEntry } from "./chatShared";

type UseChatSessionQuickControlsArgs = {
  readonly api: ConsoleApiClient;
  readonly selectedSession: SessionCatalogRecord | null;
  readonly visibleTranscript: TranscriptEntry[];
  readonly hiddenTranscriptItems: number;
  readonly setError: (next: string | null) => void;
  readonly setNotice: (next: string | null) => void;
  readonly upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
};

type UseChatSessionQuickControlsResult = {
  readonly filteredTranscript: TranscriptEntry[];
  readonly filteredHiddenTranscriptItems: number;
  readonly sessionQuickControlHeaderProps: ComponentProps<typeof ChatSessionQuickControlHeader>;
  readonly sessionQuickControlPanelProps: ComponentProps<typeof ChatSessionQuickControlPanel>;
};

export function useChatSessionQuickControls({
  api,
  selectedSession,
  visibleTranscript,
  hiddenTranscriptItems,
  setError,
  setNotice,
  upsertSession,
}: UseChatSessionQuickControlsArgs): UseChatSessionQuickControlsResult {
  const [availableAgents, setAvailableAgents] = useState<AgentRecord[]>([]);
  const [sessionQuickControlsBusy, setSessionQuickControlsBusy] = useState(false);
  const [modelDraft, setModelDraft] = useState("");

  useEffect(() => {
    let cancelled = false;

    const loadAgents = async (): Promise<void> => {
      try {
        const response = await api.listAgents();
        if (!cancelled) {
          setAvailableAgents(response.agents);
        }
      } catch (error) {
        if (!cancelled) {
          setError(`Failed to load agent catalog: ${toErrorMessage(error)}`);
        }
      }
    };

    void loadAgents();
    return () => {
      cancelled = true;
    };
  }, [api, setError]);

  useEffect(() => {
    if (selectedSession === null) {
      setModelDraft("");
      return;
    }
    if (selectedSession.quick_controls.model.source === "session_override") {
      setModelDraft(selectedSession.quick_controls.model.value ?? "");
      return;
    }
    setModelDraft("");
  }, [
    selectedSession?.session_id,
    selectedSession?.quick_controls.model.source,
    selectedSession?.quick_controls.model.value,
  ]);

  const filteredTranscript = useMemo(() => {
    const quickControls = selectedSession?.quick_controls;
    if (quickControls === undefined) {
      return visibleTranscript;
    }
    return visibleTranscript.filter((entry) => shouldRenderTranscriptEntry(entry, quickControls));
  }, [
    selectedSession?.session_id,
    selectedSession?.quick_controls.thinking.value,
    selectedSession?.quick_controls.trace.value,
    selectedSession?.quick_controls.verbose.value,
    visibleTranscript,
  ]);

  const filteredHiddenTranscriptItems = useMemo(
    () => hiddenTranscriptItems + Math.max(0, visibleTranscript.length - filteredTranscript.length),
    [filteredTranscript.length, hiddenTranscriptItems, visibleTranscript.length],
  );

  const applySessionQuickControls = useCallback(
    async (
      payload: {
        agent_id?: string | null;
        model_profile?: string | null;
        thinking?: boolean | null;
        trace?: boolean | null;
        verbose?: boolean | null;
        reset_to_default?: boolean;
      },
      successMessage: string,
    ): Promise<boolean> => {
      const sessionId = selectedSession?.session_id?.trim() ?? "";
      if (sessionId.length === 0) {
        setError("Select an active session before changing quick controls.");
        return false;
      }
      setSessionQuickControlsBusy(true);
      setError(null);
      try {
        const response = await api.updateSessionQuickControls(sessionId, payload);
        upsertSession(response.session, { select: true });
        setNotice(successMessage);
        return true;
      } catch (error) {
        setError(toErrorMessage(error));
        return false;
      } finally {
        setSessionQuickControlsBusy(false);
      }
    },
    [api, selectedSession, setError, setNotice, upsertSession],
  );

  const selectSessionAgent = useCallback(
    (agentId: string | null) => {
      void applySessionQuickControls(
        { agent_id: agentId },
        agentId === null ? "Session agent binding cleared." : `Session agent bound to ${agentId}.`,
      );
    },
    [applySessionQuickControls],
  );

  const applySessionModel = useCallback(() => {
    const trimmed = modelDraft.trim();
    if (trimmed.length === 0) {
      setError("Enter a model profile override or clear the session override.");
      return;
    }
    void applySessionQuickControls(
      { model_profile: trimmed },
      `Session model override set to ${trimmed}.`,
    );
  }, [applySessionQuickControls, modelDraft, setError]);

  const clearSessionModel = useCallback(() => {
    setModelDraft("");
    void applySessionQuickControls({ model_profile: null }, "Session model override cleared.");
  }, [applySessionQuickControls]);

  const setSessionThinking = useCallback(
    (next: boolean) => {
      void applySessionQuickControls(
        { thinking: next },
        `Thinking and status ${next ? "enabled" : "hidden"} for this session.`,
      );
    },
    [applySessionQuickControls],
  );

  const setSessionTrace = useCallback(
    (next: boolean) => {
      void applySessionQuickControls(
        { trace: next },
        `Trace and tool cards ${next ? "enabled" : "hidden"} for this session.`,
      );
    },
    [applySessionQuickControls],
  );

  const setSessionVerbose = useCallback(
    (next: boolean) => {
      void applySessionQuickControls(
        { verbose: next },
        `Verbose timeline ${next ? "enabled" : "hidden"} for this session.`,
      );
    },
    [applySessionQuickControls],
  );

  const resetSessionQuickControls = useCallback(() => {
    setModelDraft("");
    void applySessionQuickControls(
      { reset_to_default: true },
      "Session quick controls reset to inherited defaults.",
    );
  }, [applySessionQuickControls]);

  const sessionQuickControlHeaderProps = useMemo(
    () => ({
      session: selectedSession,
      busy: sessionQuickControlsBusy,
      onToggleThinking: setSessionThinking,
      onToggleTrace: setSessionTrace,
      onToggleVerbose: setSessionVerbose,
      onReset: resetSessionQuickControls,
    }),
    [
      resetSessionQuickControls,
      selectedSession,
      sessionQuickControlsBusy,
      setSessionThinking,
      setSessionTrace,
      setSessionVerbose,
    ],
  );

  const sessionQuickControlPanelProps = useMemo(
    () => ({
      session: selectedSession,
      agents: availableAgents,
      busy: sessionQuickControlsBusy,
      modelDraft,
      setModelDraft,
      onSelectAgent: selectSessionAgent,
      onApplyModel: applySessionModel,
      onClearModel: clearSessionModel,
      onToggleThinking: setSessionThinking,
      onToggleTrace: setSessionTrace,
      onToggleVerbose: setSessionVerbose,
      onReset: resetSessionQuickControls,
    }),
    [
      applySessionModel,
      availableAgents,
      clearSessionModel,
      modelDraft,
      resetSessionQuickControls,
      selectSessionAgent,
      selectedSession,
      sessionQuickControlsBusy,
      setSessionThinking,
      setSessionTrace,
      setSessionVerbose,
    ],
  );

  return {
    filteredTranscript,
    filteredHiddenTranscriptItems,
    sessionQuickControlHeaderProps,
    sessionQuickControlPanelProps,
  };
}

function shouldRenderTranscriptEntry(
  entry: TranscriptEntry,
  quickControls: SessionCatalogQuickControlsRecord,
): boolean {
  if (entry.kind === "status") {
    return quickControls.thinking.value;
  }
  if (entry.kind === "tool") {
    return quickControls.trace.value;
  }
  if (!quickControls.verbose.value) {
    return !(
      entry.kind === "meta" ||
      entry.kind === "journal" ||
      entry.kind === "event" ||
      entry.kind === "complete" ||
      entry.kind === "approval_response"
    );
  }
  return true;
}
