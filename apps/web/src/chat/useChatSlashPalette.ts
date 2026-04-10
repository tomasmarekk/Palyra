import { useCallback, useEffect, useMemo, useState } from "react";

import type {
  ChatCheckpointRecord,
  ConsoleApiClient,
  SessionCatalogRecord,
} from "../consoleApi";
import type { ChatDelegationCatalog } from "../consoleApi";
import type { JsonObject } from "../console/shared";

import {
  buildSlashSuggestions,
  toBrowserProfileSuggestionRecords,
  toBrowserSessionSuggestionRecords,
  type ChatSlashSuggestion,
} from "./chatCommandSuggestions";
import { CHAT_SLASH_COMMANDS, parseSlashCommand, toErrorMessage } from "./chatShared";

export type ChatUxMetricKey =
  | "slashCommands"
  | "paletteAccepts"
  | "keyboardAccepts"
  | "undo"
  | "interrupt"
  | "errors";

export type ChatUxMetrics = {
  readonly slashCommands: number;
  readonly paletteAccepts: number;
  readonly keyboardAccepts: number;
  readonly undo: number;
  readonly interrupt: number;
  readonly errors: number;
};

type UseChatSlashPaletteArgs = {
  api: ConsoleApiClient;
  composerText: string;
  setComposerText: (value: string) => void;
  sessions: readonly SessionCatalogRecord[];
  objectives: readonly JsonObject[];
  checkpoints: readonly ChatCheckpointRecord[];
  delegationCatalog: ChatDelegationCatalog | null;
  streaming: boolean;
  setError: (next: string | null) => void;
};

type UseChatSlashPaletteResult = {
  authProfiles: Awaited<ReturnType<ConsoleApiClient["listAuthProfiles"]>>["profiles"];
  browserProfiles: ReturnType<typeof toBrowserProfileSuggestionRecords>;
  browserSessions: ReturnType<typeof toBrowserSessionSuggestionRecords>;
  parsedSlashCommand: ReturnType<typeof parseSlashCommand>;
  showSlashPalette: boolean;
  slashCommandMatches: typeof CHAT_SLASH_COMMANDS;
  slashSuggestions: readonly ChatSlashSuggestion[];
  selectedSlashSuggestionIndex: number;
  setSelectedSlashSuggestionIndex: (value: number) => void;
  dismissSlashPalette: () => void;
  applySlashSuggestion: (replacement: string, acceptedWithKeyboard?: boolean) => void;
  updateComposerDraft: (next: string) => void;
  refreshSlashEntityCatalogs: () => Promise<void>;
  uxMetrics: ChatUxMetrics;
  recordUxMetric: (key: ChatUxMetricKey) => void;
};

export function useChatSlashPalette({
  api,
  composerText,
  setComposerText,
  sessions,
  objectives,
  checkpoints,
  delegationCatalog,
  streaming,
  setError,
}: UseChatSlashPaletteArgs): UseChatSlashPaletteResult {
  const [authProfiles, setAuthProfiles] = useState<
    Awaited<ReturnType<ConsoleApiClient["listAuthProfiles"]>>["profiles"]
  >([]);
  const [browserProfiles, setBrowserProfiles] = useState<
    ReturnType<typeof toBrowserProfileSuggestionRecords>
  >([]);
  const [browserSessions, setBrowserSessions] = useState<
    ReturnType<typeof toBrowserSessionSuggestionRecords>
  >([]);
  const [slashPaletteDismissed, setSlashPaletteDismissed] = useState(false);
  const [selectedSlashSuggestionIndex, setSelectedSlashSuggestionIndex] = useState(0);
  const [uxMetrics, setUxMetrics] = useState<ChatUxMetrics>({
    slashCommands: 0,
    paletteAccepts: 0,
    keyboardAccepts: 0,
    undo: 0,
    interrupt: 0,
    errors: 0,
  });

  const parsedSlashCommand = parseSlashCommand(composerText);
  const showSlashPalette =
    composerText.trim().startsWith("/") && !slashPaletteDismissed;
  const slashQuery = useMemo(() => {
    if (!showSlashPalette) {
      return "";
    }
    return composerText.trim().slice(1).trim().split(/\s+/, 1)[0]?.toLowerCase() ?? "";
  }, [composerText, showSlashPalette]);
  const slashCommandMatches = useMemo(
    () =>
      slashQuery.length === 0 || slashQuery === "help"
        ? CHAT_SLASH_COMMANDS
        : CHAT_SLASH_COMMANDS.filter((command) => command.name.includes(slashQuery)),
    [slashQuery],
  );
  const slashSuggestions = useMemo(
    () =>
      buildSlashSuggestions({
        surface: "web",
        input: composerText,
        commands: CHAT_SLASH_COMMANDS,
        sessions,
        objectives,
        authProfiles,
        browserProfiles,
        browserSessions,
        checkpoints,
        delegationCatalog,
        streaming,
      }).suggestions,
    [
      authProfiles,
      browserProfiles,
      browserSessions,
      checkpoints,
      composerText,
      delegationCatalog,
      objectives,
      sessions,
      streaming,
    ],
  );

  const recordUxMetric = useCallback((key: ChatUxMetricKey): void => {
    setUxMetrics((previous) => ({
      ...previous,
      [key]: previous[key] + 1,
    }));
  }, []);

  const refreshSlashEntityCatalogs = useCallback(async (): Promise<void> => {
    try {
      const [authResponse, browserProfilesResponse, browserSessionsResponse] =
        await Promise.all([
          api.listAuthProfiles(new URLSearchParams({ limit: "64" })),
          api.listBrowserProfiles(new URLSearchParams({ limit: "32" })),
          api.listBrowserSessions(new URLSearchParams({ limit: "32" })),
        ]);
      setAuthProfiles(authResponse.profiles);
      setBrowserProfiles(toBrowserProfileSuggestionRecords(browserProfilesResponse.profiles));
      setBrowserSessions(toBrowserSessionSuggestionRecords(browserSessionsResponse.sessions));
    } catch (error) {
      setError(toErrorMessage(error));
      recordUxMetric("errors");
    }
  }, [api, recordUxMetric, setError]);

  useEffect(() => {
    void refreshSlashEntityCatalogs();
  }, [refreshSlashEntityCatalogs]);

  useEffect(() => {
    if (!showSlashPalette) {
      setSelectedSlashSuggestionIndex(0);
      return;
    }
    setSelectedSlashSuggestionIndex((previous) =>
      Math.min(previous, Math.max(slashSuggestions.length - 1, 0)),
    );
  }, [showSlashPalette, slashSuggestions.length]);

  const updateComposerDraft = useCallback(
    (next: string): void => {
      setSlashPaletteDismissed(false);
      setSelectedSlashSuggestionIndex(0);
      setComposerText(next);
    },
    [setComposerText],
  );

  const dismissSlashPalette = useCallback((): void => {
    setSlashPaletteDismissed(true);
  }, []);

  const applySlashSuggestion = useCallback(
    (replacement: string, acceptedWithKeyboard = false): void => {
      recordUxMetric("paletteAccepts");
      if (acceptedWithKeyboard) {
        recordUxMetric("keyboardAccepts");
      }
      updateComposerDraft(replacement);
    },
    [recordUxMetric, updateComposerDraft],
  );

  return {
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
  };
}
