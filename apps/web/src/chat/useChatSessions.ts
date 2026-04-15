import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { ConsoleApiClient, SessionCatalogRecord } from "../consoleApi";

import { emptyToUndefined, toErrorMessage } from "./chatShared";

type UseChatSessionsArgs = {
  api: ConsoleApiClient;
  onSessionActivated?: (sessionId: string) => void | Promise<void>;
  setError: (next: string | null) => void;
  setNotice: (next: string | null) => void;
  preferredSessionId?: string | null;
};

type UseChatSessionsResult = {
  sessionsBusy: boolean;
  sortedSessions: SessionCatalogRecord[];
  activeSessionId: string;
  setActiveSessionId: (sessionId: string) => void;
  selectedSession: SessionCatalogRecord | null;
  sessionLabelDraft: string;
  setSessionLabelDraft: (value: string) => void;
  newSessionLabel: string;
  setNewSessionLabel: (value: string) => void;
  searchQuery: string;
  setSearchQuery: (value: string) => void;
  includeArchived: boolean;
  setIncludeArchived: (value: boolean) => void;
  refreshSessions: (ensureSession: boolean) => Promise<void>;
  upsertSession: (session: SessionCatalogRecord, options?: { select?: boolean }) => void;
  createSession: () => Promise<void>;
  createSessionWithLabel: (sessionLabel?: string) => Promise<string | null>;
  renameSession: (requestedLabel?: string) => Promise<void>;
  resetSession: () => Promise<boolean>;
  archiveSession: () => Promise<boolean>;
};

export function useChatSessions({
  api,
  onSessionActivated,
  setError,
  setNotice,
  preferredSessionId,
}: UseChatSessionsArgs): UseChatSessionsResult {
  const [sessionsBusy, setSessionsBusy] = useState(false);
  const [sessions, setSessions] = useState<SessionCatalogRecord[]>([]);
  const [activeSessionId, setActiveSessionId] = useState("");
  const [sessionLabelDraft, setSessionLabelDraft] = useState("");
  const [newSessionLabel, setNewSessionLabel] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [includeArchived, setIncludeArchived] = useState(false);
  const filtersHydratedRef = useRef(false);
  const activeSessionIdRef = useRef("");

  const sortedSessions = useMemo(() => {
    return [...sessions].sort((left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms);
  }, [sessions]);

  const selectedSession = useMemo(() => {
    return sortedSessions.find((session) => session.session_id === activeSessionId) ?? null;
  }, [activeSessionId, sortedSessions]);

  useEffect(() => {
    activeSessionIdRef.current = activeSessionId;
  }, [activeSessionId]);

  const activateSession = useCallback(
    (sessionId: string): void => {
      const trimmed = sessionId.trim();
      if (trimmed.length === 0) {
        activeSessionIdRef.current = "";
        setActiveSessionId("");
        return;
      }
      if (activeSessionIdRef.current === trimmed) {
        return;
      }
      activeSessionIdRef.current = trimmed;
      setActiveSessionId(trimmed);
      void onSessionActivated?.(trimmed);
    },
    [onSessionActivated],
  );

  useEffect(() => {
    if (selectedSession === null) {
      setSessionLabelDraft("");
      return;
    }
    setSessionLabelDraft(selectedSession.session_label ?? "");
  }, [selectedSession]);

  useEffect(() => {
    const preferred = preferredSessionId?.trim() ?? "";
    if (preferred.length === 0) {
      return;
    }
    if (sessions.some((session) => session.session_id === preferred)) {
      activateSession(preferred);
    }
  }, [activateSession, preferredSessionId, sessions]);

  useEffect(() => {
    if (!filtersHydratedRef.current) {
      filtersHydratedRef.current = true;
      return;
    }
    void refreshSessions(false);
  }, [includeArchived, searchQuery]);

  const refreshSessions = useCallback(
    async (ensureSession: boolean): Promise<void> => {
      setSessionsBusy(true);
      try {
        const params = new URLSearchParams();
        params.set("limit", "50");
        params.set("sort", "updated_desc");
        if (searchQuery.trim().length > 0) {
          params.set("q", searchQuery.trim());
        }
        if (includeArchived) {
          params.set("include_archived", "true");
        }
        const response = await api.listSessionCatalog(params);
        const nextSessions = [...response.sessions].sort(
          (left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms,
        );
        if (nextSessions.length === 0 && ensureSession) {
          const created = await api.resolveChatSession({
            session_label: emptyToUndefined(newSessionLabel),
          });
          const createdRecord = await api.getSessionCatalogEntry(created.session.session_id);
          setSessions([createdRecord.session]);
          activateSession(created.session.session_id);
          setNewSessionLabel("");
          setNotice("New chat session created.");
          return;
        }
        setSessions(nextSessions);
        if (nextSessions.length === 0) {
          activateSession("");
          return;
        }
        if (
          activeSessionIdRef.current.length > 0 &&
          nextSessions.some((session) => session.session_id === activeSessionIdRef.current)
        ) {
          return;
        }
        const preferred = preferredSessionId?.trim() ?? "";
        if (preferred.length > 0) {
          const preferredSession = nextSessions.find((session) => session.session_id === preferred);
          if (preferredSession !== undefined) {
            activateSession(preferredSession.session_id);
            return;
          }
        }
        activateSession(nextSessions[0].session_id);
      } catch (error) {
        setError(toErrorMessage(error));
      } finally {
        setSessionsBusy(false);
      }
    },
    [
      activateSession,
      api,
      includeArchived,
      newSessionLabel,
      preferredSessionId,
      searchQuery,
      setError,
      setNotice,
    ],
  );

  const createSessionWithLabel = useCallback(
    async (sessionLabel?: string): Promise<string | null> => {
      setError(null);
      setNotice(null);
      setSessionsBusy(true);
      try {
        const response = await api.resolveChatSession({
          session_label: emptyToUndefined(sessionLabel ?? newSessionLabel),
        });
        const createdRecord = await api.getSessionCatalogEntry(response.session.session_id);
        setSessions((previous) => {
          const without = previous.filter(
            (entry) => entry.session_id !== createdRecord.session.session_id,
          );
          return [createdRecord.session, ...without];
        });
        activateSession(createdRecord.session.session_id);
        setNewSessionLabel("");
        setNotice("Chat session created.");
        return createdRecord.session.session_id;
      } catch (error) {
        setError(toErrorMessage(error));
        return null;
      } finally {
        setSessionsBusy(false);
      }
    },
    [api, newSessionLabel, setError, setNotice],
  );

  const createSession = useCallback(async (): Promise<void> => {
    await createSessionWithLabel(newSessionLabel);
  }, [createSessionWithLabel, newSessionLabel]);

  const upsertSession = useCallback(
    (session: SessionCatalogRecord, options?: { select?: boolean }): void => {
      setSessions((previous) => {
        const without = previous.filter((entry) => entry.session_id !== session.session_id);
        return [session, ...without];
      });
      if (options?.select) {
        activateSession(session.session_id);
      }
    },
    [activateSession],
  );

  const renameSession = useCallback(async (requestedLabel?: string): Promise<void> => {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const nextLabel = emptyToUndefined(requestedLabel ?? sessionLabelDraft);
      const response = await api.renameChatSession(activeSessionId, {
        session_label: nextLabel,
        manual_title_locked: nextLabel !== undefined,
      });
      const updatedRecord = await api.getSessionCatalogEntry(response.session.session_id);
      upsertSession(updatedRecord.session);
      setSessionLabelDraft(updatedRecord.session.session_label ?? "");
      setNotice(
        nextLabel === undefined ? "Session title returned to automatic mode." : "Session title updated.",
      );
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }, [activeSessionId, api, sessionLabelDraft, setError, setNotice, upsertSession]);

  const resetSession = useCallback(async (): Promise<boolean> => {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return false;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.resetChatSession(activeSessionId);
      const updatedRecord = await api.getSessionCatalogEntry(response.session.session_id);
      upsertSession(updatedRecord.session);
      setNotice("Session reset applied.");
      return true;
    } catch (error) {
      setError(toErrorMessage(error));
      return false;
    } finally {
      setSessionsBusy(false);
    }
  }, [activeSessionId, api, setError, setNotice, upsertSession]);

  const archiveSession = useCallback(async (): Promise<boolean> => {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return false;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.archiveSession(activeSessionId);
      if (includeArchived) {
        upsertSession(response.session, { select: activeSessionIdRef.current === activeSessionId });
      } else {
        setSessions((previous) => previous.filter((entry) => entry.session_id !== activeSessionId));
      }
      if (!includeArchived && activeSessionIdRef.current === activeSessionId) {
        activateSession("");
      }
      setNotice("Session archived.");
      return true;
    } catch (error) {
      setError(toErrorMessage(error));
      return false;
    } finally {
      setSessionsBusy(false);
    }
  }, [activateSession, activeSessionId, api, includeArchived, setError, setNotice, upsertSession]);

  return {
    sessionsBusy,
    sortedSessions,
    activeSessionId,
    setActiveSessionId,
    selectedSession,
    sessionLabelDraft,
    setSessionLabelDraft,
    newSessionLabel,
    setNewSessionLabel,
    searchQuery,
    setSearchQuery,
    includeArchived,
    setIncludeArchived,
    refreshSessions,
    upsertSession,
    createSession,
    createSessionWithLabel,
    renameSession,
    resetSession,
    archiveSession,
  };
}
