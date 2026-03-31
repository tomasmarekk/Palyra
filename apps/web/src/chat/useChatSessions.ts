import { useEffect, useMemo, useState } from "react";

import type { ConsoleApiClient, SessionCatalogRecord } from "../consoleApi";

import { emptyToUndefined, toErrorMessage } from "./chatShared";

type UseChatSessionsArgs = {
  api: ConsoleApiClient;
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
  createSession: () => Promise<void>;
  renameSession: () => Promise<void>;
  resetSession: () => Promise<boolean>;
  archiveSession: () => Promise<boolean>;
};

export function useChatSessions({
  api,
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

  const sortedSessions = useMemo(() => {
    return [...sessions].sort((left, right) => right.updated_at_unix_ms - left.updated_at_unix_ms);
  }, [sessions]);

  const selectedSession = useMemo(() => {
    return sortedSessions.find((session) => session.session_id === activeSessionId) ?? null;
  }, [activeSessionId, sortedSessions]);

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
      setActiveSessionId(preferred);
    }
  }, [preferredSessionId, sessions]);

  useEffect(() => {
    void refreshSessions(false);
  }, [includeArchived, searchQuery]);

  async function refreshSessions(ensureSession: boolean): Promise<void> {
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
        setActiveSessionId(created.session.session_id);
        setNewSessionLabel("");
        setNotice("New chat session created.");
        return;
      }
      setSessions(nextSessions);
      if (nextSessions.length === 0) {
        setActiveSessionId("");
        return;
      }
      setActiveSessionId((previous) => {
        if (
          previous.length > 0 &&
          nextSessions.some((session) => session.session_id === previous)
        ) {
          return previous;
        }
        const preferred = preferredSessionId?.trim() ?? "";
        if (preferred.length > 0) {
          const preferredSession = nextSessions.find((session) => session.session_id === preferred);
          if (preferredSession !== undefined) {
            return preferredSession.session_id;
          }
        }
        return nextSessions[0].session_id;
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function createSession(): Promise<void> {
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.resolveChatSession({
        session_label: emptyToUndefined(newSessionLabel),
      });
      const createdRecord = await api.getSessionCatalogEntry(response.session.session_id);
      setSessions((previous) => {
        const without = previous.filter(
          (entry) => entry.session_id !== createdRecord.session.session_id,
        );
        return [createdRecord.session, ...without];
      });
      setActiveSessionId(createdRecord.session.session_id);
      setNewSessionLabel("");
      setNotice("Chat session created.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function renameSession(): Promise<void> {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return;
    }
    if (sessionLabelDraft.trim().length === 0) {
      setError("Session label cannot be empty.");
      return;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      const response = await api.renameChatSession(activeSessionId, {
        session_label: sessionLabelDraft.trim(),
      });
      const updatedRecord = await api.getSessionCatalogEntry(response.session.session_id);
      setSessions((previous) => {
        return previous.map((entry) => {
          if (entry.session_id !== updatedRecord.session.session_id) {
            return entry;
          }
          return updatedRecord.session;
        });
      });
      setNotice("Session label updated.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setSessionsBusy(false);
    }
  }

  async function resetSession(): Promise<boolean> {
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
      setSessions((previous) => {
        return previous.map((entry) => {
          if (entry.session_id !== updatedRecord.session.session_id) {
            return entry;
          }
          return updatedRecord.session;
        });
      });
      setNotice("Session reset applied.");
      return true;
    } catch (error) {
      setError(toErrorMessage(error));
      return false;
    } finally {
      setSessionsBusy(false);
    }
  }

  async function archiveSession(): Promise<boolean> {
    if (activeSessionId.trim().length === 0) {
      setError("Select a session first.");
      return false;
    }
    setError(null);
    setNotice(null);
    setSessionsBusy(true);
    try {
      await api.archiveSession(activeSessionId);
      setSessions((previous) => previous.filter((entry) => entry.session_id !== activeSessionId));
      setActiveSessionId((previous) => (previous === activeSessionId ? "" : previous));
      setNotice("Session archived.");
      return true;
    } catch (error) {
      setError(toErrorMessage(error));
      return false;
    } finally {
      setSessionsBusy(false);
    }
  }

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
    createSession,
    renameSession,
    resetSession,
    archiveSession,
  };
}
