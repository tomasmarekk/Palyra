import { useEffect, useMemo, useState } from "react";

import type {
  ConsoleApiClient,
  SessionCatalogRecord,
  SessionCatalogSummary,
} from "../../consoleApi";
import { toErrorMessage } from "../shared";

type SessionCatalogSort =
  | "updated_desc"
  | "updated_asc"
  | "created_desc"
  | "created_asc"
  | "title_asc";

type UseSessionCatalogDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

export function useSessionCatalogDomain({
  api,
  setError,
  setNotice,
}: UseSessionCatalogDomainArgs) {
  const [busy, setBusy] = useState(false);
  const [entries, setEntries] = useState<SessionCatalogRecord[]>([]);
  const [summary, setSummary] = useState<SessionCatalogSummary | null>(null);
  const [query, setQuery] = useState("");
  const [includeArchived, setIncludeArchived] = useState(false);
  const [sort, setSort] = useState<SessionCatalogSort>("updated_desc");
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [renameDraft, setRenameDraft] = useState("");

  const selectedSession = useMemo(
    () => entries.find((entry) => entry.session_id === selectedSessionId) ?? null,
    [entries, selectedSessionId],
  );

  useEffect(() => {
    setRenameDraft(selectedSession?.session_label ?? "");
  }, [selectedSession]);

  useEffect(() => {
    void refreshSessions();
  }, [query, includeArchived, sort]);

  async function refreshSessions(): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", "50");
      params.set("sort", sort);
      if (query.trim().length > 0) {
        params.set("q", query.trim());
      }
      if (includeArchived) {
        params.set("include_archived", "true");
      }
      const response = await api.listSessionCatalog(params);
      setEntries(response.sessions);
      setSummary(response.summary);
      setSelectedSessionId((previous) => {
        if (
          previous.length > 0 &&
          response.sessions.some((session) => session.session_id === previous)
        ) {
          return previous;
        }
        return response.sessions[0]?.session_id ?? "";
      });
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function renameSelectedSession(): Promise<void> {
    if (selectedSession === null) {
      setError("Select a session first.");
      return;
    }
    if (renameDraft.trim().length === 0) {
      setError("Session label cannot be empty.");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.renameChatSession(selectedSession.session_id, {
        session_label: renameDraft.trim(),
      });
      setEntries((previous) =>
        previous.map((entry) =>
          entry.session_id === response.session.session_id
            ? {
                ...entry,
                session_label: response.session.session_label,
                title:
                  response.session.session_label?.trim().length &&
                  response.session.session_label !== undefined
                    ? response.session.session_label
                    : entry.title,
                title_source:
                  response.session.session_label?.trim().length &&
                  response.session.session_label !== undefined
                    ? "label"
                    : entry.title_source,
                updated_at_unix_ms: response.session.updated_at_unix_ms,
              }
            : entry,
        ),
      );
      setNotice("Session label updated.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function resetSelectedSession(): Promise<void> {
    if (selectedSession === null) {
      setError("Select a session first.");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.resetChatSession(selectedSession.session_id);
      setEntries((previous) =>
        previous.map((entry) =>
          entry.session_id === response.session.session_id
            ? {
                ...entry,
                last_run_id: response.session.last_run_id,
                updated_at_unix_ms: response.session.updated_at_unix_ms,
                preview: undefined,
                preview_state: "missing",
                last_intent: undefined,
                last_intent_state: "missing",
                last_summary: undefined,
                last_summary_state: "missing",
                pending_approvals: 0,
              }
            : entry,
        ),
      );
      setNotice("Session reset applied.");
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function archiveSelectedSession(): Promise<void> {
    if (selectedSession === null) {
      setError("Select a session first.");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.archiveSession(selectedSession.session_id);
      setEntries((previous) =>
        previous.map((entry) =>
          entry.session_id === response.session.session_id ? response.session : entry,
        ),
      );
      setNotice("Session archived.");
      await refreshSessions();
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  async function abortSelectedRun(): Promise<void> {
    const runId = selectedSession?.last_run_id?.trim() ?? "";
    if (runId.length === 0) {
      setError("No run is available for cancellation.");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const response = await api.abortSessionRun(runId);
      setNotice(response.cancel_requested ? "Run cancellation requested." : "Run was already idle.");
      await refreshSessions();
    } catch (error) {
      setError(toErrorMessage(error));
    } finally {
      setBusy(false);
    }
  }

  return {
    busy,
    entries,
    summary,
    query,
    setQuery,
    includeArchived,
    setIncludeArchived,
    sort,
    setSort,
    selectedSessionId,
    setSelectedSessionId,
    selectedSession,
    renameDraft,
    setRenameDraft,
    refreshSessions,
    renameSelectedSession,
    resetSelectedSession,
    archiveSelectedSession,
    abortSelectedRun,
  };
}
