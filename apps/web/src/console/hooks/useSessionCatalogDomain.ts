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

type ToggleFilter = "all" | "yes" | "no";

type PersistedSessionCatalogFilters = {
  query: string;
  includeArchived: boolean;
  sort: SessionCatalogSort;
  titleSource: string;
  hasPendingApprovals: ToggleFilter;
  branchState: string;
  hasContextFiles: ToggleFilter;
  agentId: string;
  modelProfile: string;
  titleState: string;
};

type UseSessionCatalogDomainArgs = {
  api: ConsoleApiClient;
  setError: (message: string | null) => void;
  setNotice: (message: string | null) => void;
};

const SESSION_CATALOG_FILTERS_STORAGE_KEY = "palyra.console.sessions.filters.v1";

function readPersistedFilters(): PersistedSessionCatalogFilters {
  if (typeof window === "undefined") {
    return defaultPersistedFilters();
  }
  try {
    const raw = window.localStorage.getItem(SESSION_CATALOG_FILTERS_STORAGE_KEY);
    if (raw === null) {
      return defaultPersistedFilters();
    }
    const parsed = JSON.parse(raw) as Partial<PersistedSessionCatalogFilters>;
    return {
      query: typeof parsed.query === "string" ? parsed.query : "",
      includeArchived: parsed.includeArchived === true,
      sort: isCatalogSort(parsed.sort) ? parsed.sort : "updated_desc",
      titleSource: typeof parsed.titleSource === "string" ? parsed.titleSource : "all",
      hasPendingApprovals: isToggleFilter(parsed.hasPendingApprovals)
        ? parsed.hasPendingApprovals
        : "all",
      branchState: typeof parsed.branchState === "string" ? parsed.branchState : "all",
      hasContextFiles: isToggleFilter(parsed.hasContextFiles) ? parsed.hasContextFiles : "all",
      agentId: typeof parsed.agentId === "string" ? parsed.agentId : "",
      modelProfile: typeof parsed.modelProfile === "string" ? parsed.modelProfile : "",
      titleState: typeof parsed.titleState === "string" ? parsed.titleState : "all",
    };
  } catch {
    return defaultPersistedFilters();
  }
}

function defaultPersistedFilters(): PersistedSessionCatalogFilters {
  return {
    query: "",
    includeArchived: false,
    sort: "updated_desc",
    titleSource: "all",
    hasPendingApprovals: "all",
    branchState: "all",
    hasContextFiles: "all",
    agentId: "",
    modelProfile: "",
    titleState: "all",
  };
}

function isCatalogSort(value: unknown): value is SessionCatalogSort {
  return (
    value === "updated_desc" ||
    value === "updated_asc" ||
    value === "created_desc" ||
    value === "created_asc" ||
    value === "title_asc"
  );
}

function isToggleFilter(value: unknown): value is ToggleFilter {
  return value === "all" || value === "yes" || value === "no";
}

function encodeToggleFilter(value: ToggleFilter): string | undefined {
  if (value === "yes") {
    return "true";
  }
  if (value === "no") {
    return "false";
  }
  return undefined;
}

export function useSessionCatalogDomain({ api, setError, setNotice }: UseSessionCatalogDomainArgs) {
  const persisted = useMemo(readPersistedFilters, []);
  const [busy, setBusy] = useState(false);
  const [entries, setEntries] = useState<SessionCatalogRecord[]>([]);
  const [summary, setSummary] = useState<SessionCatalogSummary | null>(null);
  const [query, setQuery] = useState(persisted.query);
  const [includeArchived, setIncludeArchived] = useState(persisted.includeArchived);
  const [sort, setSort] = useState<SessionCatalogSort>(persisted.sort);
  const [titleSource, setTitleSource] = useState(persisted.titleSource);
  const [hasPendingApprovals, setHasPendingApprovals] = useState<ToggleFilter>(
    persisted.hasPendingApprovals,
  );
  const [branchState, setBranchState] = useState(persisted.branchState);
  const [hasContextFiles, setHasContextFiles] = useState<ToggleFilter>(persisted.hasContextFiles);
  const [agentId, setAgentId] = useState(persisted.agentId);
  const [modelProfile, setModelProfile] = useState(persisted.modelProfile);
  const [titleState, setTitleState] = useState(persisted.titleState);
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
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(
      SESSION_CATALOG_FILTERS_STORAGE_KEY,
      JSON.stringify({
        query,
        includeArchived,
        sort,
        titleSource,
        hasPendingApprovals,
        branchState,
        hasContextFiles,
        agentId,
        modelProfile,
        titleState,
      } satisfies PersistedSessionCatalogFilters),
    );
  }, [
    agentId,
    branchState,
    hasContextFiles,
    hasPendingApprovals,
    includeArchived,
    modelProfile,
    query,
    sort,
    titleSource,
    titleState,
  ]);

  useEffect(() => {
    void refreshSessions();
  }, [
    agentId,
    branchState,
    hasContextFiles,
    hasPendingApprovals,
    includeArchived,
    modelProfile,
    query,
    sort,
    titleSource,
    titleState,
  ]);

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
      if (titleSource !== "all") {
        params.set("title_source", titleSource);
      }
      const pendingApprovalsFilter = encodeToggleFilter(hasPendingApprovals);
      if (pendingApprovalsFilter !== undefined) {
        params.set("has_pending_approvals", pendingApprovalsFilter);
      }
      if (branchState !== "all") {
        params.set("branch_state", branchState);
      }
      const contextFilesFilter = encodeToggleFilter(hasContextFiles);
      if (contextFilesFilter !== undefined) {
        params.set("has_context_files", contextFilesFilter);
      }
      if (agentId.trim().length > 0) {
        params.set("agent_id", agentId.trim());
      }
      if (modelProfile.trim().length > 0) {
        params.set("model_profile", modelProfile.trim());
      }
      if (titleState !== "all") {
        params.set("title_state", titleState);
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
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const nextLabel = renameDraft.trim();
      const response = await api.renameChatSession(selectedSession.session_id, {
        session_label: nextLabel.length > 0 ? nextLabel : undefined,
        manual_title_locked: nextLabel.length > 0,
      });
      const updatedRecord = await api.getSessionCatalogEntry(response.session.session_id);
      setEntries((previous) =>
        previous.map((entry) =>
          entry.session_id === updatedRecord.session.session_id ? updatedRecord.session : entry,
        ),
      );
      setRenameDraft(updatedRecord.session.session_label ?? "");
      setNotice(
        nextLabel.length > 0
          ? "Session title updated."
          : "Session title returned to automatic mode.",
      );
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
      const updatedRecord = await api.getSessionCatalogEntry(response.session.session_id);
      setEntries((previous) =>
        previous.map((entry) =>
          entry.session_id === updatedRecord.session.session_id ? updatedRecord.session : entry,
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
      setEntries((previous) => {
        if (!includeArchived) {
          return previous.filter((entry) => entry.session_id !== response.session.session_id);
        }
        return previous.map((entry) =>
          entry.session_id === response.session.session_id ? response.session : entry,
        );
      });
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
      setNotice(
        response.cancel_requested ? "Run cancellation requested." : "Run was already idle.",
      );
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
    titleSource,
    setTitleSource,
    hasPendingApprovals,
    setHasPendingApprovals,
    branchState,
    setBranchState,
    hasContextFiles,
    setHasContextFiles,
    agentId,
    setAgentId,
    modelProfile,
    setModelProfile,
    titleState,
    setTitleState,
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
