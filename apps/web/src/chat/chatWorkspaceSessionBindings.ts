import type { SessionCatalogRecord } from "../consoleApi";
import { describeBranchState, describeTitleGenerationState, shortId } from "./chatShared";

type ChatSessionsSlice = {
  sessionsBusy: boolean;
  newSessionLabel: string;
  setNewSessionLabel: (value: string) => void;
  searchQuery: string;
  setSearchQuery: (value: string) => void;
  includeArchived: boolean;
  setIncludeArchived: (value: boolean) => void;
  sessionLabelDraft: string;
  setSessionLabelDraft: (value: string) => void;
  selectedSession: SessionCatalogRecord | null;
  sortedSessions: SessionCatalogRecord[];
  activeSessionId: string;
  setActiveSessionId: (value: string) => void;
};

type BuildSessionsSidebarPropsParams = {
  sessions: ChatSessionsSlice;
  createSession: () => void;
  renameSession: () => void;
  resetSession: () => void;
  archiveSession: () => void;
};

export function describeSelectedSessionTitle(session: SessionCatalogRecord | null): string {
  return session?.title ?? (session ? shortId(session.session_id) : "Operator workspace");
}

export function buildWorkspaceHeaderSessionState(session: SessionCatalogRecord | null) {
  return {
    selectedSessionBranchState: describeBranchState(session?.branch_state ?? "missing"),
    selectedSessionContextFileCount: session?.recap.active_context_files.length ?? 0,
    selectedSessionFamilyLabel:
      session !== null && session.family.family_size > 1
        ? `Family ${session.family.sequence}/${session.family.family_size}`
        : null,
    selectedSessionTitleState: describeTitleGenerationState(
      session?.title_generation_state ?? "idle",
      session?.manual_title_locked ?? false,
    ),
  };
}

export function buildSessionsSidebarProps({
  sessions,
  createSession,
  renameSession,
  resetSession,
  archiveSession,
}: BuildSessionsSidebarPropsParams) {
  return {
    sessionsBusy: sessions.sessionsBusy,
    newSessionLabel: sessions.newSessionLabel,
    setNewSessionLabel: sessions.setNewSessionLabel,
    searchQuery: sessions.searchQuery,
    setSearchQuery: sessions.setSearchQuery,
    includeArchived: sessions.includeArchived,
    setIncludeArchived: sessions.setIncludeArchived,
    sessionLabelDraft: sessions.sessionLabelDraft,
    setSessionLabelDraft: sessions.setSessionLabelDraft,
    selectedSession: sessions.selectedSession,
    sortedSessions: sessions.sortedSessions,
    activeSessionId: sessions.activeSessionId,
    setActiveSessionId: sessions.setActiveSessionId,
    createSession,
    renameSession,
    resetSession,
    archiveSession,
  };
}
