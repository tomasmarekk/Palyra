// @vitest-environment jsdom

import { cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";
import { MemoryRouter } from "react-router-dom";

import type { ConsoleApiClient, SessionCatalogRecord } from "../consoleApi";
import { ChatConsolePanel } from "./ChatConsolePanel";

afterEach(() => {
  cleanup();
});

describe("ChatConsolePanel bootstrap effects", () => {
  it("does not replay bootstrap and transcript loads on ordinary rerenders", async () => {
    const session = sampleSession();
    const contract = { contract_version: "2026-01-01" };
    const listSessionCatalog = vi.fn(async () => ({
      contract,
      sessions: [session],
      summary: {
        active_sessions: 1,
        archived_sessions: 0,
        sessions_with_pending_approvals: 0,
        sessions_with_active_runs: 0,
      },
      query: {
        limit: 50,
        cursor: 0,
        include_archived: false,
        sort: "updated_desc",
      },
      page: {
        limit: 50,
        returned: 1,
        has_more: false,
      },
    }));
    const getSessionTranscript = vi.fn(async () => ({
      contract,
      session,
      records: [],
      attachments: [],
      derived_artifacts: [],
      pins: [],
      compactions: [],
      checkpoints: [],
      queued_inputs: [],
      runs: [],
      background_tasks: [],
    }));
    const getDelegationCatalog = vi.fn(async () => ({
      contract,
      catalog: { profiles: [], templates: [] },
    }));
    const listObjectives = vi.fn(async () => ({
      contract,
      objectives: [],
      page: {
        limit: 64,
        returned: 0,
        has_more: false,
      },
    }));
    const listAuthProfiles = vi.fn(async () => ({ profiles: [], contract }));
    const listBrowserProfiles = vi.fn(async () => ({ profiles: [], contract }));
    const listBrowserSessions = vi.fn(async () => ({ sessions: [], contract }));

    const api = {
      listSessionCatalog,
      getSessionTranscript,
      getDelegationCatalog,
      listObjectives,
      listAuthProfiles,
      listBrowserProfiles,
      listBrowserSessions,
    } as unknown as ConsoleApiClient;

    render(
      <MemoryRouter initialEntries={["/#/chat"]}>
        <ChatConsolePanel
          api={api}
          revealSensitiveValues={false}
          setError={vi.fn()}
          setNotice={vi.fn()}
          setConsoleSection={vi.fn()}
          openBrowserSessionWorkbench={vi.fn()}
        />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(listSessionCatalog).toHaveBeenCalledTimes(1);
      expect(getDelegationCatalog).toHaveBeenCalledTimes(1);
      expect(listObjectives).toHaveBeenCalledTimes(1);
      expect(listAuthProfiles).toHaveBeenCalledTimes(1);
      expect(listBrowserProfiles).toHaveBeenCalledTimes(1);
      expect(listBrowserSessions).toHaveBeenCalledTimes(1);
      expect(getSessionTranscript).toHaveBeenCalledTimes(1);
    });
  });
});

function sampleSession(): SessionCatalogRecord {
  return {
    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    session_key: "session-local",
    session_label: "Local session",
    principal: "admin:local",
    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    channel: "cli",
    created_at_unix_ms: 1,
    updated_at_unix_ms: 1,
    last_run_id: undefined,
    title: "Local session",
    title_source: "session_label",
    preview: "Preview",
    preview_state: "ready",
    last_intent: undefined,
    last_intent_state: "missing",
    last_summary: undefined,
    last_summary_state: "missing",
    branch_state: "root",
    parent_session_id: undefined,
    branch_origin_run_id: undefined,
    last_run_state: undefined,
    last_run_started_at_unix_ms: undefined,
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    archived: false,
    archived_at_unix_ms: undefined,
    pending_approvals: 0,
  };
}
