import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { AccessControlWorkspace } from "./AccessControlWorkspace";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("AccessControlWorkspace", () => {
  it("normalizes sparse access snapshots before rendering workspace cards", async () => {
    render(
      <AccessControlWorkspace
        api={{
          getAccessSnapshot: vi.fn(async () => ({
            contract: { contract_version: "control-plane.v1" },
            snapshot: {
              version: 1,
              feature_flags: [],
              api_tokens: [],
              teams: [],
              workspaces: [{ workspace_id: "ws-1", display_name: "Primary Workspace" }],
              memberships: [],
              invitations: [],
              shares: [],
              telemetry: [],
              migration: {
                registry_path: "state/access-registry.json",
                version: 1,
                backfill_required: false,
                blocking_issues: 0,
                warning_issues: 0,
              },
              rollout: {
                staged_rollout_enabled: false,
                external_api_safe_mode: true,
                team_mode_safe_mode: true,
                telemetry_events_retained: 0,
              },
            },
          })),
          runAccessBackfill: vi.fn(),
          setAccessFeatureFlag: vi.fn(),
          createAccessApiToken: vi.fn(),
          rotateAccessApiToken: vi.fn(),
          revokeAccessApiToken: vi.fn(),
          createAccessWorkspace: vi.fn(),
          createAccessInvitation: vi.fn(),
          acceptAccessInvitation: vi.fn(),
          updateAccessMembershipRole: vi.fn(),
          removeAccessMembership: vi.fn(),
          upsertAccessShare: vi.fn(),
        }}
        setError={vi.fn()}
        setNotice={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("Workspaces, members, and invitations")).toBeInTheDocument();
    });
    expect(screen.getAllByText("Primary Workspace").length).toBeGreaterThan(0);
    expect(screen.getByText("Migration and rollout")).toBeInTheDocument();
  });
});
