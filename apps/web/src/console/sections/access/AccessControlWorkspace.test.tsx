import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import type { ConsoleApiClient } from "../../../consoleApi";
import { AccessControlWorkspace } from "./AccessControlWorkspace";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("AccessControlWorkspace", () => {
  it("normalizes sparse access snapshots before rendering workspace cards", async () => {
    const api: Pick<
      ConsoleApiClient,
      | "getAccessSnapshot"
      | "runAccessBackfill"
      | "setAccessFeatureFlag"
      | "createAccessApiToken"
      | "rotateAccessApiToken"
      | "revokeAccessApiToken"
      | "createAccessWorkspace"
      | "createAccessInvitation"
      | "acceptAccessInvitation"
      | "updateAccessMembershipRole"
      | "removeAccessMembership"
      | "upsertAccessShare"
    > = {
      getAccessSnapshot: vi.fn(async () => ({
        contract: { contract_version: "control-plane.v1" },
        snapshot: {
          version: 1,
          feature_flags: [],
          api_tokens: [],
          teams: [],
          workspaces: [
            {
              workspace_id: "ws-1",
              team_id: "team-1",
              slug: "primary-workspace",
              display_name: "Primary Workspace",
              runtime_principal: "workspace:primary",
              runtime_device_id: "device-1",
              created_by_principal: "admin:web-console",
              created_at_unix_ms: 1,
              updated_at_unix_ms: 1,
            },
          ],
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
            checks: [],
          },
          rollout: {
            staged_rollout_enabled: false,
            external_api_safe_mode: true,
            team_mode_safe_mode: true,
            telemetry_events_retained: 0,
            packages: [],
            operator_notes: [],
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
    };

    render(<AccessControlWorkspace api={api} setError={vi.fn()} setNotice={vi.fn()} />);

    await waitFor(() => {
      expect(screen.getByText("Workspaces, members, and invitations")).toBeInTheDocument();
    });
    expect(screen.getAllByText("Primary Workspace").length).toBeGreaterThan(0);
    expect(screen.getByText("Migration and rollout")).toBeInTheDocument();
  });
});
