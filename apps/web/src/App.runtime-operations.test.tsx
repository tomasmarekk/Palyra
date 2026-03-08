import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import {
  auditEventsFixture,
  capabilityCatalogFixture,
  deploymentPostureFixture,
  diagnosticsFixture,
  supportBundleJobsFixture,
} from "./console/__fixtures__/m56ControlPlane";
import {
  browserDownloadsFixture,
  browserProfilesFixture,
  browserRelayActionFixture,
  browserRelayTokenFixture,
  channelLogsFixture,
  channelStatusFixture,
  channelsListFixture,
  cronJobsFixture,
  cronRunsFixture,
  discordApplyFixture,
  discordPreflightFixture,
  memoryHitsFixture,
  memoryStatusFixture,
  routerMintFixture,
  routerPairingsFixture,
  routerPreviewFixture,
  routerRulesFixture,
  routerWarningsFixture,
  skillsFixture,
} from "./console/__fixtures__/m56Operations";
import {
  createFetchRouter,
  jsonResponse,
  sessionResponse,
} from "./console/testUtils";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("M56 runtime and operations surfaces", () => {
  it("surfaces operations handoffs and executes cron plus channels workflows", async () => {
    const fetchMock = createFetchRouter(
      routeBaseRequests,
      (request) => {
        if (request.path === "/console/v1/cron/jobs" && request.method === "GET") {
          return jsonResponse(cronJobsFixture());
        }
        if (request.path === "/console/v1/cron/jobs/cron-1/run-now" && request.method === "POST") {
          return jsonResponse({ run_id: "cron-run-1", status: "queued", message: "queued" });
        }
        if (request.path === "/console/v1/cron/jobs/cron-1/runs" && request.method === "GET") {
          return jsonResponse(cronRunsFixture());
        }
        if (request.path === "/console/v1/channels" && request.method === "GET") {
          return jsonResponse(channelsListFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault" && request.method === "GET") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/logs" && request.method === "GET") {
          return jsonResponse(channelLogsFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/health-refresh" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/queue/pause" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/queue/resume" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/queue/drain" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/dead-letters/1/replay" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/operations/dead-letters/1/discard" && request.method === "POST") {
          return jsonResponse(channelStatusFixture());
        }
        if (request.path === "/console/v1/channels/router/rules" && request.method === "GET") {
          return jsonResponse(routerRulesFixture());
        }
        if (request.path === "/console/v1/channels/router/warnings" && request.method === "GET") {
          return jsonResponse(routerWarningsFixture());
        }
        if (request.path === "/console/v1/channels/router/pairings" && request.method === "GET") {
          return jsonResponse(routerPairingsFixture());
        }
        if (request.path === "/console/v1/channels/router/preview" && request.method === "POST") {
          return jsonResponse(routerPreviewFixture());
        }
        if (request.path === "/console/v1/channels/router/pairing-codes" && request.method === "POST") {
          return jsonResponse(routerMintFixture());
        }
        if (request.path === "/console/v1/channels/discord/onboarding/probe" && request.method === "POST") {
          return jsonResponse(discordPreflightFixture());
        }
        if (request.path === "/console/v1/channels/discord/onboarding/apply" && request.method === "POST") {
          return jsonResponse(discordApplyFixture());
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/test" && request.method === "POST") {
          return jsonResponse({
            ingest: { accepted: true, immediate_delivery: 1 },
            status: channelStatusFixture().connector,
          });
        }
        if (request.path === "/console/v1/channels/discord%3Adefault/test-send" && request.method === "POST") {
          return jsonResponse({
            dispatch: { accepted: true },
            status: channelStatusFixture().connector,
            runtime: { delivered: true },
          });
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics and Audit" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics and Audit" })).toBeInTheDocument();
    expect(screen.getByText(/doctor --json/)).toBeInTheDocument();
    expect(screen.getByText(/Policy explain stays admin-only/)).toBeInTheDocument();
    expect(await screen.findByText(/message\.routed/)).toBeInTheDocument();
    expect(await screen.findByText(/attachment\.download disabled by config/)).toBeInTheDocument();
    expect(await screen.findByText("Failure classification summary")).toBeInTheDocument();
    expect(screen.getByText("Starter triage order")).toBeInTheDocument();
    expect((await screen.findAllByText(/provider_auth_refresh/)).length).toBeGreaterThan(0);
    expect(
      (await screen.findAllByText(/discord:default: attachment\.upload\.failed: remote upload rejected/)).length
    ).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Cron" }));
    expect(await screen.findByRole("heading", { name: "Cron" })).toBeInTheDocument();
    expect(await screen.findByText("nightly")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Run now" }));
    await waitFor(() => {
      expect(screen.getByText("Run-now dispatched.")).toBeInTheDocument();
    });
    expect(await screen.findByText(/cron-run-1/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Channels and Router" }));
    expect(await screen.findByRole("heading", { name: "Channels and Router" })).toBeInTheDocument();
    expect(await screen.findByText("discord:default")).toBeInTheDocument();
    expect(screen.getByText("Broadcast messages remain denied by default.")).toBeInTheDocument();
    expect(await screen.findByText(/attachment\.upload\.failed/)).toBeInTheDocument();
    expect(await screen.findByText("Queue paused: yes")).toBeInTheDocument();
    expect(screen.getByText("Permission gap: missing permissions: send messages")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Run health refresh" }));
    await waitFor(() => {
      expect(screen.getByText("Channel health refresh completed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Pause queue" }));
    await waitFor(() => {
      expect(screen.getByText("Channel queue paused.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Replay" }));
    await waitFor(() => {
      expect(screen.getByText("Dead letter 1 replayed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Discard" }));
    await waitFor(() => {
      expect(screen.getByText("Dead letter 1 discarded.")).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText("Bot token"), { target: { value: "discord-bot-token" } });
    fireEvent.change(screen.getByLabelText("Verify channel ID"), {
      target: { value: "123456789012345678" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run preflight" }));
    await waitFor(() => {
      expect(document.body.textContent ?? "").toContain("Invite URL template:");
    });
    expect(await screen.findByText("discord.com")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Apply onboarding" }));
    expect(await screen.findByText(/"status": "applied"/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Preview route" }));
    await waitFor(() => {
      expect(screen.getByText("Route preview accepted: paired_dm.")).toBeInTheDocument();
    });

    fireEvent.click(await screen.findByRole("button", { name: "Mint pairing code" }));
    await waitFor(() => {
      expect(screen.getByText("Pairing code minted: 777888.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Send connector test" }));
    await waitFor(() => {
      expect(screen.getByText(/Channel test dispatched/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByLabelText("Confirm Discord outbound test send"));
    fireEvent.click(screen.getByRole("button", { name: "Send Discord test" }));
    await waitFor(() => {
      expect(screen.getByText("Discord test send dispatched.")).toBeInTheDocument();
    });
  }, 15_000);

  it("covers memory, skills, and browser lifecycle workflows", async () => {
    const browserState = browserProfilesFixture();
    const fetchMock = createFetchRouter(
      routeBaseRequests,
      (request) => {
        if (request.path === "/console/v1/memory/status" && request.method === "GET") {
          return jsonResponse(memoryStatusFixture());
        }
        if (request.path === "/console/v1/memory/search" && request.method === "GET") {
          return jsonResponse(memoryHitsFixture());
        }
        if (request.path === "/console/v1/memory/purge" && request.method === "POST") {
          return jsonResponse({ deleted_count: 1 });
        }
        if (request.path === "/console/v1/skills" && request.method === "GET") {
          return jsonResponse(skillsFixture());
        }
        if (request.path === "/console/v1/skills/acme.echo_http/verify" && request.method === "POST") {
          return jsonResponse({ report: { verified: true } });
        }
        if (request.path === "/console/v1/skills/acme.echo_http/audit" && request.method === "POST") {
          return jsonResponse({ report: { audited: true }, quarantined: false });
        }
        if (request.path === "/console/v1/skills/acme.echo_http/quarantine" && request.method === "POST") {
          return jsonResponse({ status: "quarantined" });
        }
        if (request.path === "/console/v1/skills/acme.echo_http/enable" && request.method === "POST") {
          return jsonResponse({ status: "active" });
        }
        if (request.path === "/console/v1/browser/profiles" && request.method === "GET") {
          return jsonResponse(browserState);
        }
        if (request.path === "/console/v1/browser/profiles/create" && request.method === "POST") {
          browserState.profiles.push({
            profile_id: "profile-2",
            name: "Secondary Browser",
            principal: "admin:web-console",
            persistence_enabled: false,
            private_profile: true,
          });
          return jsonResponse({ profile: browserState.profiles[1] });
        }
        if (request.path === "/console/v1/browser/profiles/profile-1/rename" && request.method === "POST") {
          browserState.profiles[0].name = "Renamed Browser";
          return jsonResponse({ profile: browserState.profiles[0] });
        }
        if (request.path === "/console/v1/browser/profiles/profile-1/activate" && request.method === "POST") {
          browserState.active_profile_id = "profile-1";
          return jsonResponse({ profile: browserState.profiles[0] });
        }
        if (request.path === "/console/v1/browser/profiles/profile-2/delete" && request.method === "POST") {
          browserState.profiles = browserState.profiles.filter((profile) => profile.profile_id !== "profile-2");
          return jsonResponse({ deleted: true, active_profile_id: "profile-1" });
        }
        if (request.path === "/console/v1/browser/relay/tokens" && request.method === "POST") {
          return jsonResponse(browserRelayTokenFixture());
        }
        if (request.path === "/console/v1/browser/relay/actions" && request.method === "POST") {
          return jsonResponse(browserRelayActionFixture());
        }
        if (request.path === "/console/v1/browser/downloads" && request.method === "GET") {
          return jsonResponse(browserDownloadsFixture());
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Memory" }));
    expect(await screen.findByRole("heading", { name: "Memory" })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Query"), { target: { value: "paired sender" } });
    fireEvent.click(screen.getByRole("button", { name: "Search memory" }));
    expect(await screen.findByText(/paired sender prefers concise replies/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Purge memory" }));
    await waitFor(() => {
      expect(screen.getByText("Purged 1 memory item(s).")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Skills" }));
    expect(await screen.findByRole("heading", { name: "Skills" })).toBeInTheDocument();
    expect(await screen.findByText("acme.echo_http")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Verify" }));
    await waitFor(() => {
      expect(screen.getByText("Skill action 'verify' completed.")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole("button", { name: "Audit" }));
    await waitFor(() => {
      expect(screen.getByText("Skill action 'audit' completed.")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole("button", { name: "Quarantine" }));
    await waitFor(() => {
      expect(screen.getByText("Skill action 'quarantine' completed.")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole("button", { name: "Enable" }));
    await waitFor(() => {
      expect(screen.getByText("Skill action 'enable' completed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Browser" }));
    expect(await screen.findByRole("heading", { name: "Browser" })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Profile name"), { target: { value: "Secondary Browser" } });
    fireEvent.click(screen.getByRole("button", { name: "Create profile" }));
    await waitFor(() => {
      expect(screen.getByText("Browser profile created.")).toBeInTheDocument();
    });

    fireEvent.change(screen.getByLabelText("New name"), { target: { value: "Renamed Browser" } });
    fireEvent.click(screen.getAllByRole("button", { name: "Select" })[0]);
    fireEvent.click(screen.getByRole("button", { name: "Rename profile" }));
    await waitFor(() => {
      expect(screen.getByText("Browser profile renamed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getAllByRole("button", { name: "Activate" })[0]);
    await waitFor(() => {
      expect(screen.getByText("Browser profile activated.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getAllByRole("button", { name: "Delete" })[1]);
    await waitFor(() => {
      expect(screen.getByText("Browser profile deleted.")).toBeInTheDocument();
    });

    fireEvent.change(screen.getAllByLabelText("Session ID")[0], { target: { value: "browser-session-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Mint relay token" }));
    await waitFor(() => {
      expect(
        screen.getByText("Browser relay token minted. Keep it private and short-lived.")
      ).toBeInTheDocument();
    });
    expect(document.body.textContent ?? "").toContain('"relay_token": "[redacted]"');
    fireEvent.click(screen.getByRole("button", { name: "Dispatch relay action" }));
    expect(await screen.findByText("Relay action 'capture_selection' completed.")).toBeInTheDocument();
    expect(screen.getByText(/selected_text/)).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Session ID")[1], { target: { value: "browser-session-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Load downloads" }));
    expect(await screen.findByText(/report.csv/)).toBeInTheDocument();
  }, 15_000);
});

function routeBaseRequests(request: { path: string; method: string }) {
  if (request.path === "/console/v1/auth/session" && request.method === "GET") {
    return sessionResponse();
  }
  if (request.path === "/console/v1/control-plane/capabilities" && request.method === "GET") {
    return jsonResponse(capabilityCatalogFixture());
  }
  if (request.path === "/console/v1/deployment/posture" && request.method === "GET") {
    return jsonResponse(deploymentPostureFixture());
  }
  if (request.path === "/console/v1/support-bundle/jobs" && request.method === "GET") {
    return jsonResponse(supportBundleJobsFixture());
  }
  if (request.path === "/console/v1/diagnostics" && request.method === "GET") {
    return jsonResponse(diagnosticsFixture());
  }
  if (request.path === "/console/v1/audit/events" && request.method === "GET") {
    return jsonResponse(auditEventsFixture());
  }
  return undefined;
}
