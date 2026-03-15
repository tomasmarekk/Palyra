import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
  const runtimeWorkflowTimeoutMs = 30_000;

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

    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics" })).toBeInTheDocument();
    expect(screen.getByText(/doctor --json/)).toBeInTheDocument();
    expect(screen.getByText(/Policy explain stays admin-only/)).toBeInTheDocument();
    expect(await screen.findByText(/message\.routed/)).toBeInTheDocument();
    expect(await screen.findByText("Diagnostics snapshot")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cron" }));
    expect(await screen.findByRole("heading", { name: "Cron" })).toBeInTheDocument();
    expect((await screen.findAllByText("nightly")).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: /Run .* now/ }));
    await waitFor(() => {
      expect(screen.getByText("Run-now dispatched.")).toBeInTheDocument();
    });
    expect(await screen.findByText(/cron-run-1/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Channels and Router" }));
    expect(await screen.findByRole("heading", { name: "Channels" })).toBeInTheDocument();
    expect((await screen.findAllByText("discord:default")).length).toBeGreaterThan(0);
    expect(await screen.findByText(/attachment\.upload\.failed/)).toBeInTheDocument();
    expect(document.body).toHaveTextContent(/Queue paused\s*yes/);
    expect(document.body).toHaveTextContent(/Discord permission gap\s*missing permissions: send messages/);

    fireEvent.click(screen.getByRole("button", { name: "Refresh health" }));
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

    fireEvent.click(screen.getByRole("button", { name: "Send connector test" }));
    await waitFor(() => {
      expect(screen.getByText(/Channel test dispatched/)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("tab", { name: "Router" }));
    expect(await screen.findByText("Broadcast messages remain denied by default.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Preview route" }));
    await waitFor(() => {
      expect(screen.getByText("Route preview accepted: paired_dm.")).toBeInTheDocument();
    });

    fireEvent.click(await screen.findByRole("button", { name: "Mint pairing code" }));
    await waitFor(() => {
      expect(screen.getByText("Pairing code minted: 777888.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("tab", { name: "Discord setup" }));
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

    fireEvent.click(screen.getByLabelText("Confirm Discord outbound test send"));
    fireEvent.click(screen.getByRole("button", { name: "Send Discord test" }));
    await waitFor(() => {
      expect(screen.getByText("Discord test send dispatched.")).toBeInTheDocument();
    });
  }, runtimeWorkflowTimeoutMs);

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
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(await screen.findByText(/paired sender prefers concise replies/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Purge memory" }));
    const purgeDialog = await screen.findByRole("alertdialog", { name: "Purge memory" });
    fireEvent.click(within(purgeDialog).getByRole("button", { name: "Purge memory" }));
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
    fireEvent.click(
      within(await screen.findByRole("alertdialog", { name: "Quarantine skill" })).getByRole(
        "button",
        { name: "Quarantine skill" }
      )
    );
    await waitFor(() => {
      expect(screen.getByText("Skill action 'quarantine' completed.")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByRole("button", { name: "Enable" }));
    fireEvent.click(
      within(await screen.findByRole("alertdialog", { name: "Enable skill" })).getByRole(
        "button",
        { name: "Enable skill" }
      )
    );
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
    fireEvent.click(screen.getAllByRole("button", { name: /^Select / })[0]);
    fireEvent.click(screen.getByRole("button", { name: "Rename profile" }));
    await waitFor(() => {
      expect(screen.getByText("Browser profile renamed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getAllByRole("button", { name: /^Activate / })[0]);
    await waitFor(() => {
      expect(screen.getByText("Browser profile activated.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getAllByRole("button", { name: /^Delete / })[1]);
    await waitFor(() => {
      expect(screen.getByText("Browser profile deleted.")).toBeInTheDocument();
    });

    fireEvent.change(screen.getAllByLabelText("Session ID")[1], { target: { value: "browser-session-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Mint relay token" }));
    await waitFor(() => {
      expect(document.body).toHaveTextContent("Browser relay token minted. Keep it private and short-lived.");
    });
    expect(document.body.textContent ?? "").toContain('"relay_token": "[redacted]"');
    fireEvent.click(screen.getByRole("button", { name: "Dispatch relay action" }));
    expect(await screen.findByText("Relay action 'capture_selection' completed.")).toBeInTheDocument();
    expect(screen.getByText(/selected_text/)).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Session ID")[0], { target: { value: "browser-session-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Load downloads" }));
    expect((await screen.findAllByText(/report.csv/)).length).toBeGreaterThan(0);
  }, runtimeWorkflowTimeoutMs);

  it("lists agents, creates a new agent from the wizard, and updates the default agent", async () => {
    const agentState = {
      default_agent_id: "main",
      agents: [
        {
          agent_id: "main",
          display_name: "Main Agent",
          agent_dir: "state/agents/main",
          workspace_roots: ["workspace"],
          default_model_profile: "gpt-4o-mini",
          default_tool_allowlist: ["palyra.echo"],
          default_skill_allowlist: ["acme.echo"],
          created_at_unix_ms: 1_730_000_000_000,
          updated_at_unix_ms: 1_730_000_100_000
        },
        {
          agent_id: "review",
          display_name: "Review Agent",
          agent_dir: "state/agents/review",
          workspace_roots: ["workspace-review"],
          default_model_profile: "gpt-4o-mini",
          default_tool_allowlist: ["palyra.http.fetch"],
          default_skill_allowlist: ["acme.review"],
          created_at_unix_ms: 1_730_000_200_000,
          updated_at_unix_ms: 1_730_000_300_000
        }
      ]
    };

    const fetchMock = createFetchRouter(routeBaseRequests, (request) => {
      if (request.path === "/console/v1/agents" && request.method === "GET") {
        return jsonResponse(agentListFixture(agentState));
      }
      if (request.path === "/console/v1/agents/main" && request.method === "GET") {
        return jsonResponse(agentEnvelopeFixture(agentState, "main"));
      }
      if (request.path === "/console/v1/agents/review" && request.method === "GET") {
        return jsonResponse(agentEnvelopeFixture(agentState, "review"));
      }
      if (request.path === "/console/v1/agents/review-agent" && request.method === "GET") {
        return jsonResponse(agentEnvelopeFixture(agentState, "review-agent"));
      }
      if (request.path === "/console/v1/agents" && request.method === "POST") {
        agentState.agents.push({
          agent_id: "review-agent",
          display_name: "Review Agent Wizard",
          agent_dir: "state/agents/review-agent",
          workspace_roots: ["workspace", "workspace-review"],
          default_model_profile: "gpt-4.1-mini",
          default_tool_allowlist: ["palyra.echo"],
          default_skill_allowlist: ["acme.review"],
          created_at_unix_ms: 1_730_000_400_000,
          updated_at_unix_ms: 1_730_000_400_000
        });
        agentState.default_agent_id = "review-agent";
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          agent: agentState.agents[2],
          default_changed: true,
          default_agent_id: "review-agent"
        });
      }
      if (request.path === "/console/v1/agents/main/set-default" && request.method === "POST") {
        agentState.default_agent_id = "main";
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          default_agent_id: "main",
          previous_default_agent_id: "review-agent"
        });
      }
      return undefined;
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Agents" }));
    expect(await screen.findByRole("heading", { name: "Agents" })).toBeInTheDocument();
    expect((await screen.findAllByText("Main Agent")).length).toBeGreaterThan(0);
    expect((await screen.findAllByText("Review Agent")).length).toBeGreaterThan(0);
    expect(await screen.findByText("state/agents/main")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Create agent" }));
    fireEvent.change(screen.getByLabelText("Agent ID"), { target: { value: "review-agent" } });
    fireEvent.change(screen.getByLabelText("Display name"), { target: { value: "Review Agent Wizard" } });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Workspace roots"), {
      target: { value: "workspace\nworkspace-review" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.change(screen.getByLabelText("Default model profile"), {
      target: { value: "gpt-4.1-mini" }
    });
    fireEvent.change(screen.getByLabelText("Tool allowlist"), { target: { value: "palyra.echo" } });
    fireEvent.change(screen.getByLabelText("Skill allowlist"), { target: { value: "acme.review" } });
    fireEvent.click(screen.getByLabelText("Set as default agent"));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.click(screen.getByRole("button", { name: "Create agent" }));

    await waitFor(() => {
      expect(screen.getByText("Agent 'Review Agent Wizard' created.")).toBeInTheDocument();
    });
    expect((await screen.findAllByText("review-agent")).length).toBeGreaterThan(0);
    expect(await screen.findByText("state/agents/review-agent")).toBeInTheDocument();

    fireEvent.click((await screen.findAllByRole("button", { name: /^Inspect / }))[0]);
    fireEvent.click(await screen.findByRole("button", { name: "Set Main Agent as default" }));

    await waitFor(() => {
      expect(screen.getByText("Default agent set to 'main'.")).toBeInTheDocument();
    });
    expect(await screen.findByText("Default main")).toBeInTheDocument();
  }, runtimeWorkflowTimeoutMs);

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

function agentListFixture(state: {
  default_agent_id: string;
  agents: Array<{
    agent_id: string;
    display_name: string;
    agent_dir: string;
    workspace_roots: string[];
    default_model_profile: string;
    default_tool_allowlist: string[];
    default_skill_allowlist: string[];
    created_at_unix_ms: number;
    updated_at_unix_ms: number;
  }>;
}) {
  return {
    contract: { contract_version: "control-plane.v1" },
    agents: state.agents,
    default_agent_id: state.default_agent_id,
    page: {
      limit: 50,
      returned: state.agents.length,
      has_more: false,
      next_cursor: null
    }
  };
}

function agentEnvelopeFixture(
  state: {
    default_agent_id: string;
    agents: Array<{
      agent_id: string;
      display_name: string;
      agent_dir: string;
      workspace_roots: string[];
      default_model_profile: string;
      default_tool_allowlist: string[];
      default_skill_allowlist: string[];
      created_at_unix_ms: number;
      updated_at_unix_ms: number;
    }>;
  },
  agentId: string
) {
  const agent = state.agents.find((entry) => entry.agent_id === agentId);
  if (agent === undefined) {
    throw new Error(`Missing agent fixture: ${agentId}`);
  }
  return {
    contract: { contract_version: "control-plane.v1" },
    agent,
    is_default: state.default_agent_id === agentId
  };
}
