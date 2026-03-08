import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import {
  auditEventsFixture,
  capabilityCatalogFixture,
  deploymentPostureFixture,
  pairingSummaryFixture,
  supportBundleJobFixture,
  supportBundleJobsFixture,
} from "./console/__fixtures__/m56ControlPlane";
import {
  configInspectFixture,
  configMutationFixture,
  configValidationFixture,
  secretMetadataFixture,
  secretMetadataListFixture,
  secretRevealFixture,
} from "./console/__fixtures__/m56Operations";
import {
  createFetchRouter,
  jsonResponse,
  sessionResponse,
} from "./console/testUtils";
import type { MockRequest } from "./console/testUtils";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("M56 config, access, and support surfaces", () => {
  it("operates config lifecycle and explicit secret reveal with default redaction", async () => {
    const initialToml = "version = 1\n[model_provider]\nauth_profile_id = \"openai-default\"\n";
    const migratedToml = "version = 2\n[model_provider]\nauth_profile_id = \"openai-migrated\"\n";
    let currentToml = initialToml;

    const fetchMock = createFetchRouter(
      (request) => routeOverviewRequests(request),
      (request) => {
        if (request.path === "/console/v1/config/inspect" && request.method === "POST") {
          return jsonResponse(configInspectFixture(currentToml));
        }
        if (request.path === "/console/v1/config/validate" && request.method === "POST") {
          return jsonResponse(configValidationFixture(true));
        }
        if (request.path === "/console/v1/config/mutate" && request.method === "POST") {
          const body = JSON.parse(request.body) as { key: string; value?: string };
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          currentToml = `version = 1\n[model_provider]\nauth_profile_id = ${body.value ?? "\"unset\"" }\n`;
          return jsonResponse(configMutationFixture("set", body.key));
        }
        if (request.path === "/console/v1/config/migrate" && request.method === "POST") {
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          currentToml = migratedToml;
          return jsonResponse(configMutationFixture("migrate", "config_version"));
        }
        if (request.path === "/console/v1/config/recover" && request.method === "POST") {
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          currentToml = initialToml;
          return jsonResponse(configMutationFixture("recover", "config_version"));
        }
        if (request.path === "/console/v1/secrets" && request.method === "GET") {
          expect(request.url.searchParams.get("scope")).toBe("global");
          return jsonResponse(secretMetadataListFixture());
        }
        if (request.path === "/console/v1/secrets/metadata" && request.method === "GET") {
          return jsonResponse(secretMetadataFixture());
        }
        if (request.path === "/console/v1/secrets" && request.method === "POST") {
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          return jsonResponse(secretMetadataFixture());
        }
        if (request.path === "/console/v1/secrets/reveal" && request.method === "POST") {
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          return jsonResponse(secretRevealFixture());
        }
        if (request.path === "/console/v1/secrets/delete" && request.method === "POST") {
          expect(request.headers.get("x-palyra-csrf-token")).toBe("csrf-1");
          return jsonResponse(secretMetadataFixture());
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Config and Secrets" }));
    expect(await screen.findByRole("heading", { name: "Config and Secrets" })).toBeInTheDocument();
    await waitFor(() => {
      expect(document.body).toHaveTextContent(
        "Remote gateway exposure requires explicit verification and operator acknowledgement."
      );
    });

    const keyInputs = screen.getAllByLabelText("Key");
    const valueInputs = screen.getAllByLabelText("Value");
    fireEvent.change(keyInputs[0], { target: { value: "model_provider.auth_profile_id" } });
    fireEvent.change(valueInputs[0], { target: { value: "\"openai-rotated\"" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply mutation" }));

    await waitFor(() => {
      expect(screen.getByText("Config mutation applied.")).toBeInTheDocument();
    });
    expect(screen.getAllByText(/openai-rotated/).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Migrate" }));
    await waitFor(() => {
      expect(screen.getByText("Config migration completed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Recover backup" }));
    await waitFor(() => {
      expect(screen.getByText("Recovered config from backup 1.")).toBeInTheDocument();
    });

    fireEvent.change(keyInputs[1], { target: { value: "openai_api_key" } });
    fireEvent.change(valueInputs[1], { target: { value: "sk-test-key" } });
    fireEvent.click(screen.getByRole("button", { name: "Store or replace secret" }));
    await waitFor(() => {
      expect(screen.getByText("Secret metadata refreshed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Explicit reveal" }));
    await waitFor(() => {
      expect(document.body.textContent ?? "").toContain("[redacted]");
    });
    expect(screen.queryByText("sk-test-key")).not.toBeInTheDocument();

    fireEvent.click(screen.getByLabelText("Reveal sensitive values"));
    expect(await screen.findByText(/sk-test-key/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Delete secret" }));
    await waitFor(() => {
      expect(screen.getByText("Secret deleted.")).toBeInTheDocument();
    });
  });

  it("surfaces access CLI handoffs and support bundle recovery workflows", async () => {
    let pairingSummary = pairingSummaryFixture();
    const supportJobs = supportBundleJobsFixture().jobs.slice();

    const fetchMock = createFetchRouter(
      (request) => routeOverviewRequests(request, supportJobs),
      (request) => {
        if (request.path === "/console/v1/pairing" && request.method === "GET") {
          return jsonResponse(pairingSummary);
        }
        if (request.path === "/console/v1/pairing/codes" && request.method === "POST") {
          pairingSummary = {
            ...pairingSummary,
            channels: pairingSummary.channels.map((channel) =>
              channel.channel === "discord:default"
                ? {
                    ...channel,
                    active_codes: [
                      ...channel.active_codes,
                      {
                        code: "777999",
                        channel: "discord:default",
                        issued_by: "admin:web-console",
                        created_at_unix_ms: 1700000004000,
                        expires_at_unix_ms: 1700000604000,
                      },
                    ],
                  }
                : channel,
            ),
          };
          return jsonResponse(pairingSummary);
        }
        if (request.path === "/console/v1/support-bundle/jobs" && request.method === "POST") {
          supportJobs.unshift({
            job_id: "support-job-2",
            state: "queued",
            requested_at_unix_ms: 1700000005000,
            command_output: "",
          });
          return jsonResponse(supportBundleJobFixture("support-job-2"));
        }
        if (request.path === "/console/v1/support-bundle/jobs/support-job-2" && request.method === "GET") {
          return jsonResponse(supportBundleJobFixture("support-job-2"));
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Pairing and Gateway Access" }));
    expect(await screen.findByRole("heading", { name: "Pairing and Gateway Access" })).toBeInTheDocument();
    expect(screen.getByText(/dashboard-url --verify-remote/)).toBeInTheDocument();
    expect(screen.getByText(/tunnel --ssh/)).toBeInTheDocument();

    fireEvent.click(await screen.findByRole("button", { name: "Mint pairing code" }));
    await waitFor(() => {
      expect(screen.getByText("Pairing code minted.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Support and Recovery" }));
    expect(await screen.findByRole("heading", { name: "Support and Recovery" })).toBeInTheDocument();
    expect(screen.getByText("Provider auth recovery")).toBeInTheDocument();
    expect(screen.getByText("Bundle reliability")).toBeInTheDocument();
    expect(screen.getByText("Triage playbook")).toBeInTheDocument();
    expect(screen.getByText("docs/operations/observability-supportability-v1.md")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Retain jobs"), { target: { value: "8" } });
    fireEvent.click(screen.getByRole("button", { name: "Queue support bundle" }));
    await waitFor(() => {
      expect(screen.getByText("Support bundle job queued: support-job-2.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Load job" }));
    expect((await screen.findAllByText(/support-job-2/)).length).toBeGreaterThan(0);
    await waitFor(() => {
      expect(document.body).toHaveTextContent(
        "Remote gateway exposure requires explicit verification and operator acknowledgement."
      );
    });
  });

  it("renders every published CLI handoff from the capability catalog without fake direct actions", async () => {
    const fetchMock = createFetchRouter((request) => routeOverviewRequests(request));
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Web Dashboard Operator Surface" })).toBeInTheDocument();
    const cliHandoffPanel = screen.getByRole("heading", { name: "CLI handoff surface" }).closest("article");
    expect(cliHandoffPanel).not.toBeNull();
    const cliHandoffScope = within(cliHandoffPanel as HTMLElement);

    const cliHandoffs = capabilityCatalogFixture().capabilities.filter(
      (entry) => entry.dashboard_exposure === "cli_handoff"
    );
    await waitFor(() => {
      expect(cliHandoffScope.getByText(cliHandoffs[0].cli_handoff_commands[0])).toBeInTheDocument();
    });
    for (const capability of cliHandoffs) {
      for (const command of capability.cli_handoff_commands) {
        expect(cliHandoffScope.getByText(command)).toBeInTheDocument();
      }
    }

    expect(cliHandoffScope.queryByText("Chat sessions and run status")).not.toBeInTheDocument();
  });
});

function routeOverviewRequests(request: MockRequest, jobs = supportBundleJobsFixture().jobs) {
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
    return jsonResponse({
      ...supportBundleJobsFixture(),
      jobs,
    });
  }
  if (request.path === "/console/v1/diagnostics" && request.method === "GET") {
    return jsonResponse({});
  }
  if (request.path === "/console/v1/audit/events" && request.method === "GET") {
    return jsonResponse(auditEventsFixture());
  }
  return undefined;
}
