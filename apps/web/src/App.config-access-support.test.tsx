import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { App } from "./App";
import {
  auditEventsFixture,
  capabilityCatalogFixture,
  deploymentPostureFixture,
  diagnosticsFixture,
  doctorRecoveryJobFixture,
  doctorRecoveryJobsFixture,
  inventoryDeviceDetailFixture,
  inventoryListFixture,
  nodePairingListFixture,
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
import { createFetchRouter, jsonResponse, sessionResponse } from "./console/testUtils";
import type { MockRequest } from "./console/testUtils";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("M56 config, access, and support surfaces", () => {
  it("keeps config page read-only until the operator explicitly inspects or validates", async () => {
    let inspectCalls = 0;
    let validateCalls = 0;

    const fetchMock = createFetchRouter(
      (request) => routeOverviewRequests(request),
      (request) => {
        if (request.path === "/console/v1/config/inspect" && request.method === "POST") {
          inspectCalls += 1;
          return jsonResponse(configInspectFixture('version = 1\n[model_provider]\n'));
        }
        if (request.path === "/console/v1/config/validate" && request.method === "POST") {
          validateCalls += 1;
          return jsonResponse(configValidationFixture(true));
        }
        if (request.path === "/console/v1/secrets" && request.method === "GET") {
          return jsonResponse(secretMetadataListFixture());
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Config" }));
    expect(await screen.findByRole("heading", { name: "Config" })).toBeInTheDocument();
    await waitFor(() => {
      expect(document.body).toHaveTextContent(
        "Remote gateway exposure requires explicit verification and operator acknowledgement.",
      );
    });

    expect(inspectCalls).toBe(0);
    expect(validateCalls).toBe(0);
  });

  it(
    "operates config lifecycle and explicit secret reveal with default redaction",
    { timeout: 15_000 },
    async () => {
      const initialToml = 'version = 1\n[model_provider]\nauth_profile_id = "openai-default"\n';
      const migratedToml = 'version = 2\n[model_provider]\nauth_profile_id = "openai-migrated"\n';
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
            currentToml = `version = 1\n[model_provider]\nauth_profile_id = ${body.value ?? '"unset"'}\n`;
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

      fireEvent.click(await screen.findByRole("button", { name: "Config" }));
      expect(await screen.findByRole("heading", { name: "Config" })).toBeInTheDocument();
      await waitFor(() => {
        expect(document.body).toHaveTextContent(
          "Remote gateway exposure requires explicit verification and operator acknowledgement.",
        );
      });

      fireEvent.click(screen.getByRole("tab", { name: "Mutate" }));
      fireEvent.change(screen.getByLabelText("Key"), {
        target: { value: "model_provider.auth_profile_id" },
      });
      fireEvent.change(screen.getByLabelText("Value"), { target: { value: '"openai-rotated"' } });
      fireEvent.click(screen.getByRole("button", { name: "Apply mutation" }));

      await waitFor(() => {
        expect(screen.getByText("Config mutation applied.")).toBeInTheDocument();
      });
      expect(screen.getAllByText(/openai-rotated/).length).toBeGreaterThan(0);

      fireEvent.click(screen.getByRole("tab", { name: "Inspect" }));
      fireEvent.click(screen.getByRole("button", { name: "Migrate" }));
      await waitFor(() => {
        expect(screen.getByText("Config migration completed.")).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("tab", { name: "Recover" }));
      fireEvent.click(screen.getByRole("button", { name: "Recover backup" }));
      await waitFor(() => {
        expect(screen.getByText("Recovered config from backup 1.")).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: "Secrets" }));
      expect(await screen.findByRole("heading", { name: "Secrets" })).toBeInTheDocument();
      fireEvent.change(screen.getAllByLabelText("Key")[0], { target: { value: "openai_api_key" } });
      fireEvent.change(screen.getByLabelText("Value"), { target: { value: "sk-test-key" } });
      fireEvent.click(screen.getByRole("button", { name: "Store secret" }));
      await waitFor(() => {
        expect(screen.getByText("Secret metadata refreshed.")).toBeInTheDocument();
      });

      fireEvent.click(screen.getByRole("button", { name: "Explicit reveal" }));
      expect(screen.getByText("Sensitive / masked")).toBeInTheDocument();
      expect(screen.queryByText("sk-test-key")).not.toBeInTheDocument();

      fireEvent.click(screen.getByLabelText("Reveal sensitive values"));
      expect(await screen.findByText(/sk-test-key/)).toBeInTheDocument();

      fireEvent.click(screen.getAllByRole("button", { name: "Delete secret" })[0]);
      const deleteDialog = await screen.findByRole("alertdialog", { name: "Delete secret" });
      fireEvent.click(within(deleteDialog).getByRole("button", { name: "Delete secret" }));
      await waitFor(() => {
        expect(screen.getByText("Secret deleted.")).toBeInTheDocument();
      });
    },
  );

  it("uses the clicked secrets row key for inspect and reveal actions", async () => {
    const inspectedKeys: string[] = [];
    const revealedKeys: string[] = [];
    const fetchMock = createFetchRouter(
      (request) => routeOverviewRequests(request),
      (request) => {
        if (request.path === "/console/v1/secrets" && request.method === "GET") {
          expect(request.url.searchParams.get("scope")).toBe("global");
          return jsonResponse(secretMetadataListFixture());
        }
        if (request.path === "/console/v1/secrets/metadata" && request.method === "GET") {
          inspectedKeys.push(request.url.searchParams.get("key") ?? "");
          return jsonResponse(secretMetadataFixture());
        }
        if (request.path === "/console/v1/secrets/reveal" && request.method === "POST") {
          const body = JSON.parse(request.body) as { key?: string };
          revealedKeys.push(body.key ?? "");
          return jsonResponse(secretRevealFixture());
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Secrets" }));
    expect(await screen.findByRole("heading", { name: "Secrets" })).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Key")[0], { target: { value: "stale_secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Inspect" }));
    await waitFor(() => {
      expect(inspectedKeys).toEqual(["openai_api_key"]);
    });

    fireEvent.change(screen.getAllByLabelText("Key")[0], { target: { value: "another_stale" } });
    fireEvent.click(screen.getAllByRole("button", { name: "Reveal" })[0]);
    await waitFor(() => {
      expect(revealedKeys).toEqual(["openai_api_key"]);
    });
  });

  it("surfaces access CLI handoffs and support bundle recovery workflows", async () => {
    let pairingSummary = pairingSummaryFixture();
    let nodePairings: ReturnType<typeof nodePairingListFixture> = nodePairingListFixture();
    let inventory = inventoryListFixture();
    const supportJobs = supportBundleJobsFixture().jobs.slice();
    const doctorJobs = doctorRecoveryJobsFixture().jobs.slice();

    const fetchMock = createFetchRouter(
      (request) => routeOverviewRequests(request, supportJobs, doctorJobs),
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
        if (request.path === "/console/v1/pairing/requests" && request.method === "GET") {
          return jsonResponse(nodePairings);
        }
        if (request.path === "/console/v1/pairing/requests/code" && request.method === "POST") {
          nodePairings = {
            ...nodePairings,
            codes: [
              {
                code: "889900",
                method: "pin",
                issued_by: "admin:web-console",
                created_at_unix_ms: 1700000004500,
                expires_at_unix_ms: 1700000604500,
              },
              ...nodePairings.codes,
            ],
          };
          return jsonResponse({
            contract: nodePairings.contract,
            code: nodePairings.codes[0],
          });
        }
        if (
          request.path === "/console/v1/pairing/requests/pair-req-pending/approve" &&
          request.method === "POST"
        ) {
          nodePairings = {
            ...nodePairings,
            requests: nodePairings.requests.map((record) => {
              if (record.request_id !== "pair-req-pending") {
                return record;
              }
              return {
                ...record,
                state: "approved" as const,
                decision_reason: "operator confirmed trust",
              };
            }) as ReturnType<typeof nodePairingListFixture>["requests"],
          };
          return jsonResponse({
            contract: nodePairings.contract,
            request: nodePairings.requests.find(
              (record) => record.request_id === "pair-req-pending",
            ),
          });
        }
        if (request.path === "/console/v1/inventory" && request.method === "GET") {
          return jsonResponse(inventory);
        }
        if (
          request.path === "/console/v1/inventory/01ARZ3NDEKTSV4RRFFQ69G5FAZ" &&
          request.method === "GET"
        ) {
          return jsonResponse(inventoryDeviceDetailFixture("01ARZ3NDEKTSV4RRFFQ69G5FAZ"));
        }
        if (
          request.path === "/console/v1/inventory/01ARZ3NDEKTSV4RRFFQ69G5FBZ" &&
          request.method === "GET"
        ) {
          return jsonResponse(inventoryDeviceDetailFixture("01ARZ3NDEKTSV4RRFFQ69G5FBZ"));
        }
        if (
          request.path === "/console/v1/inventory/01ARZ3NDEKTSV4RRFFQ69G5FCZ" &&
          request.method === "GET"
        ) {
          return jsonResponse(inventoryDeviceDetailFixture("01ARZ3NDEKTSV4RRFFQ69G5FCZ"));
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
        if (
          request.path === "/console/v1/support-bundle/jobs/support-job-2" &&
          request.method === "GET"
        ) {
          return jsonResponse(supportBundleJobFixture("support-job-2"));
        }
        if (request.path === "/console/v1/doctor/jobs" && request.method === "POST") {
          doctorJobs.unshift({
            job_id: "doctor-job-2",
            state: "queued",
            requested_at_unix_ms: 1700000005200,
            command: ["doctor", "--json", "--repair", "--dry-run"],
            command_output: "",
          });
          return jsonResponse({
            contract: doctorRecoveryJobFixture().contract,
            job: doctorJobs[0],
          });
        }
        if (request.path === "/console/v1/doctor/jobs/doctor-job-2" && request.method === "GET") {
          return jsonResponse({
            contract: doctorRecoveryJobFixture().contract,
            job: {
              job_id: "doctor-job-2",
              state: "succeeded",
              requested_at_unix_ms: 1700000005200,
              completed_at_unix_ms: 1700000006200,
              command: ["doctor", "--json", "--repair", "--dry-run"],
              report: {
                mode: "repair_preview",
                recovery: {
                  requested: true,
                  dry_run: true,
                  force: false,
                  run_id: "01HRECOVERYRUN2",
                  backup_manifest_path: "state/recovery/runs/01HRECOVERYRUN2/manifest.json",
                  planned_steps: [{ id: "config.initialize" }],
                  applied_steps: [],
                  available_runs: [{ run_id: "01HRECOVERYRUN2" }],
                  next_steps: ["Apply repairs after reviewing the preview."],
                },
              },
              command_output: '{\\n  \\"mode\\": \\"repair_preview\\"\\n}',
            },
          });
        }
        return undefined;
      },
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Access" }));
    expect(await screen.findByRole("heading", { name: "Access" })).toBeInTheDocument();
    expect(document.body).toHaveTextContent("daemon dashboard-url --verify-remote --json");
    expect(document.body).toHaveTextContent("tunnel --ssh");
    expect(document.body).toHaveTextContent("sha256:remote-admin-1");
    expect(document.body).toHaveTextContent("Expired before the client finished bootstrap.");
    expect(document.body).toHaveTextContent("operator rejected test device");

    fireEvent.click(await screen.findByRole("button", { name: "Mint node pairing code" }));
    await waitFor(() => {
      expect(screen.getByText("Node pairing code 889900 minted.")).toBeInTheDocument();
    });
    expect(document.body).toHaveTextContent("--pairing-code 889900");

    fireEvent.change(screen.getByLabelText("Approval / rejection reason"), {
      target: { value: "operator confirmed trust" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    const approveDialog = await screen.findByRole("alertdialog", {
      name: "Approve pairing request",
    });
    fireEvent.click(within(approveDialog).getByRole("button", { name: "Approve pairing request" }));
    await waitFor(() => {
      expect(screen.getByText("Pairing request pair-req-pending approved.")).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: "Approve" })).not.toBeInTheDocument();
    expect(document.body).toHaveTextContent("heartbeat stale");
    expect(document.body).toHaveTextContent("sha256:cert-pending-2");

    fireEvent.click(screen.getByRole("button", { name: "Support and Recovery" }));
    expect(
      await screen.findByRole("heading", { name: "Support and Recovery" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Provider auth recovery")).toBeInTheDocument();
    expect(screen.getAllByText("Bundle reliability").length).toBeGreaterThan(0);
    expect(screen.getByText("Triage playbook")).toBeInTheDocument();
    expect(
      screen.getByText(
        "docs-codebase/docs-tree/web_console_operator_dashboard/console_sections_and_navigation/support_recovery.md",
      ),
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Retain jobs"), { target: { value: "8" } });
    fireEvent.click(screen.getByRole("button", { name: "Queue support bundle" }));
    await waitFor(() => {
      expect(screen.getByText("Support bundle job queued: support-job-2.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Load job" }));
    expect((await screen.findAllByText(/support-job-2/)).length).toBeGreaterThan(0);
    await waitFor(() => {
      expect(document.body).toHaveTextContent(
        "Remote gateway exposure requires explicit verification and operator acknowledgement.",
      );
    });

    fireEvent.change(screen.getByLabelText("Retain recovery jobs"), { target: { value: "6" } });
    fireEvent.change(screen.getByLabelText("Only checks"), {
      target: { value: "config.initialize,node_runtime.normalize" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Queue preview" }));
    await waitFor(() => {
      expect(screen.getByText("Recovery doctor job queued: doctor-job-2.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Load recovery job" }));
    await waitFor(() => {
      expect(document.body).toHaveTextContent("01HRECOVERYRUN2");
    });
    expect(screen.getByText("Planned steps")).toBeInTheDocument();
    expect(screen.getByText("Next steps")).toBeInTheDocument();
  }, 20_000);

  it("renders every published CLI handoff from the capability catalog without fake direct actions", async () => {
    const fetchMock = createFetchRouter((request) => routeOverviewRequests(request));
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    const cliHandoffs = capabilityCatalogFixture().capabilities.filter(
      (entry) => entry.dashboard_exposure === "cli_handoff",
    );
    fireEvent.click(await screen.findByRole("button", { name: "Access" }));
    await waitFor(() => {
      expect(screen.getAllByText(cliHandoffs[0].cli_handoff_commands[0]).length).toBeGreaterThan(0);
    });
    expect(screen.getAllByText(cliHandoffs[1].cli_handoff_commands[0]).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Diagnostics" }));
    expect(await screen.findByText(cliHandoffs[2].cli_handoff_commands[0])).toBeInTheDocument();
    expect(screen.getByText(cliHandoffs[3].cli_handoff_commands[0])).toBeInTheDocument();
    expect(screen.getByText(cliHandoffs[3].cli_handoff_commands[1])).toBeInTheDocument();

    expect(screen.queryByText("Chat sessions and run status")).not.toBeInTheDocument();
  });
});

function routeOverviewRequests(
  request: MockRequest,
  jobs = supportBundleJobsFixture().jobs,
  doctorJobs = doctorRecoveryJobsFixture().jobs,
) {
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
  if (request.path === "/console/v1/doctor/jobs" && request.method === "GET") {
    return jsonResponse({
      ...doctorRecoveryJobsFixture(),
      jobs: doctorJobs,
    });
  }
  if (request.path === "/console/v1/diagnostics" && request.method === "GET") {
    return jsonResponse(diagnosticsFixture());
  }
  if (request.path === "/console/v1/audit/events" && request.method === "GET") {
    return jsonResponse(auditEventsFixture());
  }
  return undefined;
}
