import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { App } from "./App";
import {
  auditEventsFixture,
  capabilityCatalogFixture,
  deploymentPostureFixture,
  supportBundleJobsFixture,
} from "./console/__fixtures__/m56ControlPlane";

afterEach(() => {
  cleanup();
  window.history.replaceState(null, "", "/");
  window.localStorage.clear();
  vi.unstubAllGlobals();
});

describe("M35 web console app", () => {
  it("requires authentication before showing privileged pages", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    const fetchMock = createQueuedFetch(
      Array.from({ length: 16 }, () => jsonResponse({ error: "missing session" }, 403)),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.queryByRole("button", { name: "Approvals" })).not.toBeInTheDocument();
    expect(screen.queryByText("Sign-in failed")).not.toBeInTheDocument();
    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  it("prefills a canonical device id for the advanced login form", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    const fetchMock = createQueuedFetch(
      Array.from({ length: 16 }, () => jsonResponse({ error: "missing session" }, 403)),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );

    fireEvent.click(screen.getByRole("button", { name: "Advanced session identity" }));
    await waitFor(() => {
      expect(screen.getByLabelText("Device label")).toHaveValue("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    });
  });

  it("retries bootstrap session before falling back to the auth screen", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    const fetchMock = createQueuedFetch([
      jsonResponse({ error: "admin API rate limit exceeded for 127.0.0.1" }, 429),
      jsonResponse({ error: "admin API rate limit exceeded for 127.0.0.1" }, 429),
      jsonResponse({
        principal: "admin:desktop-control-center",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    const sessionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/session",
    );
    expect(sessionCalls).toHaveLength(3);
  });

  it("consumes the desktop handoff token before showing the auth screen", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    window.history.replaceState(
      null,
      "",
      "/?desktop_handoff_token=handoff-token#/control/overview",
    );
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:desktop-control-center",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.queryByRole("heading", { name: "Operator Dashboard" })).not.toBeInTheDocument();
    const sessionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/session",
    );
    expect(sessionCalls).toHaveLength(0);
    const handoffCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/browser-handoff/session",
    );
    expect(handoffCalls).toHaveLength(1);
    expect(window.location.search).toBe("");
  });

  it("falls back to the existing session when the desktop handoff token is already spent", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    window.history.replaceState(
      null,
      "",
      "/?desktop_handoff_token=handoff-token#/control/overview",
    );
    const fetchMock = createQueuedFetch([
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({
        principal: "admin:desktop-control-center",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    const sessionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/session",
    );
    expect(sessionCalls).toHaveLength(1);
    const handoffCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/browser-handoff/session",
    );
    expect(handoffCalls).toHaveLength(1);
    expect(window.location.search).toBe("");
  });

  it("keeps the boot screen visible until a delayed desktop session arrives", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    const fetchMock = createQueuedFetch([
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({ error: "missing session" }, 403),
      jsonResponse({
        principal: "admin:desktop-control-center",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    expect(screen.getByRole("heading", { name: "Web Dashboard" })).toBeInTheDocument();
    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.queryByRole("heading", { name: "Operator Dashboard" })).not.toBeInTheDocument();
    const sessionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/auth/session",
    );
    expect(sessionCalls).toHaveLength(6);
  });

  it("does not surface a false overview error after a successful baseline refresh", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }
      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.getByRole("heading", { name: "Overview" })).toBeInTheDocument();
    await waitFor(
      () => {
        expect(screen.queryByText("Unexpected failure.")).not.toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
  });

  it("renders the shared logs page with filter controls and structured payload detail", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return jsonResponse({
          principal: "admin:web-console",
          device_id: "device-1",
          channel: "web",
          csrf_token: "csrf-1",
          issued_at_unix_ms: 100,
          expires_at_unix_ms: 300,
        });
      }

      if (path.startsWith("/console/v1/logs") && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          query: {
            limit: 120,
            direction: "before",
            start_at_unix_ms: 0,
            end_at_unix_ms: 100,
          },
          records: [
            {
              cursor: "100:browserd:relay-action-1",
              source: "browserd",
              source_kind: "browserd",
              severity: "error",
              message: "browser relay failed with token=<redacted>",
              timestamp_unix_ms: 100,
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
              run_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
              event_name: "browser.relay.action",
              structured_payload: {
                event: "browser.relay.action",
                error: "token=<redacted>",
              },
            },
          ],
          page: { limit: 120, returned: 1, has_more: false },
          newest_cursor: "100:browserd:relay-action-1",
          available_sources: ["browserd", "palyrad"],
        });
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );

    fireEvent.click(await screen.findByRole("button", { name: "Logs and Runtime Stream" }));

    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: "Logs" })).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.getByText("Auto-follow")).toBeInTheDocument();
    expect(screen.getByText("browser relay failed with token=<redacted>")).toBeInTheDocument();
    expect(screen.getByText("browser.relay.action")).toBeInTheDocument();
  });

  it("opens inventory from the logs detail and renders unified device and instance posture", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return jsonResponse({
          principal: "admin:web-console",
          device_id: "device-1",
          channel: "web",
          csrf_token: "csrf-1",
          issued_at_unix_ms: 100,
          expires_at_unix_ms: 300,
        });
      }

      if (path.startsWith("/console/v1/logs") && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          query: {
            limit: 120,
            direction: "before",
            start_at_unix_ms: 0,
            end_at_unix_ms: 100,
          },
          records: [
            {
              cursor: "100:browserd:relay-action-1",
              source: "browserd",
              source_kind: "browserd",
              severity: "error",
              message: "browser relay failed with token=<redacted>",
              timestamp_unix_ms: 100,
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
              device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
              event_name: "browser.relay.action",
              structured_payload: {
                event: "browser.relay.action",
                error: "token=<redacted>",
              },
            },
          ],
          page: { limit: 120, returned: 1, has_more: false },
          newest_cursor: "100:browserd:relay-action-1",
          available_sources: ["browserd", "palyrad"],
        });
      }

      if (path === "/console/v1/inventory" && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          generated_at_unix_ms: 100,
          summary: {
            devices: 1,
            trusted_devices: 1,
            pending_pairings: 1,
            ok_devices: 0,
            stale_devices: 0,
            degraded_devices: 1,
            offline_devices: 0,
            ok_instances: 2,
            stale_instances: 0,
            degraded_instances: 1,
            offline_instances: 0,
          },
          devices: [
            {
              device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
              client_kind: "node",
              device_status: "paired",
              trust_state: "trusted",
              presence_state: "degraded",
              paired_at_unix_ms: 90,
              updated_at_unix_ms: 100,
              last_seen_at_unix_ms: 95,
              heartbeat_age_ms: 5000,
              latest_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
              pending_pairings: 1,
              issued_by: "admin:web-console",
              approval_id: "approval-1",
              identity_fingerprint: "fingerprint-1",
              transcript_hash_hex: "hash-1",
              platform: "windows",
              capabilities: [{ name: "ping", available: true, execution_mode: "automatic" }],
              capability_summary: { total: 1, available: 1, unavailable: 0 },
              warnings: ["1 pairing requests still require completion"],
              actions: {
                can_rotate: true,
                can_revoke: true,
                can_remove: true,
                can_invoke: true,
              },
            },
          ],
          pending_pairings: [
            {
              request_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
              device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
              client_kind: "node",
              method: "pin",
              code_issued_by: "admin:web-console",
              requested_at_unix_ms: 95,
              expires_at_unix_ms: 195,
              approval_id: "approval-1",
              state: "pending_approval",
              identity_fingerprint: "fingerprint-1",
              transcript_hash_hex: "hash-1",
            },
          ],
          instances: [
            {
              instance_id: "browserd",
              label: "Browser service",
              kind: "browserd",
              presence_state: "ok",
              observed_at_unix_ms: 100,
              state_label: "ok",
              detail: "1 active sessions",
              capability_summary: { total: 1, available: 1, unavailable: 0 },
            },
          ],
          page: { limit: 1, returned: 1, has_more: false },
        });
      }

      if (path === "/console/v1/inventory/01ARZ3NDEKTSV4RRFFQ69G5FB0" && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          generated_at_unix_ms: 100,
          device: {
            device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
            client_kind: "node",
            device_status: "paired",
            trust_state: "trusted",
            presence_state: "degraded",
            paired_at_unix_ms: 90,
            updated_at_unix_ms: 100,
            last_seen_at_unix_ms: 95,
            heartbeat_age_ms: 5000,
            latest_session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
            pending_pairings: 1,
            issued_by: "admin:web-console",
            approval_id: "approval-1",
            identity_fingerprint: "fingerprint-1",
            transcript_hash_hex: "hash-1",
            platform: "windows",
            capabilities: [{ name: "ping", available: true, execution_mode: "automatic" }],
            capability_summary: { total: 1, available: 1, unavailable: 0 },
            warnings: ["1 pairing requests still require completion"],
            actions: {
              can_rotate: true,
              can_revoke: true,
              can_remove: true,
              can_invoke: true,
            },
          },
          pairings: [
            {
              request_id: "01ARZ3NDEKTSV4RRFFQ69G5FB1",
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
              device_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
              client_kind: "node",
              method: "pin",
              code_issued_by: "admin:web-console",
              requested_at_unix_ms: 95,
              expires_at_unix_ms: 195,
              approval_id: "approval-1",
              state: "pending_approval",
              identity_fingerprint: "fingerprint-1",
              transcript_hash_hex: "hash-1",
            },
          ],
          capability_requests: [],
        });
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(
          screen.getByRole("heading", { name: "Web Dashboard Operator Surface" }),
        ).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );

    fireEvent.click(await screen.findByRole("button", { name: "Logs and Runtime Stream" }));
    expect(await screen.findByRole("heading", { name: "Logs" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Open inventory" }));

    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: "Inventory" })).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.getAllByText("01ARZ3NDEKTSV4RRFFQ69G5FB0").length).toBeGreaterThan(0);
    expect(screen.getByText("Browser service")).toBeInTheDocument();
    expect(screen.getByText("Invoke capability")).toBeInTheDocument();
  });

  it("clears operator-scoped state on sign-out before next sign-in refresh completes", async () => {
    let releaseUserBApprovals: (() => void) | undefined;
    const userBApprovalsReady = new Promise<void>((resolve) => {
      releaseUserBApprovals = () => resolve();
    });
    let activePrincipal = "admin:user-a";
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:user-a",
            device_id: "device-a",
            channel: "web",
            csrf_token: "csrf-a",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/approvals" && method === "GET") {
        if (activePrincipal === "admin:user-a") {
          return Promise.resolve(
            jsonResponse({
              approvals: [{ approval_id: "APPROVAL-A", subject_type: "tool", decision: "pending" }],
            }),
          );
        }
        return userBApprovalsReady.then(() =>
          jsonResponse({
            approvals: [{ approval_id: "APPROVAL-B", subject_type: "tool", decision: "pending" }],
          }),
        );
      }

      if (path === "/console/v1/auth/logout" && method === "POST") {
        return Promise.resolve(jsonResponse({ signed_out: true }));
      }

      if (path === "/console/v1/auth/login" && method === "POST") {
        activePrincipal = "admin:user-b";
        return Promise.resolve(
          jsonResponse({
            principal: "admin:user-b",
            device_id: "device-b",
            channel: "web",
            csrf_token: "csrf-b",
            issued_at_unix_ms: 200,
            expires_at_unix_ms: 400,
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Approvals" }));
    await waitFor(
      () => {
        expect(screen.getAllByText("APPROVAL-A").length).toBeGreaterThan(0);
      },
      { timeout: 5000 },
    );

    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    expect(await screen.findByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Admin token"), { target: { value: "token-b" } });
    fireEvent.change(screen.getByLabelText("Operator principal"), {
      target: { value: "admin:user-b" },
    });
    fireEvent.change(screen.getByLabelText("Device label"), { target: { value: "device-b" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(
      await screen.findByRole("heading", { name: "Web Dashboard Operator Surface" }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Approvals" }));
    expect(screen.queryByText("APPROVAL-A")).not.toBeInTheDocument();
    expect(screen.getByText("No approval records loaded.")).toBeInTheDocument();

    const releaseApprovals = releaseUserBApprovals;
    if (releaseApprovals === undefined) {
      throw new Error("Expected deferred approvals release hook for user B.");
    }
    releaseApprovals();

    await waitFor(() => {
      expect(document.body).toHaveTextContent("APPROVAL-B");
    });
  });

  it("executes approval decision flow with CSRF-protected request", async () => {
    let approvalDecision: "pending" | "allow" = "pending";
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/approvals" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            approvals: [{ approval_id: "A1", subject_type: "tool", decision: approvalDecision }],
          }),
        );
      }

      if (path === "/console/v1/approvals/A1/decision" && method === "POST") {
        approvalDecision = "allow";
        return Promise.resolve(
          jsonResponse({ approval: { approval_id: "A1", decision: "allow" } }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Approvals" }));
    fireEvent.click(await screen.findByRole("button", { name: /A1/i }));
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));

    await waitFor(() => {
      expect(document.body).toHaveTextContent("Approval allowed.");
    });

    const decisionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/approvals/A1/decision",
    );
    expect(decisionCalls.length).toBeGreaterThan(0);
    const decisionRequest = decisionCalls[decisionCalls.length - 1][1];
    const headers = new Headers(decisionRequest?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("renders approval prompt preview payload details for Discord message mutations", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/approvals" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            approvals: [
              {
                approval_id: "APPROVAL-1",
                subject_type: "channel_send",
                subject_id: "discord:default:edit:discord:channel:1:m1",
                principal: "admin:web-console",
                requested_at_unix_ms: 100,
                request_summary: "Approve Discord message edit",
                prompt: {
                  title: "Approve Discord message edit",
                  summary: "Review preview before applying mutation.",
                  risk_level: "high",
                  timeout_seconds: 900,
                  details_json: JSON.stringify({
                    connector_id: "discord:default",
                    operation: "edit",
                    locator: {
                      conversation_id: "discord:channel:1",
                      thread_id: "thread-1",
                      message_id: "m1",
                    },
                    mutation: {
                      body: "Escalated incident summary",
                    },
                  }),
                },
                policy_snapshot: {
                  policy_id: "discord.message.mutation.approval.v1",
                  evaluation_summary: "approval_required=true",
                },
              },
            ],
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Approvals" }));

    expect(await screen.findByRole("heading", { name: "Approvals" })).toBeInTheDocument();
    expect(await screen.findByText("Approval preview payload")).toBeInTheDocument();
    expect(document.body).toHaveTextContent("discord:channel:1");
    expect(document.body).toHaveTextContent("Escalated incident summary");
  });

  it("keeps Discord message mutation previews visibly distinct from applied changes in channels UI", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/channels" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            connectors: [
              {
                connector_id: "discord:default",
                kind: "discord",
                enabled: true,
                readiness: "ready",
                availability: "supported",
              },
            ],
          }),
        );
      }

      if (path === "/console/v1/channels/discord%3Adefault" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            connector: {
              connector_id: "discord:default",
              kind: "discord",
              enabled: true,
              readiness: "ready",
              availability: "supported",
              capabilities: {
                message: {
                  action_details: [
                    {
                      action: "read",
                      supported: true,
                      approval_mode: "none",
                      risk_level: "low",
                      required_permissions: ["ViewChannel", "ReadMessageHistory"],
                    },
                    {
                      action: "edit",
                      supported: true,
                      approval_mode: "conditional",
                      risk_level: "conditional",
                      required_permissions: ["ViewChannel", "SendMessages"],
                    },
                  ],
                },
              },
            },
            operations: {
              queue: { paused: false, dead_letters: 0 },
              saturation: { state: "healthy" },
            },
          }),
        );
      }

      if (path === "/console/v1/channels/discord%3Adefault/logs" && method === "GET") {
        return Promise.resolve(jsonResponse({ events: [], dead_letters: [] }));
      }

      if (path === "/console/v1/channels/router/rules" && method === "GET") {
        return Promise.resolve(jsonResponse({ rules: {}, config_hash: "router-1" }));
      }

      if (path === "/console/v1/channels/router/warnings" && method === "GET") {
        return Promise.resolve(jsonResponse({ warnings: [], config_hash: "router-1" }));
      }

      if (path === "/console/v1/channels/router/pairings" && method === "GET") {
        return Promise.resolve(jsonResponse({ pairings: [], config_hash: "router-1" }));
      }

      if (path === "/console/v1/channels/discord%3Adefault/messages/read" && method === "POST") {
        return Promise.resolve(
          jsonResponse({
            result: {
              conversation_id: "discord:channel:1",
              thread_id: "thread-1",
              messages: [
                {
                  locator: {
                    conversation_id: "discord:channel:1",
                    thread_id: "thread-1",
                    message_id: "m1",
                  },
                  sender_id: "discord:user:42",
                  sender_display: "Ops Bot",
                  body: "Original incident summary",
                  created_at_unix_ms: 100,
                  attachments: [],
                  reactions: [],
                  link: "https://discord.com/channels/1/2/3",
                },
              ],
              preflight: {
                allowed: true,
                policy_action: "channel.message.read",
                approval_mode: "none",
                risk_level: "low",
                required_permissions: ["ViewChannel"],
              },
            },
          }),
        );
      }

      if (path === "/console/v1/channels/discord%3Adefault/messages/edit" && method === "POST") {
        return Promise.resolve(
          jsonResponse({
            approval_required: true,
            approval: {
              approval_id: "APPROVAL-1",
            },
            policy: {
              action: "channel.message.edit",
              reason: "explicit approval required",
            },
            preview: {
              locator: {
                conversation_id: "discord:channel:1",
                thread_id: "thread-1",
                message_id: "m1",
              },
              message: {
                locator: {
                  conversation_id: "discord:channel:1",
                  thread_id: "thread-1",
                  message_id: "m1",
                },
                sender_id: "discord:user:42",
                sender_display: "Ops Bot",
                body: "Original incident summary",
                created_at_unix_ms: 100,
                attachments: [],
                reactions: [],
              },
            },
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByText("Channels"));
    expect(await screen.findByRole("heading", { name: "Channels" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "Messages" }));
    fireEvent.change(await screen.findByLabelText("Conversation ID"), {
      target: { value: "discord:channel:1" },
    });
    fireEvent.change(screen.getByLabelText("Thread ID"), { target: { value: "thread-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Read messages" }));

    expect(await screen.findByText("Original incident summary")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Prefill mutation" }));
    fireEvent.change(screen.getByLabelText("Edit body"), {
      target: { value: "Escalated incident summary" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Edit message" }));

    expect(await screen.findByText("Approval required")).toBeInTheDocument();
    expect(document.body).toHaveTextContent(
      "No platform mutation has been applied yet. This response is a preview",
    );
    expect(document.body).toHaveTextContent("APPROVAL-1");
  });

  it("supports routine create and pause workflow from UI", async () => {
    const cronJobs = [
      {
        routine_id: "J1",
        job_id: "J1",
        name: "job-one",
        enabled: true,
        trigger_kind: "schedule",
        schedule_type: "every",
        schedule_payload: { interval_ms: 60000 },
        delivery_mode: "same_channel",
      },
    ];
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/routines" && method === "GET") {
        return Promise.resolve(jsonResponse({ routines: cronJobs }));
      }

      if (path === "/console/v1/routines" && method === "POST") {
        cronJobs.push({
          routine_id: "J2",
          job_id: "J2",
          name: "web-job",
          enabled: true,
          trigger_kind: "schedule",
          schedule_type: "every",
          schedule_payload: { interval_ms: 60000 },
          delivery_mode: "same_channel",
        });
        return Promise.resolve(jsonResponse({ routine: { routine_id: "J2", job_id: "J2" } }));
      }

      if (path === "/console/v1/routines/J1/enabled" && method === "POST") {
        cronJobs[0] = { ...cronJobs[0], enabled: false };
        return Promise.resolve(
          jsonResponse({ routine: { routine_id: "J1", job_id: "J1", enabled: false } }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Automations" }));
    expect((await screen.findAllByText("job-one")).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "New routine" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "web-job" } });
    fireEvent.change(screen.getByLabelText("Prompt"), {
      target: { value: "run from web console" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create routine" }));

    await waitFor(() => {
      expect(screen.getByText("Routine created.")).toBeInTheDocument();
    });

    fireEvent.click((await screen.findAllByRole("button", { name: "Pause" }))[0]);

    await waitFor(() => {
      expect(screen.getByText("Routine paused.")).toBeInTheDocument();
    });

    const [, createRequest] = findRequestCall(fetchMock, "/console/v1/routines", "POST");
    expect(createRequest?.method).toBe("POST");

    const [, toggleRequest] = findRequestCall(fetchMock, "/console/v1/routines/J1/enabled", "POST");
    expect(toggleRequest?.method).toBe("POST");
    expect(requestBody(toggleRequest?.body)).toContain('"enabled":false');
  });

  it("manages channel connectors from channels section with CSRF-protected enable toggle", async () => {
    let enabled = true;
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      const connector = {
        connector_id: "echo:default",
        kind: "echo",
        availability: "internal_test_only",
        enabled,
        readiness: "ready",
        liveness: enabled ? "running" : "stopped",
        queue_depth: { pending_outbox: 0, dead_letters: enabled ? 1 : 0 },
      };

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/channels" && method === "GET") {
        return Promise.resolve(jsonResponse({ connectors: [connector] }));
      }

      if (path === "/console/v1/channels/echo%3Adefault" && method === "GET") {
        return Promise.resolve(jsonResponse({ connector }));
      }

      if (path === "/console/v1/channels/echo%3Adefault/logs" && method === "GET") {
        return Promise.resolve(
          jsonResponse(
            enabled
              ? {
                  events: [
                    {
                      event_id: 1,
                      connector_id: "echo:default",
                      event_type: "outbox.retry",
                      level: "warn",
                      message: "retry scheduled",
                      created_at_unix_ms: 111,
                    },
                  ],
                  dead_letters: [
                    {
                      dead_letter_id: 1,
                      connector_id: "echo:default",
                      envelope_id: "env-1:0",
                      reason: "permanent",
                      payload: { text: "failed" },
                      created_at_unix_ms: 112,
                    },
                  ],
                }
              : { events: [], dead_letters: [] },
          ),
        );
      }

      if (path === "/console/v1/channels/router/rules" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            config: {
              enabled: true,
              default_direct_message_policy: "deny",
              channels: [{ channel: "echo:default", enabled: true }],
            },
            config_hash: enabled ? "router-hash-1" : "router-hash-2",
          }),
        );
      }

      if (path === "/console/v1/channels/router/warnings" && method === "GET") {
        return Promise.resolve(
          jsonResponse({ warnings: [], config_hash: enabled ? "router-hash-1" : "router-hash-2" }),
        );
      }

      if (path === "/console/v1/channels/router/pairings" && method === "GET") {
        return Promise.resolve(
          jsonResponse({ pairings: [], config_hash: enabled ? "router-hash-1" : "router-hash-2" }),
        );
      }

      if (path === "/console/v1/channels/echo%3Adefault/enabled" && method === "POST") {
        enabled = false;
        return Promise.resolve(
          jsonResponse({ connector: { ...connector, enabled: false, liveness: "stopped" } }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));
    expect(await screen.findByRole("heading", { name: "Channels" })).toBeInTheDocument();
    await waitFor(
      () => {
        expect(document.body).toHaveTextContent("echo:default");
        expect(document.body).toHaveTextContent("internal_test_only");
      },
      { timeout: 5_000 },
    );

    fireEvent.click(screen.getByRole("button", { name: "Disable echo:default" }));

    await waitFor(() => {
      expect(screen.getByText("Connector disabled.")).toBeInTheDocument();
    });

    const [, request] = findRequestCall(
      fetchMock,
      "/console/v1/channels/echo%3Adefault/enabled",
      "POST",
    );
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(request?.method).toBe("POST");
    expect(requestBody(request?.body)).toContain('"enabled":false');
  });

  it("hides deferred connectors from channels section and selects the first visible connector", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/channels" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            connectors: [
              {
                connector_id: "slack:default",
                kind: "slack",
                availability: "deferred",
                enabled: false,
                readiness: "misconfigured",
                liveness: "stopped",
                queue_depth: { pending_outbox: 0, dead_letters: 0 },
              },
              {
                connector_id: "echo:default",
                kind: "echo",
                availability: "internal_test_only",
                enabled: true,
                readiness: "ready",
                liveness: "running",
                queue_depth: { pending_outbox: 0, dead_letters: 0 },
              },
            ],
          }),
        );
      }

      if (path === "/console/v1/channels/echo%3Adefault" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            connector: {
              connector_id: "echo:default",
              kind: "echo",
              availability: "internal_test_only",
              enabled: true,
              readiness: "ready",
              liveness: "running",
              queue_depth: { pending_outbox: 0, dead_letters: 0 },
            },
          }),
        );
      }

      if (path === "/console/v1/channels/echo%3Adefault/logs" && method === "GET") {
        return Promise.resolve(jsonResponse({ events: [], dead_letters: [] }));
      }

      if (path === "/console/v1/channels/router/rules" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            config: {
              enabled: true,
              default_direct_message_policy: "deny",
              channels: [{ channel: "echo:default", enabled: true }],
            },
            config_hash: "router-hash-1",
          }),
        );
      }

      if (path === "/console/v1/channels/router/warnings" && method === "GET") {
        return Promise.resolve(jsonResponse({ warnings: [], config_hash: "router-hash-1" }));
      }

      if (path === "/console/v1/channels/router/pairings" && method === "GET") {
        return Promise.resolve(jsonResponse({ pairings: [], config_hash: "router-hash-1" }));
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));

    await waitFor(
      () => {
        expect(document.body).toHaveTextContent("echo:default");
        expect(document.body).toHaveTextContent("internal_test_only");
      },
      { timeout: 5_000 },
    );
    expect(screen.queryByText("slack:default")).not.toBeInTheDocument();
    expect(findRequestCall(fetchMock, "/console/v1/channels/echo%3Adefault", "GET")).toBeDefined();
  });

  it("runs discord onboarding preflight from channels wizard with CSRF-protected request", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();
      const connector = {
        connector_id: "discord:default",
        kind: "discord",
        availability: "supported",
        enabled: false,
        readiness: "missing_credential",
        liveness: "stopped",
        queue_depth: { pending_outbox: 0, dead_letters: 0 },
      };

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/channels" && method === "GET") {
        return Promise.resolve(jsonResponse({ connectors: [connector] }));
      }

      if (path === "/console/v1/channels/discord%3Adefault" && method === "GET") {
        return Promise.resolve(jsonResponse({ connector }));
      }

      if (path === "/console/v1/channels/discord%3Adefault/logs" && method === "GET") {
        return Promise.resolve(jsonResponse({ events: [], dead_letters: [] }));
      }

      if (path === "/console/v1/channels/router/rules" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            config: {
              enabled: true,
              default_direct_message_policy: "deny",
              channels: [{ channel: "discord:default", enabled: true }],
            },
            config_hash: "router-hash-1",
          }),
        );
      }

      if (path === "/console/v1/channels/router/warnings" && method === "GET") {
        return Promise.resolve(jsonResponse({ warnings: [], config_hash: "router-hash-1" }));
      }

      if (path === "/console/v1/channels/router/pairings" && method === "GET") {
        return Promise.resolve(jsonResponse({ pairings: [], config_hash: "router-hash-1" }));
      }

      if (path === "/console/v1/channels/discord/onboarding/probe" && method === "POST") {
        return Promise.resolve(
          jsonResponse({
            connector_id: "discord:default",
            account_id: "default",
            mode: "local",
            inbound_scope: "dm_only",
            bot: { id: "123", username: "palyra-bot" },
            required_permissions: [
              "View Channels",
              "Send Messages",
              "Read Message History",
              "Embed Links",
              "Attach Files",
              "Send Messages in Threads",
            ],
            egress_allowlist: ["discord.com", "*.discord.com"],
            security_defaults: ["Attachments ingestion is metadata only by default."],
            channel_permission_check: {
              channel_id: "123456789012345678",
              status: "ok",
              can_view_channel: true,
              can_send_messages: true,
              can_read_message_history: true,
              can_embed_links: true,
              can_attach_files: true,
              can_send_messages_in_threads: true,
            },
            warnings: [],
            policy_warnings: [],
            routing_preview: { connector_id: "discord:default" },
            invite_url_template:
              "https://discord.com/oauth2/authorize?client_id=123&scope=bot&permissions=205824",
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));
    fireEvent.click(screen.getByRole("tab", { name: "Discord setup" }));
    expect(
      await screen.findByRole("heading", { name: "Discord onboarding wizard" }),
    ).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Bot token"), {
      target: { value: "test-token" },
    });
    fireEvent.change(screen.getByLabelText("Verify channel ID"), {
      target: { value: "123456789012345678" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run preflight" }));

    await waitFor(() => {
      expect(document.body).toHaveTextContent("Discord preflight OK for palyra-bot (123).");
    });
    expect(screen.getByRole("heading", { name: "Preflight highlights" })).toBeInTheDocument();
    expect(screen.getByText("discord.com")).toBeInTheDocument();
    expect(
      screen.getByText("Attachments ingestion is metadata only by default."),
    ).toBeInTheDocument();

    const [, request] = findRequestCall(
      fetchMock,
      "/console/v1/channels/discord/onboarding/probe",
      "POST",
    );
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(request?.method).toBe("POST");
    expect(requestBody(request?.body)).toContain('"token":"test-token"');
    expect(requestBody(request?.body)).toContain('"verify_channel_id":"123456789012345678"');
  });

  it("issues browser relay token from browser section with CSRF protection", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/browser/profiles" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            active_profile_id: null,
            profiles: [],
          }),
        );
      }

      if (path === "/console/v1/browser/relay/tokens" && method === "POST") {
        return Promise.resolve(
          jsonResponse({
            relay_token: "relay-token-abc",
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            extension_id: "com.palyra.extension",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 500,
            token_ttl_ms: 300000,
            warning: "short-lived",
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Browser" }));
    expect(await screen.findByRole("heading", { name: "Browser" })).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Relay session ID")[1], {
      target: { value: "01ARZ3NDEKTSV4RRFFQ69G5FAV" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Mint relay token" }));

    await waitFor(() => {
      expect(document.body).toHaveTextContent(
        "Browser relay token minted. Keep it private and short-lived.",
      );
    });

    const [, request] = findRequestCall(fetchMock, "/console/v1/browser/relay/tokens", "POST");
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestBody(request?.body)).toContain('"extension_id":"com.palyra.extension"');
  });

  it("renders usage section with server-side filters, exports, and session drilldown", async () => {
    const openMock = vi.fn((_url?: string | URL, _target?: string, _features?: string) => null);
    vi.stubGlobal("open", openMock);

    const usageSession = {
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      session_key: "usage-session",
      session_label: "Usage session",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
      created_at_unix_ms: 100,
      updated_at_unix_ms: 220,
      last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      archived: false,
      runs: 3,
      active_runs: 1,
      completed_runs: 2,
      prompt_tokens: 120,
      completion_tokens: 80,
      total_tokens: 200,
      average_latency_ms: 2_250,
      latest_started_at_unix_ms: 210,
    };
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/usage/summary" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: requestSearchParam(input, "include_archived") === "true",
            },
            totals: {
              runs: 3,
              session_count: 1,
              active_runs: 1,
              completed_runs: 2,
              prompt_tokens: 120,
              completion_tokens: 80,
              total_tokens: 200,
              average_latency_ms: 2_250,
            },
            timeline: [
              {
                bucket_start_unix_ms: 0,
                bucket_end_unix_ms: 86_400_000,
                runs: 3,
                session_count: 1,
                active_runs: 1,
                completed_runs: 2,
                prompt_tokens: 120,
                completion_tokens: 80,
                total_tokens: 200,
                average_latency_ms: 2_250,
              },
            ],
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: requestSearchParam(input, "include_archived") === "true",
              limit: 8,
              cursor: 0,
            },
            sessions: [usageSession],
            page: { limit: 8, returned: 1, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/agents" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: requestSearchParam(input, "include_archived") === "true",
              limit: 8,
              cursor: 0,
            },
            agents: [
              {
                agent_id: "agent-1",
                display_name: "Primary Agent",
                binding_source: "session_binding",
                default_model_profile: "gpt-5.4",
                session_count: 1,
                runs: 3,
                active_runs: 1,
                completed_runs: 2,
                prompt_tokens: 120,
                completion_tokens: 80,
                total_tokens: 200,
                average_latency_ms: 2_250,
                latest_started_at_unix_ms: 210,
              },
            ],
            page: { limit: 8, returned: 1, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/models" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: requestSearchParam(input, "include_archived") === "true",
              limit: 8,
              cursor: 0,
            },
            models: [
              {
                model_id: "gpt-5.4",
                display_name: "gpt-5.4",
                model_source: "agent_default_model_profile",
                agent_count: 1,
                session_count: 1,
                runs: 3,
                active_runs: 1,
                completed_runs: 2,
                prompt_tokens: 120,
                completion_tokens: 80,
                total_tokens: 200,
                average_latency_ms: 2_250,
                latest_started_at_unix_ms: 210,
              },
            ],
            page: { limit: 8, returned: 1, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: requestSearchParam(input, "include_archived") === "true",
              run_limit: 12,
            },
            session: usageSession,
            totals: {
              runs: 3,
              session_count: 1,
              active_runs: 1,
              completed_runs: 2,
              prompt_tokens: 120,
              completion_tokens: 80,
              total_tokens: 200,
              average_latency_ms: 2_250,
            },
            timeline: [
              {
                bucket_start_unix_ms: 0,
                bucket_end_unix_ms: 86_400_000,
                runs: 3,
                session_count: 1,
                active_runs: 1,
                completed_runs: 2,
                prompt_tokens: 120,
                completion_tokens: 80,
                total_tokens: 200,
                average_latency_ms: 2_250,
              },
            ],
            runs: [
              {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                session_id: usageSession.session_id,
                state: "completed",
                total_tokens: 200,
                started_at_unix_ms: 100,
                completed_at_unix_ms: 2_350,
                updated_at_unix_ms: 2_350,
              },
            ],
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            sessions: [
              {
                ...usageSession,
                title: "Usage session",
                title_source: "label",
                preview: "latest usage snapshot",
                preview_state: "present",
                last_intent: "inspect usage",
                last_intent_state: "present",
                last_summary: "Top usage session",
                last_summary_state: "present",
                branch_state: "linear",
                last_run_state: "completed",
                last_run_started_at_unix_ms: 210,
                pending_approvals: 0,
              },
            ],
            summary: {
              active_sessions: 1,
              archived_sessions: 0,
              sessions_with_pending_approvals: 0,
              sessions_with_active_runs: 1,
            },
            query: {
              limit: 50,
              cursor: 0,
              include_archived: false,
              sort: "updated_desc",
            },
            page: { limit: 50, returned: 1, has_more: false },
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Usage and Capacity" }));

    expect(await screen.findByRole("heading", { name: "Usage" })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Usage session" })).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Open in sessions" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("switch", { name: /Show archived/i }));

    await waitFor(() => {
      expect(
        fetchMock.mock.calls.some(
          (entry) =>
            Array.isArray(entry) &&
            requestUrl(entry[0] as RequestInfo | URL) === "/console/v1/usage/summary" &&
            requestSearchParam(entry[0] as RequestInfo | URL, "include_archived") === "true",
        ),
      ).toBe(true);
    });

    fireEvent.click(screen.getByRole("button", { name: "Export timeline CSV" }));
    expect(openMock).toHaveBeenCalled();
    const exportUrl = openMock.mock.calls[0]?.[0];
    expect(exportUrl).toContain("/console/v1/usage/export?");
    expect(exportUrl).toContain("dataset=timeline");
    expect(exportUrl).toContain("format=csv");

    fireEvent.click(screen.getByRole("button", { name: "Open in sessions" }));
    expect(await screen.findByRole("heading", { name: "Sessions" })).toBeInTheDocument();
  });

  it("creates a session checkpoint from the sessions section with a CSRF-protected request", async () => {
    const session = {
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      session_key: "phase4-session",
      session_label: "Phase 4 Session",
      title: "Phase 4 Session",
      title_source: "label",
      preview: "checkpoint-ready session",
      preview_state: "present",
      last_intent: "checkpoint this",
      last_intent_state: "present",
      last_summary: "Session detail ready",
      last_summary_state: "present",
      branch_state: "active_branch",
      branch_origin_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
      principal: "admin:web-console",
      device_id: "device-1",
      created_at_unix_ms: 100,
      updated_at_unix_ms: 200,
      last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      last_run_state: "completed",
      last_run_started_at_unix_ms: 190,
      prompt_tokens: 120,
      completion_tokens: 80,
      total_tokens: 200,
      pending_approvals: 0,
      archived: false,
    };
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
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
            page: { limit: 50, returned: 1, has_more: false },
          }),
        );
      }

      if (
        path === "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/checkpoints" &&
        method === "POST"
      ) {
        return Promise.resolve(
          jsonResponse({
            session,
            checkpoint: {
              checkpoint_id: "checkpoint-1",
              session_id: session.session_id,
              run_id: session.last_run_id,
              name: "Phase 4 Session checkpoint",
              tags_json: '["web-console","sessions-section"]',
              note: "Created from the Sessions console on 4/2/2026, 3:00:00 PM.",
              branch_state: "active_branch",
              parent_session_id: undefined,
              referenced_compaction_ids_json: "[]",
              workspace_paths_json: "[]",
              created_by_principal: "admin:web-console",
              created_at_unix_ms: 300,
              restore_count: 0,
            },
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Session Catalog" }));
    expect(await screen.findByRole("heading", { name: "Sessions" })).toBeInTheDocument();

    fireEvent.click(await screen.findByRole("button", { name: "Create checkpoint" }));

    await waitFor(() => {
      expect(document.body).toHaveTextContent("Checkpoint created: Phase 4 Session checkpoint.");
    });

    const [, request] = findRequestCall(
      fetchMock,
      "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/checkpoints",
      "POST",
    );
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestBody(request?.body)).toContain('"name":"Phase 4 Session checkpoint"');
  });

  it("shows recent compactions and checkpoints in the sessions detail continuity surface", async () => {
    const session = {
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      session_key: "phase4-session",
      session_label: "Phase 4 Session",
      title: "Phase 4 Session",
      title_source: "label",
      preview: "continuity-ready session",
      preview_state: "present",
      last_intent: "compact this",
      last_intent_state: "present",
      last_summary: "Continuity detail ready",
      last_summary_state: "present",
      branch_state: "active_branch",
      branch_origin_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
      principal: "admin:web-console",
      device_id: "device-1",
      created_at_unix_ms: 100,
      updated_at_unix_ms: 200,
      last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
      last_run_state: "completed",
      last_run_started_at_unix_ms: 190,
      prompt_tokens: 120,
      completion_tokens: 80,
      total_tokens: 200,
      pending_approvals: 0,
      archived: false,
    };
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
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
            page: { limit: 50, returned: 1, has_more: false },
          }),
        );
      }

      if (
        path === "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/transcript" &&
        method === "GET"
      ) {
        return Promise.resolve(
          jsonResponse({
            session,
            records: [],
            attachments: [],
            derived_artifacts: [],
            pins: [],
            compactions: [
              {
                artifact_id: "artifact-1",
                session_id: session.session_id,
                run_id: session.last_run_id,
                mode: "manual",
                strategy: "session_window_v1",
                compressor_version: "v1",
                trigger_reason: "manual_apply",
                summary_text: "summary",
                summary_preview: "Continuity planner preserved the release gate state.",
                source_event_count: 20,
                protected_event_count: 5,
                condensed_event_count: 12,
                omitted_event_count: 3,
                estimated_input_tokens: 1000,
                estimated_output_tokens: 700,
                source_records_json: "[]",
                summary_json:
                  '{"lifecycle_state":"applied_with_pending_review","planner":{"review_candidate_count":1},"writes":[{"target_path":"MEMORY.md"}]}',
                created_by_principal: "admin:web-console",
                created_at_unix_ms: 300,
              },
            ],
            checkpoints: [
              {
                checkpoint_id: "checkpoint-1",
                session_id: session.session_id,
                run_id: session.last_run_id,
                name: "Phase 4 checkpoint",
                tags_json: "[]",
                note: "Created after compaction.",
                branch_state: "active_branch",
                parent_session_id: undefined,
                referenced_compaction_ids_json: '["artifact-1"]',
                workspace_paths_json: '["MEMORY.md"]',
                created_by_principal: "admin:web-console",
                created_at_unix_ms: 310,
                restore_count: 0,
              },
            ],
            queued_inputs: [],
            runs: [],
            background_tasks: [],
            contract: { contract_version: "control-plane.v1" },
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Session Catalog" }));
    expect(await screen.findByRole("heading", { name: "Sessions" })).toBeInTheDocument();

    expect(await screen.findByRole("heading", { name: "Recent compactions" })).toBeInTheDocument();
    expect(document.body).toHaveTextContent("applied with pending review · 1 write · 1 review");
    expect(document.body).toHaveTextContent("Continuity planner preserved the release gate state.");
    expect(document.body).toHaveTextContent("Phase 4 checkpoint");
    expect(document.body).toHaveTextContent("Created after compaction.");
  });

  it("keeps empty usage states stable and surfaces refresh errors", async () => {
    let usageSummaryCalls = 0;
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 300,
          }),
        );
      }

      if (path === "/console/v1/usage/summary" && method === "GET") {
        usageSummaryCalls += 1;
        if (usageSummaryCalls > 1) {
          return Promise.resolve(jsonResponse({ error: "usage refresh failed" }, 500));
        }
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: false,
            },
            totals: {
              runs: 0,
              session_count: 0,
              active_runs: 0,
              completed_runs: 0,
              prompt_tokens: 0,
              completion_tokens: 0,
              total_tokens: 0,
            },
            timeline: [],
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: false,
              limit: 8,
              cursor: 0,
            },
            sessions: [],
            page: { limit: 8, returned: 0, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/agents" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: false,
              limit: 8,
              cursor: 0,
            },
            agents: [],
            page: { limit: 8, returned: 0, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      if (path === "/console/v1/usage/models" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            contract: { contract_version: "control-plane.v1" },
            query: {
              start_at_unix_ms: 0,
              end_at_unix_ms: 100,
              bucket: "day",
              bucket_width_ms: 86_400_000,
              include_archived: false,
              limit: 8,
              cursor: 0,
            },
            models: [],
            page: { limit: 8, returned: 0, has_more: false },
            cost_tracking_available: false,
          }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Usage and Capacity" }));

    expect(await screen.findByRole("heading", { name: "Usage" })).toBeInTheDocument();
    expect(await screen.findByText("No timeline data")).toBeInTheDocument();
    expect(await screen.findByText("No session selected")).toBeInTheDocument();
    expect((await screen.findAllByText("No data")).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Refresh usage" }));

    await waitFor(() => {
      expect(screen.getByText("usage refresh failed")).toBeInTheDocument();
    });
  });

  it("loads diagnostics snapshot in dedicated diagnostics section", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { kind: "openai-compatible" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { summary: { total_profiles: 1 } },
        browserd: { enabled: true, sessions: { active: 0 } },
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics" })).toBeInTheDocument();
    expect((await screen.findAllByText("Browser service")).length).toBeGreaterThan(0);
    expect(findRequestCall(fetchMock, "/console/v1/diagnostics", "GET")).toBeDefined();
  });

  it("keeps the memory section stable when learning and workspace lists are empty", async () => {
    const fetchMock = withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
      const path = requestUrl(input);
      const method = (init?.method ?? "GET").toUpperCase();

      if (path === "/console/v1/auth/session" && method === "GET") {
        return jsonResponse({
          principal: "admin:web-console",
          device_id: "device-1",
          channel: "web",
          csrf_token: "csrf-1",
          issued_at_unix_ms: 100,
          expires_at_unix_ms: 300,
        });
      }

      if (path === "/console/v1/memory/status" && method === "GET") {
        return jsonResponse({
          roots: [],
          curated_paths: [],
          recent_documents: [],
          learning: { enabled: true, counters: {} },
        });
      }

      if (path === "/console/v1/memory/workspace/documents" && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          documents: [],
          roots: [],
        });
      }

      if (path === "/console/v1/memory/learning/candidates" && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
        });
      }

      if (path === "/console/v1/memory/preferences" && method === "GET") {
        return jsonResponse({
          contract: { contract_version: "control-plane.v1" },
          preferences: [],
        });
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Memory" }));

    expect(await screen.findByRole("heading", { name: "Memory" })).toBeInTheDocument();
  });

  it("redacts sensitive diagnostics values in the web console by default", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300,
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { openai_api_key: "sk-live-super-secret" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { profiles: [{ access_token: "oauth-secret" }] },
        browserd: {
          relay_token: "relay-secret",
          last_error: "Bearer browser-secret",
        },
      }),
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics" })).toBeInTheDocument();

    await waitFor(() => {
      const rendered = document.body.textContent ?? "";
      expect(rendered).not.toContain("sk-live-super-secret");
      expect(rendered).not.toContain("oauth-secret");
      expect(rendered).not.toContain("relay-secret");
      expect(rendered).not.toContain("browser-secret");
    });
  });
});

function createQueuedFetch(responses: Response[]) {
  return vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const baseline = routeM56BaselineRequest(input, init);
    if (baseline !== undefined) {
      return Promise.resolve(baseline);
    }
    const response = responses.shift();
    if (response === undefined) {
      throw new Error("No mocked response queued.");
    }
    return Promise.resolve(response);
  });
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}

function requestUrl(input: RequestInfo | URL): string {
  const raw =
    typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  try {
    // Normalize request URL shape across runtimes (relative path vs absolute URL).
    return new URL(raw, "http://localhost").pathname;
  } catch {
    return raw;
  }
}

function requestSearchParam(input: RequestInfo | URL, key: string): string | null {
  const raw =
    typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  try {
    return new URL(raw, "http://localhost").searchParams.get(key);
  } catch {
    return null;
  }
}

function requestBody(body: BodyInit | null | undefined): string {
  if (typeof body === "string") {
    return body;
  }
  return "";
}

function withM56Baseline(
  handler: (input: RequestInfo | URL, init?: RequestInit) => Response | Promise<Response>,
) {
  return vi.fn((input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const baseline = routeM56BaselineRequest(input, init);
    if (baseline !== undefined) {
      return Promise.resolve(baseline);
    }
    return Promise.resolve(handler(input, init));
  });
}

function routeM56BaselineRequest(
  input: RequestInfo | URL,
  init?: RequestInit,
): Response | undefined {
  const path = requestUrl(input);
  const method = (init?.method ?? "GET").toUpperCase();

  if (method !== "GET") {
    return undefined;
  }
  if (path === "/console/v1/control-plane/capabilities") {
    return jsonResponse(capabilityCatalogFixture());
  }
  if (path === "/console/v1/deployment/posture") {
    return jsonResponse(deploymentPostureFixture());
  }
  if (path === "/console/v1/support-bundle/jobs") {
    return jsonResponse(supportBundleJobsFixture());
  }
  if (path === "/console/v1/audit/events") {
    return jsonResponse(auditEventsFixture());
  }
  if (path === "/console/v1/usage/insights") {
    return jsonResponse({
      contract: { contract_version: "control-plane.v1" },
      query: {
        start_at_unix_ms: 0,
        end_at_unix_ms: 100,
        bucket: "day",
        bucket_width_ms: 86_400_000,
        include_archived: false,
      },
      totals: {
        runs: 0,
        session_count: 0,
        active_runs: 0,
        completed_runs: 0,
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
      },
      timeline: [],
      pricing: {
        known_entries: 1,
        estimated_models: 1,
        estimate_only: true,
      },
      health: {
        provider_state: "ok",
        provider_kind: "deterministic",
        error_rate_bps: 0,
        circuit_open: false,
        cooldown_ms: 0,
        avg_latency_ms: 0,
        recent_routing_overrides: 0,
      },
      routing: {
        default_mode: "suggest",
        suggest_runs: 0,
        dry_run_runs: 0,
        enforced_runs: 0,
        overrides: 0,
        recent_decisions: [],
      },
      budgets: {
        policies: [],
        evaluations: [],
      },
      alerts: [],
      model_mix: [],
      scope_mix: [],
      tool_mix: [],
      cost_tracking_available: false,
    });
  }
  return undefined;
}

function findRequestCall(
  fetchMock: { mock: { calls: unknown[] } },
  path: string,
  method: string,
): [RequestInfo | URL, RequestInit | undefined] {
  const match = fetchMock.mock.calls.find(
    (entry): entry is [RequestInfo | URL, RequestInit | undefined] => {
      if (!Array.isArray(entry) || entry.length === 0) {
        return false;
      }

      const [input, init] = entry as [unknown, unknown];
      const validInput =
        typeof input === "string" ||
        input instanceof URL ||
        (typeof Request !== "undefined" && input instanceof Request);
      if (!validInput) {
        return false;
      }
      if (init !== undefined && (typeof init !== "object" || init === null)) {
        return false;
      }

      const typedInit = init as RequestInit | undefined;
      return (
        requestUrl(input as RequestInfo | URL) === path &&
        (typedInit?.method ?? "GET").toUpperCase() === method
      );
    },
  );
  expect(match).toBeDefined();
  if (match === undefined) {
    throw new Error(`Missing mocked request: ${method} ${path}`);
  }
  return match;
}
