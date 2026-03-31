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
  vi.unstubAllGlobals();
});

describe("M35 web console app", () => {
  it("requires authentication before showing privileged pages", async () => {
    window.localStorage.removeItem("palyra.console.theme");
    const fetchMock = createQueuedFetch([jsonResponse({ error: "missing session" }, 403)]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    await waitFor(
      () => {
        expect(screen.getByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();
      },
      { timeout: 4_000 },
    );
    expect(screen.queryByRole("button", { name: "Approvals" })).not.toBeInTheDocument();
    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
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

  it("supports cron create and disable workflow from UI", async () => {
    const cronJobs = [{ job_id: "J1", name: "job-one", enabled: true }];
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

      if (path === "/console/v1/cron/jobs" && method === "GET") {
        return Promise.resolve(jsonResponse({ jobs: cronJobs }));
      }

      if (path === "/console/v1/cron/jobs" && method === "POST") {
        cronJobs.push({ job_id: "J2", name: "web-job", enabled: true });
        return Promise.resolve(jsonResponse({ job: { job_id: "J2" } }));
      }

      if (path === "/console/v1/cron/jobs/J1/enabled" && method === "POST") {
        cronJobs[0] = { ...cronJobs[0], enabled: false };
        return Promise.resolve(jsonResponse({ job: { job_id: "J1", enabled: false } }));
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Cron" }));
    expect((await screen.findAllByText("job-one")).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "New automation" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "web-job" } });
    fireEvent.change(screen.getByLabelText("Prompt"), {
      target: { value: "run from web console" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create automation" }));

    await waitFor(() => {
      expect(screen.getByText("Cron job created.")).toBeInTheDocument();
    });

    fireEvent.click((await screen.findAllByRole("button", { name: /^Disable / }))[0]);

    await waitFor(() => {
      expect(screen.getByText("Cron job disabled.")).toBeInTheDocument();
    });

    const [, createRequest] = findRequestCall(fetchMock, "/console/v1/cron/jobs", "POST");
    expect(createRequest?.method).toBe("POST");

    const [, toggleRequest] = findRequestCall(
      fetchMock,
      "/console/v1/cron/jobs/J1/enabled",
      "POST",
    );
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

    fireEvent.change(screen.getAllByLabelText("Session ID")[1], {
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

  it("streams chat transcript with inline approval controls and CSRF decision dispatch", async () => {
    const sessionRecord = {
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      session_key: "web",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
      created_at_unix_ms: 100,
      updated_at_unix_ms: 100,
    };
    let sessions = [] as Array<typeof sessionRecord & { last_run_id?: string }>;
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

      if (path === "/console/v1/chat/sessions" && method === "GET") {
        return Promise.resolve(jsonResponse({ sessions }));
      }

      if (path === "/console/v1/chat/sessions" && method === "POST") {
        sessions = [sessionRecord];
        return Promise.resolve(
          jsonResponse({
            session: sessionRecord,
            created: true,
            reset_applied: false,
          }),
        );
      }

      if (
        path === "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream" &&
        method === "POST"
      ) {
        sessions = [
          { ...sessionRecord, updated_at_unix_ms: 200, last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX" },
        ];
        return Promise.resolve(
          ndjsonResponse([
            {
              type: "meta",
              run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            },
            {
              type: "event",
              event: {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                event_type: "model_token",
                model_token: {
                  token: "hello from model",
                  is_final: false,
                },
              },
            },
            {
              type: "event",
              event: {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                event_type: "tool_approval_request",
                tool_approval_request: {
                  proposal_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
                  approval_id: "A1",
                  tool_name: "palyra.fs.apply_patch",
                  request_summary: "Needs approval",
                },
              },
            },
            {
              type: "complete",
              run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
              status: "done",
            },
          ]),
        );
      }

      if (path === "/console/v1/approvals/A1/decision" && method === "POST") {
        return Promise.resolve(
          jsonResponse({ approval: { approval_id: "A1", decision: "allow" } }),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Chat and Sessions" }));
    expect(await screen.findByText("Conversation rail")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Send" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Message"), { target: { value: "run task" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));

    expect(await screen.findByText("hello from model")).toBeInTheDocument();
    expect(await screen.findByText("Needs approval")).toBeInTheDocument();

    const approveButtons = screen.getAllByRole("button", { name: "Approve" });
    fireEvent.click(approveButtons[0]);

    await waitFor(() => {
      expect(screen.getByText("Approval A1 allowed.")).toBeInTheDocument();
    });

    const [, streamRequest] = findRequestCall(
      fetchMock,
      "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream",
      "POST",
    );
    expect(streamRequest?.method).toBe("POST");

    const [, decisionRequest] = findRequestCall(
      fetchMock,
      "/console/v1/approvals/A1/decision",
      "POST",
    );
    const decisionHeaders = new Headers(decisionRequest?.headers);
    expect(decisionHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("escapes user/model/tool chat payloads and keeps canvas iframe sandboxed", async () => {
    const sessionRecord = {
      session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      session_key: "web",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
      created_at_unix_ms: 100,
      updated_at_unix_ms: 100,
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

      if (path === "/console/v1/chat/sessions" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            sessions: [
              {
                ...sessionRecord,
                updated_at_unix_ms: 200,
                last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
              },
            ],
          }),
        );
      }

      if (
        path === "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream" &&
        method === "POST"
      ) {
        return Promise.resolve(
          ndjsonResponse([
            {
              type: "meta",
              run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
              session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            },
            {
              type: "event",
              event: {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                event_type: "model_token",
                model_token: {
                  token: "<img src='x' onerror='alert(1)'>",
                  is_final: false,
                },
              },
            },
            {
              type: "event",
              event: {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                event_type: "tool_result",
                tool_result: {
                  proposal_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0",
                  success: true,
                  output_json: {
                    payload: "<script>alert(1)</script>",
                    frame_url: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token",
                    malicious_frame_url: "/canvas/v1/frame/../../console/v1/diagnostics?token=evil",
                  },
                },
              },
            },
            {
              type: "complete",
              run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
              status: "done",
            },
          ]),
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    const rendered = render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Chat and Sessions" }));
    expect(await screen.findByText("Conversation rail")).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Send" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Message"), {
      target: { value: "<img src='x' onerror='alert(1)'>" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));

    expect(await screen.findByText("<img src='x' onerror='alert(1)'>")).toBeInTheDocument();
    expect(document.body.textContent ?? "").toContain("alert(1)");

    const injectedImage = rendered.container.querySelector("img[src='x']");
    expect(injectedImage).toBeNull();

    const frame = await screen.findByTitle("Canvas 01ARZ3NDEKTSV4RRFFQ69G5FAX");
    expect(frame).toHaveAttribute("sandbox", "allow-scripts allow-same-origin");
    expect(frame).toHaveAttribute(
      "src",
      "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token",
    );
    expect(
      rendered.container.querySelector("iframe[src='/console/v1/diagnostics?token=evil']"),
    ).toBeNull();
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

function ndjsonResponse(lines: unknown[]): Response {
  const body = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/x-ndjson",
    },
  });
}
