import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import {
  auditEventsFixture,
  capabilityCatalogFixture,
  deploymentPostureFixture,
  supportBundleJobsFixture
} from "./console/__fixtures__/m56ControlPlane";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("M35 web console app", () => {
  it("requires authentication before showing privileged pages", async () => {
    const fetchMock = createQueuedFetch([jsonResponse({ error: "missing session" }, 403)]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Approvals" })).not.toBeInTheDocument();
  });

  it("clears operator-scoped state on sign-out before next sign-in refresh completes", async () => {
    const delayedApprovals = createDeferredResponse();
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
            expires_at_unix_ms: 300
          })
        );
      }

      if (path === "/console/v1/approvals" && method === "GET") {
        if (activePrincipal === "admin:user-a") {
          return Promise.resolve(
            jsonResponse({
              approvals: [{ approval_id: "APPROVAL-A", subject_type: "tool", decision: "pending" }]
            })
          );
        }
        return delayedApprovals.promise;
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
            expires_at_unix_ms: 400
          })
        );
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Approvals" }));
    await waitFor(() => {
      expect(screen.getByText("APPROVAL-A")).toBeInTheDocument();
    }, { timeout: 5000 });

    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
    expect(await screen.findByRole("heading", { name: "Operator Dashboard" })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Admin token"), { target: { value: "token-b" } });
    fireEvent.change(screen.getByLabelText("Principal"), { target: { value: "admin:user-b" } });
    fireEvent.change(screen.getByLabelText("Device ID"), { target: { value: "device-b" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Web Dashboard Operator Surface" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Approvals" }));
    expect(screen.queryByText("APPROVAL-A")).not.toBeInTheDocument();
    expect(screen.getByText("No approvals found.")).toBeInTheDocument();

    delayedApprovals.resolve(
      jsonResponse({
        approvals: [{ approval_id: "APPROVAL-B", subject_type: "tool", decision: "pending" }]
      })
    );

    expect(await screen.findByText("APPROVAL-B")).toBeInTheDocument();
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
            expires_at_unix_ms: 300
          })
        );
      }

      if (path === "/console/v1/approvals" && method === "GET") {
        return Promise.resolve(
          jsonResponse({
            approvals: [{ approval_id: "A1", subject_type: "tool", decision: approvalDecision }]
          })
        );
      }

      if (path === "/console/v1/approvals/A1/decision" && method === "POST") {
        approvalDecision = "allow";
        return Promise.resolve(jsonResponse({ approval: { approval_id: "A1", decision: "allow" } }));
      }

      throw new Error(`Unhandled mocked request: ${method} ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Approvals" }));
    expect(await screen.findByText("A1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Select" }));
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));

    await waitFor(() => {
      expect(screen.getByText("Approval allowed.")).toBeInTheDocument();
    });

    const decisionCalls = fetchMock.mock.calls.filter(
      (call) => requestUrl(call[0]) === "/console/v1/approvals/A1/decision"
    );
    expect(decisionCalls.length).toBeGreaterThan(0);
    const decisionRequest = decisionCalls[decisionCalls.length - 1][1];
    const headers = new Headers(decisionRequest?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("supports cron create and disable workflow from UI", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({ jobs: [{ job_id: "J1", name: "job-one", enabled: true }] }),
      jsonResponse({ job: { job_id: "J2" } }),
      jsonResponse({
        jobs: [
          { job_id: "J1", name: "job-one", enabled: true },
          { job_id: "J2", name: "web-job", enabled: true }
        ]
      }),
      jsonResponse({ job: { job_id: "J1", enabled: false } }),
      jsonResponse({ jobs: [{ job_id: "J1", name: "job-one", enabled: false }] })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Cron" }));
    expect(await screen.findByText("job-one")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "web-job" } });
    fireEvent.change(screen.getByLabelText("Prompt"), { target: { value: "run from web console" } });
    fireEvent.click(screen.getByRole("button", { name: "Create job" }));

    await waitFor(() => {
      expect(screen.getByText("Cron job created.")).toBeInTheDocument();
    });

    const disableButtons = screen.getAllByRole("button", { name: "Disable" });
    fireEvent.click(disableButtons[0]);

    await waitFor(() => {
      expect(screen.getByText("Cron job disabled.")).toBeInTheDocument();
    });

    const [, createRequest] = findRequestCall(fetchMock, "/console/v1/cron/jobs", "POST");
    expect(createRequest?.method).toBe("POST");

    const [, toggleRequest] = findRequestCall(fetchMock, "/console/v1/cron/jobs/J1/enabled", "POST");
    expect(toggleRequest?.method).toBe("POST");
    expect(requestBody(toggleRequest?.body)).toContain("\"enabled\":false");
  });

  it("manages channel connectors from channels section with CSRF-protected enable toggle", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "echo:default",
            kind: "echo",
            availability: "internal_test_only",
            enabled: true,
            readiness: "ready",
            liveness: "running",
            queue_depth: { pending_outbox: 0, dead_letters: 1 }
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          availability: "internal_test_only",
          enabled: true,
          readiness: "ready",
          liveness: "running",
          queue_depth: { pending_outbox: 0, dead_letters: 1 }
        }
      }),
      jsonResponse({
        events: [
          {
            event_id: 1,
            connector_id: "echo:default",
            event_type: "outbox.retry",
            level: "warn",
            message: "retry scheduled",
            created_at_unix_ms: 111
          }
        ],
        dead_letters: [
          {
            dead_letter_id: 1,
            connector_id: "echo:default",
            envelope_id: "env-1:0",
            reason: "permanent",
            payload: { text: "failed" },
            created_at_unix_ms: 112
          }
        ]
      }),
      jsonResponse({
        config: {
          enabled: true,
          default_direct_message_policy: "deny",
          channels: [{ channel: "echo:default", enabled: true }]
        },
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        warnings: [],
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        pairings: [],
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          availability: "internal_test_only",
          enabled: false,
          readiness: "ready",
          liveness: "stopped",
          queue_depth: { pending_outbox: 0, dead_letters: 1 }
        }
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "echo:default",
            kind: "echo",
            availability: "internal_test_only",
            enabled: false,
            readiness: "ready",
            liveness: "stopped",
            queue_depth: { pending_outbox: 0, dead_letters: 1 }
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          availability: "internal_test_only",
          enabled: false,
          readiness: "ready",
          liveness: "stopped",
          queue_depth: { pending_outbox: 0, dead_letters: 1 }
        }
      }),
      jsonResponse({
        events: [],
        dead_letters: []
      }),
      jsonResponse({
        config: {
          enabled: true,
          default_direct_message_policy: "deny",
          channels: [{ channel: "echo:default", enabled: true }]
        },
        config_hash: "router-hash-2"
      }),
      jsonResponse({
        warnings: [],
        config_hash: "router-hash-2"
      }),
      jsonResponse({
        pairings: [],
        config_hash: "router-hash-2"
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));
    expect(await screen.findByRole("heading", { name: "Channels and Router" })).toBeInTheDocument();
    expect(await screen.findByText("echo:default")).toBeInTheDocument();
    expect(await screen.findByText("internal_test_only")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Disable" }));

    await waitFor(() => {
      expect(screen.getByText("Connector disabled.")).toBeInTheDocument();
    });

    const [, request] = findRequestCall(fetchMock, "/console/v1/channels/echo%3Adefault/enabled", "POST");
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(request?.method).toBe("POST");
    expect(requestBody(request?.body)).toContain("\"enabled\":false");
  });

  it("hides deferred connectors from channels section and selects the first visible connector", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "slack:default",
            kind: "slack",
            availability: "deferred",
            enabled: false,
            readiness: "misconfigured",
            liveness: "stopped",
            queue_depth: { pending_outbox: 0, dead_letters: 0 }
          },
          {
            connector_id: "echo:default",
            kind: "echo",
            availability: "internal_test_only",
            enabled: true,
            readiness: "ready",
            liveness: "running",
            queue_depth: { pending_outbox: 0, dead_letters: 0 }
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          availability: "internal_test_only",
          enabled: true,
          readiness: "ready",
          liveness: "running",
          queue_depth: { pending_outbox: 0, dead_letters: 0 }
        }
      }),
      jsonResponse({ events: [], dead_letters: [] }),
      jsonResponse({
        config: {
          enabled: true,
          default_direct_message_policy: "deny",
          channels: [{ channel: "echo:default", enabled: true }]
        },
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        warnings: [],
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        pairings: [],
        config_hash: "router-hash-1"
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));

    expect(await screen.findByText("echo:default")).toBeInTheDocument();
    expect(screen.queryByText("slack:default")).not.toBeInTheDocument();
    expect(await screen.findByText("internal_test_only")).toBeInTheDocument();
    expect(findRequestCall(fetchMock, "/console/v1/channels/echo%3Adefault", "GET")).toBeDefined();
  });

  it("runs discord onboarding preflight from channels wizard with CSRF-protected request", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "discord:default",
            kind: "discord",
            availability: "supported",
            enabled: false,
            readiness: "missing_credential",
            liveness: "stopped",
            queue_depth: { pending_outbox: 0, dead_letters: 0 }
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "discord:default",
          kind: "discord",
          availability: "supported",
          enabled: false,
          readiness: "missing_credential",
          liveness: "stopped",
          queue_depth: { pending_outbox: 0, dead_letters: 0 }
        }
      }),
      jsonResponse({ events: [], dead_letters: [] }),
      jsonResponse({
        config: {
          enabled: true,
          default_direct_message_policy: "deny",
          channels: [{ channel: "discord:default", enabled: true }]
        },
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        warnings: [],
        config_hash: "router-hash-1"
      }),
      jsonResponse({
        pairings: [],
        config_hash: "router-hash-1"
      }),
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
          "Send Messages in Threads"
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
          can_send_messages_in_threads: true
        },
        warnings: [],
        policy_warnings: [],
        routing_preview: { connector_id: "discord:default" },
        invite_url_template: "https://discord.com/oauth2/authorize?client_id=123&scope=bot&permissions=205824"
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "discord:default",
            kind: "discord",
            availability: "supported",
            enabled: false,
            readiness: "missing_credential",
            liveness: "stopped",
            queue_depth: { pending_outbox: 0, dead_letters: 0 }
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "discord:default",
          kind: "discord",
          availability: "supported",
          enabled: false,
          readiness: "missing_credential",
          liveness: "stopped",
          queue_depth: { pending_outbox: 0, dead_letters: 0 }
        }
      }),
      jsonResponse({ events: [], dead_letters: [] }),
      jsonResponse({
        config: {
          enabled: true,
          default_direct_message_policy: "deny",
          channels: [{ channel: "discord:default", enabled: true }]
        },
        config_hash: "router-hash-2"
      }),
      jsonResponse({
        warnings: [],
        config_hash: "router-hash-2"
      }),
      jsonResponse({
        pairings: [],
        config_hash: "router-hash-2"
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Channels and Router" }));
    expect(await screen.findByRole("heading", { name: "Discord onboarding wizard" })).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Bot token"), {
      target: { value: "test-token" }
    });
    fireEvent.change(screen.getByLabelText("Verify channel ID"), {
      target: { value: "123456789012345678" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Run preflight" }));

    await waitFor(() => {
      expect(screen.getByText("Discord preflight OK for palyra-bot (123).")).toBeInTheDocument();
    });
    expect(screen.getByRole("heading", { name: "Preflight highlights" })).toBeInTheDocument();
    expect(screen.getByText("discord.com")).toBeInTheDocument();
    expect(screen.getByText("Attachments ingestion is metadata only by default.")).toBeInTheDocument();

    const [, request] = findRequestCall(fetchMock, "/console/v1/channels/discord/onboarding/probe", "POST");
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(request?.method).toBe("POST");
    expect(requestBody(request?.body)).toContain("\"token\":\"test-token\"");
    expect(requestBody(request?.body)).toContain("\"verify_channel_id\":\"123456789012345678\"");
  });

  it("issues browser relay token from browser section with CSRF protection", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        principal: "admin:web-console",
        active_profile_id: null,
        profiles: []
      }),
      jsonResponse({
        relay_token: "relay-token-abc",
        session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        extension_id: "com.palyra.extension",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 500,
        token_ttl_ms: 300000,
        warning: "short-lived"
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Browser" }));
    expect(await screen.findByRole("heading", { name: "Browser" })).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Session ID")[0], {
      target: { value: "01ARZ3NDEKTSV4RRFFQ69G5FAV" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Mint relay token" }));

    await waitFor(() => {
      expect(screen.getByText("Browser relay token minted. Keep it private and short-lived.")).toBeInTheDocument();
    });

    const [, request] = findRequestCall(fetchMock, "/console/v1/browser/relay/tokens", "POST");
    const headers = new Headers(request?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestBody(request?.body)).toContain("\"extension_id\":\"com.palyra.extension\"");
  });

  it("loads diagnostics snapshot in dedicated diagnostics section", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { kind: "openai-compatible" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { summary: { total_profiles: 1 } },
        browserd: { enabled: true, sessions: { active: 0 } }
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics and Audit" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics and Audit" })).toBeInTheDocument();
    expect(await screen.findByText("Browser service")).toBeInTheDocument();
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
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { openai_api_key: "sk-live-super-secret" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { profiles: [{ access_token: "oauth-secret" }] },
        browserd: {
          relay_token: "relay-secret",
          last_error: "Bearer browser-secret"
        }
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics and Audit" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics and Audit" })).toBeInTheDocument();

    await waitFor(() => {
      const rendered = document.body.textContent ?? "";
      expect(rendered).toContain("[redacted]");
      expect(rendered).not.toContain("sk-live-super-secret");
      expect(rendered).not.toContain("oauth-secret");
      expect(rendered).not.toContain("relay-secret");
      expect(rendered).not.toContain("browser-secret");
    });
  });

  it("streams chat transcript with inline approval controls and CSRF decision dispatch", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({ sessions: [] }),
      jsonResponse({
        session: {
          session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
          session_key: "web",
          principal: "admin:web-console",
          device_id: "device-1",
          channel: "web",
          created_at_unix_ms: 100,
          updated_at_unix_ms: 100
        },
        created: true,
        reset_applied: false
      }),
      ndjsonResponse([
        {
          type: "meta",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        },
        {
          type: "event",
          event: {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            event_type: "model_token",
            model_token: {
              token: "hello from model",
              is_final: false
            }
          }
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
              request_summary: "Needs approval"
            }
          }
        },
        {
          type: "complete",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          status: "done"
        }
      ]),
      jsonResponse({
        sessions: [
          {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            session_key: "web",
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            created_at_unix_ms: 100,
            updated_at_unix_ms: 200,
            last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX"
          }
        ]
      }),
      jsonResponse({ approval: { approval_id: "A1", decision: "allow" } })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Chat and Sessions" }));
    expect(await screen.findByRole("heading", { name: "Chat Workspace" })).toBeInTheDocument();
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
      "POST"
    );
    expect(streamRequest?.method).toBe("POST");

    const [, decisionRequest] = findRequestCall(fetchMock, "/console/v1/approvals/A1/decision", "POST");
    const decisionHeaders = new Headers(decisionRequest?.headers);
    expect(decisionHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("escapes user/model/tool chat payloads and keeps canvas iframe sandboxed", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({
        sessions: [
          {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            session_key: "web",
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            created_at_unix_ms: 100,
            updated_at_unix_ms: 100
          }
        ]
      }),
      ndjsonResponse([
        {
          type: "meta",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        },
        {
          type: "event",
          event: {
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            event_type: "model_token",
            model_token: {
              token: "<img src='x' onerror='alert(1)'>",
              is_final: false
            }
          }
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
                frame_url: "/canvas/v1/frame/01ARZ3NDEKTSV4RRFFQ69G5FB1?token=test-token"
              }
            }
          }
        },
        {
          type: "complete",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          status: "done"
        }
      ]),
      jsonResponse({
        sessions: [
          {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            session_key: "web",
            principal: "admin:web-console",
            device_id: "device-1",
            channel: "web",
            created_at_unix_ms: 100,
            updated_at_unix_ms: 200,
            last_run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX"
          }
        ]
      })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    const rendered = render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Chat and Sessions" }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Send" })).toBeEnabled();
    });

    fireEvent.change(screen.getByLabelText("Message"), {
      target: { value: "<img src='x' onerror='alert(1)'>" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));

    expect(await screen.findByText("<img src='x' onerror='alert(1)'>")).toBeInTheDocument();
    expect(await screen.findByText(/<script>alert\(1\)<\/script>/)).toBeInTheDocument();

    const injectedImage = rendered.container.querySelector("img[src='x']");
    expect(injectedImage).toBeNull();

    const frame = await screen.findByTitle("Canvas 01ARZ3NDEKTSV4RRFFQ69G5FAX");
    expect(frame).toHaveAttribute("sandbox", "allow-scripts allow-same-origin");
  });
});

function createQueuedFetch(responses: Response[]) {
  return withM56Baseline((input: RequestInfo | URL, init?: RequestInit) => {
    void input;
    void init;
    const response = responses.shift();
    if (response === undefined) {
      throw new Error("No mocked response queued.");
    }
    return response;
  });
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json"
    }
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

function requestBody(body: BodyInit | null | undefined): string {
  if (typeof body === "string") {
    return body;
  }
  return "";
}

function withM56Baseline(handler: (input: RequestInfo | URL, init?: RequestInit) => Response | Promise<Response>) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const baseline = routeM56BaselineRequest(input, init);
    if (baseline !== undefined) {
      return baseline;
    }
    return await handler(input, init);
  });
}

function routeM56BaselineRequest(input: RequestInfo | URL, init?: RequestInit): Response | undefined {
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
  method: string
): [RequestInfo | URL, RequestInit | undefined] {
  const match = fetchMock.mock.calls.find((entry): entry is [RequestInfo | URL, RequestInit | undefined] => {
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
    return requestUrl(input as RequestInfo | URL) === path && (typedInit?.method ?? "GET").toUpperCase() === method;
  });
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
      "content-type": "application/x-ndjson"
    }
  });
}

function createDeferredResponse(): {
  promise: Promise<Response>;
  resolve: (response: Response) => void;
  reject: (error: unknown) => void;
} {
  let resolve: (response: Response) => void = () => {};
  let reject: (error: unknown) => void = () => {};
  const promise = new Promise<Response>((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}
