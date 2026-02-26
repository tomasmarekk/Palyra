import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("M35 web console app", () => {
  it("requires authentication before showing privileged pages", async () => {
    const fetchMock = createQueuedFetch([jsonResponse({ error: "missing session" }, 403)]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Operator Console" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Approvals" })).not.toBeInTheDocument();
  });

  it("executes approval decision flow with CSRF-protected request", async () => {
    const fetchMock = createQueuedFetch([
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        channel: "web",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 300
      }),
      jsonResponse({ approvals: [{ approval_id: "A1", subject_type: "tool", decision: "pending" }] }),
      jsonResponse({ approval: { approval_id: "A1", decision: "allow" } }),
      jsonResponse({ approvals: [{ approval_id: "A1", subject_type: "tool", decision: "allow" }] })
    ]);
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    expect(await screen.findByText("A1")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Select" }));
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));

    await waitFor(() => {
      expect(screen.getByText("Approval allowed.")).toBeInTheDocument();
    });

    expect(fetchMock.mock.calls).toHaveLength(4);
    expect(requestUrl(fetchMock.mock.calls[2][0])).toBe("/console/v1/approvals/A1/decision");
    const decisionRequest = fetchMock.mock.calls[2][1];
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
      jsonResponse({ approvals: [] }),
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

    expect(requestUrl(fetchMock.mock.calls[3][0])).toBe("/console/v1/cron/jobs");
    const createRequest = fetchMock.mock.calls[3][1];
    expect(createRequest?.method).toBe("POST");

    expect(requestUrl(fetchMock.mock.calls[5][0])).toBe("/console/v1/cron/jobs/J1/enabled");
    const toggleRequest = fetchMock.mock.calls[5][1];
    expect(toggleRequest?.method).toBe("POST");
    expect(requestBody(toggleRequest?.body)).toContain("\"enabled\":false");
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
      jsonResponse({ approvals: [] }),
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
    expect(await screen.findByRole("heading", { name: "Browser Profiles + Relay" })).toBeInTheDocument();

    fireEvent.change(screen.getByPlaceholderText("01ARZ3NDEKTSV4RRFFQ69G5FAV"), {
      target: { value: "01ARZ3NDEKTSV4RRFFQ69G5FAV" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Mint relay token" }));

    await waitFor(() => {
      expect(screen.getByText("Browser relay token minted. Keep it private and short-lived.")).toBeInTheDocument();
    });

    expect(requestUrl(fetchMock.mock.calls[3][0])).toBe("/console/v1/browser/relay/tokens");
    const request = fetchMock.mock.calls[3][1];
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
      jsonResponse({ approvals: [] }),
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
    fireEvent.click(await screen.findByRole("button", { name: "Diagnostics" }));
    expect(await screen.findByRole("heading", { name: "Diagnostics" })).toBeInTheDocument();
    expect(await screen.findByText("Model Provider + Rate Limits")).toBeInTheDocument();
    expect(requestUrl(fetchMock.mock.calls[2][0])).toBe("/console/v1/diagnostics");
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
      jsonResponse({ approvals: [] }),
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
    fireEvent.click(await screen.findByRole("button", { name: "Chat" }));
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

    expect(requestUrl(fetchMock.mock.calls[4][0])).toBe(
      "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream"
    );
    expect(requestUrl(fetchMock.mock.calls[6][0])).toBe("/console/v1/approvals/A1/decision");
    const decisionHeaders = new Headers(fetchMock.mock.calls[6][1]?.headers);
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
      jsonResponse({ approvals: [] }),
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
    fireEvent.click(await screen.findByRole("button", { name: "Chat" }));
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
  return vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    void input;
    void init;
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
      "content-type": "application/json"
    }
  });
}

function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  return input.url;
}

function requestBody(body: BodyInit | null | undefined): string {
  if (typeof body === "string") {
    return body;
  }
  return "";
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
