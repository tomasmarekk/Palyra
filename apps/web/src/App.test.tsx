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
