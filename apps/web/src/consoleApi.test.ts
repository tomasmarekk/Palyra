import { describe, expect, it } from "vitest";

import { ConsoleApiClient } from "./consoleApi";

describe("ConsoleApiClient", () => {
  it("uses CSRF token for mutating requests after login", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200
      }),
      jsonResponse({ jobs: [] }),
      jsonResponse({ job: { job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV" } })
    ];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("No response queued for fetch mock.");
      }
      return Promise.resolve(response);
    };

    const client = new ConsoleApiClient("", fetcher);
    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web"
    });

    await client.listCronJobs();
    await client.createCronJob({
      name: "nightly",
      prompt: "run nightly",
      schedule_type: "every",
      every_interval_ms: 60000
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/cron/jobs");
    const getHeaders = new Headers(calls[1]?.init?.headers);
    expect(getHeaders.get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/cron/jobs");
    const postHeaders = new Headers(calls[2]?.init?.headers);
    expect(postHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(calls[2]?.init?.credentials).toBe("include");
  });

  it("fails closed when CSRF token is missing for mutating request", async () => {
    const fetcher: typeof fetch = () => {
      return Promise.resolve(jsonResponse({ jobs: [] }));
    };
    const client = new ConsoleApiClient("", fetcher);

    await expect(
      client.createCronJob({
        name: "nightly",
        prompt: "run nightly",
        schedule_type: "every",
        every_interval_ms: 60000
      })
    ).rejects.toThrow("Missing CSRF token");
  });

  it("propagates structured backend errors", async () => {
    const fetcher: typeof fetch = () => {
      return Promise.resolve(jsonResponse({ error: "permission denied" }, 403));
    };
    const client = new ConsoleApiClient("", fetcher);

    await expect(client.getSession()).rejects.toThrow("permission denied");
  });
});

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json"
    }
  });
}

function requestUrl(input: RequestInfo | URL | undefined): string {
  if (input === undefined) {
    return "";
  }
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  return input.url;
}
