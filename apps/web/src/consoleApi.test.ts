import { describe, expect, it } from "vitest";

import { ConsoleApiClient, type ChatStreamLine } from "./consoleApi";

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

  it("sends relay action with bearer token and no CSRF requirement", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      return Promise.resolve(
        jsonResponse({
          success: true,
          action: "capture_selection",
          error: "",
          result: {
            selection: {
              selector: "body",
              selected_text: "ok",
              truncated: false
            }
          }
        })
      );
    };
    const client = new ConsoleApiClient("", fetcher);

    await client.relayBrowserAction(
      {
        session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        extension_id: "com.palyra.extension",
        action: "capture_selection",
        capture_selection: {
          selector: "body",
          max_selection_bytes: 128
        }
      },
      "relay-token-1"
    );

    expect(requestUrl(calls[0]?.input)).toBe("/console/v1/browser/relay/actions");
    const headers = new Headers(calls[0]?.init?.headers);
    expect(headers.get("authorization")).toBe("Bearer relay-token-1");
    expect(headers.get("x-palyra-csrf-token")).toBeNull();
  });

  it("loads diagnostics snapshot without requiring CSRF header", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { kind: "openai-compatible" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { summary: { total_profiles: 1 } },
        browserd: { enabled: true }
      })
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
    await client.getDiagnostics();

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/diagnostics");
    const headers = new Headers(calls[1]?.init?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBeNull();
  });

  it("lists chat sessions and streams NDJSON responses with CSRF", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200
      }),
      jsonResponse({
        sessions: [
          {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            session_key: "web",
            principal: "admin:web-console",
            device_id: "device-1",
            created_at_unix_ms: 100,
            updated_at_unix_ms: 150
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
            event_type: "status",
            status: {
              kind: "in_progress",
              message: "ok"
            }
          }
        },
        {
          type: "complete",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          status: "done"
        }
      ])
    ];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("No response queued for fetch mock.");
      }
      return Promise.resolve(response);
    };

    const lines: ChatStreamLine[] = [];
    const client = new ConsoleApiClient("", fetcher);
    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web"
    });

    const sessions = await client.listChatSessions();
    expect(sessions.sessions).toHaveLength(1);

    await client.streamChatMessage(
      "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      {
        text: "hello"
      },
      {
        onLine: (line) => {
          lines.push(line);
        }
      }
    );

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/chat/sessions");
    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream");
    const streamHeaders = new Headers(calls[2]?.init?.headers);
    expect(streamHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(streamHeaders.get("content-type")).toBe("application/json");
    expect(lines).toHaveLength(3);
    expect(lines[0]).toMatchObject({ type: "meta" });
    expect(lines[1]).toMatchObject({ type: "event" });
    expect(lines[2]).toMatchObject({ type: "complete", status: "done" });
  });

  it("fails when stream emits invalid NDJSON line", async () => {
    const fetcher: typeof fetch = (input, init) => {
      if (requestUrl(input) === "/console/v1/auth/login") {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 200
          })
        );
      }
      void init;
      return Promise.resolve(
        new Response("this-is-not-json\n", {
          status: 200,
          headers: {
            "content-type": "application/x-ndjson"
          }
        })
      );
    };
    const client = new ConsoleApiClient("", fetcher);
    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web"
    });

    await expect(
      client.streamChatMessage(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        { text: "hello" },
        { onLine: () => {} }
      )
    ).rejects.toThrow("malformed JSON line");
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

function ndjsonResponse(lines: unknown[]): Response {
  const encoded = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;
  return new Response(encoded, {
    status: 200,
    headers: {
      "content-type": "application/x-ndjson"
    }
  });
}
