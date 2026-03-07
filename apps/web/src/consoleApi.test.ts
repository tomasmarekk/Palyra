import { describe, expect, it, vi } from "vitest";

import {
  ConsoleApiClient,
  ControlPlaneApiError,
  type ChatStreamLine,
  type JsonValue
} from "./consoleApi";

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

  it("uses GET without CSRF and POST with CSRF for channel operations", async () => {
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
        connectors: [
          {
            connector_id: "echo:default",
            kind: "echo",
            enabled: true,
            readiness: "ready",
            liveness: "running"
          }
        ]
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          enabled: false,
          readiness: "ready",
          liveness: "stopped"
        }
      }),
      jsonResponse({
        connector_id: "discord:default",
        bot: {
          id: "123",
          username: "bot"
        },
        warnings: [],
        policy_warnings: []
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
    await client.listChannels();
    await client.setChannelEnabled("echo:default", false);
    await client.probeDiscordOnboarding({
      token: "bot-token"
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/channels");
    const listHeaders = new Headers(calls[1]?.init?.headers);
    expect(listHeaders.get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/channels/echo%3Adefault/enabled");
    const toggleHeaders = new Headers(calls[2]?.init?.headers);
    expect(toggleHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(calls[2]?.init?.method).toBe("POST");

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/channels/discord/onboarding/probe");
    const probeHeaders = new Headers(calls[3]?.init?.headers);
    expect(probeHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(calls[3]?.init?.method).toBe("POST");
  });

  it("propagates richer backend error envelopes", async () => {
    const fetcher: typeof fetch = () => {
      return Promise.resolve(
        jsonResponse(
          {
            error: "permission denied",
            code: "forbidden",
            category: "policy",
            retryable: false,
            redacted: false,
            validation_errors: []
          },
          403
        )
      );
    };
    const client = new ConsoleApiClient("", fetcher);

    const result = await client.getSession().catch((error: unknown) => error);
    expect(result).toBeInstanceOf(ControlPlaneApiError);
    expect((result as ControlPlaneApiError).message).toBe("permission denied");
    expect((result as ControlPlaneApiError).status).toBe(403);
    expect((result as ControlPlaneApiError).code).toBe("forbidden");
    expect((result as ControlPlaneApiError).category).toBe("policy");
  });

  it("fails requests that exceed timeout budget", async () => {
    vi.useFakeTimers();
    try {
      const fetcher: typeof fetch = (_input, init) => {
        return new Promise<Response>((_resolve, reject) => {
          const signal = init?.signal;
          if (signal === undefined || signal === null) {
            reject(new Error("Missing request abort signal."));
            return;
          }
          signal.addEventListener(
            "abort",
            () => {
              const error = new Error("aborted");
              error.name = "AbortError";
              reject(error);
            },
            { once: true }
          );
        });
      };
      const client = new ConsoleApiClient("", fetcher);
      const internalClient = client as unknown as {
        request<T>(path: string, init?: RequestInit, options?: { csrf?: boolean; timeoutMs?: number }): Promise<T>;
      };

      const pending = internalClient.request<JsonValue>(
        "/console/v1/diagnostics",
        undefined,
        { timeoutMs: 50, csrf: false }
      );
      const settled = pending.then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ ok: false as const, error })
      );
      await vi.advanceTimersByTimeAsync(50);

      const outcome = await settled;
      expect(outcome.ok).toBe(false);
      if (!outcome.ok) {
        expect(String(outcome.error)).toContain("Request timed out after 50 ms.");
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves caller abort signal cancellation semantics", async () => {
    const fetcher: typeof fetch = (_input, init) => {
      return new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal;
        if (signal === undefined || signal === null) {
          reject(new Error("Missing request abort signal."));
          return;
        }
        signal.addEventListener(
          "abort",
          () => {
            const error = new Error("aborted");
            error.name = "AbortError";
            reject(error);
          },
          { once: true }
        );
      });
    };
    const client = new ConsoleApiClient("", fetcher);
    const internalClient = client as unknown as {
      request<T>(path: string, init?: RequestInit, options?: { csrf?: boolean; timeoutMs?: number }): Promise<T>;
    };
    const controller = new AbortController();
    const pending = internalClient.request<JsonValue>(
      "/console/v1/diagnostics",
      { signal: controller.signal },
      { timeoutMs: 1_000, csrf: false }
    );

    controller.abort();

    await expect(pending).rejects.toThrow("Request canceled.");
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
    const relayBodyText = (calls[0]?.init?.body as string | undefined) ?? "{}";
    expect(relayBodyText).not.toContain("\"relay_token\"");
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

  it("retries safe-read GET requests once after a transport failure", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      if (calls.length === 1) {
        return Promise.reject(new TypeError("network down"));
      }
      return Promise.resolve(
        jsonResponse({
          generated_at_unix_ms: 123,
          model_provider: {},
          rate_limits: {},
          auth_profiles: {},
          browserd: {}
        })
      );
    };
    const client = new ConsoleApiClient("", fetcher);

    const diagnostics = await client.getDiagnostics();

    expect(diagnostics.generated_at_unix_ms).toBe(123);
    expect(calls).toHaveLength(2);
    expect(requestUrl(calls[0]?.input)).toBe("/console/v1/diagnostics");
    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/diagnostics");
  });

  it("supports M52 control-plane domains with additive CSRF behavior", async () => {
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
        contract: { contract_version: "control-plane.v1" },
        version: "capability-catalog.v1",
        generated_at_unix_ms: 123,
        capabilities: [],
        migration_notes: []
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        source_path: "defaults",
        config_version: 1,
        redacted: true,
        document_toml: "version = 1\n",
        backups: []
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        operation: "set",
        source_path: "palyra.toml",
        backups_retained: 5,
        config_version: 1,
        changed_key: "model_provider.auth_profile_id"
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        job: {
          job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
          state: "queued",
          requested_at_unix_ms: 100,
          command_output: ""
        }
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
    await client.getCapabilityCatalog();
    await client.inspectConfig({ path: "palyra.toml", show_secrets: false });
    await client.mutateConfig({
      key: "model_provider.auth_profile_id",
      value: "\"openai-default\""
    });
    await client.createSupportBundleJob({ retain_jobs: 8 });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/control-plane/capabilities");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/config/inspect");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();
    expect(calls[2]?.init?.method).toBe("POST");

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/config/mutate");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/support-bundle/jobs");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
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

  it("uses the OpenAI API key connect endpoint with CSRF", async () => {
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
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        action: "api-key",
        state: "connected",
        message: "OpenAI API key stored.",
        profile_id: "openai-default"
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
    await client.connectOpenAiApiKey({
      profile_name: "default-openai",
      scope: { kind: "global" },
      api_key: "sk-test-key",
      set_default: true
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/auth/providers/openai/api-key");
    expect(calls[1]?.init?.method).toBe("POST");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    const connectBody = typeof calls[1]?.init?.body === "string" ? calls[1]?.init?.body : "";
    expect(connectBody).toContain("\"profile_name\":\"default-openai\"");
    expect(connectBody).toContain("\"set_default\":true");
  });

  it("queries OpenAI callback state with attempt_id and keeps GET requests CSRF-free", async () => {
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
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        attempt_id: "attempt-1",
        state: "pending",
        message: "Waiting for callback.",
        profile_id: "openai-default",
        expires_at_unix_ms: 10_000
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        action: "refresh",
        state: "refreshed",
        message: "OpenAI token refreshed.",
        profile_id: "openai-default"
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
    await client.getOpenAiProviderCallbackState("attempt-1");
    await client.refreshOpenAiProvider({ profile_id: "openai-default" });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/auth/providers/openai/callback-state?attempt_id=attempt-1");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/auth/providers/openai/refresh");
    expect(calls[2]?.init?.method).toBe("POST");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
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
