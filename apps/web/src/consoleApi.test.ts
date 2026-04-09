import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import {
  ConsoleApiClient,
  ControlPlaneApiError,
  type ChatStreamLine,
  type JsonValue,
} from "./consoleApi";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ConsoleApiClient", () => {
  it("invokes the default global fetch with the browser global as its receiver", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const fetcher = vi.fn(function (
      this: unknown,
      input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> {
      expect(this).toBe(globalThis);
      calls.push({ input, init });
      return Promise.resolve(
        jsonResponse({
          principal: "admin:web-console",
          device_id: "device-1",
          csrf_token: "csrf-1",
          issued_at_unix_ms: 100,
          expires_at_unix_ms: 200,
        }),
      );
    }) as typeof fetch;
    vi.stubGlobal("fetch", fetcher);

    const client = new ConsoleApiClient("");
    const session = await client.getSession();

    expect(session.principal).toBe("admin:web-console");
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(requestUrl(calls[0]?.input)).toBe("/console/v1/auth/session");
  });

  it("uses CSRF token for mutating requests after login", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({ jobs: [] }),
      jsonResponse({ job: { job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV" } }),
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
      channel: "web",
    });

    await client.listCronJobs();
    await client.createCronJob({
      name: "nightly",
      prompt: "run nightly",
      schedule_type: "every",
      every_interval_ms: 60000,
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/routines");
    const getHeaders = new Headers(calls[1]?.init?.headers);
    expect(getHeaders.get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/routines");
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
        every_interval_ms: 60000,
      }),
    ).rejects.toThrow("Missing CSRF token");
  });

  it("uses routines endpoints for templates, previews, import-export, dispatch, and system events", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({ version: 1, templates: [] }),
      jsonResponse({ preview: { schedule_type: "every" } }),
      jsonResponse({ export: { schema_id: "palyra.routine.export" } }),
      jsonResponse({ routine: { routine_id: "R1" }, imported_from: "R0" }),
      jsonResponse({ routine_id: "R1", run_id: "run-1", status: "queued", message: "queued" }),
      jsonResponse({ status: "emitted", event: "system.operator.nightly", routine_dispatches: [] }),
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
      channel: "web",
    });
    await client.listRoutineTemplates();
    await client.previewRoutineSchedule({ phrase: "every 2h", timezone: "utc" });
    await client.exportRoutine("R1");
    await client.importRoutine({ export: { schema_id: "palyra.routine.export" } });
    await client.dispatchRoutine("R1", { trigger_kind: "manual", trigger_payload: {} });
    await client.emitSystemEvent({ name: "nightly", details: { ok: true } });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/routines/templates");
    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/routines/schedule-preview");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/routines/R1/export");
    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/routines/import");
    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/routines/R1/dispatch");
    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/system/events/emit");
  });

  it("uses GET without CSRF and POST with CSRF for channel operations", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        connectors: [
          {
            connector_id: "echo:default",
            kind: "echo",
            enabled: true,
            readiness: "ready",
            liveness: "running",
          },
        ],
      }),
      jsonResponse({
        connector: {
          connector_id: "echo:default",
          kind: "echo",
          enabled: false,
          readiness: "ready",
          liveness: "stopped",
        },
      }),
      jsonResponse({
        connector_id: "discord:default",
        bot: {
          id: "123",
          username: "bot",
        },
        warnings: [],
        policy_warnings: [],
      }),
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
      channel: "web",
    });
    await client.listChannels();
    await client.setChannelEnabled("echo:default", false);
    await client.probeDiscordOnboarding({
      token: "bot-token",
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
            validation_errors: [],
          },
          403,
        ),
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
            { once: true },
          );
        });
      };
      const client = new ConsoleApiClient("", fetcher);
      const internalClient = client as unknown as {
        request<T>(
          path: string,
          init?: RequestInit,
          options?: { csrf?: boolean; timeoutMs?: number },
        ): Promise<T>;
      };

      const pending = internalClient.request<JsonValue>("/console/v1/diagnostics", undefined, {
        timeoutMs: 50,
        csrf: false,
      });
      const settled = pending.then(
        () => ({ ok: true as const }),
        (error: unknown) => ({ ok: false as const, error }),
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
          { once: true },
        );
      });
    };
    const client = new ConsoleApiClient("", fetcher);
    const internalClient = client as unknown as {
      request<T>(
        path: string,
        init?: RequestInit,
        options?: { csrf?: boolean; timeoutMs?: number },
      ): Promise<T>;
    };
    const controller = new AbortController();
    const pending = internalClient.request<JsonValue>(
      "/console/v1/diagnostics",
      { signal: controller.signal },
      { timeoutMs: 1_000, csrf: false },
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
              truncated: false,
            },
          },
        }),
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
          max_selection_bytes: 128,
        },
      },
      "relay-token-1",
    );

    expect(requestUrl(calls[0]?.input)).toBe("/console/v1/browser/relay/actions");
    const headers = new Headers(calls[0]?.init?.headers);
    expect(headers.get("authorization")).toBe("Bearer relay-token-1");
    expect(headers.get("x-palyra-csrf-token")).toBeNull();
    const relayBodyText = (calls[0]?.init?.body as string | undefined) ?? "{}";
    expect(relayBodyText).not.toContain('"relay_token"');
  });

  it("loads diagnostics snapshot without requiring CSRF header", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        generated_at_unix_ms: 123,
        model_provider: { kind: "openai-compatible" },
        rate_limits: { admin_api_max_requests_per_window: 30 },
        auth_profiles: { summary: { total_profiles: 1 } },
        browserd: { enabled: true },
      }),
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
      channel: "web",
    });
    await client.getDiagnostics();

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/diagnostics");
    const headers = new Headers(calls[1]?.init?.headers);
    expect(headers.get("x-palyra-csrf-token")).toBeNull();
  });

  it("supports usage aggregation endpoints with read-only requests and stable export paths", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
          bucket: "hour",
          bucket_width_ms: 3_600_000,
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
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
          bucket: "hour",
          bucket_width_ms: 3_600_000,
          include_archived: false,
          limit: 8,
          cursor: 0,
        },
        sessions: [],
        page: { limit: 8, returned: 0, has_more: false },
        cost_tracking_available: false,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
          bucket: "hour",
          bucket_width_ms: 3_600_000,
          include_archived: false,
          run_limit: 12,
        },
        session: {
          session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
          session_key: "usage-session",
          principal: "admin:web-console",
          device_id: "device-1",
          created_at_unix_ms: 10,
          updated_at_unix_ms: 20,
          archived: false,
          runs: 0,
          active_runs: 0,
          completed_runs: 0,
          prompt_tokens: 0,
          completion_tokens: 0,
          total_tokens: 0,
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
        runs: [],
        cost_tracking_available: false,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
          bucket: "hour",
          bucket_width_ms: 3_600_000,
          include_archived: false,
          limit: 8,
          cursor: 0,
        },
        agents: [],
        page: { limit: 8, returned: 0, has_more: false },
        cost_tracking_available: false,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
          bucket: "hour",
          bucket_width_ms: 3_600_000,
          include_archived: false,
          limit: 8,
          cursor: 0,
        },
        models: [],
        page: { limit: 8, returned: 0, has_more: false },
        cost_tracking_available: false,
      }),
    ];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("No response queued for fetch mock.");
      }
      return Promise.resolve(response);
    };
    const client = new ConsoleApiClient("/api", fetcher);

    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
    });

    const params = new URLSearchParams({
      start_at_unix_ms: "0",
      end_at_unix_ms: "100",
      bucket: "hour",
    });
    await client.getUsageSummary(params);
    await client.listUsageSessions(params);
    await client.getUsageSessionDetail("01ARZ3NDEKTSV4RRFFQ69G5FAV", params);
    await client.listUsageAgents(params);
    await client.listUsageModels(params);

    expect(requestUrl(calls[1]?.input)).toBe(
      "/api/console/v1/usage/summary?start_at_unix_ms=0&end_at_unix_ms=100&bucket=hour",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe(
      "/api/console/v1/usage/sessions?start_at_unix_ms=0&end_at_unix_ms=100&bucket=hour",
    );
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[3]?.input)).toBe(
      "/api/console/v1/usage/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV?start_at_unix_ms=0&end_at_unix_ms=100&bucket=hour",
    );
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[4]?.input)).toBe(
      "/api/console/v1/usage/agents?start_at_unix_ms=0&end_at_unix_ms=100&bucket=hour",
    );
    expect(requestUrl(calls[5]?.input)).toBe(
      "/api/console/v1/usage/models?start_at_unix_ms=0&end_at_unix_ms=100&bucket=hour",
    );

    expect(client.resolvePath("/console/v1/usage/export?dataset=timeline&format=csv")).toBe(
      "/api/console/v1/usage/export?dataset=timeline&format=csv",
    );
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
          browserd: {},
        }),
      );
    };
    const client = new ConsoleApiClient("", fetcher);

    const diagnostics = await client.getDiagnostics();

    expect(diagnostics.generated_at_unix_ms).toBe(123);
    expect(calls).toHaveLength(2);
    expect(requestUrl(calls[0]?.input)).toBe("/console/v1/diagnostics");
    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/diagnostics");
  });

  it("supports the shared logs contract and export path resolution", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        query: {
          limit: 120,
          direction: "before",
          source: "browserd",
          severity: "error",
          contains: "relay",
          start_at_unix_ms: 0,
          end_at_unix_ms: 100,
        },
        records: [],
        page: { limit: 120, returned: 0, has_more: false },
        newest_cursor: "100:browserd:1",
        available_sources: ["browserd", "palyrad"],
      }),
    ];
    const fetcher: typeof fetch = (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("No response queued for fetch mock.");
      }
      return Promise.resolve(response);
    };
    const client = new ConsoleApiClient("/api", fetcher);

    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
    });

    const params = new URLSearchParams({
      source: "browserd",
      severity: "error",
      contains: "relay",
      start_at_unix_ms: "0",
      end_at_unix_ms: "100",
    });
    await client.listLogs(params);

    expect(requestUrl(calls[1]?.input)).toBe(
      "/api/console/v1/logs?source=browserd&severity=error&contains=relay&start_at_unix_ms=0&end_at_unix_ms=100",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();
    expect(client.resolvePath("/console/v1/logs/export?format=csv")).toBe(
      "/api/console/v1/logs/export?format=csv",
    );
  });

  it("supports the inventory contract and CSRF-protected device actions", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        generated_at_unix_ms: 100,
        summary: {
          devices: 1,
          trusted_devices: 1,
          pending_pairings: 1,
          ok_devices: 1,
          stale_devices: 0,
          degraded_devices: 0,
          offline_devices: 0,
          ok_instances: 2,
          stale_instances: 0,
          degraded_instances: 1,
          offline_instances: 0,
        },
        devices: [],
        pending_pairings: [],
        instances: [],
        page: { limit: 1, returned: 0, has_more: false },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        generated_at_unix_ms: 100,
        device: {
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          device_status: "paired",
          trust_state: "trusted",
          presence_state: "ok",
          paired_at_unix_ms: 100,
          updated_at_unix_ms: 100,
          pending_pairings: 0,
          issued_by: "admin:web-console",
          approval_id: "approval-1",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
          current_certificate_fingerprint: "cert-1",
          certificate_fingerprint_history: ["cert-0", "cert-1"],
          capabilities: [{ name: "ping", available: true }],
          capability_summary: { total: 1, available: 1, unavailable: 0 },
          warnings: [],
          actions: {
            can_rotate: true,
            can_revoke: true,
            can_remove: true,
            can_invoke: true,
          },
        },
        pairings: [],
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        device: {
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          status: "paired",
          paired_at_unix_ms: 100,
          updated_at_unix_ms: 101,
          issued_by: "admin:web-console",
          approval_id: "approval-1",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
          current_certificate_fingerprint: "cert-1",
          certificate_fingerprint_history: ["cert-0", "cert-1"],
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        device: {
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          status: "revoked",
          paired_at_unix_ms: 100,
          updated_at_unix_ms: 102,
          issued_by: "admin:web-console",
          approval_id: "approval-1",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
          certificate_fingerprint_history: [],
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        device: {
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          status: "removed",
          paired_at_unix_ms: 100,
          updated_at_unix_ms: 103,
          issued_by: "admin:web-console",
          approval_id: "approval-1",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
          certificate_fingerprint_history: [],
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        capability: "ping",
        success: true,
        output_json: { ok: true },
        error: "",
      }),
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
      channel: "web",
    });
    await client.listInventory();
    await client.getInventoryDevice("01ARZ3NDEKTSV4RRFFQ69G5FAZ");
    await client.rotateDevice("01ARZ3NDEKTSV4RRFFQ69G5FAZ");
    await client.revokeDevice("01ARZ3NDEKTSV4RRFFQ69G5FAZ", { reason: "operator_revoke" });
    await client.removeDevice("01ARZ3NDEKTSV4RRFFQ69G5FAZ", { reason: "operator_remove" });
    await client.invokeNode("01ARZ3NDEKTSV4RRFFQ69G5FAZ", {
      capability: "ping",
      input_json: { echo: true },
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/inventory");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/inventory/01ARZ3NDEKTSV4RRFFQ69G5FAZ");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[3]?.input)).toBe(
      "/console/v1/devices/01ARZ3NDEKTSV4RRFFQ69G5FAZ/rotate",
    );
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[4]?.input)).toBe(
      "/console/v1/devices/01ARZ3NDEKTSV4RRFFQ69G5FAZ/revoke",
    );
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[5]?.input)).toBe(
      "/console/v1/devices/01ARZ3NDEKTSV4RRFFQ69G5FAZ/remove",
    );
    expect(new Headers(calls[5]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/nodes/01ARZ3NDEKTSV4RRFFQ69G5FAZ/invoke");
    expect(new Headers(calls[6]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("supports phase 9 node pairing lifecycle endpoints with additive CSRF behavior", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        codes: [],
        requests: [],
        page: { limit: 20, returned: 0, has_more: false },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        code: {
          code: "112233",
          method: "pin",
          issued_by: "admin:web-console",
          created_at_unix_ms: 100,
          expires_at_unix_ms: 200,
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        request: {
          request_id: "pair-req-1",
          session_id: "session-1",
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          method: "pin",
          code_issued_by: "admin:web-console",
          requested_at_unix_ms: 100,
          expires_at_unix_ms: 200,
          approval_id: "approval-1",
          state: "approved",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        request: {
          request_id: "pair-req-1",
          session_id: "session-1",
          device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          client_kind: "node",
          method: "pin",
          code_issued_by: "admin:web-console",
          requested_at_unix_ms: 100,
          expires_at_unix_ms: 200,
          approval_id: "approval-1",
          state: "rejected",
          identity_fingerprint: "fingerprint-1",
          transcript_hash_hex: "hash-1",
        },
      }),
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
      channel: "web",
    });
    await client.listNodePairingRequests({ state: "pending_approval", client_kind: "node" });
    await client.mintNodePairingCode({
      method: "pin",
      issued_by: "admin:web-console",
      ttl_ms: 600000,
    });
    await client.approveNodePairingRequest("pair-req-1", { reason: "looks good" });
    await client.rejectNodePairingRequest("pair-req-1", { reason: "second pass reject" });

    expect(requestUrl(calls[1]?.input)).toBe(
      "/console/v1/pairing/requests?client_kind=node&state=pending_approval",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/pairing/requests/code");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/pairing/requests/pair-req-1/approve");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/pairing/requests/pair-req-1/reject");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("supports M52 control-plane domains with additive CSRF behavior", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        version: "capability-catalog.v1",
        generated_at_unix_ms: 123,
        capabilities: [],
        migration_notes: [],
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        source_path: "defaults",
        config_version: 1,
        redacted: true,
        document_toml: "version = 1\n",
        backups: [],
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        operation: "set",
        source_path: "palyra.toml",
        backups_retained: 5,
        config_version: 1,
        changed_key: "model_provider.auth_profile_id",
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        job: {
          job_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
          state: "queued",
          requested_at_unix_ms: 100,
          command_output: "",
        },
      }),
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
      channel: "web",
    });
    await client.getCapabilityCatalog();
    await client.inspectConfig({ path: "palyra.toml", show_secrets: false });
    await client.mutateConfig({
      key: "model_provider.auth_profile_id",
      value: '"openai-default"',
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

  it("supports M56 config, access, and support contract additions", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        operation: "migrate",
        source_path: "palyra.toml",
        backups_retained: 3,
        config_version: 2,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        operation: "recover",
        source_path: "palyra.toml",
        backups_retained: 3,
        config_version: 1,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        channels: [],
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        channels: [],
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        jobs: [],
        page: { limit: 20, returned: 0, has_more: false },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        job: {
          job_id: "support-job-1",
          state: "queued",
          requested_at_unix_ms: 100,
          command_output: "",
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        job: {
          job_id: "support-job-1",
          state: "queued",
          requested_at_unix_ms: 100,
          command_output: "",
        },
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        secret: {
          scope: "global",
          key: "openai_api_key",
          created_at_unix_ms: 100,
          updated_at_unix_ms: 120,
          value_bytes: 32,
        },
      }),
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
      channel: "web",
    });
    await client.migrateConfig({ path: "palyra.toml", backups: 3 });
    await client.recoverConfig({ path: "palyra.toml", backup: 1, backups: 3 });
    await client.getPairingSummary();
    await client.mintPairingCode({ channel: "discord:default", ttl_ms: 600000 });
    await client.listSupportBundleJobs();
    await client.createSupportBundleJob({ retain_jobs: 8 });
    await client.getSupportBundleJob("support-job-1");
    await client.getSecretMetadata("global", "openai_api_key");

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/config/migrate");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/config/recover");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/pairing");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/pairing/codes");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/support-bundle/jobs");
    expect(new Headers(calls[5]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/support-bundle/jobs");
    expect(new Headers(calls[6]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[7]?.input)).toBe("/console/v1/support-bundle/jobs/support-job-1");
    expect(new Headers(calls[7]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[8]?.input)).toBe(
      "/console/v1/secrets/metadata?scope=global&key=openai_api_key",
    );
  });

  it("supports M56 runtime operations contract additions", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({ runs: [] }),
      jsonResponse({ config: {}, config_hash: "router-hash-1" }),
      jsonResponse({ warnings: [], config_hash: "router-hash-1" }),
      jsonResponse({ preview: { accepted: true } }),
      jsonResponse({ pairings: [], config_hash: "router-hash-1" }),
      jsonResponse({ code: { code: "777888" }, config_hash: "router-hash-1" }),
      jsonResponse({ connector: { connector_id: "discord:default" }, operations: {} }),
      jsonResponse({ connector: { connector_id: "discord:default" }, operations: {} }),
      jsonResponse({ connector: { connector_id: "discord:default" }, operations: {} }),
      jsonResponse({
        connector: { connector_id: "discord:default" },
        operations: {},
        health_refresh: {},
      }),
      jsonResponse({ connector: { connector_id: "discord:default" }, operations: {} }),
      jsonResponse({ connector: { connector_id: "discord:default" }, operations: {} }),
      jsonResponse({ report: { verified: true } }),
      jsonResponse({ report: { audited: true }, quarantined: false }),
      jsonResponse({ status: "quarantined" }),
      jsonResponse({ status: "active" }),
      jsonResponse({
        principal: "admin:web-console",
        active_profile_id: "profile-1",
        profiles: [],
      }),
      jsonResponse({ profile: { profile_id: "profile-1" } }),
      jsonResponse({ profile: { profile_id: "profile-1" } }),
      jsonResponse({ profile: { profile_id: "profile-1" } }),
      jsonResponse({ deleted: true, active_profile_id: "profile-1" }),
      jsonResponse({ artifacts: [], truncated: false, error: "" }),
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
      channel: "web",
    });
    await client.listCronRuns("cron-1");
    await client.getChannelRouterRules();
    await client.getChannelRouterWarnings();
    await client.previewChannelRoute({ channel: "discord:default", text: "pair 123456" });
    await client.listChannelRouterPairings();
    await client.mintChannelRouterPairingCode({ channel: "discord:default" });
    await client.pauseChannelQueue("discord:default");
    await client.resumeChannelQueue("discord:default");
    await client.drainChannelQueue("discord:default");
    await client.refreshChannelHealth("discord:default", {
      verify_channel_id: "123456789012345678",
    });
    await client.replayChannelDeadLetter("discord:default", 41);
    await client.discardChannelDeadLetter("discord:default", 41);
    await client.verifySkill("acme.echo_http", { version: "1.2.3" });
    await client.auditSkill("acme.echo_http", { version: "1.2.3", quarantine_on_fail: true });
    await client.quarantineSkill({ skill_id: "acme.echo_http", version: "1.2.3" });
    await client.enableSkill({ skill_id: "acme.echo_http", version: "1.2.3" });
    await client.listBrowserProfiles();
    await client.createBrowserProfile({ name: "Primary Browser" });
    await client.renameBrowserProfile("profile-1", { name: "Renamed Browser" });
    await client.activateBrowserProfile("profile-1");
    await client.deleteBrowserProfile("profile-1");
    await client.listBrowserDownloads();

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/routines/cron-1/runs");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/channels/router/rules");
    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/channels/router/warnings");

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/channels/router/preview");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/channels/router/pairings");
    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/channels/router/pairing-codes");

    expect(requestUrl(calls[7]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/queue/pause",
    );
    expect(requestUrl(calls[8]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/queue/resume",
    );
    expect(requestUrl(calls[9]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/queue/drain",
    );
    expect(requestUrl(calls[10]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/health-refresh",
    );
    expect(new Headers(calls[10]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestUrl(calls[11]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/dead-letters/41/replay",
    );
    expect(requestUrl(calls[12]?.input)).toBe(
      "/console/v1/channels/discord%3Adefault/operations/dead-letters/41/discard",
    );

    expect(requestUrl(calls[13]?.input)).toBe("/console/v1/skills/acme.echo_http/verify");
    expect(requestUrl(calls[14]?.input)).toBe("/console/v1/skills/acme.echo_http/audit");
    expect(requestUrl(calls[15]?.input)).toBe("/console/v1/skills/acme.echo_http/quarantine");
    expect(requestUrl(calls[16]?.input)).toBe("/console/v1/skills/acme.echo_http/enable");

    expect(requestUrl(calls[17]?.input)).toBe("/console/v1/browser/profiles");
    expect(requestUrl(calls[18]?.input)).toBe("/console/v1/browser/profiles/create");
    expect(requestUrl(calls[19]?.input)).toBe("/console/v1/browser/profiles/profile-1/rename");
    expect(requestUrl(calls[20]?.input)).toBe("/console/v1/browser/profiles/profile-1/activate");
    expect(requestUrl(calls[21]?.input)).toBe("/console/v1/browser/profiles/profile-1/delete");
    expect(requestUrl(calls[22]?.input)).toBe("/console/v1/browser/downloads");
  });

  it("lists chat sessions and streams NDJSON responses with CSRF", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        sessions: [
          {
            session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            session_key: "web",
            principal: "admin:web-console",
            device_id: "device-1",
            created_at_unix_ms: 100,
            updated_at_unix_ms: 150,
          },
        ],
      }),
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
            event_type: "status",
            status: {
              kind: "in_progress",
              message: "ok",
            },
          },
        },
        {
          type: "complete",
          run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX",
          status: "done",
        },
      ]),
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
      channel: "web",
    });

    const sessions = await client.listChatSessions();
    expect(sessions.sessions).toHaveLength(1);

    await client.streamChatMessage(
      "01ARZ3NDEKTSV4RRFFQ69G5FAV",
      {
        text: "hello",
      },
      {
        onLine: (line) => {
          lines.push(line);
        },
      },
    );

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/chat/sessions");
    expect(requestUrl(calls[2]?.input)).toBe(
      "/console/v1/chat/sessions/01ARZ3NDEKTSV4RRFFQ69G5FAV/messages/stream",
    );
    const streamHeaders = new Headers(calls[2]?.init?.headers);
    expect(streamHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(streamHeaders.get("content-type")).toBe("application/json");
    expect(lines).toHaveLength(3);
    expect(lines[0]).toMatchObject({ type: "meta" });
    expect(lines[1]).toMatchObject({ type: "event" });
    expect(lines[2]).toMatchObject({ type: "complete", status: "done" });
  });

  it("supports compaction and checkpoint chat endpoints with the expected CSRF posture", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        preview: {
          eligible: true,
          strategy: "head_tail_v1",
          compressor_version: "v1",
          trigger_reason: "manual_preview",
          estimated_input_tokens: 1000,
          estimated_output_tokens: 700,
          token_delta: 300,
          source_event_count: 20,
          protected_event_count: 5,
          condensed_event_count: 12,
          omitted_event_count: 3,
          summary_text: "summary",
          summary_preview: "summary",
          source_records: [],
          summary: {},
        },
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        artifact: {
          artifact_id: "artifact-1",
          session_id: "session-1",
          mode: "manual",
          strategy: "head_tail_v1",
          compressor_version: "v1",
          trigger_reason: "manual_apply",
          summary_text: "summary",
          summary_preview: "summary",
          source_event_count: 20,
          protected_event_count: 5,
          condensed_event_count: 12,
          omitted_event_count: 3,
          estimated_input_tokens: 1000,
          estimated_output_tokens: 700,
          source_records_json: "[]",
          summary_json: "{}",
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
        },
        checkpoint: {
          checkpoint_id: "checkpoint-1",
          session_id: "session-1",
          name: "Checkpoint 1",
          tags_json: "[]",
          branch_state: "active_branch",
          referenced_compaction_ids_json: '["artifact-1"]',
          workspace_paths_json: '["MEMORY.md"]',
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
          restore_count: 0,
        },
        preview: {
          eligible: true,
          strategy: "head_tail_v1",
          compressor_version: "v1",
          trigger_reason: "manual_apply",
          estimated_input_tokens: 1000,
          estimated_output_tokens: 700,
          token_delta: 300,
          source_event_count: 20,
          protected_event_count: 5,
          condensed_event_count: 12,
          omitted_event_count: 3,
          summary_text: "summary",
          summary_preview: "summary",
          source_records: [],
          summary: {},
        },
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        artifact: {
          artifact_id: "artifact-1",
          session_id: "session-1",
          mode: "manual",
          strategy: "head_tail_v1",
          compressor_version: "v1",
          trigger_reason: "manual_apply",
          summary_text: "summary",
          summary_preview: "summary",
          source_event_count: 20,
          protected_event_count: 5,
          condensed_event_count: 12,
          omitted_event_count: 3,
          estimated_input_tokens: 1000,
          estimated_output_tokens: 700,
          source_records_json: "[]",
          summary_json: "{}",
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
        },
        related_checkpoints: [
          {
            checkpoint_id: "checkpoint-1",
            session_id: "session-1",
            name: "Checkpoint 1",
            tags_json: "[]",
            branch_state: "active_branch",
            referenced_compaction_ids_json: '["artifact-1"]',
            workspace_paths_json: '["MEMORY.md"]',
            created_by_principal: "admin:web-console",
            created_at_unix_ms: 100,
            restore_count: 0,
          },
        ],
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        checkpoint: {
          checkpoint_id: "checkpoint-1",
          session_id: "session-1",
          name: "Checkpoint 1",
          tags_json: "[]",
          branch_state: "active_branch",
          referenced_compaction_ids_json: "[]",
          workspace_paths_json: "[]",
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
          restore_count: 0,
        },
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        checkpoint: {
          checkpoint_id: "checkpoint-1",
          session_id: "session-1",
          name: "Checkpoint 1",
          tags_json: "[]",
          branch_state: "active_branch",
          referenced_compaction_ids_json: "[]",
          workspace_paths_json: "[]",
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
          restore_count: 0,
        },
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({
        session: { session_id: "session-2", title: "Checkpoint restore" },
        checkpoint: {
          checkpoint_id: "checkpoint-1",
          session_id: "session-1",
          name: "Checkpoint 1",
          tags_json: "[]",
          branch_state: "active_branch",
          referenced_compaction_ids_json: "[]",
          workspace_paths_json: "[]",
          created_by_principal: "admin:web-console",
          created_at_unix_ms: 100,
          restore_count: 1,
        },
        action: "restored",
        contract: { contract_version: "control-plane.v1" },
      }),
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
      channel: "web",
    });
    await client.previewSessionCompaction("session-1", { trigger_reason: "manual_preview" });
    await client.applySessionCompaction("session-1", {
      trigger_reason: "manual_apply",
      accept_candidate_ids: ["candidate-1"],
      reject_candidate_ids: ["candidate-2"],
    });
    await client.getSessionCompactionArtifact("artifact-1");
    await client.createSessionCheckpoint("session-1", { name: "Checkpoint 1" });
    await client.getSessionCheckpoint("checkpoint-1");
    await client.restoreSessionCheckpoint("checkpoint-1", { session_label: "Checkpoint restore" });

    expect(requestUrl(calls[1]?.input)).toBe(
      "/console/v1/chat/sessions/session-1/compactions/preview",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/chat/sessions/session-1/compactions");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestBody(calls[2]?.init?.body)).toContain('"accept_candidate_ids":["candidate-1"]');
    expect(requestBody(calls[2]?.init?.body)).toContain('"reject_candidate_ids":["candidate-2"]');

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/chat/compactions/artifact-1");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/chat/sessions/session-1/checkpoints");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/chat/checkpoints/checkpoint-1");
    expect(new Headers(calls[5]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/chat/checkpoints/checkpoint-1/restore");
    expect(new Headers(calls[6]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("posts chat context reference preview requests with CSRF protection", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const client = new ConsoleApiClient("", (input, init) => {
      calls.push({ input, init });
      if (calls.length === 1) {
        return Promise.resolve(
          jsonResponse({
            principal: "admin:web-console",
            device_id: "device-1",
            csrf_token: "csrf-1",
            issued_at_unix_ms: 100,
            expires_at_unix_ms: 200,
          }),
        );
      }
      return Promise.resolve(
        jsonResponse({
          clean_prompt: "Summarize",
          references: [],
          total_estimated_tokens: 0,
          warnings: [],
          errors: [],
          contract: { contract_version: "control-plane.v1" },
        }),
      );
    });
    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
    });

    await client.previewChatContextReferences("session-1", { text: "Summarize @file:README.md" });

    expect(requestUrl(calls[1]?.input)).toBe(
      "/console/v1/chat/sessions/session-1/references/preview",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("supports background task chat endpoints with the expected CSRF posture", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const task = {
      task_id: "task-1",
      task_kind: "chat_followup",
      session_id: "session-1",
      owner_principal: "admin:web-console",
      device_id: "device-1",
      state: "queued",
      priority: 50,
      attempt_count: 0,
      max_attempts: 3,
      budget_tokens: 512,
      created_at_unix_ms: 100,
      updated_at_unix_ms: 100,
    };
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({ tasks: [task], contract: { contract_version: "control-plane.v1" } }),
      jsonResponse({
        session: { session_id: "session-1", title: "Phase 4 Session" },
        task,
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({ task, run: undefined, contract: { contract_version: "control-plane.v1" } }),
      jsonResponse({
        task: { ...task, state: "paused" },
        action: "paused",
        contract: { contract_version: "control-plane.v1" },
      }),
      jsonResponse({ task, action: "resumed", contract: { contract_version: "control-plane.v1" } }),
      jsonResponse({ task, action: "retried", contract: { contract_version: "control-plane.v1" } }),
      jsonResponse({
        task: { ...task, state: "cancelled" },
        action: "cancelled",
        contract: { contract_version: "control-plane.v1" },
      }),
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
      channel: "web",
    });
    await client.listBackgroundTasks({
      session_id: "session-1",
      include_completed: false,
      limit: 5,
    });
    await client.createBackgroundTask("session-1", { text: "follow up later" });
    await client.getBackgroundTask("task-1");
    await client.pauseBackgroundTask("task-1");
    await client.resumeBackgroundTask("task-1");
    await client.retryBackgroundTask("task-1");
    await client.cancelBackgroundTask("task-1");

    expect(requestUrl(calls[1]?.input)).toBe(
      "/console/v1/chat/background-tasks?session_id=session-1&include_completed=false&limit=5",
    );
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe(
      "/console/v1/chat/sessions/session-1/background-tasks",
    );
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/chat/background-tasks/task-1");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/chat/background-tasks/task-1/pause");
    expect(new Headers(calls[4]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/chat/background-tasks/task-1/resume");
    expect(new Headers(calls[5]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/chat/background-tasks/task-1/retry");
    expect(new Headers(calls[6]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[7]?.input)).toBe("/console/v1/chat/background-tasks/task-1/cancel");
    expect(new Headers(calls[7]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });

  it("submits approval decisions with CSRF protection", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        approval: {
          approval_id: "A1",
          decision: "allow",
        },
      }),
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
      channel: "web",
    });

    await client.decideApproval("A1", {
      approved: true,
      reason: "safe",
      decision_scope: "session",
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/approvals/A1/decision");
    const decisionHeaders = new Headers(calls[1]?.init?.headers);
    expect(decisionHeaders.get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(decisionHeaders.get("content-type")).toBe("application/json");
    const decisionBody = typeof calls[1]?.init?.body === "string" ? calls[1].init.body : "";
    expect(decisionBody).toContain('"approved":true');
    expect(decisionBody).toContain('"decision_scope":"session"');
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
            expires_at_unix_ms: 200,
          }),
        );
      }
      void init;
      return Promise.resolve(
        new Response("this-is-not-json\n", {
          status: 200,
          headers: {
            "content-type": "application/x-ndjson",
          },
        }),
      );
    };
    const client = new ConsoleApiClient("", fetcher);
    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
    });

    await expect(
      client.streamChatMessage(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        { text: "hello" },
        { onLine: () => {} },
      ),
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
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        action: "api-key",
        state: "connected",
        message: "OpenAI API key stored.",
        profile_id: "openai-default",
      }),
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
      channel: "web",
    });
    await client.connectOpenAiApiKey({
      profile_name: "default-openai",
      scope: { kind: "global" },
      api_key: "sk-test-key",
      set_default: true,
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/auth/providers/openai/api-key");
    expect(calls[1]?.init?.method).toBe("POST");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    const connectBody = typeof calls[1]?.init?.body === "string" ? calls[1]?.init?.body : "";
    expect(connectBody).toContain('"profile_name":"default-openai"');
    expect(connectBody).toContain('"set_default":true');
  });

  it("queries OpenAI callback state with attempt_id and keeps GET requests CSRF-free", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        attempt_id: "attempt-1",
        state: "pending",
        message: "Waiting for callback.",
        profile_id: "openai-default",
        expires_at_unix_ms: 10_000,
      }),
      jsonResponse({
        contract: { contract_version: "control-plane.v1" },
        provider: "openai",
        action: "refresh",
        state: "refreshed",
        message: "OpenAI token refreshed.",
        profile_id: "openai-default",
      }),
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
      channel: "web",
    });
    await client.getOpenAiProviderCallbackState("attempt-1");
    await client.refreshOpenAiProvider({ profile_id: "openai-default" });

    expect(requestUrl(calls[1]?.input)).toBe(
      "/console/v1/auth/providers/openai/callback-state?attempt_id=attempt-1",
    );
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
      "content-type": "application/json",
    },
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

function requestBody(body: BodyInit | null | undefined): string {
  return typeof body === "string" ? body : "";
}

function ndjsonResponse(lines: unknown[]): Response {
  const encoded = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;
  return new Response(encoded, {
    status: 200,
    headers: {
      "content-type": "application/x-ndjson",
    },
  });
}
