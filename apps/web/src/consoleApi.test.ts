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
        every_interval_ms: 60000,
      }),
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

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/cron/jobs/cron-1/runs");
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

function ndjsonResponse(lines: unknown[]): Response {
  const encoded = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;
  return new Response(encoded, {
    status: 200,
    headers: {
      "content-type": "application/x-ndjson",
    },
  });
}
