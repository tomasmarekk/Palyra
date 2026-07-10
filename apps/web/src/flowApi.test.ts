import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { ConsoleApiClient } from "./consoleApi";
import {
  cancelFlow,
  compensateFlowStep,
  getFlow,
  listFlows,
  pauseFlow,
  repairFlowDependencies,
  resumeFlow,
  retryFlowStep,
  skipFlowStep,
} from "./flowApi";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("flowApi", () => {
  it("uses GET without CSRF and mutating requests with CSRF", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const flow = {
      flow_id: "FLOW1",
      mode: "managed",
      state: "running",
      owner_principal: "admin:web-console",
      device_id: "device-1",
      title: "Routine flow",
      revision: 1,
    };
    const bundle = {
      flow,
      steps: [],
      events: [],
      revisions: [],
      blockers: [],
      retry_history: [],
    };
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      jsonResponse({ flows: [flow] }),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
      jsonResponse(bundle),
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
    await listFlows(client, new URLSearchParams({ limit: "5", include_terminal: "true" }));
    await getFlow(client, "FLOW1");
    await pauseFlow(client, "FLOW1", { reason: "operator" });
    await resumeFlow(client, "FLOW1", { reason: "operator" });
    await cancelFlow(client, "FLOW1", { reason: "operator" });
    await retryFlowStep(client, "FLOW1", "STEP1", { reason: "operator" });
    await skipFlowStep(client, "FLOW1", "STEP1", { reason: "operator" });
    await compensateFlowStep(client, "FLOW1", "STEP1", { reason: "operator" });
    await repairFlowDependencies(client, "FLOW1", {
      expected_revision: 1,
      replacements: [{ step_id: "STEP1", depends_on_step_ids: ["STEP0"] }],
    });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/flows?limit=5&include_terminal=true");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/flows/FLOW1");
    expect(new Headers(calls[2]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();

    expect(requestUrl(calls[3]?.input)).toBe("/console/v1/flows/FLOW1/pause");
    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/flows/FLOW1/resume");
    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/flows/FLOW1/cancel");
    expect(new Headers(calls[5]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");

    expect(requestUrl(calls[6]?.input)).toBe("/console/v1/flows/FLOW1/steps/STEP1/retry");
    expect(requestUrl(calls[7]?.input)).toBe("/console/v1/flows/FLOW1/steps/STEP1/skip");
    expect(requestUrl(calls[8]?.input)).toBe("/console/v1/flows/FLOW1/steps/STEP1/compensate");
    expect(new Headers(calls[8]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(requestUrl(calls[9]?.input)).toBe("/console/v1/flows/FLOW1/dependencies/repair");
    expect(new Headers(calls[9]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
  });
});

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestUrl(input: RequestInfo | URL | undefined): string {
  if (typeof input === "string") {
    return input;
  }
  if (input instanceof URL) {
    return input.toString();
  }
  return input?.url ?? "";
}
