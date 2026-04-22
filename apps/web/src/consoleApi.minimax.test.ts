import { describe, expect, it } from "vite-plus/test";

import { ConsoleApiClient } from "./consoleApi";

describe("ConsoleApiClient MiniMax auth routes", () => {
  it("posts API key, OAuth polling, default, and revoke actions to MiniMax paths", async () => {
    const calls: Array<{ input: RequestInfo | URL; init?: RequestInit }> = [];
    const responses = [
      jsonResponse({
        principal: "admin:web-console",
        device_id: "device-1",
        csrf_token: "csrf-1",
        issued_at_unix_ms: 100,
        expires_at_unix_ms: 200,
      }),
      providerAction("api_key", "selected"),
      jsonResponse({
        contract: contract(),
        provider: "minimax",
        attempt_id: "minimax-attempt-1",
        authorization_url: "https://api.minimax.io/oauth/authorize?user_code=ABC123",
        expires_at_unix_ms: 300,
        profile_id: "minimax-oauth",
        message: "MiniMax OAuth user code issued.",
      }),
      jsonResponse({
        contract: contract(),
        provider: "minimax",
        attempt_id: "minimax-attempt-1",
        state: "pending",
        message: "Waiting for MiniMax authorization.",
      }),
      providerAction("default_profile", "selected"),
      providerAction("revoke", "revoked"),
    ];
    const client = new ConsoleApiClient("", (input, init) => {
      calls.push({ input, init });
      const response = responses.shift();
      if (response === undefined) {
        throw new Error("No response queued for fetch mock.");
      }
      return Promise.resolve(response);
    });

    await client.login({
      admin_token: "token",
      principal: "admin:web-console",
      device_id: "device-1",
      channel: "web",
    });
    await client.connectProviderApiKey("minimax", {
      profile_name: "minimax-primary",
      scope: { kind: "global" },
      api_key: "minimax-key",
      set_default: true,
    });
    await client.startProviderBootstrap("minimax", {
      profile_name: "minimax-oauth",
      scope: { kind: "global" },
      scopes: ["group_id", "profile", "model.completion"],
      set_default: true,
    });
    await client.getProviderCallbackState("minimax", "minimax-attempt-1");
    await client.setProviderDefaultProfile("minimax", { profile_id: "minimax-default" });
    await client.revokeProvider("minimax", { profile_id: "minimax-default" });

    expect(requestUrl(calls[1]?.input)).toBe("/console/v1/auth/providers/minimax/api-key");
    expect(requestUrl(calls[2]?.input)).toBe("/console/v1/auth/providers/minimax/bootstrap");
    expect(requestUrl(calls[3]?.input)).toBe(
      "/console/v1/auth/providers/minimax/callback-state?attempt_id=minimax-attempt-1",
    );
    expect(requestUrl(calls[4]?.input)).toBe("/console/v1/auth/providers/minimax/default-profile");
    expect(requestUrl(calls[5]?.input)).toBe("/console/v1/auth/providers/minimax/revoke");
    expect(new Headers(calls[1]?.init?.headers).get("x-palyra-csrf-token")).toBe("csrf-1");
    expect(new Headers(calls[3]?.init?.headers).get("x-palyra-csrf-token")).toBeNull();
    expect(requestBody(calls[2]?.init?.body)).toContain(
      '"scopes":["group_id","profile","model.completion"]',
    );
  });
});

function contract() {
  return { contract_version: "control-plane.v1" };
}

function providerAction(action: string, state: string): Response {
  return jsonResponse({
    contract: contract(),
    provider: "minimax",
    action,
    state,
    message: `MiniMax ${action} ${state}.`,
    profile_id: "minimax-default",
  });
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestUrl(input: RequestInfo | URL | undefined): string {
  if (input === undefined) {
    throw new Error("Missing fetch call input.");
  }
  const raw =
    typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  const url = new URL(raw, "http://localhost");
  return `${url.pathname}${url.search}`;
}

function requestBody(body: BodyInit | null | undefined): string {
  return typeof body === "string" ? body : "";
}
