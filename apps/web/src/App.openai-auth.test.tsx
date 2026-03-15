import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("M54 web auth surface", () => {
  it("connects an OpenAI API key profile and refreshes the auth surface", async () => {
    const state = createAuthSurfaceState();
    let apiKeyConnectBody = "";
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = requestDescriptor(input, init);
      if (request.path === "/console/v1/auth/session" && request.method === "GET") {
        return Promise.resolve(sessionResponse());
      }
      if (request.path === "/console/v1/approvals" && request.method === "GET") {
        return Promise.resolve(jsonResponse({ approvals: [] }));
      }
      if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authProfilesEnvelope(state.profiles)));
      }
      if (request.path === "/console/v1/auth/health" && request.method === "GET") {
        expect(request.url.searchParams.get("include_profiles")).toBe("true");
        return Promise.resolve(jsonResponse(authHealthEnvelope(state.healthProfiles)));
      }
      if (request.path === "/console/v1/auth/providers/openai" && request.method === "GET") {
        return Promise.resolve(jsonResponse(providerStateEnvelope(state.profiles, state.defaultProfileId)));
      }
      if (request.path === "/console/v1/auth/providers/openai/api-key" && request.method === "POST") {
        apiKeyConnectBody = request.body;
        state.defaultProfileId = "openai-default";
        state.profiles = [
          createApiKeyProfile({
            profile_id: "openai-default",
            profile_name: "default-openai"
          })
        ];
        state.healthProfiles = [
          createHealthProfile({
            profile_id: "openai-default",
            profile_name: "default-openai",
            credential_type: "api_key",
            state: "static",
            reason: "API key profile validated."
          })
        ];
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            action: "api-key",
            state: "connected",
            message: "OpenAI API key stored.",
            profile_id: "openai-default"
          })
        );
      }
      throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));
    expect(await screen.findByRole("heading", { name: "Connect via API key" })).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Profile name")[0], {
      target: { value: "default-openai" }
    });
    fireEvent.change(screen.getByLabelText("API key"), {
      target: { value: "sk-test-key" }
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Create profile" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "Create profile" }));

    await waitFor(() => {
      expect(screen.getByText("OpenAI API key stored.")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(document.body).toHaveTextContent("default-openai");
    }, { timeout: 5_000 });
    expect(apiKeyConnectBody).toContain("\"profile_name\":\"default-openai\"");
    expect(apiKeyConnectBody).toContain("\"api_key\":\"sk-test-key\"");
    expect(apiKeyConnectBody).toContain("\"set_default\":true");
  });

  it("surfaces OpenAI API key validation failures without leaving the auth section", async () => {
    const state = createAuthSurfaceState();
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = requestDescriptor(input, init);
      if (request.path === "/console/v1/auth/session" && request.method === "GET") {
        return Promise.resolve(sessionResponse());
      }
      if (request.path === "/console/v1/approvals" && request.method === "GET") {
        return Promise.resolve(jsonResponse({ approvals: [] }));
      }
      if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authProfilesEnvelope(state.profiles)));
      }
      if (request.path === "/console/v1/auth/health" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authHealthEnvelope(state.healthProfiles)));
      }
      if (request.path === "/console/v1/auth/providers/openai" && request.method === "GET") {
        return Promise.resolve(jsonResponse(providerStateEnvelope(state.profiles, state.defaultProfileId)));
      }
      if (request.path === "/console/v1/auth/providers/openai/api-key" && request.method === "POST") {
        return Promise.resolve(
          jsonResponse(
            {
              error: "OpenAI API key rejected by provider validation.",
              category: "validation",
              retryable: false,
              redacted: false
            },
            400
          )
        );
      }
      throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));

    fireEvent.change(screen.getAllByLabelText("Profile name")[0], {
      target: { value: "default-openai" }
    });
    fireEvent.change(screen.getByLabelText("API key"), {
      target: { value: "sk-invalid" }
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Create profile" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "Create profile" }));

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "OpenAI API key rejected by provider validation."
    );
    expect(screen.queryByText("OpenAI API key stored.")).not.toBeInTheDocument();
  });

  it("starts OpenAI OAuth in a popup and completes from callback-state polling", async () => {
    const popupClose = vi.fn();
    const popup = {
      close: popupClose,
      focus: vi.fn(),
      closed: false
    } as unknown as Window;
    vi.spyOn(window, "open").mockReturnValue(popup);

    const state = createAuthSurfaceState();
    let callbackState = createCallbackState({
      attempt_id: "attempt-1",
      state: "pending",
      message: "Waiting for callback.",
      profile_id: "openai-oauth"
    });
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = requestDescriptor(input, init);
      if (request.path === "/console/v1/auth/session" && request.method === "GET") {
        return Promise.resolve(sessionResponse());
      }
      if (request.path === "/console/v1/approvals" && request.method === "GET") {
        return Promise.resolve(jsonResponse({ approvals: [] }));
      }
      if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authProfilesEnvelope(state.profiles)));
      }
      if (request.path === "/console/v1/auth/health" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authHealthEnvelope(state.healthProfiles)));
      }
      if (request.path === "/console/v1/auth/providers/openai" && request.method === "GET") {
        return Promise.resolve(jsonResponse(providerStateEnvelope(state.profiles, state.defaultProfileId)));
      }
      if (request.path === "/console/v1/auth/providers/openai/bootstrap" && request.method === "POST") {
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            attempt_id: "attempt-1",
            authorization_url: "https://auth.openai.test/authorize?state=attempt-1",
            expires_at_unix_ms: 200_000,
            profile_id: "openai-oauth",
            message: "OpenAI OAuth authorization URL issued."
          })
        );
      }
      if (request.path === "/console/v1/auth/providers/openai/callback-state" && request.method === "GET") {
        if (callbackState.state === "pending") {
          callbackState = createCallbackState({
            attempt_id: "attempt-1",
            state: "succeeded",
            message: "OpenAI OAuth connected.",
            profile_id: "openai-oauth",
            completed_at_unix_ms: 150_000
          });
          state.defaultProfileId = "openai-oauth";
          state.profiles = [
            createOauthProfile({
              profile_id: "openai-oauth",
              profile_name: "oauth-primary"
            })
          ];
          state.healthProfiles = [
            createHealthProfile({
              profile_id: "openai-oauth",
              profile_name: "oauth-primary",
              credential_type: "oauth",
              state: "ok",
              reason: "OAuth token valid."
            })
          ];
        }
        return Promise.resolve(jsonResponse(callbackState));
      }
      throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));
    expect(await screen.findByRole("heading", { name: "Connect via OAuth" })).toBeInTheDocument();

    fireEvent.change(screen.getAllByLabelText("Profile name")[1], {
      target: { value: "oauth-primary" }
    });
    fireEvent.change(screen.getByLabelText("Client id"), {
      target: { value: "client-id-1" }
    });
    fireEvent.change(screen.getByLabelText("Client secret"), {
      target: { value: "client-secret-1" }
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Start OpenAI OAuth" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "Start OpenAI OAuth" }));

    await waitFor(() => {
      expect(window.open).toHaveBeenCalled();
    });
    fireEvent.click((await screen.findAllByRole("button", { name: "Poll callback" }))[0]);

    await waitFor(() => {
      expect(screen.getAllByText("OpenAI OAuth connected.").length).toBeGreaterThan(0);
    });
    await waitFor(() => {
      expect(document.body).toHaveTextContent("oauth-primary");
    }, { timeout: 5_000 });
    expect(popupClose).toHaveBeenCalled();
  });

  it("shows OAuth callback failure messaging after polling", async () => {
    vi.spyOn(window, "open").mockReturnValue({
      close: vi.fn(),
      focus: vi.fn(),
      closed: false
    } as unknown as Window);

    const state = createAuthSurfaceState();
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = requestDescriptor(input, init);
      if (request.path === "/console/v1/auth/session" && request.method === "GET") {
        return Promise.resolve(sessionResponse());
      }
      if (request.path === "/console/v1/approvals" && request.method === "GET") {
        return Promise.resolve(jsonResponse({ approvals: [] }));
      }
      if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authProfilesEnvelope(state.profiles)));
      }
      if (request.path === "/console/v1/auth/health" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authHealthEnvelope(state.healthProfiles)));
      }
      if (request.path === "/console/v1/auth/providers/openai" && request.method === "GET") {
        return Promise.resolve(jsonResponse(providerStateEnvelope(state.profiles, state.defaultProfileId)));
      }
      if (request.path === "/console/v1/auth/providers/openai/bootstrap" && request.method === "POST") {
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            attempt_id: "attempt-9",
            authorization_url: "https://auth.openai.test/authorize?state=attempt-9",
            expires_at_unix_ms: 200_000,
            profile_id: "openai-oauth",
            message: "OpenAI OAuth authorization URL issued."
          })
        );
      }
      if (request.path === "/console/v1/auth/providers/openai/callback-state" && request.method === "GET") {
        return Promise.resolve(
          jsonResponse(
            createCallbackState({
              attempt_id: "attempt-9",
              state: "failed",
              message: "OpenAI OAuth attempt expired before the callback completed.",
              profile_id: "openai-oauth",
              completed_at_unix_ms: 160_000
            })
          )
        );
      }
      throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));

    fireEvent.change(screen.getAllByLabelText("Profile name")[1], {
      target: { value: "oauth-primary" }
    });
    fireEvent.change(screen.getByLabelText("Client id"), {
      target: { value: "client-id-1" }
    });
    fireEvent.change(screen.getByLabelText("Client secret"), {
      target: { value: "client-secret-1" }
    });
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Start OpenAI OAuth" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "Start OpenAI OAuth" }));
    fireEvent.click((await screen.findAllByRole("button", { name: "Poll callback" }))[0]);

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "OpenAI OAuth attempt expired before the callback completed."
    );
  });

  it("supports default selection plus refresh reconnect and revoke for stored profiles", async () => {
    const popupClose = vi.fn();
    vi.spyOn(window, "open").mockReturnValue({
      close: popupClose,
      focus: vi.fn(),
      closed: false
    } as unknown as Window);

    const state = createAuthSurfaceState();
    state.defaultProfileId = "openai-api";
    state.profiles = [
      createApiKeyProfile({
        profile_id: "openai-api",
        profile_name: "api-primary"
      }),
      createOauthProfile({
        profile_id: "openai-oauth",
        profile_name: "oauth-primary"
      })
    ];
    state.healthProfiles = [
      createHealthProfile({
        profile_id: "openai-api",
        profile_name: "api-primary",
        credential_type: "api_key",
        state: "static",
        reason: "API key profile validated."
      }),
      createHealthProfile({
        profile_id: "openai-oauth",
        profile_name: "oauth-primary",
        credential_type: "oauth",
        state: "ok",
        reason: "OAuth token valid."
      })
    ];

    let defaultProfileBody = "";
    let refreshBody = "";
    let reconnectBody = "";
    let revokeBody = "";
    const fetchMock = vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
      const request = requestDescriptor(input, init);
      if (request.path === "/console/v1/auth/session" && request.method === "GET") {
        return Promise.resolve(sessionResponse());
      }
      if (request.path === "/console/v1/approvals" && request.method === "GET") {
        return Promise.resolve(jsonResponse({ approvals: [] }));
      }
      if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
        return Promise.resolve(jsonResponse(authProfilesEnvelope(state.profiles)));
      }
      if (request.path === "/console/v1/auth/health" && request.method === "GET") {
        expect(request.url.searchParams.get("include_profiles")).toBe("true");
        return Promise.resolve(jsonResponse(authHealthEnvelope(state.healthProfiles)));
      }
      if (request.path === "/console/v1/auth/providers/openai" && request.method === "GET") {
        return Promise.resolve(jsonResponse(providerStateEnvelope(state.profiles, state.defaultProfileId)));
      }
      if (request.path === "/console/v1/auth/providers/openai/default-profile" && request.method === "POST") {
        defaultProfileBody = request.body;
        state.defaultProfileId = "openai-oauth";
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            action: "default_profile",
            state: "selected",
            message: "OpenAI default profile updated.",
            profile_id: "openai-oauth"
          })
        );
      }
      if (request.path === "/console/v1/auth/providers/openai/refresh" && request.method === "POST") {
        refreshBody = request.body;
        state.healthProfiles = [
          createHealthProfile({
            profile_id: "openai-api",
            profile_name: "api-primary",
            credential_type: "api_key",
            state: "static",
            reason: "API key profile validated."
          }),
          createHealthProfile({
            profile_id: "openai-oauth",
            profile_name: "oauth-primary",
            credential_type: "oauth",
            state: "ok",
            reason: "OAuth token refreshed."
          })
        ];
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            action: "refresh",
            state: "refreshed",
            message: "OpenAI OAuth token refreshed.",
            profile_id: "openai-oauth"
          })
        );
      }
      if (request.path === "/console/v1/auth/providers/openai/reconnect" && request.method === "POST") {
        reconnectBody = request.body;
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            attempt_id: "attempt-2",
            authorization_url: "https://auth.openai.test/authorize?state=attempt-2",
            expires_at_unix_ms: 260_000,
            profile_id: "openai-oauth",
            message: "OpenAI OAuth reconnect ready."
          })
        );
      }
      if (request.path === "/console/v1/auth/providers/openai/revoke" && request.method === "POST") {
        revokeBody = request.body;
        state.defaultProfileId = undefined;
        state.profiles = [
          createApiKeyProfile({
            profile_id: "openai-api",
            profile_name: "api-primary"
          })
        ];
        state.healthProfiles = [
          createHealthProfile({
            profile_id: "openai-api",
            profile_name: "api-primary",
            credential_type: "api_key",
            state: "static",
            reason: "API key profile validated."
          })
        ];
        return Promise.resolve(
          jsonResponse({
            contract: controlPlaneContract(),
            provider: "openai",
            action: "revoke",
            state: "revoked",
            message: "OpenAI auth profile revoked.",
            profile_id: "openai-oauth"
          })
        );
      }
      throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));

    await waitFor(() => {
      expect(document.body).toHaveTextContent("api-primary");
      expect(document.body).toHaveTextContent("oauth-primary");
    }, { timeout: 5_000 });

    fireEvent.click((await screen.findAllByRole("button", { name: /^Inspect / }))[1]);

    fireEvent.click(screen.getByRole("button", { name: "Set as default" }));
    await waitFor(() => {
      expect(screen.getByText("OpenAI default profile updated.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Refresh token" }));
    await waitFor(() => {
      expect(screen.getByText("OpenAI OAuth token refreshed.")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Reconnect" }));
    await waitFor(() => {
      expect(window.open).toHaveBeenCalledWith(
        "https://auth.openai.test/authorize?state=attempt-2",
        "palyra-openai-auth",
        "popup=yes,width=720,height=860,resizable=yes,scrollbars=yes"
      );
    });
    expect(
      await screen.findByText("OpenAI OAuth window opened. Finish the authorization to complete the profile.")
    ).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Poll callback" }).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "Revoke" }));
    await waitFor(() => {
      expect(screen.getByText("OpenAI auth profile revoked.")).toBeInTheDocument();
    });
    expect(screen.queryByText("oauth-primary")).not.toBeInTheDocument();
    expect(popupClose).not.toHaveBeenCalled();

    expect(defaultProfileBody).toContain("\"profile_id\":\"openai-oauth\"");
    expect(refreshBody).toContain("\"profile_id\":\"openai-oauth\"");
    expect(reconnectBody).toContain("\"profile_id\":\"openai-oauth\"");
    expect(revokeBody).toContain("\"profile_id\":\"openai-oauth\"");
  });
});

function sessionResponse(): Response {
  return jsonResponse({
    principal: "admin:web-console",
    device_id: "device-1",
    channel: "web",
    csrf_token: "csrf-1",
    issued_at_unix_ms: 100,
    expires_at_unix_ms: 200
  });
}

function controlPlaneContract() {
  return { contract_version: "control-plane.v1" };
}

function createAuthSurfaceState() {
  return {
    profiles: [] as unknown[],
    healthProfiles: [] as unknown[],
    defaultProfileId: undefined as string | undefined
  };
}

function authProfilesEnvelope(profiles: unknown[]) {
  return {
    contract: controlPlaneContract(),
    profiles,
    page: {
      limit: 100,
      returned: profiles.length,
      has_more: false
    }
  };
}

function authHealthEnvelope(profiles: unknown[]) {
  const typedProfiles = profiles as Array<{ state: string }>;
  return {
    contract: controlPlaneContract(),
    summary: {
      total: typedProfiles.length,
      ok: typedProfiles.filter((profile) => profile.state === "ok").length,
      expiring: typedProfiles.filter((profile) => profile.state === "expiring").length,
      expired: typedProfiles.filter((profile) => profile.state === "expired").length,
      missing: typedProfiles.filter((profile) => profile.state === "missing").length,
      static_count: typedProfiles.filter((profile) => profile.state === "static").length
    },
    expiry_distribution: {
      expired: 0,
      under_5m: 0,
      between_5m_15m: 0,
      between_15m_60m: 0,
      between_1h_24h: 0,
      over_24h: 0,
      unknown: 0,
      static_count: typedProfiles.filter((profile) => profile.state === "static").length,
      missing: typedProfiles.filter((profile) => profile.state === "missing").length
    },
    profiles,
    refresh_metrics: {
      attempts: 1,
      successes: typedProfiles.filter((profile) => profile.state === "ok").length,
      failures: typedProfiles.filter((profile) => profile.state !== "ok" && profile.state !== "static").length,
      by_provider: [
        {
          provider: "openai",
          attempts: 1,
          successes: typedProfiles.filter((profile) => profile.state === "ok").length,
          failures: typedProfiles.filter((profile) => profile.state !== "ok" && profile.state !== "static").length
        }
      ]
    }
  };
}

function providerStateEnvelope(profiles: unknown[], defaultProfileId?: string) {
  const profileIds = (profiles as Array<{ profile_id: string }>).map((profile) => profile.profile_id);
  return {
    contract: controlPlaneContract(),
    provider: "openai",
    oauth_supported: true,
    bootstrap_supported: true,
    callback_supported: true,
    reconnect_supported: true,
    revoke_supported: true,
    default_selection_supported: true,
    default_profile_id: defaultProfileId,
    available_profile_ids: profileIds,
    state: profileIds.length > 0 ? "configured" : "unconfigured",
    note: profileIds.length > 0 ? "OpenAI provider has available auth profiles." : "Connect a profile to continue."
  };
}

function createApiKeyProfile(overrides: {
  profile_id: string;
  profile_name: string;
}) {
  return {
    profile_id: overrides.profile_id,
    provider: { kind: "openai" },
    profile_name: overrides.profile_name,
    scope: { kind: "global" },
    credential: {
      type: "api_key",
      api_key_vault_ref: "vault://openai/api-key"
    },
    created_at_unix_ms: 100_000,
    updated_at_unix_ms: 100_500
  };
}

function createOauthProfile(overrides: {
  profile_id: string;
  profile_name: string;
}) {
  return {
    profile_id: overrides.profile_id,
    provider: { kind: "openai" },
    profile_name: overrides.profile_name,
    scope: { kind: "global" },
    credential: {
      type: "oauth",
      access_token_vault_ref: "vault://openai/access-token",
      refresh_token_vault_ref: "vault://openai/refresh-token",
      token_endpoint: "https://auth.openai.test/oauth/token",
      client_id: "client-id-1",
      client_secret_vault_ref: "vault://openai/client-secret",
      scopes: ["openid", "profile", "email"],
      expires_at_unix_ms: 220_000,
      refresh_state: {
        failure_count: 0,
        last_success_unix_ms: 150_000,
        next_allowed_refresh_unix_ms: 180_000
      }
    },
    created_at_unix_ms: 140_000,
    updated_at_unix_ms: 150_000
  };
}

function createHealthProfile(overrides: {
  profile_id: string;
  profile_name: string;
  credential_type: string;
  state: string;
  reason: string;
}) {
  return {
    profile_id: overrides.profile_id,
    provider: "openai",
    profile_name: overrides.profile_name,
    scope: "global",
    credential_type: overrides.credential_type,
    state: overrides.state,
    reason: overrides.reason,
    expires_at_unix_ms: overrides.credential_type === "oauth" ? 220_000 : undefined
  };
}

function createCallbackState(overrides: {
  attempt_id: string;
  state: string;
  message: string;
  profile_id?: string;
  completed_at_unix_ms?: number;
}) {
  return {
    contract: controlPlaneContract(),
    provider: "openai",
    attempt_id: overrides.attempt_id,
    state: overrides.state,
    message: overrides.message,
    profile_id: overrides.profile_id,
    completed_at_unix_ms: overrides.completed_at_unix_ms,
    expires_at_unix_ms: overrides.state === "pending" ? 200_000 : undefined
  };
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json"
    }
  });
}

function requestDescriptor(input: RequestInfo | URL, init?: RequestInit) {
  const raw = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  const url = new URL(raw, "http://localhost");
  return {
    url,
    path: url.pathname,
    method: (init?.method ?? "GET").toUpperCase(),
    body: typeof init?.body === "string" ? init.body : ""
  };
}
