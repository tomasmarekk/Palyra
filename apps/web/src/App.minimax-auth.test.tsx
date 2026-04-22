import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { App } from "./App";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("MiniMax web auth surface", () => {
  it("connects API key auth and starts OAuth without a client secret", async () => {
    vi.spyOn(window, "open").mockReturnValue({
      close: vi.fn(),
      focus: vi.fn(),
      closed: false,
    } as unknown as Window);

    const state = {
      profiles: [] as unknown[],
      healthProfiles: [] as unknown[],
      defaultProfileId: undefined as string | undefined,
    };
    let apiKeyBody = "";
    let oauthBody = "";

    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        const request = requestDescriptor(input, init);
        if (request.path === "/console/v1/auth/session" && request.method === "GET") {
          return Promise.resolve(jsonResponse(sessionEnvelope()));
        }
        if (request.path === "/console/v1/approvals" && request.method === "GET") {
          return Promise.resolve(jsonResponse({ approvals: [] }));
        }
        if (request.path === "/console/v1/diagnostics" && request.method === "GET") {
          return Promise.resolve(jsonResponse(diagnosticsEnvelope()));
        }
        if (request.path === "/console/v1/auth/profiles" && request.method === "GET") {
          return Promise.resolve(jsonResponse(profileListEnvelope(state.profiles)));
        }
        if (request.path === "/console/v1/auth/health" && request.method === "GET") {
          return Promise.resolve(jsonResponse(healthEnvelope(state.healthProfiles)));
        }
        if (request.path.startsWith("/console/v1/auth/providers/") && request.method === "GET") {
          return Promise.resolve(
            jsonResponse(providerStateEnvelope(request.path.split("/").at(-1) ?? "openai", state)),
          );
        }
        if (
          request.path === "/console/v1/auth/providers/minimax/api-key" &&
          request.method === "POST"
        ) {
          apiKeyBody = request.body;
          state.defaultProfileId = "minimax-api";
          state.profiles = [minimaxApiProfile()];
          state.healthProfiles = [minimaxHealthProfile("api_key", "static")];
          return Promise.resolve(
            jsonResponse({
              contract: contract(),
              provider: "minimax",
              action: "api_key",
              state: "selected",
              message: "MiniMax API key profile saved.",
              profile_id: "minimax-api",
            }),
          );
        }
        if (
          request.path === "/console/v1/auth/providers/minimax/bootstrap" &&
          request.method === "POST"
        ) {
          oauthBody = request.body;
          return Promise.resolve(
            jsonResponse({
              contract: contract(),
              provider: "minimax",
              attempt_id: "minimax-attempt-1",
              authorization_url: "https://api.minimax.io/oauth/authorize?user_code=ABC123",
              expires_at_unix_ms: 300_000,
              profile_id: "minimax-oauth",
              message: "MiniMax OAuth user code issued.",
            }),
          );
        }
        throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
      }),
    );

    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Profiles" }));

    await selectProvider(firstSubmitButton(), "MiniMax");
    fireEvent.change(screen.getAllByLabelText("Profile name")[0], {
      target: { value: "default-minimax" },
    });
    fireEvent.change(screen.getByLabelText("API key"), { target: { value: "minimax-api-key" } });
    fireEvent.click(firstSubmitButton());

    await waitFor(() => {
      expect(document.body).toHaveTextContent("default-minimax");
    });
    expect(apiKeyBody).toContain('"profile_name":"default-minimax"');
    expect(apiKeyBody).toContain('"api_key":"minimax-api-key"');

    await selectProvider(screen.getByRole("button", { name: /^Start .* OAuth$/ }), "MiniMax");
    fireEvent.change(screen.getAllByLabelText("Profile name")[1], {
      target: { value: "minimax-oauth" },
    });
    fireEvent.click(await screen.findByRole("button", { name: "Start MiniMax OAuth" }));

    await waitFor(() => {
      expect(window.open).toHaveBeenCalledWith(
        "https://api.minimax.io/oauth/authorize?user_code=ABC123",
        "palyra-openai-auth",
        "popup=yes,width=720,height=860,resizable=yes,scrollbars=yes",
      );
    });
    expect(oauthBody).toContain('"profile_name":"minimax-oauth"');
    expect(oauthBody).toContain('"scopes":["group_id","profile","model.completion"]');
    expect(oauthBody).not.toContain("client_secret");
    expect(
      await screen.findByText(
        "MiniMax OAuth window opened. Finish the authorization to complete the profile.",
      ),
    ).toBeInTheDocument();
  }, 20_000);
});

function sessionEnvelope() {
  return {
    principal: "admin:web-console",
    device_id: "device-1",
    channel: "web",
    csrf_token: "csrf-1",
    issued_at_unix_ms: 100,
    expires_at_unix_ms: 200,
  };
}

function contract() {
  return { contract_version: "control-plane.v1" };
}

function profileListEnvelope(profiles: unknown[]) {
  return { contract: contract(), profiles, page: { limit: 100, returned: profiles.length } };
}

function healthEnvelope(profiles: unknown[]) {
  return {
    contract: contract(),
    summary: {
      total: profiles.length,
      ok: 0,
      expiring: 0,
      expired: 0,
      missing: 0,
      static_count: 1,
    },
    expiry_distribution: { static_count: 1, missing: 0 },
    profiles,
  };
}

function providerStateEnvelope(
  provider: string,
  state: { profiles: unknown[]; defaultProfileId?: string },
) {
  const profileIds = state.profiles
    .filter((profile) => providerKey(profile) === provider)
    .map((profile) => (profile as { profile_id: string }).profile_id);
  return {
    contract: contract(),
    provider,
    oauth_supported: provider !== "anthropic",
    bootstrap_supported: provider !== "anthropic",
    callback_supported: provider !== "anthropic",
    reconnect_supported: provider !== "anthropic",
    revoke_supported: true,
    default_selection_supported: true,
    default_profile_id: profileIds.includes(state.defaultProfileId ?? "")
      ? state.defaultProfileId
      : undefined,
    available_profile_ids: profileIds,
    state: profileIds.length > 0 ? "configured" : "unconfigured",
  };
}

function diagnosticsEnvelope() {
  return {
    contract: contract(),
    model_provider: { registry: { providers: [], models: [] } },
  };
}

function minimaxApiProfile() {
  return {
    profile_id: "minimax-api",
    provider: { kind: "custom", custom_name: "minimax" },
    profile_name: "default-minimax",
    scope: { kind: "global" },
    credential: { type: "api_key", api_key_vault_ref: "vault://minimax/api-key" },
    created_at_unix_ms: 100_000,
    updated_at_unix_ms: 100_500,
  };
}

function minimaxHealthProfile(credentialType: string, state: string) {
  return {
    profile_id: "minimax-api",
    provider: "minimax",
    profile_name: "default-minimax",
    scope: "global",
    credential_type: credentialType,
    state,
    reason: "MiniMax credential is ready.",
  };
}

function providerKey(profile: unknown): string {
  const provider = (profile as { provider?: { kind?: string; custom_name?: string } }).provider;
  return provider?.kind === "custom" && provider.custom_name === "minimax"
    ? "minimax"
    : (provider?.kind ?? "openai");
}

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function requestDescriptor(input: RequestInfo | URL, init?: RequestInit) {
  const raw =
    typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  const url = new URL(raw, "http://localhost");
  return {
    path: url.pathname,
    method: (init?.method ?? "GET").toUpperCase(),
    body: typeof init?.body === "string" ? init.body : "",
  };
}

function firstSubmitButton(): HTMLButtonElement {
  const submitButton = screen
    .getAllByRole("button")
    .find((button) => button.getAttribute("type") === "submit");
  if (!(submitButton instanceof HTMLButtonElement)) {
    throw new Error("Expected API key form submit button.");
  }
  return submitButton;
}

async function selectProvider(submitButton: HTMLElement, providerLabel: string): Promise<void> {
  const form = submitButton.closest("form");
  if (!(form instanceof HTMLFormElement)) {
    throw new Error("Expected submit button to belong to a form.");
  }
  const providerTrigger = within(form)
    .getAllByRole("button")
    .find(
      (button) =>
        button.getAttribute("type") === "button" && button.textContent?.trim() === "OpenAI",
    );
  if (!(providerTrigger instanceof HTMLButtonElement)) {
    throw new Error("Expected provider select trigger.");
  }
  fireEvent.click(providerTrigger);
  fireEvent.click(await screen.findByRole("option", { name: providerLabel }));
}
