import { vi } from "vitest";

export type MockRequest = {
  url: URL;
  path: string;
  method: string;
  body: string;
  headers: Headers;
  init?: RequestInit;
};

type RouteHandler = (request: MockRequest) => Response | Promise<Response> | undefined;

export function createFetchRouter(...handlers: RouteHandler[]) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = requestDescriptor(input, init);
    for (const handler of handlers) {
      const response = await handler(request);
      if (response !== undefined) {
        return response;
      }
    }
    throw new Error(`Unhandled mocked request: ${request.method} ${request.path}`);
  });
}

export function requestDescriptor(input: RequestInfo | URL, init?: RequestInit): MockRequest {
  const raw =
    typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  const url = new URL(raw, "http://localhost");
  return {
    url,
    path: url.pathname,
    method: (init?.method ?? "GET").toUpperCase(),
    body: requestBody(init?.body),
    headers: new Headers(init?.headers),
    init,
  };
}

export function sessionPayload(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    principal: "admin:web-console",
    device_id: "device-1",
    channel: "web",
    csrf_token: "csrf-1",
    issued_at_unix_ms: 100,
    expires_at_unix_ms: 300,
    ...overrides,
  };
}

export function sessionResponse(overrides: Partial<Record<string, unknown>> = {}) {
  return jsonResponse(sessionPayload(overrides));
}

export function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}

export function ndjsonResponse(lines: unknown[]): Response {
  const body = `${lines.map((line) => JSON.stringify(line)).join("\n")}\n`;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/x-ndjson",
    },
  });
}

export function requestBody(body: BodyInit | null | undefined): string {
  if (typeof body === "string") {
    return body;
  }
  return "";
}
