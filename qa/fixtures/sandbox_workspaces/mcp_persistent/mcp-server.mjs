import readline from "node:readline";
import { existsSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const protocolVersion = "2025-06-18";
const crashMarker = fileURLToPath(new URL(".mcp-crash-once", import.meta.url));
let toolCallCount = 0;

function send(payload) {
  process.stdout.write(`${JSON.stringify(payload)}\n`);
}

function result(id, payload) {
  send({ jsonrpc: "2.0", id, result: payload });
}

function error(id, code, message) {
  send({ jsonrpc: "2.0", id, error: { code, message } });
}

function handle(message) {
  const { id, method, params = {} } = message;
  if (method === "initialize") {
    result(id, {
      protocolVersion,
      capabilities: {
        tools: { listChanged: true },
        resources: {},
        prompts: {},
      },
      serverInfo: { name: "palyra-qa-mcp", version: "1.0.0" },
    });
    return;
  }
  if (method === "notifications/initialized") {
    return;
  }
  if (method === "tools/list") {
    result(id, {
      tools: [
        {
          name: "inspect",
          description: "Confirms that two calls share one persistent MCP session.",
          inputSchema: {
            type: "object",
            properties: {
              ordinal: { type: "integer", minimum: 1, maximum: 2 },
              mode: { type: "string", enum: ["crash_once"] },
            },
            additionalProperties: false,
          },
        },
      ],
    });
    return;
  }
  if (method === "resources/list") {
    result(id, { resources: [] });
    return;
  }
  if (method === "prompts/list") {
    result(id, { prompts: [] });
    return;
  }
  if (method === "ping") {
    result(id, {});
    return;
  }
  if (method === "tools/call") {
    if (params.name === "inspect" && params.arguments?.mode === "crash_once") {
      if (!existsSync(crashMarker)) {
        writeFileSync(crashMarker, "crashed\n", { encoding: "utf8", mode: 0o600 });
        process.exit(42);
      }
      result(id, {
        content: [{ type: "text", text: "reconnect-recovered" }],
        structuredContent: { reconnectRecovered: true },
      });
      return;
    }
    toolCallCount += 1;
    const ordinal = params.arguments?.ordinal;
    if (params.name !== "inspect" || ordinal !== toolCallCount) {
      error(id, -32001, "persistent session call order mismatch");
      return;
    }
    result(id, {
      content: [
        {
          type: "text",
          text: `persistent-call-${toolCallCount}`,
        },
      ],
      structuredContent: {
        persistentCallCount: toolCallCount,
      },
    });
    return;
  }
  error(id, -32601, "method not found");
}

const input = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

input.on("line", (line) => {
  try {
    handle(JSON.parse(line));
  } catch {
    error(null, -32700, "parse error");
  }
});
