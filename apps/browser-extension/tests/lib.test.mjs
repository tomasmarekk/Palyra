import test from "node:test";
import assert from "node:assert/strict";

import {
  clampUtf8Bytes,
  decodeDataUrlByteLength,
  normalizeExtensionId,
  normalizeRelayBaseUrl,
  parseAllowlistPrefixes,
  validateOpenTabUrl,
} from "../lib.mjs";

test("normalizeRelayBaseUrl accepts loopback hosts", () => {
  assert.equal(normalizeRelayBaseUrl("http://127.0.0.1:7142/"), "http://127.0.0.1:7142");
  assert.equal(normalizeRelayBaseUrl("https://localhost:9443"), "https://localhost:9443");
});

test("normalizeRelayBaseUrl rejects non-loopback hosts", () => {
  assert.throws(
    () => normalizeRelayBaseUrl("https://example.com:7142"),
    /loopback host/,
  );
});

test("normalizeExtensionId enforces allowed characters", () => {
  assert.equal(normalizeExtensionId("com.palyra.extension"), "com.palyra.extension");
  assert.throws(() => normalizeExtensionId("bad id"), /unsupported characters/);
});

test("parseAllowlistPrefixes normalizes CSV and line input", () => {
  const parsed = parseAllowlistPrefixes("https://,\nhttp://localhost");
  assert.deepEqual(parsed, ["https://", "http://localhost"]);
});

test("validateOpenTabUrl enforces prefix allowlist", () => {
  assert.equal(
    validateOpenTabUrl("https://docs.palyra.dev", ["https://docs."]),
    "https://docs.palyra.dev/",
  );
  assert.throws(
    () => validateOpenTabUrl("https://malicious.example", ["https://docs."]),
    /not allowed/,
  );
});

test("clampUtf8Bytes truncates without splitting UTF-8 scalars", () => {
  const input = "alpha-žluťoučký-kůň";
  const result = clampUtf8Bytes(input, 10);
  assert.equal(result.truncated, true);
  assert.ok(Buffer.byteLength(result.value, "utf8") <= 10);
});

test("decodeDataUrlByteLength estimates base64 payload bytes", () => {
  const dataUrl = "data:image/png;base64,QUJDRA==";
  assert.equal(decodeDataUrlByteLength(dataUrl), 4);
});
