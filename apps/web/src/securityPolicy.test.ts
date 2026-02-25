import { describe, expect, it } from "vitest";

import indexHtml from "../index.html?raw";

function parseCspFromIndexHtml(html: string): string {
  const document = new DOMParser().parseFromString(html, "text/html");
  const meta = document.querySelector("meta[http-equiv='Content-Security-Policy']");
  return meta?.getAttribute("content")?.trim() ?? "";
}

describe("Web Security Policy", () => {
  it("declares strict CSP directives for scripts and framing", () => {
    const csp = parseCspFromIndexHtml(indexHtml);
    expect(csp).not.toBe("");
    expect(csp).toContain("script-src 'self'");
    expect(csp).toContain("frame-ancestors 'none'");
    expect(csp).toContain("object-src 'none'");
    expect(csp).not.toContain("'unsafe-inline'");
  });

  it("does not include inline script blocks in index.html", () => {
    const document = new DOMParser().parseFromString(indexHtml, "text/html");
    const scripts = Array.from(document.querySelectorAll("script"));
    const inlineScripts = scripts.filter((script) => !script.hasAttribute("src"));
    expect(inlineScripts).toHaveLength(0);
  });
});
