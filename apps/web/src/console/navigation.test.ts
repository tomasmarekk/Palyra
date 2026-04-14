import { describe, expect, it } from "vite-plus/test";

import { getNavigationGroups } from "./navigation";

describe("console navigation modes", () => {
  it("keeps only the first-success rail visible in basic mode", () => {
    const ids = getNavigationGroups("basic", "overview").flatMap((group) =>
      group.items.map((item) => item.id),
    );

    expect(ids).toEqual(["chat", "overview", "sessions", "support", "approvals", "access"]);
  });

  it("keeps the active section reachable even when it is normally advanced-only", () => {
    const ids = getNavigationGroups("basic", "config").flatMap((group) =>
      group.items.map((item) => item.id),
    );

    expect(ids).toContain("config");
    expect(ids).toContain("approvals");
    expect(ids).not.toContain("usage");
  });
});
