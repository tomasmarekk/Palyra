// Direct action tests pin chat command boundaries independently of component rendering.
// Delegation must remain fail-closed when only session-level authority is available.

import { describe, expect, it, vi } from "vite-plus/test";

import type { ChatDelegationCatalog } from "../consoleApi";
import { delegateWorkAction } from "./chatSessionActions";

describe("delegateWorkAction", () => {
  it("rejects a valid delegation because the console cannot create child-run authority", async () => {
    const delegationCatalog: ChatDelegationCatalog = {
      profiles: [],
      templates: [
        {
          template_id: "review_and_patch",
          display_name: "Review and patch",
          description: "Review a bounded change and apply the approved patch.",
          primary_profile_id: "reviewer",
          recommended_profiles: [],
          execution_mode: "serial",
          merge_strategy: "reviewed_patch",
          examples: [],
        },
      ],
    };
    const setError = vi.fn();

    await delegateWorkAction({
      sessionId: "session-1",
      raw: "review_and_patch Inspect the failing lint job",
      delegationCatalog,
      setError,
    });

    expect(setError).toHaveBeenCalledOnce();
    expect(setError).toHaveBeenCalledWith(
      "Delegation is available only from an active agent run; the console cannot create child-run authority.",
    );
  });
});
