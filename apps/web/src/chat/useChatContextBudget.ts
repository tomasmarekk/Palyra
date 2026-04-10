import { useMemo } from "react";

import { buildContextBudgetSummary, type ComposerAttachment } from "./chatShared";

type UseChatContextBudgetParams = {
  attachments: ComposerAttachment[];
  composerText: string;
  contextReferencePreview: {
    total_estimated_tokens: number;
  } | null;
  contextReferencePreviewStale: boolean;
  runTotalTokens: number;
  sessionTotalTokens: number;
};

export function useChatContextBudget({
  attachments,
  composerText,
  contextReferencePreview,
  contextReferencePreviewStale,
  runTotalTokens,
  sessionTotalTokens,
}: UseChatContextBudgetParams) {
  return useMemo(
    () =>
      buildContextBudgetSummary({
        baseline_tokens: Math.max(sessionTotalTokens, runTotalTokens),
        draft_text: composerText,
        attachments,
        reference_tokens:
          contextReferencePreview !== null && !contextReferencePreviewStale
            ? contextReferencePreview.total_estimated_tokens
            : 0,
      }),
    [
      attachments,
      composerText,
      contextReferencePreview,
      contextReferencePreviewStale,
      runTotalTokens,
      sessionTotalTokens,
    ],
  );
}
