import { ScrollShadow, Skeleton } from "@heroui/react";

import { attentionTone } from "./desktopPresentation";
import { SectionCard, StatusChip } from "./ui";

type AttentionCardProps = {
  attentionItems: readonly string[];
  loading: boolean;
  previewMode: boolean;
};

export function AttentionCard({
  attentionItems,
  loading,
  previewMode
}: AttentionCardProps) {
  const tone = attentionTone(attentionItems.length);

  return (
    <SectionCard
      eyebrow="Attention"
      title="Warnings and recovery notes"
      description="Redacted warnings and diagnostics are folded into one small operator queue for the launcher."
      actions={
        <StatusChip tone={tone}>
          {attentionItems.length === 0 ? "Stable" : "Review"}
        </StatusChip>
      }
    >
      {loading ? (
        <div className="desktop-loading-stack">
          <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
          <Skeleton className="desktop-skeleton desktop-skeleton--detail" />
        </div>
      ) : attentionItems.length === 0 ? (
        <p className="desktop-muted">
          Local runtime signals are currently clean. {previewMode ? "Preview data is active for this desktop surface." : "If the dashboard still refuses to open, refresh the snapshot once before retrying the handoff."}
        </p>
      ) : (
        <ScrollShadow className="desktop-scroll-list" hideScrollBar size={56}>
          <ul className="desktop-attention-list">
            {attentionItems.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </ScrollShadow>
      )}
    </SectionCard>
  );
}
