import { Button } from "@heroui/react";
import { useEffect, useState } from "react";

import { StatusChip } from "./StatusChip";

type RedactedValueProps = {
  label: string;
  value: string | null | undefined;
  sensitive?: boolean;
  revealed?: boolean;
  onReveal?: () => void;
  allowCopy?: boolean;
  placeholder?: string;
  hint?: string;
};

export function RedactedValue({
  label,
  value,
  sensitive = true,
  revealed = false,
  onReveal,
  allowCopy = false,
  placeholder = "Not loaded",
  hint
}: RedactedValueProps) {
  const [copied, setCopied] = useState(false);
  const canCopy = allowCopy && typeof value === "string" && value.length > 0;
  const displayValue =
    value === null || value === undefined || value.length === 0
      ? placeholder
      : sensitive && !revealed
        ? maskSensitiveValue(value)
        : value;
  const sensitivityHint =
    hint ??
    (sensitive
      ? revealed
        ? "Sensitive value is visible only in this session."
        : "Sensitive value stays masked until you explicitly reveal it."
      : "Value is non-sensitive.");

  useEffect(() => {
    if (!copied) {
      return;
    }

    const timeout = window.setTimeout(() => setCopied(false), 1500);
    return () => window.clearTimeout(timeout);
  }, [copied]);

  async function handleCopy(): Promise<void> {
    if (!canCopy || navigator.clipboard === undefined || value === null || value === undefined) {
      return;
    }

    await navigator.clipboard.writeText(value);
    setCopied(true);
  }

  return (
    <div className="workspace-redacted-value">
      <div className="workspace-redacted-value__header">
        <div>
          <p className="console-label">{label}</p>
          <p className="chat-muted">{sensitivityHint}</p>
        </div>
        <div className="workspace-inline">
          <StatusChip tone={sensitive ? "warning" : "default"}>
            {sensitive ? (revealed ? "Sensitive / visible" : "Sensitive / masked") : "Standard"}
          </StatusChip>
          {sensitive && !revealed && onReveal !== undefined ? (
            <Button size="sm" variant="secondary" onPress={onReveal}>
              Reveal
            </Button>
          ) : null}
          {canCopy && revealed ? (
            <Button
              aria-label={copied ? "Copied to clipboard" : "Copy to clipboard"}
              size="sm"
              variant="ghost"
              onPress={() => void handleCopy()}
            >
              {copied ? "Copied" : "Copy"}
            </Button>
          ) : null}
        </div>
      </div>
      <pre className="workspace-redacted-value__body">{displayValue}</pre>
    </div>
  );
}

function maskSensitiveValue(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length <= 6) {
    return "[redacted]";
  }

  return `••••••••${trimmed.slice(-4)}`;
}
