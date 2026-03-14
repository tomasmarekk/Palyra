import type { PropsWithChildren, ReactNode } from "react";
import { useEffect, useState } from "react";

import { WorkspaceStatusChip, type WorkspaceTone } from "./WorkspaceChrome";

type WorkspaceInlineNoticeProps = PropsWithChildren<{
  title?: string;
  tone?: WorkspaceTone;
  className?: string;
}>;

type WorkspaceEmptyStateProps = {
  title: string;
  description: string;
  action?: ReactNode;
  compact?: boolean;
};

type WorkspaceRedactedValueProps = {
  label: string;
  value: string | null | undefined;
  sensitive?: boolean;
  revealed?: boolean;
  onReveal?: () => void;
  allowCopy?: boolean;
  placeholder?: string;
  hint?: string;
};

type WorkspaceConfirmDialogProps = {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  title: string;
  description: string;
  confirmLabel: string;
  confirmTone?: WorkspaceTone;
  isBusy?: boolean;
  onConfirm: () => void;
};

type WorkspaceTableProps = {
  ariaLabel: string;
  columns: readonly ReactNode[];
  children: ReactNode;
  className?: string;
};

function joinClassNames(...values: Array<string | undefined | false>): string {
  return values.filter(Boolean).join(" ");
}

export function workspaceToneForState(state: string | null | undefined): WorkspaceTone {
  const normalized = state?.trim().toLowerCase() ?? "";

  if (
    normalized === "healthy" ||
    normalized === "running" ||
    normalized === "active" ||
    normalized === "ready" ||
    normalized === "ok" ||
    normalized === "success" ||
    normalized === "succeeded" ||
    normalized === "enabled" ||
    normalized === "paired" ||
    normalized === "connected" ||
    normalized === "static"
  ) {
    return "success";
  }

  if (
    normalized === "degraded" ||
    normalized === "warning" ||
    normalized === "expiring" ||
    normalized === "cooldown" ||
    normalized === "not_due" ||
    normalized === "pending" ||
    normalized === "queued"
  ) {
    return "warning";
  }

  if (
    normalized === "down" ||
    normalized === "failed" ||
    normalized === "blocked" ||
    normalized === "error" ||
    normalized === "expired" ||
    normalized === "missing" ||
    normalized === "quarantined"
  ) {
    return "danger";
  }

  if (normalized === "configured" || normalized === "custom") {
    return "accent";
  }

  return "default";
}

export function WorkspaceInlineNotice({
  title,
  tone = "default",
  className,
  children
}: WorkspaceInlineNoticeProps) {
  return (
    <section
      className={joinClassNames(
        "workspace-inline-notice",
        tone !== "default" && `workspace-inline-notice--${tone}`,
        className
      )}
    >
      {title !== undefined && (
        <div className="workspace-inline-notice__header">
          <WorkspaceStatusChip tone={tone}>{title}</WorkspaceStatusChip>
        </div>
      )}
      <div className="workspace-inline-notice__body">{children}</div>
    </section>
  );
}

export function WorkspaceEmptyState({
  title,
  description,
  action,
  compact = false
}: WorkspaceEmptyStateProps) {
  return (
    <section
      className={joinClassNames(
        "workspace-empty-state",
        compact && "workspace-empty-state--compact"
      )}
    >
      <div className="workspace-empty-state__copy">
        <h4>{title}</h4>
        <p className="chat-muted">{description}</p>
      </div>
      {action !== undefined && <div className="workspace-empty-state__action">{action}</div>}
    </section>
  );
}

export function WorkspaceRedactedValue({
  label,
  value,
  sensitive = true,
  revealed = false,
  onReveal,
  allowCopy = false,
  placeholder = "Not loaded",
  hint
}: WorkspaceRedactedValueProps) {
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
    if (!canCopy || navigator.clipboard === undefined) {
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
          <WorkspaceStatusChip tone={sensitive ? "warning" : "default"}>
            {sensitive ? (revealed ? "Sensitive / visible" : "Sensitive / masked") : "Standard"}
          </WorkspaceStatusChip>
          {sensitive && !revealed && onReveal !== undefined && (
            <button type="button" className="secondary" onClick={onReveal}>
              Reveal
            </button>
          )}
          {canCopy && revealed && (
            <button type="button" className="secondary" onClick={() => void handleCopy()}>
              {copied ? "Copied" : "Copy"}
            </button>
          )}
        </div>
      </div>
      <pre className="workspace-redacted-value__body">{displayValue}</pre>
    </div>
  );
}

export function WorkspaceConfirmDialog({
  isOpen,
  onOpenChange,
  title,
  description,
  confirmLabel,
  confirmTone = "danger",
  isBusy = false,
  onConfirm
}: WorkspaceConfirmDialogProps) {
  if (!isOpen) {
    return null;
  }

  return (
    <div className="workspace-dialog-backdrop" role="presentation" onClick={() => onOpenChange(false)}>
      <div
        className="workspace-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="workspace-dialog-title"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="workspace-dialog__header">
          <h3 id="workspace-dialog-title">{title}</h3>
        </header>
        <div className="workspace-dialog__body">
          <p>{description}</p>
        </div>
        <footer className="workspace-dialog__footer">
          <button type="button" className="secondary" onClick={() => onOpenChange(false)} disabled={isBusy}>
            Cancel
          </button>
          <button
            type="button"
            className={confirmTone === "danger" ? "button--warn" : undefined}
            onClick={onConfirm}
            disabled={isBusy}
          >
            {isBusy ? "Working..." : confirmLabel}
          </button>
        </footer>
      </div>
    </div>
  );
}

export function WorkspaceTable({
  ariaLabel,
  columns,
  children,
  className
}: WorkspaceTableProps) {
  return (
    <div className={joinClassNames("workspace-table-wrap", className)}>
      <table className="workspace-table" aria-label={ariaLabel}>
        <thead>
          <tr>
            {columns.map((column, index) => (
              <th key={`workspace-table-column-${index}`}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

function maskSensitiveValue(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length <= 6) {
    return "[redacted]";
  }
  const suffix = trimmed.slice(-4);
  return `••••••••${suffix}`;
}
