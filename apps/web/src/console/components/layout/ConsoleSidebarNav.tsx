import { Button, Card, CardContent } from "@heroui/react";
import { useNavigate } from "react-router-dom";

import type { ConsoleMessageKey } from "../../i18n";
import { getNavigationGroups, getSectionPath } from "../../navigation";
import type { ConsoleUiMode } from "../../preferences";
import type { Section } from "../../useConsoleAppState";

type ConsoleSidebarNavProps = {
  currentSection: Section;
  onSelect: (section: Section) => void;
  onSwitchMode: () => void;
  t: (key: ConsoleMessageKey, variables?: Record<string, string | number>) => string;
  uiMode: ConsoleUiMode;
};

export function ConsoleSidebarNav({
  currentSection,
  onSelect,
  onSwitchMode,
  t,
  uiMode,
}: ConsoleSidebarNavProps) {
  const navigate = useNavigate();
  const navigationGroups = getNavigationGroups(uiMode, currentSection);
  const currentModeLabel = uiMode === "basic" ? t("shell.basic") : t("shell.advanced");
  const nextModeLabel = uiMode === "basic" ? t("nav.switchAdvanced") : t("nav.switchBasic");

  return (
    <Card className="workspace-card" variant="default">
      <CardContent className="grid gap-4 p-4">
        <div className="console-sidebar-header">
          <p className="console-label">{t("nav.title")}</p>
          <div className="grid gap-1">
            <h2 className="text-lg font-semibold tracking-tight">{currentModeLabel}</h2>
            <p className="console-copy">{t("nav.subtitle")}</p>
          </div>
        </div>
        {uiMode === "basic" ? (
          <div className="rounded-xl border border-border/80 bg-surface/60 p-3">
            <p className="text-sm font-semibold">{t("nav.basicTitle")}</p>
            <p className="mt-1 text-xs text-muted">{t("nav.basicBody")}</p>
            <Button className="mt-3" size="sm" variant="secondary" onPress={onSwitchMode}>
              {nextModeLabel}
            </Button>
          </div>
        ) : (
          <Button size="sm" variant="ghost" onPress={onSwitchMode}>
            {nextModeLabel}
          </Button>
        )}

        <div className="console-sidebar-scroll">
          <nav aria-label="Dashboard domains" className="grid gap-4 pr-2">
            {navigationGroups.map((group) => (
              <div key={group.id} className="console-sidebar-group">
                <p className="text-xs font-semibold uppercase tracking-[0.2em] text-muted">
                  {t(group.labelKey)}
                </p>
                <div className="console-nav-list">
                  {group.items.map((entry) => {
                    const selected = currentSection === entry.id;
                    const accessibleLabel = legacyNavigationLabel(entry.id) ?? entry.label;
                    return (
                      <Button
                        key={entry.id}
                        aria-current={selected ? "page" : undefined}
                        aria-label={accessibleLabel}
                        className="console-nav-item"
                        fullWidth
                        size="sm"
                        variant={selected ? "secondary" : "ghost"}
                        onPress={() => {
                          onSelect(entry.id);
                          void navigate(getSectionPath(entry.id));
                        }}
                      >
                        <span
                          aria-hidden="true"
                          className="flex h-8 w-8 items-center justify-center rounded-lg border"
                        >
                          <NavigationGlyph section={entry.id} />
                        </span>
                        <span className="console-nav-item__content">
                          <span className="text-sm font-semibold">{t(entry.labelKey)}</span>
                          <span className="text-xs text-muted">{t(entry.detailKey)}</span>
                        </span>
                      </Button>
                    );
                  })}
                </div>
              </div>
            ))}
          </nav>
        </div>
      </CardContent>
    </Card>
  );
}

function legacyNavigationLabel(section: Section): string | null {
  switch (section) {
    case "chat":
      return "Chat and Sessions";
    case "canvas":
      return "Canvas workspace";
    case "sessions":
      return "Session Catalog";
    case "usage":
      return "Usage and Capacity";
    case "logs":
      return "Logs and Runtime Stream";
    case "inventory":
      return "Inventory and Runtime Health";
    case "cron":
      return "Automations";
    case "channels":
      return "Channels and Router";
    case "support":
      return "Support and Recovery";
    default:
      return null;
  }
}

function NavigationGlyph({ section }: { section: Section }) {
  const commonProps = {
    className: "h-5 w-5",
    fill: "none",
    stroke: "currentColor",
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
    strokeWidth: 1.8,
    viewBox: "0 0 24 24",
  };

  switch (section) {
    case "chat":
      return (
        <svg {...commonProps}>
          <path d="M5 7.5A2.5 2.5 0 0 1 7.5 5h9A2.5 2.5 0 0 1 19 7.5v5A2.5 2.5 0 0 1 16.5 15H11l-4 4V15.5A2.5 2.5 0 0 1 5 13.1z" />
        </svg>
      );
    case "canvas":
      return (
        <svg {...commonProps}>
          <rect x="5" y="5" width="14" height="14" rx="2.5" />
          <path d="M9 9h6" />
          <path d="M9 12h6" />
          <path d="M9 15h3" />
        </svg>
      );
    case "overview":
      return (
        <svg {...commonProps}>
          <path d="M5 19V9l7-4 7 4v10" />
          <path d="M9 19v-5h6v5" />
        </svg>
      );
    case "sessions":
      return (
        <svg {...commonProps}>
          <rect x="5" y="6" width="14" height="12" rx="2" />
          <path d="M8 10h8" />
          <path d="M8 14h5" />
        </svg>
      );
    case "usage":
      return (
        <svg {...commonProps}>
          <path d="M6 18h12" />
          <path d="M8 18v-5" />
          <path d="M12 18V8" />
          <path d="M16 18v-8" />
        </svg>
      );
    case "logs":
      return (
        <svg {...commonProps}>
          <path d="M6 7h12" />
          <path d="M6 12h8" />
          <path d="M6 17h12" />
          <path d="M18 10.5 20.5 12 18 13.5" />
        </svg>
      );
    case "inventory":
      return (
        <svg {...commonProps}>
          <rect x="5" y="6" width="14" height="5" rx="1.5" />
          <rect x="5" y="13" width="6" height="5" rx="1.5" />
          <rect x="13" y="13" width="6" height="5" rx="1.5" />
        </svg>
      );
    case "approvals":
      return (
        <svg {...commonProps}>
          <path d="M7 12.5 10.5 16 17 8" />
          <path d="M12 3 5 6v5c0 4.2 2.7 8 7 10 4.3-2 7-5.8 7-10V6z" />
        </svg>
      );
    case "cron":
      return (
        <svg {...commonProps}>
          <circle cx="12" cy="12" r="8" />
          <path d="M12 8v4l3 2" />
        </svg>
      );
    case "channels":
      return (
        <svg {...commonProps}>
          <path d="M6 8h12" />
          <path d="M6 12h8" />
          <path d="M6 16h10" />
          <path d="M18 9.5 21 12l-3 2.5" />
        </svg>
      );
    case "browser":
      return (
        <svg {...commonProps}>
          <rect x="4" y="5" width="16" height="14" rx="3" />
          <path d="M4 9h16" />
          <path d="M8 7.25h.01" />
          <path d="M11 7.25h.01" />
        </svg>
      );
    case "skills":
      return (
        <svg {...commonProps}>
          <path d="m8 13 3 3 5-6" />
          <path d="M6 5h12v14H6z" />
        </svg>
      );
    case "memory":
      return (
        <svg {...commonProps}>
          <path d="M8 7.5A3.5 3.5 0 0 1 11.5 4H14a4 4 0 0 1 0 8h-2.5A3.5 3.5 0 0 0 8 15.5V20" />
          <path d="M16 20H8" />
        </svg>
      );
    case "auth":
      return (
        <svg {...commonProps}>
          <rect x="6" y="10" width="12" height="9" rx="2" />
          <path d="M9 10V8a3 3 0 1 1 6 0v2" />
        </svg>
      );
    case "config":
      return (
        <svg {...commonProps}>
          <path d="M12 4v4" />
          <path d="M12 16v4" />
          <path d="M4 12h4" />
          <path d="M16 12h4" />
          <circle cx="12" cy="12" r="3" />
        </svg>
      );
    case "access":
      return (
        <svg {...commonProps}>
          <path d="M8 12a4 4 0 0 1 8 0" />
          <path d="M8 12v4" />
          <path d="M16 12v4" />
          <path d="M6 19h12" />
        </svg>
      );
    case "operations":
      return (
        <svg {...commonProps}>
          <path d="M12 5v7l4 2" />
          <circle cx="12" cy="12" r="8" />
        </svg>
      );
    case "support":
      return (
        <svg {...commonProps}>
          <circle cx="12" cy="12" r="8" />
          <path d="M9.5 9.5a2.5 2.5 0 1 1 4.5 1.5c-.5.7-1.5 1.2-2 2" />
          <path d="M12 16h.01" />
        </svg>
      );
  }
}
