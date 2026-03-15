import { Button, Card, CardContent, Chip, ScrollShadow } from "@heroui/react";
import { useNavigate } from "react-router-dom";

import { CONSOLE_NAV_GROUPS, getSectionPath } from "../../navigation";
import type { Section } from "../../useConsoleAppState";

type ConsoleSidebarNavProps = {
  currentSection: Section;
  onSelect: (section: Section) => void;
};

export function ConsoleSidebarNav({ currentSection, onSelect }: ConsoleSidebarNavProps) {
  const navigate = useNavigate();

  return (
    <Card className="border border-white/30 bg-white/75 shadow-xl shadow-slate-900/10 backdrop-blur-xl dark:border-white/10 dark:bg-slate-950/70">
      <CardContent className="gap-6 p-5">
        <div className="space-y-2">
          <p className="console-label">Navigation</p>
          <div className="space-y-1">
            <h2 className="text-xl font-semibold tracking-tight text-slate-950 dark:text-slate-50">
              Operator domains
            </h2>
            <p className="text-sm leading-6 text-slate-600 dark:text-slate-300">
              Chat, control, agent, and settings areas stay grouped around the live operator
              surface rather than backend taxonomy.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Chip size="sm" variant="secondary">
              Route-driven shell
            </Chip>
            <Chip size="sm" variant="soft">
              Hash links
            </Chip>
          </div>
        </div>

        <ScrollShadow className="console-sidebar__scroll">
          <nav aria-label="Dashboard domains" className="space-y-5 pr-2">
            {CONSOLE_NAV_GROUPS.map((group) => (
              <div key={group.id} className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-slate-400 dark:text-slate-500">
                  {group.label}
                </p>
                <div className="space-y-2">
                  {group.items.map((entry) => {
                    const selected = currentSection === entry.id;
                    const accessibleLabel = legacyNavigationLabel(entry.id) ?? entry.label;
                    return (
                      <Button
                        key={entry.id}
                        aria-current={selected ? "page" : undefined}
                        aria-label={accessibleLabel}
                        className="justify-start px-4 py-3"
                        fullWidth
                        variant={selected ? "secondary" : "ghost"}
                        onPress={() => {
                          onSelect(entry.id);
                          void navigate(getSectionPath(entry.id));
                        }}
                      >
                        <span
                          aria-hidden="true"
                          className={`flex h-10 w-10 items-center justify-center rounded-2xl border ${selected ? "border-accent/35 bg-accent/10 text-accent-700 dark:text-accent-300" : "border-white/40 bg-white/60 text-slate-500 dark:border-white/10 dark:bg-slate-900/80 dark:text-slate-300"}`}
                        >
                          <NavigationGlyph section={entry.id} />
                        </span>
                        <span className="flex flex-1 flex-col items-start text-left">
                          <span className="text-sm font-semibold">{entry.label}</span>
                          <span className="text-xs text-foreground-500">{entry.detail}</span>
                        </span>
                      </Button>
                    );
                  })}
                </div>
              </div>
            ))}
          </nav>
        </ScrollShadow>
      </CardContent>
    </Card>
  );
}

function legacyNavigationLabel(section: Section): string | null {
  switch (section) {
    case "chat":
      return "Chat and Sessions";
    case "cron":
      return "Cron";
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
    viewBox: "0 0 24 24"
  };

  switch (section) {
    case "chat":
      return (
        <svg {...commonProps}>
          <path d="M5 7.5A2.5 2.5 0 0 1 7.5 5h9A2.5 2.5 0 0 1 19 7.5v5A2.5 2.5 0 0 1 16.5 15H11l-4 4V15.5A2.5 2.5 0 0 1 5 13.1z" />
        </svg>
      );
    case "overview":
      return (
        <svg {...commonProps}>
          <path d="M5 19V9l7-4 7 4v10" />
          <path d="M9 19v-5h6v5" />
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
