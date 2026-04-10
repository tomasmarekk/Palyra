import { ChatConsolePanel } from "../chat/ChatConsolePanel";
import { AccessSection } from "./sections/AccessSection";
import { AgentsSection } from "./sections/AgentsSection";
import { ApprovalsSection } from "./sections/ApprovalsSection";
import { AuthSection } from "./sections/AuthSection";
import { BrowserSection } from "./sections/BrowserSection";
import { ChannelsSection } from "./sections/ChannelsSection";
import { ConfigSection } from "./sections/ConfigSection";
import { CronSection } from "./sections/CronSection";
import { InventorySection } from "./sections/InventorySection";
import { MemorySection } from "./sections/MemorySection";
import { LogsSection } from "./sections/LogsSection";
import { OperationsSection } from "./sections/OperationsSection";
import { OverviewSection } from "./sections/OverviewSection";
import { SecretsSection } from "./sections/SecretsSection";
import { SessionsSection } from "./sections/SessionsSection";
import { SkillsSection } from "./sections/SkillsSection";
import { SupportSection } from "./sections/SupportSection";
import { UsageSection } from "./sections/UsageSection";
import type { ConsoleAppState } from "./useConsoleAppState";

type ConsoleSectionContentProps = {
  app: ConsoleAppState;
};

export function ConsoleSectionContent({ app }: ConsoleSectionContentProps) {
  switch (app.section) {
    case "overview":
      return <OverviewSection app={app} />;
    case "chat":
      return (
        <ChatConsolePanel
          api={app.api}
          revealSensitiveValues={app.revealSensitiveValues}
          setError={app.setError}
          setNotice={app.setNotice}
          openBrowserSessionWorkbench={app.openBrowserSessionWorkbench}
        />
      );
    case "sessions":
      return (
        <SessionsSection app={{ api: app.api, setError: app.setError, setNotice: app.setNotice }} />
      );
    case "usage":
      return (
        <UsageSection
          app={{
            api: app.api,
            setError: app.setError,
            setNotice: app.setNotice,
            diagnosticsSnapshot: app.diagnosticsSnapshot,
            memoryStatus: app.memoryStatus,
          }}
        />
      );
    case "logs":
      return (
        <LogsSection
          app={{
            api: app.api,
            setError: app.setError,
            setNotice: app.setNotice,
            revealSensitiveValues: app.revealSensitiveValues,
          }}
        />
      );
    case "inventory":
      return (
        <InventorySection
          app={{
            api: app.api,
            setError: app.setError,
            setNotice: app.setNotice,
            revealSensitiveValues: app.revealSensitiveValues,
          }}
        />
      );
    case "approvals":
      return <ApprovalsSection app={app} />;
    case "cron":
      return <CronSection app={app} />;
    case "channels":
      return <ChannelsSection app={app} />;
    case "browser":
      return <BrowserSection app={app} />;
    case "agents":
      return <AgentsSection app={app} />;
    case "memory":
      return <MemorySection app={app} />;
    case "skills":
      return <SkillsSection app={app} />;
    case "auth":
      return <AuthSection app={app} />;
    case "config":
      return <ConfigSection app={app} />;
    case "secrets":
      return <SecretsSection app={app} />;
    case "access":
      return <AccessSection app={app} />;
    case "operations":
      return <OperationsSection app={app} />;
    case "support":
      return <SupportSection app={app} />;
    default:
      return null;
  }
}
