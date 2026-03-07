import { ChatConsolePanel } from "../chat/ChatConsolePanel";
import { AccessSection } from "./sections/AccessSection";
import { ApprovalsSection } from "./sections/ApprovalsSection";
import { AuthSection } from "./sections/AuthSection";
import { BrowserSection } from "./sections/BrowserSection";
import { ChannelsSection } from "./sections/ChannelsSection";
import { ConfigSection } from "./sections/ConfigSection";
import { CronSection } from "./sections/CronSection";
import { MemorySection } from "./sections/MemorySection";
import { OperationsSection } from "./sections/OperationsSection";
import { OverviewSection } from "./sections/OverviewSection";
import { SkillsSection } from "./sections/SkillsSection";
import { SupportSection } from "./sections/SupportSection";
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
    case "memory":
      return <MemorySection app={app} />;
    case "skills":
      return <SkillsSection app={app} />;
    case "auth":
      return <AuthSection app={app} />;
    case "config":
      return <ConfigSection app={app} />;
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
