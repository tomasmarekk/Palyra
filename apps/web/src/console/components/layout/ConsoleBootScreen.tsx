import { Card, CardContent, Spinner } from "@heroui/react";

import type { ConsoleLocale } from "../../preferences";
import type { ConsoleMessageKey } from "../../i18n";

type ConsoleBootScreenProps = {
  locale: ConsoleLocale;
  t: (key: ConsoleMessageKey, variables?: Record<string, string | number>) => string;
};

export function ConsoleBootScreen({ t }: ConsoleBootScreenProps) {
  return (
    <div className="console-root console-root--auth flex min-h-screen items-center justify-center">
      <Card className="workspace-card w-full max-w-lg" variant="secondary">
        <CardContent className="grid gap-4 px-6 py-7 text-center">
          <div className="grid justify-items-center gap-3">
            <Spinner color="current" size="sm" />
            <p className="console-label">{t("boot.label")}</p>
          </div>
          <div className="grid gap-2">
            <h1 className="text-2xl font-semibold tracking-tight">{t("boot.title")}</h1>
            <p className="console-copy">{t("boot.body")}</p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
