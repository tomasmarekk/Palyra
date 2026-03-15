import { Alert } from "@heroui/react";
import type { PropsWithChildren } from "react";

import { joinClassNames, resolveAlertStatus, type UiTone } from "./utils";

type InlineNoticeProps = PropsWithChildren<{
  title?: string;
  tone?: UiTone;
  className?: string;
}>;

export function InlineNotice({
  title,
  tone = "default",
  className,
  children
}: InlineNoticeProps) {
  return (
    <Alert className={joinClassNames("desktop-inline-notice", className)} status={resolveAlertStatus(tone)}>
      <Alert.Content>
        {title !== undefined && <Alert.Title>{title}</Alert.Title>}
        <Alert.Description>{children}</Alert.Description>
      </Alert.Content>
    </Alert>
  );
}
