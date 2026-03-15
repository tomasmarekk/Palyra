import { Button, ButtonGroup, Card, CardContent, Spinner } from "@heroui/react";

import { type ActionName, actionLabel } from "./desktopPresentation";

type LifecycleActionBarProps = {
  actionState: ActionName;
  isGatewayRunning: boolean;
  onAction: (action: Exclude<ActionName, null>) => void;
  onRefresh: () => void;
};

export function LifecycleActionBar({
  actionState,
  isGatewayRunning,
  onAction,
  onRefresh
}: LifecycleActionBarProps) {
  const isBusy = actionState !== null;

  return (
    <Card className="desktop-surface">
      <CardContent className="desktop-action-bar">
        <div className="desktop-action-bar__cluster">
          <p className="desktop-label">Lifecycle</p>
          <ButtonGroup className="desktop-action-group" hideSeparator>
            <Button
              isDisabled={isBusy}
              variant="primary"
              onPress={() => onAction("start")}
            >
              {actionLabel(actionState, "start", "Start Palyra", "Starting Palyra...")}
            </Button>
            <Button
              isDisabled={isBusy || !isGatewayRunning}
              variant="outline"
              onPress={() => onAction("stop")}
            >
              {actionLabel(actionState, "stop", "Stop Runtime", "Stopping Runtime...")}
            </Button>
            <Button
              isDisabled={isBusy || !isGatewayRunning}
              variant="secondary"
              onPress={() => onAction("restart")}
            >
              {actionLabel(actionState, "restart", "Restart Runtime", "Restarting Runtime...")}
            </Button>
          </ButtonGroup>
        </div>

        <div className="desktop-action-bar__cluster desktop-action-bar__cluster--end">
          <p className="desktop-label">Handoff</p>
          <ButtonGroup className="desktop-action-group" hideSeparator>
            <Button
              isDisabled={isBusy}
              variant="ghost"
              onPress={() => onAction("dashboard")}
            >
              {actionLabel(actionState, "dashboard", "Open Dashboard", "Opening Dashboard...")}
            </Button>
            <Button isDisabled={isBusy} variant="ghost" onPress={onRefresh}>
              Refresh Snapshot
            </Button>
          </ButtonGroup>
          <div className="desktop-action-meta" aria-live="polite">
            {isBusy ? (
              <>
                <Spinner size="sm" color="current" />
                <span>Waiting for the desktop bridge to finish the requested action.</span>
              </>
            ) : (
              <span>Actions keep the existing Tauri bridge and snapshot refresh flow unchanged.</span>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
