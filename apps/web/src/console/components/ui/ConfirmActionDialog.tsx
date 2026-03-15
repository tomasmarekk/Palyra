import { AlertDialog, Button } from "@heroui/react";

import type { UiTone } from "./utils";

type ConfirmActionDialogProps = {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  title: string;
  description: string;
  confirmLabel: string;
  confirmTone?: UiTone;
  isBusy?: boolean;
  onConfirm: () => void;
};

export function ConfirmActionDialog({
  isOpen,
  onOpenChange,
  title,
  description,
  confirmLabel,
  confirmTone = "danger",
  isBusy = false,
  onConfirm
}: ConfirmActionDialogProps) {
  return (
    <AlertDialog isOpen={isOpen} onOpenChange={onOpenChange}>
      <AlertDialog.Trigger aria-hidden="true" className="sr-only">
        Open confirmation dialog
      </AlertDialog.Trigger>
      <AlertDialog.Backdrop />
      <AlertDialog.Container placement="center" size="md">
        <AlertDialog.Dialog>
          <AlertDialog.Header>
            <AlertDialog.Heading>{title}</AlertDialog.Heading>
          </AlertDialog.Header>
          <AlertDialog.Body>{description}</AlertDialog.Body>
          <AlertDialog.Footer>
            <Button
              isDisabled={isBusy}
              variant="secondary"
              onPress={() => onOpenChange(false)}
            >
              Cancel
            </Button>
            <Button
              isDisabled={isBusy}
              onPress={onConfirm}
              variant={confirmTone === "danger" ? "danger" : "primary"}
            >
              {isBusy ? "Working..." : confirmLabel}
            </Button>
          </AlertDialog.Footer>
        </AlertDialog.Dialog>
      </AlertDialog.Container>
    </AlertDialog>
  );
}
