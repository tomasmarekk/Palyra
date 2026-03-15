import { Children, isValidElement, type ReactElement, type ReactNode } from "react";

import {
  ConfirmActionDialog as WorkspaceConfirmDialog,
  EmptyState as WorkspaceEmptyState,
  EntityTable,
  InlineNotice as WorkspaceInlineNotice,
  KeyValueList as WorkspaceKeyValueList,
  RedactedValue as WorkspaceRedactedValue,
  workspaceToneForState
} from "../ui";

type WorkspaceTableProps = {
  ariaLabel: string;
  columns: readonly ReactNode[];
  children: ReactNode;
  className?: string;
};

type LegacyRow = {
  id: string;
  cells: ReactNode[];
};

export {
  WorkspaceConfirmDialog,
  WorkspaceEmptyState,
  WorkspaceInlineNotice,
  WorkspaceKeyValueList,
  WorkspaceRedactedValue,
  workspaceToneForState
};

export function WorkspaceTable({
  ariaLabel,
  columns,
  children,
  className
}: WorkspaceTableProps) {
  const rows = Children.toArray(children).flatMap((child, rowIndex) => {
    if (!isValidElement<{ children?: ReactNode }>(child)) {
      return [];
    }

    const rowElement = child as ReactElement<{ children?: ReactNode }>;

    return [
      {
        id: String(rowElement.key ?? `workspace-row-${rowIndex}`),
        cells: Children.toArray(rowElement.props.children).map((cell) => {
          if (!isValidElement<{ children?: ReactNode }>(cell)) {
            return cell;
          }

          return (cell as ReactElement<{ children?: ReactNode }>).props.children ?? null;
        })
      }
    ] satisfies LegacyRow[];
  });

  return (
    <EntityTable
      ariaLabel={ariaLabel}
      className={className}
      columns={columns.map((column, index) => ({
        key: `column-${index}`,
        label: column,
        render: (row: LegacyRow) => row.cells[index] ?? null,
        align: index === columns.length - 1 ? "end" : "start"
      }))}
      emptyDescription="No rows are currently available for this table."
      emptyTitle="No rows loaded"
      getRowId={(row) => row.id}
      rows={rows}
    />
  );
}
