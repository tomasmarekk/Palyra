import { Skeleton, Table } from "@heroui/react";
import type { ReactNode } from "react";
import { useMemo } from "react";

import { EmptyState } from "./EmptyState";
import { joinClassNames } from "./utils";

export type EntityTableColumn<T extends object> = {
  key: string;
  label: ReactNode;
  render: (item: T) => ReactNode;
  isRowHeader?: boolean;
  align?: "start" | "end";
};

type EntityTableProps<T extends object> = {
  ariaLabel: string;
  columns: readonly EntityTableColumn<T>[];
  rows: readonly T[];
  getRowId: (item: T, index: number) => string;
  emptyTitle?: string;
  emptyDescription?: string;
  loading?: boolean;
  className?: string;
};

type TableRowItem<T extends object> = {
  rowKey: string;
  value: T;
};

export function EntityTable<T extends object>({
  ariaLabel,
  columns,
  rows,
  getRowId,
  emptyTitle = "No records loaded",
  emptyDescription = "Refresh or adjust the current filters to load records.",
  loading = false,
  className
}: EntityTableProps<T>) {
  const resolvedColumns = useMemo<readonly EntityTableColumn<T>[]>(() => {
    if (columns.some((column) => column.isRowHeader)) {
      return columns;
    }
    return columns.map((column, index) =>
      index === 0 ? { ...column, isRowHeader: true } : column
    );
  }, [columns]);
  const columnMap = useMemo(
    () => new Map(resolvedColumns.map((column) => [column.key, column])),
    [resolvedColumns]
  );
  const tableRows = useMemo<TableRowItem<T>[]>(
    () =>
      rows.map((item, index) => ({
        rowKey: getRowId(item, index),
        value: item
      })),
    [getRowId, rows]
  );

  if (loading) {
    return (
      <div className={joinClassNames("workspace-table-wrap", className)}>
        <Skeleton className="h-12 rounded-2xl" />
        <div className="mt-3 grid gap-3">
          <Skeleton className="h-16 rounded-2xl" />
          <Skeleton className="h-16 rounded-2xl" />
          <Skeleton className="h-16 rounded-2xl" />
        </div>
      </div>
    );
  }

  return (
    <div className={joinClassNames("workspace-table-wrap", className)}>
      <Table className="workspace-entity-table">
        <Table.ScrollContainer>
          <Table.Content aria-label={ariaLabel}>
            <Table.Header columns={resolvedColumns}>
              {(column) => (
                <Table.Column id={column.key} isRowHeader={column.isRowHeader}>
                  {column.label}
                </Table.Column>
              )}
            </Table.Header>
            <Table.Body
              items={tableRows}
              renderEmptyState={() => (
                <EmptyState
                  compact
                  description={emptyDescription}
                  title={emptyTitle}
                />
              )}
            >
              {(item) => (
                <Table.Row id={item.rowKey}>
                  <Table.Collection items={resolvedColumns}>
                    {(column) => (
                      <Table.Cell
                        id={column.key}
                        className={column.align === "end" ? "workspace-table-cell--end" : undefined}
                      >
                        {columnMap.get(column.key)?.render(item.value) ?? null}
                      </Table.Cell>
                    )}
                  </Table.Collection>
                </Table.Row>
              )}
            </Table.Body>
          </Table.Content>
        </Table.ScrollContainer>
      </Table>
    </div>
  );
}
