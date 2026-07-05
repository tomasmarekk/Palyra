export interface Widget {
  name: string;
}

export function buildWidget(name: string): Widget {
  return { name };
}

const privateDetail = (widget: Widget): string => widget.name;
