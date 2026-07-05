class Widget:
    def __init__(self, name: str) -> None:
        self.name = name


def build_widget(name: str) -> Widget:
    return Widget(name)


def _private_detail(widget: Widget) -> str:
    return widget.name
