pub struct Widget {
    name: String,
}

pub fn build_widget(name: String) -> Widget {
    Widget { name }
}

fn private_detail(widget: &Widget) -> &str {
    widget.name.as_str()
}
