package fixtures;

public final class Widget {
    public static Widget buildWidget(String name) {
        return new Widget(name);
    }

    private final String name;

    private Widget(String name) {
        this.name = name;
    }
}
