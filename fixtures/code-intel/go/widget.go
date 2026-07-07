package widget

type Builder struct {
	Name string
}

func BuildWidget(name string) Builder {
	return Builder{Name: name}
}

func (builder Builder) Label() string {
	return builder.Name
}
