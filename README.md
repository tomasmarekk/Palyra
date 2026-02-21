# Palyra

This repository is currently in the development phase.

## Developer Bootstrap

Use a single environment entrypoint before local development:

```bash
just doctor
```

`just doctor` runs `palyra doctor --strict` and fails fast on missing required dependencies.

Then bootstrap and build:

```bash
just dev
```

If you prefer Make, equivalent commands are available:

```bash
make doctor
make dev
```

![This is Fine](https://user-images.githubusercontent.com/93007558/216892041-5599d3d8-50e0-4d46-8171-5021d69d7745.gif)
