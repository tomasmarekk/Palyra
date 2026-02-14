# Fuzzing Harness Skeleton (M03)

This folder contains parser-focused fuzz targets required by M03:

- `config_path_parser`
- `a2ui_json_parser`
- `webhook_payload_parser`

## Prerequisites

- nightly Rust toolchain
- `cargo-fuzz` (`cargo install cargo-fuzz`)

## Build all targets

```bash
cargo +nightly fuzz build config_path_parser
cargo +nightly fuzz build a2ui_json_parser
cargo +nightly fuzz build webhook_payload_parser
```

## Run a short campaign

```bash
cargo +nightly fuzz run config_path_parser -- -max_total_time=60
cargo +nightly fuzz run a2ui_json_parser -- -max_total_time=60
cargo +nightly fuzz run webhook_payload_parser -- -max_total_time=60
```
