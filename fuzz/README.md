# Fuzzing Harness Skeleton (M03)

This folder contains parser-focused fuzz targets required by M03:

- `config_path_parser`
- `a2ui_json_parser`
- `webhook_payload_parser`
- `workspace_patch_parser`
- `process_runner_input_parser`
- `auth_profile_registry_parser`
- `redaction_routines`
- `channel_payload_validation`
- `webhook_replay_verifier`

## Prerequisites

- nightly Rust toolchain
- `cargo-fuzz` (`cargo install cargo-fuzz`)

## Build all targets

```bash
cargo +nightly fuzz build config_path_parser
cargo +nightly fuzz build a2ui_json_parser
cargo +nightly fuzz build webhook_payload_parser
cargo +nightly fuzz build workspace_patch_parser
cargo +nightly fuzz build process_runner_input_parser
cargo +nightly fuzz build auth_profile_registry_parser
cargo +nightly fuzz build redaction_routines
cargo +nightly fuzz build channel_payload_validation
cargo +nightly fuzz build webhook_replay_verifier
```

## Run a short campaign

```bash
cargo +nightly fuzz run config_path_parser -- -max_total_time=60
cargo +nightly fuzz run a2ui_json_parser -- -max_total_time=60
cargo +nightly fuzz run webhook_payload_parser -- -max_total_time=60
cargo +nightly fuzz run workspace_patch_parser -- -max_total_time=60
cargo +nightly fuzz run process_runner_input_parser -- -max_total_time=60
cargo +nightly fuzz run auth_profile_registry_parser -- -max_total_time=60
cargo +nightly fuzz run redaction_routines -- -max_total_time=60
cargo +nightly fuzz run channel_payload_validation -- -max_total_time=60
cargo +nightly fuzz run webhook_replay_verifier -- -max_total_time=60
```

## Shortcut commands

Repository helpers keep a lightweight baseline build:

```bash
just fuzz-build
# or
make fuzz-build
```

Those shortcuts currently compile:

- `config_path_parser`
- `a2ui_json_parser`
- `webhook_payload_parser`
- `auth_profile_registry_parser`
- `redaction_routines`
- `channel_payload_validation`
- `webhook_replay_verifier`
