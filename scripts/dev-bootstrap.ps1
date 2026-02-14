$ErrorActionPreference = "Stop"

if (Get-Command just -ErrorAction SilentlyContinue) {
  just dev
  exit 0
}

Write-Output "just is not installed; running fallback bootstrap sequence."
cargo run -p palyra-cli --bin palyra -- doctor --strict
cargo build --workspace --locked
