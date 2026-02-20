# palyra-skills

`palyra-skills` provides the M20 skill artifact contract:

- parse + validate versioned `skill.toml`
- build signed `.palyra-skill` ZIP artifacts
- verify signature, integrity, SBOM, and provenance
- map capabilities to runtime grants and policy request bindings
- evaluate publisher trust using allowlist and TOFU

## CLI integration

The main CLI exposes this crate via:

- `palyra skills package build ...`
- `palyra skills package verify ...`

## Example

A complete example skill manifest and payload skeleton is in:

- `examples/echo-http/skill.toml`
- `examples/echo-http/module.wasm`
- `examples/echo-http/sbom.cdx.json`
- `examples/echo-http/provenance.json`

Build an artifact:

```powershell
$signingKeyHex = "<64-hex-chars>"
$signingKeyHex | palyra skills package build `
  --manifest crates/palyra-skills/examples/echo-http/skill.toml `
  --module crates/palyra-skills/examples/echo-http/module.wasm `
  --asset crates/palyra-skills/examples/echo-http/assets/prompt.txt `
  --sbom crates/palyra-skills/examples/echo-http/sbom.cdx.json `
  --provenance crates/palyra-skills/examples/echo-http/provenance.json `
  --output dist/acme.echo_http.palyra-skill `
  --signing-key-stdin
```
