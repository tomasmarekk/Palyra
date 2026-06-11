//! Output rendering for `palyra support-bundle export`.
//!
//! The text line and JSON field names are pinned by CLI parity tests.

use crate::*;

/// Emits the support-bundle export summary as pretty JSON or a pinned text
/// line; this honors the root context's JSON preference, not just the flag.
///
/// # Errors
/// Returns an error when JSON encoding or the stdout write/flush fails.
pub(crate) fn emit_export(
    output_path: &Path,
    encoded_bytes: usize,
    bundle: &SupportBundle,
    json: bool,
) -> Result<()> {
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &json!({
                "path": output_path.display().to_string(),
                "bytes": encoded_bytes,
                "truncated": bundle.truncated,
                "warnings": bundle.warnings,
            }),
            "failed to encode support bundle export as JSON",
        );
    }
    println!(
        "support_bundle.export path={} bytes={} truncated={} warnings={}",
        output_path.display(),
        encoded_bytes,
        bundle.truncated,
        bundle.warnings.len()
    );
    std::io::stdout().flush().context("stdout flush failed")
}
