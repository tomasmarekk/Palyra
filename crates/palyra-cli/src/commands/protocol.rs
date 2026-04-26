use crate::*;

pub(crate) fn run_protocol(command: ProtocolCommand) -> Result<()> {
    match command {
        ProtocolCommand::Version { json } => {
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &json!({
                        "protocol_major": CANONICAL_PROTOCOL_MAJOR,
                        "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
                    }),
                    "failed to encode protocol version output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "protocol_major": CANONICAL_PROTOCOL_MAJOR,
                        "json_envelope_version": CANONICAL_JSON_ENVELOPE_VERSION,
                    }),
                    "failed to encode protocol version output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!(
                "protocol.major={} json.envelope.v={}",
                CANONICAL_PROTOCOL_MAJOR, CANONICAL_JSON_ENVELOPE_VERSION
            );
            std::io::stdout().flush().context("stdout flush failed")
        }
        ProtocolCommand::ValidateId { id, json } => {
            validate_canonical_id(&id).with_context(|| format!("invalid canonical ID: {}", id))?;
            if output::preferred_json(json) {
                return output::print_json_pretty(
                    &json!({
                        "valid": true,
                        "id": id,
                    }),
                    "failed to encode canonical ID validation output as JSON",
                );
            }
            if output::preferred_ndjson(json, false) {
                output::print_json_line(
                    &json!({
                        "valid": true,
                        "id": id,
                    }),
                    "failed to encode canonical ID validation output as NDJSON",
                )?;
                return std::io::stdout().flush().context("stdout flush failed");
            }
            println!("canonical_id.valid=true id={id}");
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}
