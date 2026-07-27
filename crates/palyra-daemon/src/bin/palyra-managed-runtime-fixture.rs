//! Deterministic child process for managed-runtime conformance tests.
//!
//! It speaks only the bounded JSON-line protocol and has no host authority.

use std::{
    env,
    io::{self, BufRead, Write},
};

use serde_json::{json, Value};

fn main() -> io::Result<()> {
    let protocol_version = required_env("PALYRA_RUNTIME_PROTOCOL_VERSION")?;
    let capability_digest = required_env("PALYRA_RUNTIME_CAPABILITY_DIGEST")?;
    let nonce = required_env("PALYRA_RUNTIME_NONCE")?;
    let generation = required_env("PALYRA_RUNTIME_GENERATION")?
        .parse::<u64>()
        .map_err(|_| io::Error::other("invalid runtime generation"))?;
    write_frame(&json!({
        "type": "hello",
        "protocol_version": protocol_version,
        "capability_digest": capability_digest,
        "nonce": nonce,
        "generation": generation,
    }))?;

    let stdin = io::stdin();
    let mut sequence = 0_u64;
    for line in stdin.lock().lines() {
        let line = line?;
        let frame: Value =
            serde_json::from_str(line.as_str()).map_err(|_| io::Error::other("invalid frame"))?;
        let frame_type = frame.get("type").and_then(Value::as_str).unwrap_or_default();
        match frame_type {
            "command" => {
                let command_id =
                    frame.get("command_id").and_then(Value::as_str).unwrap_or("missing");
                let method = frame.get("method").and_then(Value::as_str).unwrap_or_default();
                let fixture_mode =
                    frame.pointer("/payload/model_id").and_then(Value::as_str).unwrap_or_default();
                if method == "crash" || fixture_mode == "crash" {
                    std::process::exit(17);
                }
                if method == "malformed" {
                    io::stdout().write_all(b"{malformed\n")?;
                    io::stdout().flush()?;
                    continue;
                }
                sequence = sequence.saturating_add(1);
                write_frame(&json!({
                    "type": "accepted",
                    "command_id": command_id,
                    "generation": generation,
                    "sequence": sequence,
                }))?;
                if method == "hang" || fixture_mode == "hang" {
                    continue;
                }
                if method == "flood" {
                    for ordinal in 0..4_097_u64 {
                        sequence = sequence.saturating_add(1);
                        write_frame(&json!({
                            "type": "event",
                            "command_id": command_id,
                            "generation": generation,
                            "sequence": sequence,
                            "method": "heartbeat",
                            "payload": {"ordinal": ordinal},
                        }))?;
                    }
                    continue;
                }
                sequence = sequence.saturating_add(1);
                write_frame(&json!({
                    "type": "event",
                    "command_id": command_id,
                    "generation": generation,
                    "sequence": sequence,
                    "method": "text_delta",
                    "payload": {"text": "fixture"},
                }))?;
                sequence = sequence.saturating_add(1);
                write_frame(&json!({
                    "type": "terminal",
                    "command_id": command_id,
                    "generation": generation,
                    "sequence": sequence,
                    "outcome": "completed",
                    "payload": {"final_message": "fixture complete"},
                }))?;
            }
            "cancel" => {
                let command_id =
                    frame.get("command_id").and_then(Value::as_str).unwrap_or("missing");
                sequence = sequence.saturating_add(1);
                write_frame(&json!({
                    "type": "terminal",
                    "command_id": command_id,
                    "generation": generation,
                    "sequence": sequence,
                    "outcome": "cancelled",
                    "payload": {"reason_code": "runtime.fixture.cancelled"},
                }))?;
            }
            "close" => return Ok(()),
            _ => return Err(io::Error::other("unsupported frame type")),
        }
    }
    Ok(())
}

fn required_env(key: &str) -> io::Result<String> {
    env::var(key).map_err(|_| io::Error::other(format!("missing required environment key {key}")))
}

fn write_frame(frame: &Value) -> io::Result<()> {
    serde_json::to_writer(io::stdout(), frame)?;
    io::stdout().write_all(b"\n")?;
    io::stdout().flush()
}
