//! Deterministic stdio language server used by managed LSP conformance tests.
//!
//! The fixture implements only the bounded protocol surface needed by the
//! tests. It performs no filesystem or network access.

use std::env;
use std::io::{self, BufRead, Write as _};

use serde_json::{json, Value};

const MAX_FIXTURE_FRAME_BYTES: usize = 1024 * 1024;

fn main() -> io::Result<()> {
    let mode = env::var("PALYRA_LSP_FIXTURE_MODE").unwrap_or_else(|_| "normal".to_owned());
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut initialized = false;
    loop {
        let Some(message) = read_message(&mut reader)? else {
            return Ok(());
        };
        let method = message.get("method").and_then(Value::as_str);
        let id = message.get("id").cloned();
        match method {
            Some("initialize") => {
                if mode == "initialize_timeout" {
                    continue;
                }
                if mode == "malformed" {
                    io::stdout().write_all(b"Content-Length: 4\r\n\r\nnope")?;
                    io::stdout().flush()?;
                    continue;
                }
                if mode == "oversize" {
                    io::stdout().write_all(b"Content-Length: 999999999\r\n\r\n")?;
                    io::stdout().flush()?;
                    continue;
                }
                let capabilities = if mode == "sensitive_capabilities" {
                    json!({
                        "workspace": {
                            "fixtureCanary": "fixture-secret-capability",
                            "fixturePath": "C:\\private\\workspace"
                        }
                    })
                } else {
                    json!({
                        "textDocumentSync": 1,
                        "diagnosticProvider": false
                    })
                };
                write_message(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "capabilities": capabilities,
                        "serverInfo": {
                            "name": "palyra-lsp-fixture",
                            "version": "1"
                        }
                    }
                }))?;
                io::stderr().write_all(b"fixture initialized\n")?;
                io::stderr().flush()?;
                initialized = true;
                if mode == "crash_after_initialize" {
                    std::process::exit(23);
                }
            }
            Some("initialized") => {}
            Some("fixture/echo") if initialized => {
                write_message(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": message.get("params").cloned().unwrap_or(Value::Null)
                }))?;
            }
            Some("fixture/crash") if initialized => std::process::exit(23),
            Some("fixture/hang") if initialized => {}
            Some("textDocument/didOpen") if initialized => {
                let document =
                    message.pointer("/params/textDocument").cloned().unwrap_or(Value::Null);
                if mode != "no_diagnostics" {
                    publish_diagnostics(
                        document.get("uri").and_then(Value::as_str).unwrap_or_default(),
                        document.get("version").and_then(Value::as_i64).unwrap_or_default(),
                        document.get("text").and_then(Value::as_str).unwrap_or_default(),
                    )?;
                }
            }
            Some("textDocument/didChange") if initialized => {
                let uri = message
                    .pointer("/params/textDocument/uri")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let version = message
                    .pointer("/params/textDocument/version")
                    .and_then(Value::as_i64)
                    .unwrap_or_default();
                let text = message
                    .pointer("/params/contentChanges/0/text")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if mode != "no_diagnostics" && mode != "diagnostics_once" {
                    publish_diagnostics(uri, version, text)?;
                }
            }
            Some("textDocument/didClose") if initialized => {
                let uri = message
                    .pointer("/params/textDocument/uri")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                publish_diagnostics(uri, 0, "")?;
            }
            Some("shutdown") => {
                write_message(&json!({"jsonrpc": "2.0", "id": id, "result": null}))?;
            }
            Some("exit") => return Ok(()),
            Some("$/cancelRequest") => {}
            Some(_) if id.is_some() => {
                write_message(&json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32601, "message": "method not found"}
                }))?;
            }
            _ => {}
        }
    }
}

fn publish_diagnostics(uri: &str, version: i64, text: &str) -> io::Result<()> {
    let diagnostics = text
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains("ERROR"))
        .map(|(line, _)| {
            json!({
                "range": {
                    "start": {"line": line, "character": 0},
                    "end": {"line": line, "character": 5}
                },
                "severity": 1,
                "code": "fixture.error",
                "source": "palyra-lsp-fixture",
                "message": "fixture error"
            })
        })
        .collect::<Vec<_>>();
    write_message(&json!({
        "jsonrpc": "2.0",
        "method": "textDocument/publishDiagnostics",
        "params": {
            "uri": uri,
            "version": version,
            "diagnostics": diagnostics
        }
    }))
}

fn read_message(reader: &mut impl BufRead) -> io::Result<Option<Value>> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) =
            line.strip_prefix("Content-Length:").or_else(|| line.strip_prefix("content-length:"))
        {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| io::Error::other("invalid content length"))?,
            );
        }
    }
    let length = content_length.ok_or_else(|| io::Error::other("missing content length"))?;
    if length == 0 || length > MAX_FIXTURE_FRAME_BYTES {
        return Err(io::Error::other("fixture frame exceeds limit"));
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body)?;
    serde_json::from_slice(body.as_slice()).map(Some).map_err(io::Error::other)
}

fn write_message(message: &Value) -> io::Result<()> {
    let body = serde_json::to_vec(message).map_err(io::Error::other)?;
    let stdout = io::stdout();
    let mut writer = stdout.lock();
    write!(writer, "Content-Length: {}\r\n\r\n", body.len())?;
    writer.write_all(body.as_slice())?;
    writer.flush()
}
