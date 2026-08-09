//! Deterministic Codex JSON-RPC fixture for managed-bridge conformance.
//!
//! The fixture exercises initialize, thread/turn, dynamic tools, steering, and
//! interrupt semantics without network, credentials, or filesystem authority.

use std::{
    env,
    io::{self, BufRead, Write},
};

use serde_json::{json, Value};

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut active_turn: Option<(String, String)> = None;
    let mut active_model = String::new();
    let mut waiting_for_tool = false;
    for line in stdin.lock().lines() {
        let line = line?;
        let message: Value = serde_json::from_str(line.as_str())
            .map_err(|_| io::Error::other("invalid JSON-RPC"))?;
        if matches!(message.get("id").and_then(Value::as_u64), Some(900 | 901)) {
            waiting_for_tool = false;
            if active_model == "crash-after-tool"
                && message.get("id").and_then(Value::as_u64) == Some(900)
            {
                std::process::exit(29);
            }
            if let Some((thread_id, turn_id)) = active_turn.take() {
                write_turn_completed(&thread_id, &turn_id, "completed")?;
            }
            continue;
        }
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            continue;
        };
        let request_id = message.get("id").cloned();
        match method {
            "initialize" => write_response(
                request_id,
                json!({
                    "userAgent": env::var("PALYRA_FAKE_CODEX_VERSION")
                        .unwrap_or_else(|_| "codex-cli/0.147.0".to_owned()),
                    "codexHome": "fixture",
                    "platformFamily": "fixture",
                    "platformOs": "fixture"
                }),
            )?,
            "initialized" => {}
            "thread/start" => {
                validate_host_owned_tool_boundary(&message, "/params/sandbox")?;
                write_response(
                    request_id,
                    json!({"thread": {"id": "codex-thread-fixture", "turns": []}}),
                )?;
            }
            "thread/resume" => write_response(
                request_id,
                json!({"thread": {"id": "codex-thread-fixture", "turns": []}}),
            )?,
            "turn/start" => {
                validate_host_owned_tool_boundary(&message, "/params/sandboxPolicy/type")?;
                let thread_id = message
                    .pointer("/params/threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("codex-thread-fixture")
                    .to_owned();
                let model =
                    message.pointer("/params/model").and_then(Value::as_str).unwrap_or_default();
                let turn_id = "codex-turn-fixture".to_owned();
                if model == "early-event" {
                    write_agent_delta(&thread_id, &turn_id, "early codex fixture")?;
                }
                write_response(
                    request_id,
                    json!({
                        "turn": {"id": turn_id, "items": [], "status": "inProgress"}
                    }),
                )?;
                active_turn = Some((thread_id.clone(), turn_id.clone()));
                active_model = model.to_owned();
                if model == "crash-after-start" {
                    std::process::exit(23);
                }
                if model == "stderr-secret" {
                    eprintln!("Authorization: Bearer fixture-secret-token");
                }
                if model == "unknown-event" {
                    write_frame(&json!({
                        "method": "fixture/unknown-event",
                        "params": {"secret": "must-not-be-projected"},
                    }))?;
                }
                if model != "hang" && model != "steer" {
                    if model != "early-event" {
                        write_agent_delta(&thread_id, &turn_id, "codex fixture")?;
                    }
                    if matches!(model, "text-only" | "unknown-event" | "stderr-secret") {
                        write_turn_completed(&thread_id, &turn_id, "completed")?;
                        active_turn = None;
                        continue;
                    }
                    if model == "approval" {
                        waiting_for_tool = true;
                        write_frame(&json!({
                            "id": 901,
                            "method": "item/commandExecution/requestApproval",
                            "params": {
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "itemId": "codex-approval-item",
                                "approvalId": "codex-approval-fixture",
                            }
                        }))?;
                        continue;
                    }
                    waiting_for_tool = true;
                    write_frame(&json!({
                        "id": 900,
                        "method": "item/tool/call",
                        "params": {
                            "threadId": "codex-thread-fixture",
                            "turnId": "codex-turn-fixture",
                            "callId": "codex-tool-fixture",
                            "tool": "palyra.fixture",
                            "arguments": {"value": 1}
                        }
                    }))?;
                }
            }
            "turn/steer" => {
                write_response(
                    request_id,
                    json!({
                        "turnId": active_turn
                            .as_ref()
                            .map(|(_, turn_id)| turn_id.as_str())
                            .unwrap_or("codex-turn-fixture")
                    }),
                )?;
                if active_model == "steer" {
                    if let Some((thread_id, turn_id)) = active_turn.take() {
                        write_agent_delta(&thread_id, &turn_id, "steered codex fixture")?;
                        write_turn_completed(&thread_id, &turn_id, "completed")?;
                    }
                }
            }
            "turn/interrupt" => {
                write_response(request_id, json!({}))?;
                if let Some((thread_id, turn_id)) = active_turn.take() {
                    write_turn_completed(&thread_id, &turn_id, "interrupted")?;
                }
            }
            _ => {
                if let Some(id) = request_id {
                    write_frame(&json!({
                        "id": id,
                        "error": {"code": -32601, "message": "fixture method unsupported"}
                    }))?;
                }
            }
        }
    }
    if waiting_for_tool {
        return Err(io::Error::other("fixture closed while waiting for host tool result"));
    }
    Ok(())
}

fn validate_host_owned_tool_boundary(message: &Value, sandbox_pointer: &str) -> io::Result<()> {
    let environments = message
        .pointer("/params/environments")
        .and_then(Value::as_array)
        .ok_or_else(|| io::Error::other("Codex request omitted environment isolation"))?;
    let sandbox = message.pointer(sandbox_pointer).and_then(Value::as_str);
    if !environments.is_empty()
        || message.pointer("/params/cwd").is_some()
        || !matches!(sandbox, Some("read-only" | "readOnly"))
    {
        return Err(io::Error::other("Codex request exposed native environment access"));
    }
    Ok(())
}

fn write_response(id: Option<Value>, result: Value) -> io::Result<()> {
    let id = id.ok_or_else(|| io::Error::other("request omitted id"))?;
    write_frame(&json!({"id": id, "result": result}))
}

fn write_agent_delta(thread_id: &str, turn_id: &str, delta: &str) -> io::Result<()> {
    write_frame(&json!({
        "method": "item/agentMessage/delta",
        "params": {
            "threadId": thread_id,
            "turnId": turn_id,
            "itemId": "agent-message-fixture",
            "delta": delta,
        }
    }))
}

fn write_turn_completed(thread_id: &str, turn_id: &str, status: &str) -> io::Result<()> {
    write_frame(&json!({
        "method": "turn/completed",
        "params": {
            "threadId": thread_id,
            "turn": {
                "id": turn_id,
                "items": [],
                "status": status,
            }
        }
    }))
}

fn write_frame(frame: &Value) -> io::Result<()> {
    serde_json::to_writer(io::stdout(), frame)?;
    io::stdout().write_all(b"\n")?;
    io::stdout().flush()
}
