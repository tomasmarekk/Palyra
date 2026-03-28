use palyra_control_plane as control_plane;

use crate::args::DevicesCommand;
use crate::*;

pub(crate) fn run_devices(command: DevicesCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_devices_async(command))
}

async fn run_devices_async(command: DevicesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;

    match command {
        DevicesCommand::List { json, ndjson } => {
            let json = output::preferred_json(json);
            let ndjson = output::preferred_ndjson(json, ndjson);
            let envelope = context.client.list_devices().await?;
            if json {
                output::print_json_pretty(&envelope, "failed to encode devices list as JSON")
            } else if ndjson {
                for device in &envelope.devices {
                    output::print_json_line(
                        &json!({ "type": "device", "device": device }),
                        "failed to encode devices list item as NDJSON",
                    )?;
                }
                Ok(())
            } else {
                println!("devices.list count={}", envelope.devices.len());
                for device in &envelope.devices {
                    println!(
                        "device id={} client_kind={} status={} paired_at_unix_ms={} updated_at_unix_ms={} issued_by={} approval_id={}",
                        device.device_id,
                        device.client_kind,
                        device.status,
                        device.paired_at_unix_ms,
                        device.updated_at_unix_ms,
                        text_or_none(device.issued_by.as_str()),
                        text_or_none(device.approval_id.as_str()),
                    );
                }
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
        DevicesCommand::Show { device_id, json } => {
            let envelope = context.client.get_device(device_id.as_str()).await?;
            emit_device_envelope("devices.show", &envelope, output::preferred_json(json))
        }
        DevicesCommand::Rotate { device_id, json } => {
            let envelope = context.client.rotate_device(device_id.as_str()).await?;
            emit_device_envelope("devices.rotate", &envelope, output::preferred_json(json))
        }
        DevicesCommand::Revoke { device_id, reason, json } => {
            let envelope = context
                .client
                .revoke_device(device_id.as_str(), &control_plane::DeviceActionRequest { reason })
                .await?;
            emit_device_envelope("devices.revoke", &envelope, output::preferred_json(json))
        }
        DevicesCommand::Remove { device_id, reason, json } => {
            let envelope = context
                .client
                .remove_device(device_id.as_str(), &control_plane::DeviceActionRequest { reason })
                .await?;
            emit_device_envelope("devices.remove", &envelope, output::preferred_json(json))
        }
        DevicesCommand::Clear { revoked_only, json } => {
            let envelope = context
                .client
                .clear_devices(&control_plane::DeviceClearRequest { revoked_only })
                .await?;
            if output::preferred_json(json) {
                output::print_json_pretty(&envelope, "failed to encode device clear result as JSON")
            } else {
                println!("devices.clear deleted={}", envelope.deleted);
                std::io::stdout().flush().context("stdout flush failed")
            }
        }
    }
}

fn emit_device_envelope(
    event: &str,
    envelope: &control_plane::DeviceEnvelope,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(envelope, "failed to encode device output as JSON");
    }

    let device = &envelope.device;
    println!(
        "{event} id={} client_kind={} status={} paired_at_unix_ms={} updated_at_unix_ms={} approval_id={} cert_expires_at_unix_ms={} revoked_reason={} revoked_at_unix_ms={} removed_at_unix_ms={}",
        device.device_id,
        device.client_kind,
        device.status,
        device.paired_at_unix_ms,
        device.updated_at_unix_ms,
        text_or_none(device.approval_id.as_str()),
        option_i64_text(device.current_certificate_expires_at_unix_ms),
        option_text(device.revoked_reason.as_deref()),
        option_i64_text(device.revoked_at_unix_ms),
        option_i64_text(device.removed_at_unix_ms),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn option_i64_text(value: Option<i64>) -> String {
    value.map(|inner| inner.to_string()).unwrap_or_else(|| "none".to_owned())
}

fn option_text(value: Option<&str>) -> &str {
    value.filter(|inner| !inner.trim().is_empty()).unwrap_or("none")
}

fn text_or_none(value: &str) -> &str {
    if value.trim().is_empty() {
        "none"
    } else {
        value
    }
}
