use anyhow::{bail, Context, Result};

use crate::{client::channels as channels_client, output};

use super::{emit, request};

#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    account_id: String,
    to: String,
    text: String,
    confirm: bool,
    auto_reaction: Option<String>,
    thread_id: Option<String>,
    url: Option<String>,
    token: Option<String>,
    principal: String,
    device_id: String,
    channel: Option<String>,
    json_output: bool,
) -> Result<()> {
    if !confirm {
        bail!("discord verify requires explicit confirmation (--confirm)");
    }
    let connector_id = request::connector_id(account_id.as_str())?;
    let request_context =
        channels_client::resolve_request_context(url, token, principal, device_id, channel)?;
    let endpoint =
        format!(
            "{}/admin/v1/channels/{}/test-send",
            request_context.base_url.trim_end_matches('/'),
            connector_id
        );
    let client = channels_client::build_client()?;
    let response = channels_client::send_request(
        client.post(endpoint).json(&request::verify_payload(
            to.as_str(),
            text.as_str(),
            auto_reaction.as_deref(),
            thread_id.as_deref(),
        )),
        request_context,
        "failed to call discord channels test-send endpoint",
    )?;
    if output::preferred_json(json_output) {
        println!(
            "{}",
            serde_json::to_string_pretty(&response)
                .context("failed to encode discord channels test-send payload as JSON")?
        );
    } else {
        emit::emit_verify_success(connector_id.as_str(), &response);
    }
    Ok(())
}
