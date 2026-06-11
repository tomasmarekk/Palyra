//! Interactive prompts for the Discord guided setup flow.
//!
//! Prompts write to stderr and read from stdin so stdout stays reserved for
//! the pinned machine-readable result lines; defaults favor the most
//! restrictive inbound scope.

use anyhow::{bail, Context, Result};
use std::io::Write;

use crate::{prompt_secret_value, prompt_yes_no};

/// Asks whether the deployment is remote/VPS and returns the setup mode
/// keyword.
///
/// # Errors
/// Fails when the prompt cannot be read.
pub(crate) fn setup_mode() -> Result<String> {
    if prompt_yes_no("Deployment mode remote/VPS? [y/N]: ")? {
        Ok("remote_vps".to_owned())
    } else {
        Ok("local".to_owned())
    }
}

/// Prompts for the bot credential with hidden input.
///
/// # Errors
/// Fails when the hidden prompt cannot be read or the input is empty.
pub(crate) fn setup_token() -> Result<String> {
    let value = prompt_secret_value("Discord bot token (input hidden, paste and press Enter): ")?;
    if value.trim().is_empty() {
        bail!("discord setup requires a non-empty bot token");
    }
    Ok(value)
}

/// Prompts for the inbound scope; the empty default selects DM-only, the
/// most restrictive option.
///
/// # Errors
/// Fails when the selection cannot be read or is not a known scope.
pub(crate) fn inbound_scope() -> Result<String> {
    eprint!(
        "Inbound scope: [1] DM only, [2] Allowlisted guild senders (recommended), [3] Open guild channels: "
    );
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .context("failed to read discord inbound scope selection")?;
    let normalized = answer.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "1" | "dm" | "dm_only" | "dm-only" => Ok("dm_only".to_owned()),
        "2" | "allowlisted" | "allowlisted_guild_channels" | "allowlisted-guild-channels" => {
            Ok("allowlisted_guild_channels".to_owned())
        }
        "3" | "open" | "open_guild_channels" | "open-guild-channels" => {
            Ok("open_guild_channels".to_owned())
        }
        _ => bail!("unsupported inbound scope selection: {}", answer.trim()),
    }
}

/// Prompts for a comma-separated sender filter list, validating, lowercasing,
/// and deduplicating the entries.
///
/// # Errors
/// Fails when input cannot be read or an entry contains characters outside
/// the allowed identifier set.
pub(crate) fn sender_filters(prompt: &str) -> Result<Vec<String>> {
    eprint!("{prompt}");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).context("failed to read sender filter input")?;
    let mut values = Vec::new();
    for candidate in line.split(',').map(str::trim).filter(|value| !value.is_empty()) {
        if !candidate.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '@' | ':' | '/' | '#')
        }) {
            bail!("sender filter contains unsupported value '{}'", candidate);
        }
        let normalized = candidate.to_ascii_lowercase();
        if !values.iter().any(|existing| existing == &normalized) {
            values.push(normalized);
        }
    }
    Ok(values)
}

/// Prompts for the broadcast strategy; the empty default selects deny.
///
/// # Errors
/// Fails when the selection cannot be read or is not a known strategy.
pub(crate) fn broadcast_strategy() -> Result<String> {
    eprint!("Broadcast strategy [deny|mention_only|allow] (default deny): ");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).context("failed to read broadcast strategy")?;
    let normalized = answer.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" | "deny" => Ok("deny".to_owned()),
        "mention_only" | "mention-only" => Ok("mention_only".to_owned()),
        "allow" => Ok("allow".to_owned()),
        _ => bail!("unsupported broadcast strategy: {}", answer.trim()),
    }
}

/// Prompts for the per-channel concurrency limit (1-32, default 2).
///
/// # Errors
/// Fails when the input cannot be read, does not parse, or is out of range.
pub(crate) fn concurrency_limit() -> Result<u64> {
    eprint!("Concurrency limit per channel (1-32, default 2): ");
    std::io::stderr().flush().context("stderr flush failed")?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).context("failed to read concurrency limit")?;
    let trimmed = answer.trim();
    if trimmed.is_empty() {
        return Ok(2);
    }
    let parsed = trimmed
        .parse::<u64>()
        .with_context(|| format!("invalid concurrency limit '{}'", trimmed))?;
    if !(1..=32).contains(&parsed) {
        bail!("concurrency limit must be within 1..=32");
    }
    Ok(parsed)
}
