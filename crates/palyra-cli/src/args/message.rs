//! Arguments for `palyra message`: routed channel messaging through a connector
//! id (send, threads, read/search, edit/delete, reactions). Help text is pinned
//! by snapshot tests; see the doc-comment rules in `mod.rs`.

use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum MessageCommand {
    #[command(about = "Show message command status guidance")]
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Capabilities {
        connector_id: String,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Send {
        connector_id: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        text: String,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long)]
        auto_reaction: Option<String>,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        reply_to_message_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Thread {
        connector_id: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        thread_id: String,
        #[arg(long, default_value_t = false)]
        confirm: bool,
        #[arg(long)]
        auto_reaction: Option<String>,
        #[arg(long)]
        reply_to_message_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Read {
        connector_id: String,
        #[arg(long)]
        conversation_id: String,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        message_id: Option<String>,
        #[arg(long)]
        before_message_id: Option<String>,
        #[arg(long)]
        after_message_id: Option<String>,
        #[arg(long)]
        around_message_id: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Search {
        connector_id: String,
        #[arg(long)]
        conversation_id: String,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long)]
        author_id: Option<String>,
        #[arg(long)]
        has_attachments: Option<bool>,
        #[arg(long)]
        before_message_id: Option<String>,
        #[arg(long, default_value_t = 25)]
        limit: usize,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Edit {
        connector_id: String,
        #[arg(long)]
        conversation_id: String,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        message_id: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        approval_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    Delete {
        connector_id: String,
        #[arg(long)]
        conversation_id: String,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        message_id: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        approval_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    React {
        connector_id: String,
        #[arg(long)]
        conversation_id: String,
        #[arg(long)]
        thread_id: Option<String>,
        #[arg(long)]
        message_id: String,
        #[arg(long)]
        emoji: String,
        #[arg(long, default_value_t = false)]
        remove: bool,
        #[arg(long)]
        approval_id: Option<String>,
        #[arg(long)]
        url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "user:local")]
        principal: String,
        #[arg(long, default_value = "01ARZ3NDEKTSV4RRFFQ69G5FAV")]
        device_id: String,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}
