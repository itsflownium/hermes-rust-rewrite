//! Read access to the opaque attachments of the active channel turn.

use crate::ToolCall;
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

pub(crate) const TOOL: &str = "read_channel_attachment";
pub const MAX_CHANNEL_ATTACHMENT_READ: usize = 16 * 1024;

#[derive(Clone, Serialize)]
pub struct ChannelAttachment {
    pub id: String,
    pub filename: String,
    pub bytes: usize,
}

pub type ChannelAttachmentReader = Arc<dyn Fn(&str, usize, usize) -> Result<Vec<u8>> + Send + Sync>;

#[derive(Clone)]
pub struct ChannelAttachments {
    pub files: Vec<ChannelAttachment>,
    pub recheck: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    pub read: ChannelAttachmentReader,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Read {
    id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "default_read")]
    limit: usize,
}

fn default_read() -> usize {
    4096
}

#[cfg(test)]
#[test]
fn revocation_withholds_metadata_and_pending_read() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let current = Arc::new(AtomicBool::new(true));
    let lease = current.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let source = ChannelAttachments {
        files: vec![ChannelAttachment {
            id: "this-turn".into(),
            filename: "private.zip".into(),
            bytes: 4,
        }],
        recheck: Arc::new(move || {
            ensure!(lease.load(Ordering::Acquire), "Lease expired");
            Ok(())
        }),
        read: Arc::new(move |_, _, _| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(b"data".to_vec())
        }),
    };
    let read = source
        .parse(&ToolCall {
            id: "tool-1".into(),
            name: TOOL.into(),
            arguments: json!({"id":"this-turn"}).to_string(),
        })
        .unwrap();
    let payload = vec![
        json!({"role":"system","content":""}),
        json!({"role":"user","content":"inspect the file"}),
        json!({"role":"assistant","content":""}),
        json!({"role":"tool","tool_call_id":"tool-1","content":"pending read"}),
    ];
    let disclosures = [(1, read)];
    let mut authorized = payload.clone();
    source
        .append_to_request(&mut authorized, 2, &disclosures, true)
        .unwrap();
    assert!(
        authorized[1]["content"]
            .as_str()
            .unwrap()
            .contains("private.zip")
    );
    assert!(
        authorized[3]["content"]
            .as_str()
            .unwrap()
            .contains("\"content\":\"data\"")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    current.store(false, Ordering::Release);
    let mut revoked = payload;
    source
        .append_to_request(&mut revoked, 2, &disclosures, true)
        .unwrap();
    assert_eq!(revoked[1]["content"], "inspect the file");
    assert!(
        revoked[3]["content"]
            .as_str()
            .unwrap()
            .contains("unavailable")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

pub(crate) fn spec() -> Value {
    json!({"type":"function","function":{
        "name":TOOL,
        "description":"Read a page of an attachment from this channel message. Returns text when the selected bytes are UTF-8, otherwise base64. Use next_offset for more bytes.",
        "parameters":{"type":"object","properties":{
            "id":{"type":"string","description":"Attachment ID from the current message"},
            "offset":{"type":"integer","minimum":0,"default":0},
            "limit":{"type":"integer","minimum":1,"maximum":MAX_CHANNEL_ATTACHMENT_READ,"default":default_read()}
        },"required":["id"],"additionalProperties":false}
    }})
}

impl ChannelAttachments {
    pub(crate) fn parse(&self, call: &ToolCall) -> Result<Read> {
        let read: Read =
            serde_json::from_str(&call.arguments).context("Invalid channel attachment read")?;
        let file = self
            .files
            .iter()
            .find(|file| file.id == read.id)
            .context("Attachment is unavailable for this turn")?;
        ensure!(
            read.offset < file.bytes && (1..=MAX_CHANNEL_ATTACHMENT_READ).contains(&read.limit),
            "Invalid attachment offset or read size"
        );
        Ok(read)
    }

    pub(crate) fn append_to_request(
        &self,
        messages: &mut [Value],
        current_entries: usize,
        disclosures: &[(usize, Read)],
        tool_available: bool,
    ) -> Result<()> {
        let start = messages
            .len()
            .checked_sub(current_entries)
            .context("Missing current attachment source")?;
        let authorized = (self.recheck)().is_ok();
        if authorized {
            let user = messages
                .get_mut(start.checked_sub(1).context("Missing attachment input")?)
                .context("Missing attachment input")?;
            ensure!(user["role"] == "user", "Invalid attachment source");
            let mut text = format!(
                "\n\nFiles attached to this message:\n{}",
                serde_json::to_string(&self.files)?
            );
            if tool_available {
                text.push_str("\nUse read_channel_attachment for a bounded read of a file.");
            }
            match &mut user["content"] {
                Value::String(message) => message.push_str(&text),
                Value::Array(parts) => parts.push(json!({"type":"text","text":text})),
                _ => anyhow::bail!("Invalid channel input"),
            }
        }

        for (index, read) in disclosures {
            let tool = messages
                .get_mut(start + index)
                .context("Missing attachment tool result")?;
            ensure!(tool["role"] == "tool", "Invalid attachment tool position");
            let page = if authorized && tool_available {
                self.page(read)
            } else {
                None
            };
            tool["content"] = Value::String(page.unwrap_or_else(|| {
                json!({"error":"Attachment is unavailable for this turn"}).to_string()
            }));
        }
        Ok(())
    }

    fn page(&self, read: &Read) -> Option<String> {
        let file = self.files.iter().find(|file| file.id == read.id)?;
        let limit = read.limit.min(file.bytes.checked_sub(read.offset)?);
        let data = (self.read)(&read.id, read.offset, limit).ok()?;
        if data.len() != limit || data.is_empty() {
            return None;
        }
        let next = read.offset.checked_add(data.len())?;
        let (encoding, content) = match std::str::from_utf8(&data) {
            Ok(text) if !text.contains('\0') => ("utf-8", text.to_owned()),
            _ => ("base64", STANDARD.encode(data)),
        };
        Some(
            json!({
                "id":read.id,"offset":read.offset,"encoding":encoding,"content":content,
                "next_offset": (next < file.bytes).then_some(next),
            })
            .to_string(),
        )
    }
}
