use crate::{
    Connection,
    api::Failure,
    identity::{id, string, timestamp, ts},
    intake::{self, FileSource, attached_files},
};
use hermes_engine::ChannelHistoryMessage;
use hermes_gateway_core::messaging::{
    Advance, DeferredInput, GatewayError, InboundEvent, Ingress, Message, QuoteHydration,
    QuoteLookup, QuoteReference, RoomKind, ScrollbackReference,
    quotes::{MAX_SCROLLBACK_BYTES, MAX_SCROLLBACK_MESSAGE_BYTES},
    runtime::db,
    storage,
};
use serde_json::{Value, json};
use std::{cmp::Ordering, future::Future};
use tokio_util::sync::CancellationToken;

const HISTORY_LIMIT: u8 = 12;

pub(crate) fn observe_view(connection: &Connection, event: &Value, field: &str) {
    let Some(actor) = string(event, "user", b"UW") else {
        return;
    };
    let Some(time) = ts(event, "event_ts").or_else(|| ts(event, "ts")) else {
        return;
    };
    if !connection.settings.agent || actor == connection.id.user || event.get(field).is_none() {
        return;
    }
    let channel = event[field]["entities"].as_array().and_then(|entities| {
        entities.iter().take(16).find_map(|entity| {
            (entity["type"] == "slack#/types/channel_id" && entity["team_id"] == connection.id.team)
                .then(|| string(entity, "value", b"CGD"))
                .flatten()
        })
    });
    let mut status = crate::lock(&connection.verification);
    if status["agent_context"]["observed_at"]
        .as_str()
        .is_none_or(|prior| compare(time, prior) != Ordering::Less)
    {
        status["agent_context"] = json!({"actor":actor,"channel":channel,"observed_at":time});
    }
}

pub(crate) fn reference(
    connection: &Connection,
    event: &InboundEvent,
) -> Option<ScrollbackReference> {
    if connection.settings.shared_dm_history && event.room.kind == RoomKind::Direct {
        return None;
    }
    let thread = event.room.thread.as_deref()?;
    let before = event
        .reply_anchor
        .as_deref()
        .filter(|before| *before != thread)?;
    connection
        .settings
        .thread_history
        .then(|| ScrollbackReference {
            before: QuoteReference {
                room: event.room.clone(),
                message_id: before.into(),
            },
            reply: None,
            count: HISTORY_LIMIT,
        })
}

fn compare(left: &str, right: &str) -> Ordering {
    let seconds = |value: &str| {
        value
            .split_once('.')
            .map_or(0, |(seconds, _)| seconds.len())
    };
    seconds(left)
        .cmp(&seconds(right))
        .then_with(|| left.cmp(right))
}

pub(crate) async fn hydrate<T, Delivery>(
    connection: &Connection,
    ingress: &Ingress,
    item: &DeferredInput,
    cancel: &CancellationToken,
    body: Delivery,
) -> Result<(), GatewayError>
where
    Delivery: FnOnce(Vec<FileSource>) -> T,
    T: Future<Output = Result<Message, GatewayError>>,
{
    let room = &item.event.room;
    let thread = room.thread.as_deref().ok_or(GatewayError::Invalid)?;
    let actor = &item.event.sender.id;
    let id = item.event.id.as_str();
    let token = item.preparation.token.as_str();
    let stream = db(storage::key("slack-thread-context", &room))?;
    let previous = ingress.quote_checkpoint(id, token, &stream).await?;
    ingress
        .hydrate_quote(id, token, |source| async move {
            intake::conversation(connection, ingress, room, Some(actor), cancel).await?;
            let mut request = json!({
                "channel":source.room.chat,"team_id":connection.id.team,"ts":thread,
                "latest":source.message_id,"inclusive":false,"limit":HISTORY_LIMIT,
            });
            if let Some(previous) = previous.as_deref() {
                if !timestamp(previous) || compare(previous, &source.message_id) != Ordering::Less {
                    return Ok(QuoteHydration {
                        message: Some(body(vec![]).await?),
                        quote: QuoteLookup::Scrollback {
                            messages: Vec::new(),
                            quote: None,
                        },
                        advance: None,
                    });
                }
                request["oldest"] = json!(previous);
            }
            let response = connection
                .api
                .scoped(ingress, "conversations.replies", request, cancel)
                .await;
            let (messages, root, advance) = match response {
                Ok(response) => {
                    let (messages, root) = bounded_history(
                        &response,
                        thread,
                        &source.message_id,
                        previous.as_deref(),
                        connection.settings.files,
                    )?;
                    let advance = Advance {
                        stream,
                        previous,
                        checkpoint: source.message_id,
                    };
                    (messages, root, Some(advance))
                }
                Err(Failure::Credentials | Failure::Cancelled) => {
                    return Err(GatewayError::Forbidden);
                }
                Err(_) => (Vec::new(), Vec::new(), None),
            };
            Ok(QuoteHydration {
                message: Some(body(root).await?),
                quote: QuoteLookup::Scrollback {
                    messages,
                    quote: None,
                },
                advance,
            })
        })
        .await?;
    Ok(())
}

fn bounded_history(
    response: &Value,
    thread: &str,
    before: &str,
    after: Option<&str>,
    include_media: bool,
) -> Result<(Vec<ChannelHistoryMessage>, Vec<FileSource>), GatewayError> {
    let messages = response["messages"]
        .as_array()
        .ok_or(GatewayError::Invalid)?;
    let mut budget = MAX_SCROLLBACK_BYTES;
    let mut out = Vec::new();
    let mut root = Vec::new();
    for message in messages {
        let Some(time) = ts(message, "ts") else {
            continue;
        };
        if compare(time, before) != Ordering::Less
            || after.is_some_and(|after| compare(time, after) != Ordering::Greater)
        {
            continue;
        }
        if time != thread && ts(message, "thread_ts") != Some(thread) {
            return Err(GatewayError::Forbidden);
        }
        if include_media && time == thread {
            root.extend(attached_files(message));
        }
        let author = message["user"]
            .as_str()
            .filter(|actor| id(actor, b"UW"))
            .or_else(|| message["bot_id"].as_str().filter(|actor| id(actor, b"B")));
        let (Some(author), Some(text)) = (author, message["text"].as_str()) else {
            continue;
        };
        let limit = budget.min(MAX_SCROLLBACK_MESSAGE_BYTES).min(text.len());
        let text = &text[..text.floor_char_boundary(limit)];
        if text.is_empty() || text.contains('\0') {
            continue;
        }
        budget -= text.len();
        out.push(ChannelHistoryMessage {
            message_id: time.into(),
            author_id: author.into(),
            text: text.into(),
        });
        if out.len() == usize::from(HISTORY_LIMIT) || budget == 0 {
            break;
        }
    }
    Ok((out, root))
}
