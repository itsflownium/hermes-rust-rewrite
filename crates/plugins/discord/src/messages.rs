use crate::messaging::{
    Advance, ConnectionFailure, DeferredInput, DeliveryResult, Disposition, EffectReceipt,
    GatewayError, Handoff, HandoffItem, InboundBody, InboundEvent, MediaSource, Message,
    Preparation, PreparationPurpose, QuoteHydration, QuoteLookup, QuoteReference, ReceiptKind,
    RoomKind, ScrollbackQuote, ScrollbackReference,
    prepared_threads::ThreadPreparation,
    quotes::{MAX_SCROLLBACK_BYTES, MAX_SCROLLBACK_MESSAGE_BYTES},
    runtime::{blocking, db, now},
    storage as store,
};
use super::{
    DiscordAdapter, Ingress,
    channels::{read_id, snowflake},
    client::{DiscordClient, RestFailure, json_response},
    dispatch::refuse,
    media::{self, AttachmentSource},
    socket::intake_failure,
};
use hermes_engine::ChannelHistoryMessage;
use reqwest::Method;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const PREPARATION_TTL_MS: i64 = 10 * 60 * 1000;
const MAX_WORKERS: usize = 12;
const TOKEN_PREFIX: &str = "discord-preparation-";

pub(crate) const MESSAGE_TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_preparation (
    connection TEXT NOT NULL, event TEXT NOT NULL, expires INTEGER NOT NULL, manifest TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Manifest {
    channel: String,
    message: String,
    attachments: Vec<AttachmentSource>,
    quote: Option<QuoteReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    thread: Option<AutoThread>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scrollback: Option<u8>,
}

impl Manifest {
    fn same_input(&self, other: &Self) -> bool {
        self.channel == other.channel
            && self.message == other.message
            && self.attachments == other.attachments
            && self.quote == other.quote
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct AutoThread {
    guild: String,
    inline_on_refusal: bool,
}

impl DiscordAdapter {
    pub(super) fn is_auto_thread_echo(
        &self,
        ingress: &Ingress,
        parent: &str,
        message: &str,
    ) -> Result<bool, GatewayError> {
        db(ingress.storage().read(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_discord_preparation WHERE connection=?1
                 AND event=?2 AND json_extract(manifest,'$.channel')=?3
                 AND json_type(manifest,'$.thread')='object')",
                params![
                    ingress.connection_name(),
                    format!("discord-message-{message}"),
                    parent
                ],
                |row| row.get(0),
            )?)
        }))
    }
    pub(super) async fn accept_message(
        &self,
        ingress: &Ingress,
        event: InboundEvent,
        raw: Value,
        continued: Vec<InboundEvent>,
        advance: Advance,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        let channel = read_id(&raw, "channel_id").ok_or(ConnectionFailure::Retryable)?;
        let message = read_id(&raw, "id").ok_or(ConnectionFailure::Retryable)?;
        let attachments = match media::attachment_sources(&raw) {
            Ok(attachments) => attachments,
            Err(_) => {
                refuse(ingress, event, Disposition::Invalid, advance).await?;
                return Ok(());
            }
        };
        if !attachments.is_empty()
            && (!self.settings.media.attachments
                || !media::within_inbound_limit(
                    &attachments,
                    self.settings.media.max_inbound_bytes,
                ))
        {
            refuse(ingress, event, Disposition::Unsupported, advance).await?;
            return Ok(());
        }
        let person = raw.pointer("/author/bot").and_then(Value::as_bool) != Some(true);
        let quote = raw
            .get("message_reference")
            .filter(|_| person)
            .filter(|reference| {
                reference
                    .get("channel_id")
                    .and_then(Value::as_str)
                    .is_none_or(|id| id == channel)
            })
            .and_then(|reference| read_id(reference, "message_id"))
            .filter(|source| *source != message)
            .map(|message_id| QuoteReference {
                room: event.room.clone(),
                message_id: message_id.to_owned(),
            });
        let thread = if self.settings.auto_thread
            && event.room.kind == RoomKind::Group
            && event.room.thread.is_none()
            && quote.is_none()
            && raw.get("type").and_then(Value::as_u64).unwrap_or(0) == 0
            && crate::lock(&self.channels)
                .get(channel)
                .is_some_and(|channel| matches!(channel.kind, 0 | 5))
        {
            read_id(&raw, "guild_id").map(|guild| AutoThread {
                guild: guild.to_owned(),
                inline_on_refusal: self.settings.auto_thread_inline_fallback,
            })
        } else {
            None
        };
        let mut scrollback = None;
        if person && thread.is_none() && self.settings.recovery.context_messages > 0 {
            let trigger = QuoteReference {
                room: event.room.clone(),
                message_id: message.to_owned(),
            };
            let can_read = ingress
                .checked(true, |state| {
                    let Ok(route) = ingress.route_candidate(&state.policy, &event, true)? else {
                        return Ok(false);
                    };
                    ingress.allows_quote(state, &event, &route.id, &trigger)
                })
                .map_err(intake_failure)?;
            if can_read {
                scrollback = Some(self.settings.recovery.context_messages as u8);
            }
        }
        if attachments.is_empty() && quote.is_none() && thread.is_none() && scrollback.is_none() {
            if continued.is_empty() {
                ingress
                    .submit(vec![event], Some(advance))
                    .await
                    .map_err(intake_failure)?;
            } else {
                commit_messages(ingress, event, None, continued, advance).await?;
            }
            return Ok(());
        }

        let authorized = ingress
            .checked(true, |state| {
                Ok(ingress
                    .route_candidate(&state.policy, &event, true)?
                    .is_ok())
            })
            .map_err(intake_failure)?;
        if !authorized {
            refuse(ingress, event, Disposition::Forbidden, advance).await?;
            return Ok(());
        }

        let manifest = Manifest {
            channel: channel.to_owned(),
            message: message.to_owned(),
            attachments,
            quote,
            thread,
            scrollback,
        };
        let id = event.id.clone();
        let scope = ingress.clone();
        let durable_manifest = manifest.clone();
        let event_key = id.clone();
        let reservation = blocking(move || scope.checked(true, |_| {
            db(scope.storage().write(|tx| {
                let serialized = serde_json::to_string(&durable_manifest)?;
                let expires = now() + PREPARATION_TTL_MS;
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_discord_preparation(connection,event,expires,manifest)
                       VALUES (?1,?2,?3,?4)",
                    params![scope.connection_name(),event_key,expires,serialized],
                )?;
                let stored: (i64, Manifest) = tx.query_row(
                    "SELECT expires,manifest FROM messaging_discord_preparation WHERE connection=?1 AND event=?2",
                    params![scope.connection_name(),event_key], |row| Ok((row.get(0)?,store::decode(row,1)?)),
                )?;
                Ok(stored.1.same_input(&durable_manifest).then_some(stored))
            }))
        })).await.map_err(intake_failure)?;
        let Some((expires_ms, manifest)) = reservation else {
            let scope = ingress.clone();
            let key = event.id.clone();
            let cursor = advance.clone();
            blocking(move || scope.checked(true, |_| {
                let cursor = scope.scoped_advance(Some(cursor))?;
                db(scope.storage().write(|tx| {
                    if !store::check_advance(tx, scope.connection_name(), cursor.as_ref())? {
                        return Ok(Err(GatewayError::CheckpointChanged));
                    }
                    tx.execute("UPDATE messaging_ingress SET conflict=1 WHERE connection=?1 AND event=?2",
                        params![scope.connection_name(),key])?;
                    store::write_advance(tx, scope.connection_name(), cursor.as_ref())?;
                    Ok(Ok(()))
                }))?
            })).await.map_err(intake_failure)?;
            return Ok(());
        };
        let token = db(store::key("discord-preparation", &id)).map_err(intake_failure)?;
        let preparation = Preparation {
            token,
            expires_ms,
            purpose: if let Some(thread) = &manifest.thread {
                PreparationPurpose::Thread {
                    id: manifest.message.clone(),
                    inline_on_refusal: thread.inline_on_refusal,
                }
            } else if let Some(count) = manifest.scrollback {
                PreparationPurpose::Scrollback(ScrollbackReference {
                    before: QuoteReference {
                        room: event.room.clone(),
                        message_id: manifest.message.clone(),
                    },
                    reply: manifest.quote.clone(),
                    count,
                })
            } else {
                manifest
                    .quote
                    .clone()
                    .map_or(PreparationPurpose::Media, PreparationPurpose::Quote)
            },
            protection: None,
        };
        let receipt = commit_messages(
            ingress,
            event,
            Some(preparation.clone()),
            continued,
            advance,
        )
        .await?;
        if receipt
            .receipts
            .first()
            .is_some_and(|receipt| receipt.disposition == Disposition::Deferred)
            && let Some(staged) =
                db(ingress.storage().preparation(&id)).map_err(intake_failure)?
            && let Some(event) = staged.into_event()
        {
            self.start_preparation(
                ingress,
                DeferredInput { event, preparation },
                manifest,
                cancel,
            );
        }
        Ok(())
    }

    pub(super) async fn resume_preparations(
        &self,
        ingress: &Ingress,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        let pending = ingress
            .pending_preparations()
            .await
            .map_err(intake_failure)?;
        for input in pending {
            if !input.preparation.token.starts_with(TOKEN_PREFIX) {
                continue;
            }
            let manifest: Option<Manifest> = db(ingress.storage().read(|tx| {
                Ok(tx.query_row(
                    "SELECT manifest FROM messaging_discord_preparation WHERE connection=?1 AND event=?2",
                    params![ingress.connection_name(),input.event.id], |row| store::decode(row, 0),
                ).optional()?)
            })).map_err(intake_failure)?;
            if let Some(manifest) = manifest {
                self.start_preparation(ingress, input, manifest, cancel);
            }
        }
        Ok(())
    }

    fn start_preparation(
        &self,
        ingress: &Ingress,
        input: DeferredInput,
        manifest: Manifest,
        cancel: &CancellationToken,
    ) {
        let event = input.event.id.clone();
        let mut active = crate::lock(&self.preparations);
        if active.len() >= MAX_WORKERS || !active.insert(event.clone()) {
            return;
        }
        drop(active);
        let lease = PreparationWorker {
            workers: self.preparations.clone(),
            event,
        };
        let client = self.client.clone();
        let bot = self.bot.bot_id.clone();
        let max_inbound_bytes = self.settings.media.max_inbound_bytes;
        let ingress = ingress.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _worker = lease;
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = hydrate(&client, &bot, &ingress, input, manifest, max_inbound_bytes) => {},
            }
        });
    }
}

async fn commit_messages(
    ingress: &Ingress,
    first: InboundEvent,
    preparation: Option<Preparation>,
    continued: Vec<InboundEvent>,
    advance: Advance,
) -> Result<crate::messaging::IntakeResult, ConnectionFailure> {
    let id = db(store::key(
        "discord-handoff",
        &(&first.id, &advance.checkpoint),
    ))
    .map_err(intake_failure)?;
    let burst = !continued.is_empty();
    let items = std::iter::once(HandoffItem {
        event: first,
        preparation,
    })
    .chain(continued.into_iter().map(|event| HandoffItem {
        event,
        preparation: None,
    }))
    .collect();
    let handoff = Handoff {
        id,
        items,
        advance: Some(advance),
    };
    if burst {
        ingress
            .stage_addressed_burst(handoff)
            .await
            .map_err(intake_failure)
    } else {
        ingress.stage_handoff(handoff).await.map_err(intake_failure)
    }
}

struct PreparationWorker {
    workers: Arc<Mutex<BTreeSet<String>>>,
    event: String,
}

impl Drop for PreparationWorker {
    fn drop(&mut self) {
        crate::lock(&self.workers).remove(&self.event);
    }
}

async fn hydrate(
    client: &DiscordClient,
    bot: &str,
    ingress: &Ingress,
    mut input: DeferredInput,
    manifest: Manifest,
    max_inbound_bytes: u64,
) -> Result<(), GatewayError> {
    if let Some(thread) = &manifest.thread {
        match ingress
            .prepare_thread(input.event.id.clone(), input.preparation.token.clone())
            .await?
        {
            ThreadPreparation::Ready(event) => input.event = event,
            ThreadPreparation::Reconcile(attempt) => {
                let thread_id = manifest.message.as_str();
                let channel: Result<Value, _> = client.get(&["channels", thread_id]).await;
                if let Ok(channel) = channel
                    && read_id(&channel, "id") == Some(thread_id)
                    && read_id(&channel, "parent_id") == Some(manifest.channel.as_str())
                    && read_id(&channel, "guild_id") == Some(thread.guild.as_str())
                    && read_id(&channel, "owner_id") == Some(bot)
                    && matches!(channel["type"].as_u64(), Some(10 | 11))
                {
                    ingress
                        .reconcile_delivery(
                            attempt,
                            DeliveryResult::EffectConfirmed(EffectReceipt {
                                kind: ReceiptKind::Destination,
                                id: thread_id.to_owned(),
                            }),
                        )
                        .await?;
                }
                return Ok(());
            }
            ThreadPreparation::Waiting | ThreadPreparation::Closed => return Ok(()),
        }
    }
    let source = MediaSource::from(&input.event);
    let InboundBody::Message(body) = input.event.body else {
        return Err(GatewayError::Invalid);
    };
    let event = &source.event_id;
    let token = &input.preparation.token;
    if input.preparation.purpose.context_reference().is_some() {
        ingress
            .hydrate_quote(event, token, |requested| async {
                let message = if manifest.attachments.is_empty() {
                    None
                } else {
                    Some(
                        media_body(client, ingress, &source, &manifest, body, max_inbound_bytes)
                            .await?,
                    )
                };
                let quote = if let Some(count) = manifest.scrollback {
                    let (messages, quote) = tokio::join!(
                        read_scrollback(client, &manifest.channel, &requested.message_id, count),
                        read_scrollback_quote(client, &manifest.channel, manifest.quote.clone()),
                    );
                    QuoteLookup::Scrollback {
                        messages,
                        quote: quote?,
                    }
                } else {
                    get_quote(client, &manifest.channel, requested).await?
                };
                Ok(QuoteHydration {
                    message,
                    quote,
                    advance: None,
                })
            })
            .await?;
    } else {
        ingress
            .authorize_preparation(event.clone(), token.clone())
            .await?;
        let message =
            match media_body(client, ingress, &source, &manifest, body, max_inbound_bytes).await {
                Ok(message) => message,
                Err(
                    GatewayError::Invalid | GatewayError::Unsupported | GatewayError::Forbidden,
                ) => Message::text(""),
                Err(error) => return Err(error),
            };
        ingress
            .complete_preparation(event.clone(), token.clone(), message)
            .await?;
    }
    Ok(())
}

async fn media_body(
    client: &DiscordClient,
    ingress: &Ingress,
    source: &MediaSource,
    manifest: &Manifest,
    body: Message,
    max_inbound_bytes: u64,
) -> Result<Message, GatewayError> {
    if manifest.attachments.is_empty() {
        return Ok(body);
    }
    media::hydrate_attachments(
        client,
        ingress,
        source,
        &manifest.channel,
        &manifest.message,
        body,
        &manifest.attachments,
        max_inbound_bytes,
    )
    .await
}

async fn read_scrollback(
    client: &DiscordClient,
    channel: &str,
    before: &str,
    count: u8,
) -> Vec<ChannelHistoryMessage> {
    let Ok(upper) = before.parse::<u64>() else {
        return Vec::new();
    };
    let request = client
        .bot(Method::GET, &["channels", channel, "messages"])
        .query(&[("before", before), ("limit", &count.to_string())]);
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let response = request.send().await.map_err(|_| RestFailure::Unavailable)?;
        json_response::<Vec<Value>>(response).await
    })
    .await;
    let Ok(Ok(mut rows)) = result else {
        return Vec::new();
    };
    if rows.len() > usize::from(count) {
        return Vec::new();
    }
    rows.sort_by_key(|row| {
        std::cmp::Reverse(
            read_id(row, "id")
                .and_then(|id| id.parse::<u64>().ok())
                .unwrap_or(0),
        )
    });
    let mut bytes = 0;
    let mut seen = BTreeSet::new();
    let mut messages = Vec::new();
    for row in rows {
        if read_id(&row, "channel_id") != Some(channel)
            || row.get("webhook_id").is_some_and(|id| !id.is_null())
        {
            continue;
        }
        let Some(id) = read_id(&row, "id").filter(|id| id.parse::<u64>().is_ok_and(|n| n < upper))
        else {
            continue;
        };
        let Some(author) = row.get("author").and_then(|user| read_id(user, "id")) else {
            continue;
        };
        let Some(text) = row
            .get("content")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty() && !text.contains('\0'))
        else {
            continue;
        };
        if !seen.insert(id.to_owned()) {
            continue;
        }
        let budget = MAX_SCROLLBACK_MESSAGE_BYTES.min(MAX_SCROLLBACK_BYTES - bytes);
        let text = &text[..text.floor_char_boundary(budget.min(text.len()))];
        if text.is_empty() {
            break;
        }
        bytes += text.len();
        messages.push(ChannelHistoryMessage {
            message_id: id.to_owned(),
            author_id: author.to_owned(),
            text: text.to_owned(),
        });
    }
    messages.reverse();
    messages
}

async fn read_scrollback_quote(
    client: &DiscordClient,
    channel: &str,
    quote: Option<QuoteReference>,
) -> Result<Option<ScrollbackQuote>, GatewayError> {
    let Some(reference) = quote else {
        return Ok(None);
    };
    match get_quote(client, channel, reference).await? {
        QuoteLookup::Found { source, text } => Ok(Some(ScrollbackQuote { source, text })),
        QuoteLookup::Unavailable => Ok(None),
        _ => Err(GatewayError::Invalid),
    }
}

async fn get_quote(
    client: &DiscordClient,
    channel: &str,
    requested: QuoteReference,
) -> Result<QuoteLookup, GatewayError> {
    if !snowflake(&requested.message_id) {
        return Ok(QuoteLookup::Unavailable);
    }
    let response: Result<Value, _> = client
        .get(&["channels", channel, "messages", &requested.message_id])
        .await;
    match response {
        Ok(message)
            if read_id(&message, "channel_id") == Some(channel)
                && read_id(&message, "id") == Some(requested.message_id.as_str()) =>
        {
            let text = message.get("content").and_then(Value::as_str).unwrap_or("");
            if text.trim().is_empty() || text.len() > 8 * 1024 {
                return Ok(QuoteLookup::Unavailable);
            }
            Ok(QuoteLookup::Found {
                source: requested,
                text: text.into(),
            })
        }
        Ok(_) | Err(RestFailure::Missing | RestFailure::Forbidden) => Ok(QuoteLookup::Unavailable),
        Err(_) => Err(GatewayError::TemporarilyUnavailable),
    }
}
