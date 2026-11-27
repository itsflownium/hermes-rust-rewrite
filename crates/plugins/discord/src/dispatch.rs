use crate::messaging::{
    Advance, ConnectionFailure, Disposition, GatewayError, InboundBody, InboundEvent, Message,
    Room, RoomKind, Sender,
    runtime::{blocking, db},
    storage::{self as store, Candidate},
};
use super::{
    DiscordAdapter, Ingress,
    channels::{ChannelRoute, Member, User, read_id, snowflake},
    socket::{Dispatch, intake_failure},
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
pub(super) struct MessageCreate {
    pub id: String,
    pub channel_id: String,
    pub guild_id: Option<String>,
    pub author: User,
    pub member: Option<Member>,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub mentions: Vec<User>,
    #[serde(rename = "type", default)]
    pub kind: u8,
    pub webhook_id: Option<String>,
}

pub(super) struct TrustedBurst {
    channel: String,
    guild: Option<String>,
    actor: String,
    ids: std::collections::BTreeSet<String>,
}

fn plain_bot_message(raw: &Value) -> Option<MessageCreate> {
    let message = serde_json::from_value::<MessageCreate>(raw.clone()).ok()?;
    (message.author.bot
        && !message.author.system
        && message.webhook_id.is_none()
        && message.kind == 0
        && snowflake(&message.id)
        && !message.content.trim().is_empty()
        && raw.get("message_reference").is_none_or(Value::is_null)
        && raw
            .get("attachments")
            .is_none_or(|files| files.as_array().is_some_and(Vec::is_empty)))
    .then_some(message)
}

impl TrustedBurst {
    pub(super) fn take(&mut self, data: &Value) -> bool {
        let Some(message) = plain_bot_message(data) else {
            return false;
        };
        message.channel_id == self.channel
            && message.guild_id == self.guild
            && message.author.id == self.actor
            && message.mentions.is_empty()
            && self.ids.insert(message.id)
    }
}

impl DiscordAdapter {
    pub(super) fn trusted_burst(&self, data: &Value) -> Option<TrustedBurst> {
        let message = plain_bot_message(data)?;
        let addressed = message
            .mentions
            .iter()
            .any(|user| user.id == self.bot.bot_id)
            && (message.content.contains(&format!("<@{}>", self.bot.bot_id))
                || message
                    .content
                    .contains(&format!("<@!{}>", self.bot.bot_id)));
        (addressed
            && message.author.id != self.bot.bot_id
            && self.settings.trusted_bots.contains(&message.author.id))
        .then(|| TrustedBurst {
            channel: message.channel_id,
            guild: message.guild_id,
            actor: message.author.id,
            ids: [message.id].into(),
        })
    }

    pub(super) async fn dispatch(
        &self,
        ingress: &Ingress,
        frame: Dispatch,
        continuations: Vec<Value>,
        advance: Advance,
        outgoing: &mpsc::Sender<Value>,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        self.observe_channels(&frame.kind, &frame.data);
        self.observe_voice(&frame.kind, &frame.data, ingress);
        match frame.kind.as_str() {
            "MESSAGE_CREATE" => {
                return self
                    .message(ingress, frame.data, continuations, advance, cancel)
                    .await;
            }
            "INTERACTION_CREATE" => {
                return self
                    .interaction(ingress, frame, advance, outgoing, cancel)
                    .await;
            }
            "GUILD_MEMBER_UPDATE" | "GUILD_MEMBER_REMOVE" => {
                if let (Some(guild), Some(user)) = (
                    read_id(&frame.data, "guild_id"),
                    frame.data.get("user").and_then(|user| read_id(user, "id")),
                ) {
                    self.invalidate_member(ingress, guild, user)
                        .await
                        .map_err(intake_failure)?;
                }
            }
            "CHANNEL_UPDATE" | "CHANNEL_DELETE" => {
                if let Some(chat) = read_id(&frame.data, "id") {
                    if frame.data.get("type").and_then(Value::as_u64) == Some(1) {
                        ingress.invalidate_actor_evidence(Some(Room {
                            kind: RoomKind::Direct, chat: chat.to_owned(), thread: None,
                        })).await.map_err(intake_failure)?;
                    } else {
                        ingress.withdraw_observed_destinations(vec![chat.to_owned()], None, false)
                            .await.map_err(intake_failure)?;
                    }
                }
            }
            "THREAD_UPDATE" | "THREAD_DELETE" | "THREAD_MEMBERS_UPDATE" => {
                if let Some(thread) = read_id(&frame.data, "id") {
                    ingress.withdraw_observed_destinations(Vec::new(), Some(thread.to_owned()), false)
                        .await.map_err(intake_failure)?;
                }
            }
            "GUILD_ROLE_UPDATE" | "GUILD_ROLE_DELETE" | "GUILD_DELETE" => {
                let key = if frame.kind == "GUILD_DELETE" { "id" } else { "guild_id" };
                if let Some(guild) = read_id(&frame.data, key) {
                    let channels: Vec<_> = self.settings.guilds.get(guild).into_iter()
                        .flat_map(|guild| guild.channels.keys().cloned()).collect();
                    let direct = self.settings.dm_role_guild.as_deref() == Some(guild);
                    if !channels.is_empty() || direct {
                        ingress.withdraw_observed_destinations(channels, None, direct)
                            .await.map_err(intake_failure)?;
                    }
                }
            }
            _ => {}
        }
        skip(ingress, advance).await
    }

    pub(super) async fn message(
        &self,
        ingress: &Ingress,
        data: Value,
        continuations: Vec<Value>,
        advance: Advance,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        let Ok(message) = serde_json::from_value::<MessageCreate>(data.clone()) else {
            return skip(ingress, advance).await;
        };
        let Some(route) = self.route_for_message(&message).await? else {
            return skip(ingress, advance).await;
        };
        if route.room.thread.as_deref() == Some(message.id.as_str())
            && self
                .is_auto_thread_echo(ingress, &route.room.chat, &message.id)
                .map_err(intake_failure)?
        {
            return skip(ingress, advance).await;
        }
        if message.author.id == self.bot.bot_id {
            super::delivery::observe_create(ingress, &self.bot.bot_id, &route.room, &data)
                .await
                .map_err(intake_failure)?;
            return skip(ingress, advance).await;
        }
        if message.webhook_id.is_some() || message.author.system || !matches!(message.kind, 0 | 19)
        {
            return skip(ingress, advance).await;
        }
        let event_id = format!("discord-message-{}", message.id);
        let recorded = db(ingress.storage().read(|tx| store::intake_receipt(tx, ingress.connection_name(), &event_id)))
            .map_err(intake_failure)?;
        let sender = if recorded.is_some() {
            Sender { id: message.author.id.clone(), roles: Default::default() }
        } else {
            let Some(roles) = self.roles(&route, &message.author.id, message.member).await? else {
                return skip(ingress, advance).await;
            };
            self.record_sender(ingress, &route, &message.author, &roles).await.map_err(intake_failure)?
        };
        let mention = format!("<@{}>", self.bot.bot_id);
        let nick_mention = format!("<@!{}>", self.bot.bot_id);
        let addressed = message
            .mentions
            .iter()
            .any(|user| user.id == self.bot.bot_id)
            && (message.content.contains(&mention) || message.content.contains(&nick_mention));
        let group = route.room.kind == RoomKind::Group;
        let requires_mention = group
            && self.settings.mention_required(
                route.guild.as_deref(),
                &route.room.chat,
                route.room.thread.as_deref(),
            );
        let wrong_mention = group
            && self.settings.suppress_other_mentions
            && !addressed
            && message
                .mentions
                .iter()
                .any(|user| user.id != self.bot.bot_id);
        let thread_continuation = !addressed
            && !wrong_mention
            && self.settings.participated_thread_exemption
            && !route
                .guild
                .as_deref()
                .zip(route.room.thread.as_deref())
                .is_some_and(|(guild, thread)| {
                    self.settings
                        .has_thread_mention_override(guild, &route.room.chat, thread)
                })
            && self
                .participated(ingress, route.room.thread.as_deref())
                .map_err(intake_failure)?;
        let event = InboundEvent {
            id: event_id,
            room: route.room,
            sender,
            reply_anchor: Some(message.id.clone()),
            mentioned: !requires_mention || addressed || thread_continuation,
            body: InboundBody::Message(Message::text(
                message
                    .content
                    .replace(&mention, "")
                    .replace(&nick_mention, "")
                    .trim(),
            )),
        };
        let trusted = !message.author.bot
            || addressed && self.settings.trusted_bots.contains(&message.author.id);
        if !trusted || wrong_mention || !event.mentioned {
            refuse(ingress, event, Disposition::Forbidden, advance).await?;
            return Ok(());
        }
        let continued = continuations
            .into_iter()
            .map(|raw| {
                let continued = plain_bot_message(&raw).ok_or(ConnectionFailure::Retryable)?;
                Ok(InboundEvent {
                    id: format!("discord-message-{}", continued.id),
                    room: event.room.clone(),
                    sender: event.sender.clone(),
                    reply_anchor: Some(continued.id),
                    mentioned: true,
                    body: InboundBody::Message(Message::text(continued.content.trim())),
                })
            })
            .collect::<Result<Vec<_>, ConnectionFailure>>()?;
        self.accept_message(ingress, event, data, continued, advance, cancel)
            .await
    }

    pub(super) async fn route_for_message(
        &self,
        message: &MessageCreate,
    ) -> Result<Option<ChannelRoute>, ConnectionFailure> {
        if !snowflake(&message.id)
            || !snowflake(&message.author.id)
            || message
                .guild_id
                .as_deref()
                .is_some_and(|guild| !snowflake(guild))
        {
            return Ok(None);
        }
        Ok(self
            .channel_route(&message.channel_id, message.guild_id.as_deref())
            .await?
            .filter(|route| route.guild == message.guild_id))
    }

    fn participated(&self, ingress: &Ingress, thread: Option<&str>) -> Result<bool, GatewayError> {
        let Some(thread) = thread else {
            return Ok(false);
        };
        db(ingress.storage().read(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM messaging_deliveries d JOIN messaging_parts p ON p.delivery=d.id
                   WHERE d.connection=?1 AND json_extract(d.intent,'$.room.thread')=?2
                   AND p.stage='sent' AND p.editable_message IS NOT NULL
                 )",
                rusqlite::params![ingress.connection_name(), thread],
                |row| row.get(0),
            )?)
        }))
    }
}

pub(super) async fn skip(ingress: &Ingress, advance: Advance) -> Result<(), ConnectionFailure> {
    ingress
        .submit(Vec::new(), Some(advance))
        .await
        .map_err(intake_failure)?;
    Ok(())
}

/// Commit refusals that depend on Discord-only authorization alongside the
/// same receive position used for allowed events.
pub(super) async fn refuse(
    ingress: &Ingress,
    event: InboundEvent,
    reason: Disposition,
    advance: Advance,
) -> Result<Disposition, ConnectionFailure> {
    let scope = ingress.clone();
    blocking(move || {
        scope.checked(true, |state| {
            let advance = scope.scoped_advance(Some(advance))?;
            let receipt = db(scope.storage().intake(
                state.revision(),
                vec![Candidate {
                    event,
                    decision: Err(reason),
                    protection: None,
                }],
                advance.as_ref(),
            ))?
            .ok_or(GatewayError::CheckpointChanged)?;
            receipt
                .receipts
                .into_iter()
                .next()
                .map(|receipt| receipt.disposition)
                .ok_or(GatewayError::TemporarilyUnavailable)
        })
    })
    .await
    .map_err(intake_failure)
}
