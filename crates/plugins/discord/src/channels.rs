use crate::messaging::{
    ActorKind, ConnectionFailure, GatewayError, MemberEvidence, Room, RoomKind,
    Sender, runtime::now,
};
use super::{
    DiscordAdapter, Ingress,
    client::RestFailure,
    membership::MEMBERSHIP_MS,
    settings::scoped_role,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn snowflake(value: &str) -> bool {
    !value.starts_with('0')
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok()
}

pub(super) fn snowflake_timestamp(value: &str) -> Option<i64> {
    let epoch = 1_420_070_400_000_i64;
    Some(epoch + i64::try_from(value.parse::<u64>().ok()? >> 22).ok()?)
}

#[derive(Clone, Debug, Deserialize)]
pub(super) struct Channel {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: u8,
    pub guild_id: Option<String>,
    pub parent_id: Option<String>,
}

impl Channel {
    fn is_valid(&self) -> bool {
        snowflake(&self.id)
            && self.guild_id.as_deref().is_none_or(snowflake)
            && self.parent_id.as_deref().is_none_or(snowflake)
    }

    fn is_thread(&self) -> bool {
        (10..=12).contains(&self.kind)
    }

    fn can_parent(&self) -> bool {
        matches!(self.kind, 0 | 5 | 15 | 16)
    }
}

#[derive(Clone)]
pub(super) struct ChannelRoute {
    pub room: Room,
    pub guild: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct User {
    pub id: String,
    #[serde(default)]
    pub bot: bool,
    #[serde(default)]
    pub system: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct Member {
    #[serde(default)]
    pub roles: Vec<String>,
}

pub(super) fn read_id<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| snowflake(value))
}

pub(super) fn rest_failure(error: RestFailure) -> ConnectionFailure {
    match error {
        RestFailure::Credentials => ConnectionFailure::Credentials,
        _ => ConnectionFailure::Retryable,
    }
}

impl DiscordAdapter {
    pub(super) fn remember_channel(&self, channel: Channel) {
        if !channel.is_valid() {
            return;
        }
        let mut known = crate::lock(&self.channels);
        if known.len() < 16_384 || known.contains_key(&channel.id) {
            known.insert(channel.id.clone(), channel);
        }
    }

    pub(super) fn observe_channels(&self, kind: &str, data: &Value) {
        let read = |object: &Value| serde_json::from_value::<Channel>(object.clone()).ok();
        match kind {
            "GUILD_CREATE" => {
                if let Some(guild) = read_id(data, "id") {
                    for key in ["channels", "threads"] {
                        for raw in data
                            .get(key)
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                        {
                            let Some(mut channel) = read(raw) else {
                                continue;
                            };
                            if channel.guild_id.as_deref().is_some_and(|id| id != guild) {
                                continue;
                            }
                            channel.guild_id = Some(guild.to_owned());
                            self.remember_channel(channel);
                        }
                    }
                }
            }
            "CHANNEL_CREATE" | "CHANNEL_UPDATE" | "THREAD_CREATE" | "THREAD_UPDATE" => {
                if let Some(channel) = read(data) {
                    self.remember_channel(channel);
                }
            }
            "CHANNEL_DELETE" | "THREAD_DELETE" => {
                if let Some(id) = read_id(data, "id") {
                    let mut known = crate::lock(&self.channels);
                    known.remove(id);
                    known.retain(|_, channel| channel.parent_id.as_deref() != Some(id));
                }
            }
            "GUILD_DELETE" => {
                if let Some(guild) = read_id(data, "id") {
                    crate::lock(&self.channels)
                        .retain(|_, channel| channel.guild_id.as_deref() != Some(guild));
                }
            }
            _ => {}
        }
    }

    async fn channel(&self, id: &str) -> Result<Option<Channel>, ConnectionFailure> {
        let cached = crate::lock(&self.channels).get(id).cloned();
        if cached.is_some() {
            return Ok(cached);
        }
        match self.client.get::<Channel>(&["channels", id]).await {
            Ok(channel) if channel.id == id && channel.is_valid() => {
                self.remember_channel(channel.clone());
                Ok(Some(channel))
            }
            Ok(_) | Err(RestFailure::Missing | RestFailure::Forbidden) => Ok(None),
            Err(error) => Err(rest_failure(error)),
        }
    }

    /// Normalize only a channel whose provider-verified guild and parent agree.
    pub(super) async fn channel_route(
        &self,
        id: &str,
        guild_hint: Option<&str>,
    ) -> Result<Option<ChannelRoute>, ConnectionFailure> {
        if !snowflake(id) {
            return Ok(None);
        }
        let Some(channel) = self.channel(id).await? else {
            return Ok(None);
        };
        if guild_hint.is_some_and(|hint| channel.guild_id.as_deref() != Some(hint)) {
            return Ok(None);
        }
        let (kind, chat, thread) = if channel.kind == 1 && channel.guild_id.is_none() {
            (RoomKind::Direct, id.to_owned(), None)
        } else if channel.is_thread() {
            let (Some(guild), Some(parent)) = (&channel.guild_id, &channel.parent_id) else {
                return Ok(None);
            };
            let Some(parent_channel) = self.channel(parent).await? else {
                return Ok(None);
            };
            if parent_channel.guild_id.as_ref() != Some(guild) || !parent_channel.can_parent() {
                return Ok(None);
            }
            (RoomKind::Group, parent.clone(), Some(id.to_owned()))
        } else if channel.can_parent() && channel.guild_id.is_some() {
            (RoomKind::Group, id.to_owned(), None)
        } else {
            return Ok(None);
        };
        if !self
            .settings
            .allows_surface(channel.guild_id.as_deref(), &chat, thread.as_deref())
        {
            return Ok(None);
        }
        Ok(Some(ChannelRoute {
            guild: channel.guild_id,
            room: Room { kind, chat, thread },
        }))
    }

    /// Discord user roles are qualified by the guild that proved them.
    pub(super) async fn roles(
        &self,
        route: &ChannelRoute,
        actor: &str,
        supplied: Option<Member>,
    ) -> Result<Option<BTreeSet<String>>, ConnectionFailure> {
        let Some(guild) = route
            .guild
            .as_deref()
            .or(self.settings.dm_role_guild.as_deref())
        else {
            return Ok(Some(BTreeSet::new()));
        };
        let member = if route.guild.is_some() && supplied.is_some() {
            supplied
        } else {
            match self
                .client
                .get::<Member>(&["guilds", guild, "members", actor])
                .await
            {
                Ok(member) => Some(member),
                Err(RestFailure::Missing | RestFailure::Forbidden) if route.guild.is_none() => {
                    return Ok(Some(BTreeSet::new()));
                }
                Err(RestFailure::Missing | RestFailure::Forbidden) => return Ok(None),
                Err(error) => return Err(rest_failure(error)),
            }
        };
        Ok(member.and_then(|member| {
            if member.roles.len() > 128 || member.roles.iter().any(|role| !snowflake(role)) {
                return None;
            }
            Some(member.roles.into_iter().collect())
        }))
    }

    pub(super) async fn record_sender(
        &self,
        ingress: &Ingress,
        route: &ChannelRoute,
        user: &User,
        roles: &BTreeSet<String>,
    ) -> Result<Sender, GatewayError> {
        if !snowflake(&user.id) {
            return Err(GatewayError::Invalid);
        }
        let guild = route
            .guild
            .as_deref()
            .or(self.settings.dm_role_guild.as_deref());
        let roles = roles
            .iter()
            .filter_map(|role| guild.map(|guild| scoped_role(guild, role)))
            .collect::<BTreeSet<_>>();
        let kind = if user.bot || user.system {
            ActorKind::Shared
        } else {
            ActorKind::Person
        };
        ingress.observe_membership(
            user.id.clone(), kind,
            MemberEvidence { room: route.room.clone(), roles: roles.clone() },
            now() + MEMBERSHIP_MS,
        ).await?;
        Ok(Sender {
            id: user.id.clone(),
            roles,
        })
    }

    pub(super) async fn invalidate_member(
        &self,
        ingress: &Ingress,
        guild: &str,
        actor: &str,
    ) -> Result<(), GatewayError> {
        let affected: BTreeSet<_> = self.settings.guilds.get(guild)
            .into_iter().flat_map(|guild| guild.channels.keys().cloned()).collect();
        let affects_direct = self.settings.dm_role_guild.as_deref() == Some(guild);
        ingress.withdraw_observed_memberships(actor.to_owned(), move |member| {
            if member.room.kind == RoomKind::Direct {
                !affects_direct
            } else {
                !affected.contains(&member.room.chat)
            }
        }).await
    }
}
