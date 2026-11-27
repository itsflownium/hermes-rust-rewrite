use super::{
    DiscordAdapter, Ingress,
    channels::snowflake,
    client::RestFailure,
    settings::{
        PermissionOverwrite, RemoteGuild, RemoteMember, RemoteRole, RemoteUser, SEE_CHANNEL,
        effective_permissions, scoped_role,
    },
};
use crate::messaging::{ActorEvidence, ActorKind, GatewayError, Room, RoomKind, runtime::now};
use serde::Deserialize;
use std::{collections::BTreeSet, time::Duration};
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;

const POLL: Duration = Duration::from_secs(1);
const LOOKUP: Duration = Duration::from_secs(3);
const RATE_LIMITED: Duration = Duration::from_secs(30);
const MANAGE_THREADS: u64 = 1 << 34;
pub(super) const MEMBERSHIP_MS: i64 = 300_000;

#[derive(Deserialize)]
struct CurrentChannel {
    id: String,
    guild_id: Option<String>,
    parent_id: Option<String>,
    #[serde(rename = "type")]
    kind: u8,
    permission_overwrites: Option<Vec<PermissionOverwrite>>,
    recipients: Option<Vec<RemoteUser>>,
}

#[derive(Deserialize)]
struct CurrentThreadMember {
    id: String,
    user_id: String,
}

impl DiscordAdapter {
    pub(super) async fn refresh_pending_memberships(
        &self,
        ingress: &Ingress,
        cancel: &CancellationToken,
    ) {
        let mut tick = interval(POLL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut cursor = None;
        let mut retry_at = Instant::now();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            if Instant::now() < retry_at {
                continue;
            }
            match ingress.next_actor_refresh(cursor.take()).await {
                Ok(Some((next, refresh))) => {
                    cursor = Some(next);
                    let Some(refresh) = refresh else { continue };
                    let proof = timeout(
                        LOOKUP,
                        self.current_membership(&refresh.room, &refresh.actor),
                    )
                    .await;
                    match proof {
                        Ok(Ok(roles)) => {
                            if ingress
                                .commit_actor_refresh(refresh, roles, now() + MEMBERSHIP_MS)
                                .await
                                .is_ok_and(|applied| applied)
                            {
                                let _ = self.resume_preparations(ingress, cancel).await;
                            }
                        }
                        Ok(Err(RestFailure::RateLimited)) => {
                            retry_at = Instant::now() + RATE_LIMITED
                        }
                        _ => {}
                    }
                }
                Ok(None) => {}
                Err(GatewayError::StaleConnection) => return,
                Err(_) => {}
            }
        }
    }

    async fn current_membership(
        &self,
        room: &Room,
        actor: &ActorEvidence,
    ) -> Result<BTreeSet<String>, RestFailure> {
        if !snowflake(&actor.actor)
            || !snowflake(&room.chat)
            || room.thread.as_deref().is_some_and(|id| !snowflake(id))
        {
            return Err(RestFailure::Invalid);
        }

        if room.kind == RoomKind::Direct {
            return self.direct_membership(room, actor).await;
        }
        let guild = self
            .settings
            .guilds
            .keys()
            .find(|guild| {
                self.settings
                    .allows_surface(Some(guild), &room.chat, room.thread.as_deref())
            })
            .ok_or(RestFailure::Forbidden)?;
        let parent: CurrentChannel = self.client.get(&["channels", &room.chat]).await?;
        if parent.id != room.chat
            || parent.guild_id.as_deref() != Some(guild)
            || !matches!(parent.kind, 0 | 5 | 15 | 16)
        {
            return Err(RestFailure::Forbidden);
        }
        let overwrites = parent.permission_overwrites.ok_or(RestFailure::Invalid)?;
        let private_thread = if let Some(thread) = &room.thread {
            let channel: CurrentChannel = self.client.get(&["channels", thread]).await?;
            if channel.id != *thread
                || channel.guild_id.as_deref() != Some(guild)
                || channel.parent_id.as_deref() != Some(&room.chat)
                || !matches!(channel.kind, 10..=12)
            {
                return Err(RestFailure::Forbidden);
            }
            channel.kind == 12
        } else {
            false
        };

        let guild_path = ["guilds", guild.as_str()];
        let roles_path = ["guilds", guild.as_str(), "roles"];
        let (installed, member, roles): (RemoteGuild, RemoteMember, Vec<RemoteRole>) = tokio::try_join!(
            self.client.get(&guild_path),
            self.current_guild_member(guild, actor),
            self.client.get(&roles_path),
        )?;
        let permissions = effective_permissions(
            guild,
            &actor.actor,
            actor.kind == ActorKind::Shared,
            &installed,
            &member,
            &roles,
            &overwrites,
        )
        .ok_or(RestFailure::Invalid)?;
        if permissions & SEE_CHANNEL == 0 {
            return Err(RestFailure::Forbidden);
        }
        if let Some(thread) = &room.thread
            && private_thread
            && permissions & MANAGE_THREADS == 0
        {
            let membership: CurrentThreadMember = self
                .client
                .get(&["channels", thread, "thread-members", &actor.actor])
                .await?;
            if membership.id != *thread || membership.user_id != actor.actor {
                return Err(RestFailure::Forbidden);
            }
        }
        Ok(member
            .roles
            .iter()
            .map(|role| scoped_role(guild, role))
            .collect())
    }

    async fn direct_membership(
        &self,
        room: &Room,
        actor: &ActorEvidence,
    ) -> Result<BTreeSet<String>, RestFailure> {
        if room.thread.is_some() || !self.settings.allows_surface(None, &room.chat, None) {
            return Err(RestFailure::Forbidden);
        }
        let channel: CurrentChannel = self.client.get(&["channels", &room.chat]).await?;
        if channel.id != room.chat
            || channel.kind != 1
            || channel.guild_id.is_some()
            || channel.parent_id.is_some()
            || !channel.recipients.is_some_and(|recipients| {
                recipients.len() == 1 && matches_actor(&recipients[0], actor)
            })
        {
            return Err(RestFailure::Forbidden);
        }
        let Some(guild) = self.settings.dm_role_guild.as_deref() else {
            return Ok(BTreeSet::new());
        };
        match self.current_guild_member(guild, actor).await {
            Ok(member) => Ok(member
                .roles
                .iter()
                .map(|role| scoped_role(guild, role))
                .collect()),
            // The exact DM proves its recipient independently of guild membership.
            Err(RestFailure::Missing) => Ok(BTreeSet::new()),
            Err(error) => Err(error),
        }
    }

    async fn current_guild_member(
        &self,
        guild: &str,
        actor: &ActorEvidence,
    ) -> Result<RemoteMember, RestFailure> {
        let member: RemoteMember = self
            .client
            .get(&["guilds", guild, "members", &actor.actor])
            .await?;
        if !matches_actor(&member.user, actor)
            || member.roles.len() > 128
            || member.roles.iter().any(|role| !snowflake(role))
        {
            return Err(RestFailure::Forbidden);
        }
        Ok(member)
    }
}

fn matches_actor(user: &RemoteUser, actor: &ActorEvidence) -> bool {
    !user.system
        && user.id == actor.actor
        && match actor.kind {
            ActorKind::Person => !user.bot,
            ActorKind::Shared => user.bot,
            ActorKind::Connection => false,
        }
}
