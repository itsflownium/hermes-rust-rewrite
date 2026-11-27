use super::{
    ActorEvidence, ActorKind, Ingress, MemberEvidence, Room, Store, StoredActor, db, key, time_ms,
};
use crate::messaging::{GatewayError, Health, runtime::blocking, store::decode};
use rusqlite::{OptionalExtension, params};

/// Refresh only actors who have durable work under the active account, including
/// the original parent of a prepared Discord thread that still gates its child.
const ORIGINS: &str = "
WITH work(actor,room,parent) AS (
    SELECT json_extract(i.input,'$.sender.id'), r.room, t.parent
    FROM messaging_ingress i JOIN messaging_routes r ON r.id=i.route
    LEFT JOIN messaging_prepared_threads t
        ON t.connection=i.connection AND t.event=i.event AND t.selected
    WHERE i.connection=?1 AND r.blocked=0 AND i.input IS NOT NULL
      AND i.stage IN ('preparing','pending','admitting','admitted')
    UNION ALL
    SELECT json_extract(d.sender,'$.id'), r.room, t.parent
    FROM messaging_deliveries d JOIN messaging_routes r ON r.id=d.route
    JOIN messaging_effect_sources s ON s.delivery=d.id
    LEFT JOIN messaging_prepared_threads t
        ON t.connection=d.connection AND t.event=s.event AND t.selected
    WHERE d.connection=?1 AND r.blocked=0 AND d.sender IS NOT NULL
      AND d.stage IN ('pending','planned')
), origins(actor,room) AS (
    SELECT actor,room FROM work
    UNION
    SELECT w.actor,r.room FROM work w JOIN messaging_routes r ON r.id=w.parent
    WHERE r.connection=?1 AND r.blocked=0
)
";

pub type ActorRefreshCursor = (String, String);

pub struct ActorRefresh {
    pub actor: ActorEvidence,
    pub room: Room,
    cursor: ActorRefreshCursor,
    policy_revision: i64,
    evidence_epoch: u64,
}

impl ActorEvidence {
    fn update_member(&mut self, member: MemberEvidence, expires_at: i64) {
        if self.expires_at <= time_ms() {
            self.memberships.clear();
            self.expires_at = expires_at;
        }
        self.memberships.retain(|old| old.room != member.room);
        if self.memberships.len() == 256 {
            self.memberships.remove(0);
        }
        self.memberships.push(member);
        self.revision = self.revision.saturating_add(1);
    }
}

impl Ingress {
    /// Serialize a live observation with any concurrent provider refresh or
    /// withdrawal, so the latest dispatch never loses its update to a lookup.
    pub async fn observe_membership(
        &self,
        actor: String,
        kind: ActorKind,
        member: MemberEvidence,
        expires_at: i64,
    ) -> Result<(), GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                let transport_scope = db(key("actor-transport", &scope.installation()))?;
                let previous = db(scope.store.actor(&scope.connection, &actor))?;
                let mut evidence = previous
                    .filter(|old| old.transport_scope == transport_scope)
                    .map(|old| old.evidence)
                    .unwrap_or_else(|| ActorEvidence {
                        actor: actor.clone(),
                        kind,
                        expires_at: 0,
                        revision: 0,
                        aliases: Vec::new(),
                        memberships: Vec::new(),
                    });
                if evidence.kind != kind || evidence.actor != actor {
                    return Err(GatewayError::Forbidden);
                }
                evidence.update_member(member, expires_at);
                if !db(scope.store.record_actor(
                    &scope.connection,
                    StoredActor {
                        transport_scope,
                        evidence,
                    },
                ))? {
                    return Err(GatewayError::Forbidden);
                }
                Ok(())
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    pub async fn withdraw_observed_memberships(
        &self,
        actor: String,
        retain: impl Fn(&MemberEvidence) -> bool + Send + 'static,
    ) -> Result<(), GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                let transport_scope = db(key("actor-transport", &scope.installation()))?;
                let Some(mut old) =
                    db(scope.store.actor(&scope.connection, &actor))?.filter(|old| {
                        old.transport_scope == transport_scope && old.evidence.actor == actor
                    })
                else {
                    return Ok(());
                };
                old.evidence.memberships.retain(retain);
                old.evidence.revision = old.evidence.revision.saturating_add(1);
                if !db(scope.store.record_actor(&scope.connection, old))? {
                    return Err(GatewayError::Forbidden);
                }
                Ok(())
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    /// Invalidate cached observations for a Discord channel and its children,
    /// one thread, or the rooms and optional DMs that use one guild's roles.
    pub async fn withdraw_observed_destinations(
        &self,
        channels: Vec<String>,
        thread: Option<String>,
        direct_guild: bool,
    ) -> Result<(), GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.apply_evidence_invalidation(|| {
                let transport = db(key("actor-transport", &scope.installation()))?;
                db(scope.store.withdraw_actor_destinations(
                    &scope.connection,
                    &transport,
                    channels,
                    thread,
                    direct_guild,
                ))
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    /// The cursor walks the bounded worker through exact actor and room pairs.
    /// A denied pair advances the cursor without permitting any provider read.
    pub async fn next_actor_refresh(
        &self,
        after: Option<ActorRefreshCursor>,
    ) -> Result<Option<(ActorRefreshCursor, Option<ActorRefresh>)>, GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                if state.health != Health::Ready {
                    return Ok(None);
                }
                let transport = db(key("actor-transport", &scope.installation()))?;
                let Some((cursor, room, current)) = db(scope.store.actor_refresh_origin(
                    &scope.connection,
                    &transport,
                    after.as_ref(),
                ))?
                else {
                    return Ok(None);
                };
                let refresh = state
                    .policy
                    .permits_membership_lookup(&room, &current.evidence.actor)
                    .then(|| ActorRefresh {
                        actor: current.evidence,
                        room,
                        cursor: cursor.clone(),
                        policy_revision: state.revision,
                        evidence_epoch: state.evidence_epoch,
                    });
                Ok(Some((cursor, refresh)))
            })
        })
        .await
    }

    /// A late REST response is usable only against the exact observation,
    /// owner policy, active installation, and still-pending work it began with.
    pub async fn commit_actor_refresh(
        &self,
        refresh: ActorRefresh,
        roles: std::collections::BTreeSet<String>,
        expires_at: i64,
    ) -> Result<bool, GatewayError> {
        let scope = self.clone();
        let applied = blocking(move || {
            scope.checked(true, |state| {
                if state.health != Health::Ready
                    || state.revision != refresh.policy_revision
                    || state.evidence_epoch != refresh.evidence_epoch
                    || expires_at <= time_ms()
                    || !state
                        .policy
                        .permits_membership_lookup(&refresh.room, &refresh.actor.actor)
                {
                    return Ok(false);
                }
                let transport = db(key("actor-transport", &scope.installation()))?;
                db(scope.store.publish_actor_refresh(
                    &scope.connection,
                    transport,
                    refresh,
                    roles,
                    expires_at,
                ))
            })
        })
        .await?;
        if applied {
            self.notify();
        }
        Ok(applied)
    }
}

impl Store {
    fn withdraw_actor_destinations(
        &self,
        connection: &str,
        scope: &str,
        channels: Vec<String>,
        thread: Option<String>,
        direct_guild: bool,
    ) -> anyhow::Result<()> {
        let matches = "(json_extract(member.value,'$.room.kind')='direct' AND ?5)
            OR (json_extract(member.value,'$.room.kind')='group' AND (
                (?3 IS NOT NULL AND json_extract(member.value,'$.room.thread') IS ?3)
                OR json_extract(member.value,'$.room.chat') IN (SELECT value FROM json_each(?4))
            ))";
        let sql = format!("UPDATE messaging_actor_evidence
            SET data=json_set(data,'$.evidence.memberships', json(
                (SELECT json_group_array(json(member.value)) FROM json_each(data,'$.evidence.memberships') member
                 WHERE NOT ({matches}))
            ))
            WHERE connection=?1 AND json_extract(data,'$.transport_scope')=?2 AND EXISTS (
                SELECT 1 FROM json_each(data,'$.evidence.memberships') member WHERE {matches}
            )");
        self.write(|tx| {
            tx.execute(
                &sql,
                params![
                    connection,
                    scope,
                    thread,
                    serde_json::to_string(&channels)?,
                    direct_guild
                ],
            )?;
            Ok(())
        })
    }

    fn actor_refresh_origin(
        &self,
        connection: &str,
        scope: &str,
        after: Option<&ActorRefreshCursor>,
    ) -> anyhow::Result<Option<(ActorRefreshCursor, Room, StoredActor)>> {
        let sql = format!(
            "{ORIGINS}
            SELECT o.actor,o.room,a.data FROM origins o JOIN messaging_actor_evidence a
                ON a.connection=?1 AND a.actor=o.actor
            WHERE json_extract(a.data,'$.transport_scope')=?2
              AND (?3 IS NULL OR (o.actor,o.room)>(?3,?4))
              AND (json_extract(a.data,'$.evidence.expires_at')<=?5 OR NOT EXISTS (
                  SELECT 1 FROM json_each(a.data,'$.evidence.memberships') m
                  WHERE json_extract(m.value,'$.room.kind') IS json_extract(o.room,'$.kind')
                    AND json_extract(m.value,'$.room.chat') IS json_extract(o.room,'$.chat')
                    AND json_extract(m.value,'$.room.thread') IS json_extract(o.room,'$.thread')
              ))
            ORDER BY o.actor,o.room LIMIT 1"
        );
        self.read(|tx| {
            Ok(tx
                .query_row(
                    &sql,
                    params![
                        connection,
                        scope,
                        after.map(|p| &p.0),
                        after.map(|p| &p.1),
                        time_ms()
                    ],
                    |row| Ok(((row.get(0)?, row.get(1)?), decode(row, 1)?, decode(row, 2)?)),
                )
                .optional()?)
        })
    }

    fn publish_actor_refresh(
        &self,
        connection: &str,
        transport_scope: String,
        refresh: ActorRefresh,
        roles: std::collections::BTreeSet<String>,
        expires_at: i64,
    ) -> anyhow::Result<bool> {
        self.write(|tx| {
            let old = StoredActor { transport_scope, evidence: refresh.actor };
            let current: Option<StoredActor> = tx.query_row(
                "SELECT data FROM messaging_actor_evidence WHERE connection=?1 AND actor=?2",
                params![connection, old.evidence.actor], |row| decode(row, 0),
            ).optional()?;
            if current.as_ref() != Some(&old) {
                return Ok(false);
            }
            let sql = format!("{ORIGINS}
                SELECT EXISTS(SELECT 1 FROM origins WHERE actor=?2 AND room=?3)");
            let pending = tx.query_row(&sql, params![connection, refresh.cursor.0, refresh.cursor.1],
                |row| row.get::<_,bool>(0),
            )?;
            if !pending {
                return Ok(false);
            }

            let mut next = old;
            next.evidence.update_member(MemberEvidence { room: refresh.room, roles }, expires_at);
            tx.execute(
                "UPDATE messaging_actor_evidence SET data=?3,revision=?4 WHERE connection=?1 AND actor=?2",
                params![connection, next.evidence.actor, serde_json::to_string(&next)?, i64::try_from(next.evidence.revision)?],
            )?;
            Ok(true)
        })
    }
}
