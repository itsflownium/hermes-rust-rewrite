use super::{
    runtime::{Ingress, db},
    store::{self, Candidate, Route},
    types::*,
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_input_protection (
    connection TEXT NOT NULL, event TEXT NOT NULL, room TEXT NOT NULL,
    actor TEXT NOT NULL, class TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
";

pub(super) struct InputProtection {
    pub room: Room,
    actor: String,
    pub class: String,
}

pub(super) fn input_protection(
    tx: &Connection,
    connection: &str,
    event: &str,
) -> Result<Option<InputProtection>> {
    let target = super::input_aliases::event_target(tx, connection, event)?;
    let event = target.as_deref().unwrap_or(event);
    Ok(tx.query_row(
        "SELECT room,actor,class FROM messaging_input_protection WHERE connection=?1 AND event=?2",
        params![connection,event],
        read_protection,
    ).optional()?)
}

pub(super) fn delivery_protection(
    tx: &Connection,
    connection: &str,
    delivery: &str,
) -> Result<Option<InputProtection>> {
    Ok(tx
        .query_row(
            "SELECT p.room,p.actor,p.class FROM messaging_input_protection p
         JOIN messaging_effect_sources s ON s.event=p.event
         WHERE p.connection=?1 AND s.delivery=?2",
            params![connection, delivery],
            read_protection,
        )
        .optional()?)
}

fn read_protection(row: &rusqlite::Row<'_>) -> rusqlite::Result<InputProtection> {
    Ok(InputProtection {
        room: store::decode(row, 0)?,
        actor: row.get(1)?,
        class: row.get(2)?,
    })
}

pub(super) fn pin_input(
    tx: &Connection,
    connection: &str,
    event: &str,
    room: &Room,
    actor: &str,
    class: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_input_protection(connection,event,room,actor,class)
         VALUES (?1,?2,?3,?4,?5) ON CONFLICT(connection,event) DO NOTHING",
        params![
            connection,
            event,
            serde_json::to_string(room)?,
            actor,
            class
        ],
    )?;
    Ok(())
}

impl Ingress {
    pub(super) fn intake_candidate(
        &self,
        policy: &AccessPolicy,
        mut event: InboundEvent,
        decision: Result<Route, Disposition>,
        declared: Option<&str>,
    ) -> Result<Candidate, GatewayError> {
        self.normalize_event(&mut event)?;
        let pinned = db(self
            .store
            .read(|tx| input_protection(tx, &self.connection, &event.id)))?;
        let decision = if pinned.as_ref().is_some_and(|pin| {
            pin.room != event.room
                || pin.actor != event.sender.id
                || declared.is_some_and(|class| pin.class != class)
        }) {
            decision.and(Err(Disposition::ProtectionUnavailable))
        } else {
            decision
        };
        let protection = pinned
            .map(|pin| pin.class)
            .or_else(|| declared.map(str::to_owned))
            .or_else(|| policy.delivery_policy(&event.room).protection);
        Ok(Candidate {
            event,
            decision,
            protection,
        })
    }

    pub(super) fn check_input_protection(
        &self,
        policy: &AccessPolicy,
        event: &InboundEvent,
    ) -> Result<(), GatewayError> {
        self.check_source_protection(policy, &event.room, &event.sender.id, &event.id)
            .map(|_| ())
    }

    pub(super) fn check_source_protection(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        actor: &str,
        event: &str,
    ) -> Result<Option<String>, GatewayError> {
        let current = policy.delivery_policy(room).protection;
        db(self.store.read(|tx| {
            let pinned = input_protection(tx, &self.connection, event)?;
            if pinned
                .as_ref()
                .is_some_and(|pin| pin.room != *room || pin.actor != actor)
            {
                return Ok(Err(GatewayError::ProtectionUnavailable));
            }
            let original = pinned.map(|pin| pin.class);
            if !store::protection_matches(
                tx,
                &self.connection,
                &self.generation,
                room,
                [original.as_deref(), current.as_deref()],
            )? {
                return Ok(Err(GatewayError::ProtectionUnavailable));
            }
            Ok(Ok(original.or(current)))
        }))?
    }

    pub fn delivery_protection(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        delivery: &str,
    ) -> Result<Option<String>, GatewayError> {
        let original = db(self
            .store
            .read(|tx| delivery_protection(tx, &self.connection, delivery)))?;
        if original.as_ref().is_some_and(|pin| pin.room != *room) {
            return Err(GatewayError::ProtectionUnavailable);
        }
        Ok(original
            .map(|pin| pin.class)
            .or_else(|| policy.delivery_policy(room).protection))
    }

    pub(super) fn check_delivery_protection(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        delivery: &str,
        part: Option<&str>,
    ) -> Result<(), GatewayError> {
        let current = policy.delivery_policy(room).protection;
        db(self.store.read(|tx| {
            let original = delivery_protection(tx, &self.connection, delivery)?;
            if original
                .as_ref()
                .is_some_and(|pin| pin.room != *room || Some(pin.class.as_str()) != part)
            {
                return Ok(Err(GatewayError::ProtectionUnavailable));
            }
            if !store::protection_matches(
                tx,
                &self.connection,
                &self.generation,
                room,
                [part, current.as_deref()],
            )? {
                return Ok(Err(GatewayError::ProtectionUnavailable));
            }
            Ok(Ok(()))
        }))?
    }
}
