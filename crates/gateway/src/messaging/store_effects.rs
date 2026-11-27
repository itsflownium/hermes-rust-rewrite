use super::{PendingDelivery, Store, enqueue, key, read_route};
use crate::messaging::{
    profile::VerifiedInstallation, provision::ProvisionScope, ConsentUpdate, DeliveryBody, DeliveryIntent, DeliveryPart, EffectDependency, EffectIdentity,
    EffectObservation, EffectOperation, EffectReceipt, EffectTransportPin, EffectTransportRef,
    EffectVisibility, MarkerRef, ObservationResult, PresentationPhase, ProviderDeliveryState,
    ReceiptKind, RecipientOutcome, RecipientResults, ReplayScope, ReplyGrant, ReplyOperation,
    ResolvedEffect, RetryScope, Room, SendAttempt, Sender,
};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_effect_sources (
    delivery TEXT PRIMARY KEY REFERENCES messaging_deliveries(id), event TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_effect_grants (
    connection TEXT NOT NULL, id TEXT NOT NULL, evidence TEXT NOT NULL,
    PRIMARY KEY(connection,id)
);
CREATE TABLE IF NOT EXISTS messaging_effect_reservations (
    connection TEXT NOT NULL, grant_id TEXT NOT NULL, delivery TEXT NOT NULL, part INTEGER NOT NULL,
    PRIMARY KEY(connection,grant_id,delivery,part)
);
CREATE TABLE IF NOT EXISTS messaging_effect_consent (
    connection TEXT NOT NULL, destination TEXT NOT NULL, evidence TEXT NOT NULL,
    PRIMARY KEY(connection,destination)
);
CREATE TABLE IF NOT EXISTS messaging_effect_consent_events (
    connection TEXT NOT NULL, event TEXT NOT NULL, evidence TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_effect_scope (
    connection TEXT PRIMARY KEY, generation TEXT NOT NULL, replay TEXT
);
CREATE TABLE IF NOT EXISTS messaging_effect_protection (
    connection TEXT NOT NULL, destination TEXT NOT NULL, generation TEXT NOT NULL, class TEXT,
    PRIMARY KEY(connection,destination)
);
CREATE TABLE IF NOT EXISTS messaging_effect_parts (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, receipt TEXT, retry_after_ms INTEGER,
    PRIMARY KEY(delivery,ordinal), FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_effect_attempts (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, attempt INTEGER NOT NULL,
    base_request TEXT NOT NULL, lease TEXT NOT NULL, retransmission INTEGER NOT NULL,
    started_at INTEGER NOT NULL, submitted INTEGER NOT NULL, subsets INTEGER NOT NULL,
    PRIMARY KEY(delivery,ordinal,attempt), FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_effect_pins (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, attempt INTEGER NOT NULL, receipt TEXT NOT NULL,
    PRIMARY KEY(delivery,ordinal,attempt),
    FOREIGN KEY(delivery,ordinal,attempt) REFERENCES messaging_effect_attempts(delivery,ordinal,attempt)
);
CREATE TABLE IF NOT EXISTS messaging_effect_identifiers (
    connection TEXT NOT NULL, destination TEXT NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL,
    recipient TEXT NOT NULL, delivery TEXT NOT NULL, ordinal INTEGER NOT NULL,
    UNIQUE(connection,destination,kind,value,recipient,delivery,ordinal),
    FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
CREATE INDEX IF NOT EXISTS messaging_effect_ids_part ON messaging_effect_identifiers(delivery,ordinal);
CREATE TABLE IF NOT EXISTS messaging_effect_observations (
    connection TEXT NOT NULL, event TEXT NOT NULL, destination TEXT NOT NULL, kind TEXT NOT NULL,
    value TEXT NOT NULL, evidence TEXT NOT NULL, seen INTEGER NOT NULL, applied INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,event)
);
CREATE INDEX IF NOT EXISTS messaging_effect_observation_subject
    ON messaging_effect_observations(connection,destination,kind,value) WHERE applied = 0;
CREATE TABLE IF NOT EXISTS messaging_effect_recipients (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, recipient TEXT NOT NULL, outcome TEXT NOT NULL,
    PRIMARY KEY(delivery,ordinal,recipient), FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_effect_status (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, recipient TEXT NOT NULL, state TEXT NOT NULL,
    at_ms INTEGER NOT NULL, PRIMARY KEY(delivery,ordinal,recipient),
    FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_effect_markers (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, receipt TEXT NOT NULL, retired INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(delivery,ordinal), FOREIGN KEY(delivery,ordinal) REFERENCES messaging_parts(delivery,ordinal)
);
";

/// Authorization captured from current, host-owned profile policy under the lease.
pub(in crate::messaging) struct OutboundPolicy {
    pub now: i64,
    pub installation: Option<VerifiedInstallation>,
    pub visibility: EffectVisibility,
    pub protection: Option<String>,
    pub submitted: bool,
    pub transport: Option<EffectTransportPin>,
    pub subsets: bool,
    pub retransmission: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(in crate::messaging) struct ProviderStatus {
    pub recipient: Option<String>,
    pub state: ProviderDeliveryState,
    pub observed_at_ms: i64,
}

pub(super) fn source(tx: &Transaction<'_>, intent: &DeliveryIntent, event: &str) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_effect_sources (delivery,event) VALUES (?1,?2)
         ON CONFLICT(delivery) DO NOTHING",
        params![intent.id, event],
    )?;
    let existing: String = tx.query_row(
        "SELECT event FROM messaging_effect_sources WHERE delivery = ?1",
        [&intent.id],
        |row| row.get(0),
    )?;
    ensure!(existing == event, "Delivery source changed");
    Ok(())
}

pub(super) fn destination(room: &Room) -> Result<String> {
    key("destination", &(&room.chat, &room.thread))
}

fn json<T: DeserializeOwned>(value: String) -> Result<T> {
    Ok(serde_json::from_str(&value)?)
}

impl Store {
    pub fn next_transport(
        &self,
        connection: &str,
        delivery: &str,
    ) -> Result<Option<EffectTransportRef>> {
        self.read(|db| {
            let body: Option<String> = db.query_row(
                "SELECT p.body FROM messaging_parts p JOIN messaging_deliveries d ON d.id=p.delivery
                 WHERE d.connection=?1 AND p.delivery=?2 AND p.stage NOT IN ('sent','discarded','partial','submitted')
                 ORDER BY p.ordinal LIMIT 1",
                params![connection,delivery], |row| row.get(0),
            ).optional()?;
            Ok(body.map(json::<DeliveryPart>).transpose()?.and_then(|part| part.effect.transport))
        })
    }

    pub fn dispatch_transport(
        &self,
        connection: &str,
        generation: &str,
        attempt: &SendAttempt,
    ) -> Result<Option<(EffectTransportPin, Option<Sender>)>> {
        self.read(|db| {
            let row = db.query_row(
                "SELECT t.receipt,d.sender,p.body FROM messaging_effect_pins t
                 JOIN messaging_parts p ON p.delivery=t.delivery AND p.ordinal=t.ordinal AND p.attempt=t.attempt
                 JOIN messaging_deliveries d ON d.id=p.delivery
                 WHERE d.connection=?1 AND t.delivery=?2 AND t.ordinal=?3 AND t.attempt=?4
                     AND p.generation=?5 AND p.stage='sending'",
                params![connection,attempt.delivery_id,attempt.part,attempt.attempt,generation],
                |row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?,row.get::<_,String>(2)?)),
            ).optional()?;
            let Some((receipt,sender,body)) = row else { return Ok(None) };
            let pin = json::<EffectTransportPin>(receipt)?;
            if attempt.effect.transport.as_ref()!=Some(&pin) || json::<DeliveryPart>(body)?!=attempt.payload {
                return Ok(None);
            }
            Ok(Some((pin,sender.map(json).transpose()?)))
        })
    }

    pub fn reply_grant(&self, connection: &str, grant: &ReplyGrant, refresh: bool) -> Result<bool> {
        self.write(|tx| {
            let source: Option<String> = tx.query_row(
                "SELECT r.room FROM messaging_ingress i JOIN messaging_routes r ON i.route = r.id
                 WHERE i.connection = ?1 AND i.event = ?2 AND i.stage != 'refused'",
                params![connection, grant.source_event], |r| r.get(0),
            ).optional()?;
            if source.map(json::<Room>).transpose()?.as_ref() != Some(&grant.room) {
                return Ok(false);
            }
            let old = load_grant(tx, connection, &grant.id)?;
            if let Some(old) = old {
                if grant == &old {
                    return Ok(true);
                }
                let reserved: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_effect_reservations WHERE connection = ?1 AND grant_id = ?2)",
                    params![connection,grant.id], |r| r.get(0),
                )?;
                if !refresh || reserved || grant.source_event != old.source_event || grant.room != old.room
                    || grant.operation != old.operation || grant.slots != old.slots
                    || grant.absolute_expires_at_ms != old.absolute_expires_at_ms
                    || grant.observed_at_ms <= old.observed_at_ms
                    || grant.observed_at_ms >= old.absolute_expires_at_ms
                {
                    return Ok(false);
                }
            } else if refresh {
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO messaging_effect_grants (connection,id,evidence) VALUES (?1,?2,?3)
                 ON CONFLICT(connection,id) DO UPDATE SET evidence=excluded.evidence",
                params![connection,grant.id,serde_json::to_string(grant)?],
            )?;
            Ok(true)
        })
    }

    pub fn update_delivery_consent(
        &self,
        connection: &str,
        update: &ConsentUpdate,
    ) -> Result<bool> {
        self.write(|tx| {
            let dest = destination(&update.room)?;
            if tx.execute(
                "INSERT OR IGNORE INTO messaging_effect_consent_events (connection,event,evidence) VALUES (?1,?2,?3)",
                params![connection,update.event,serde_json::to_string(update)?],
            )? == 0 {
                return Ok(false);
            }
            let previous = tx.query_row(
                "SELECT evidence FROM messaging_effect_consent WHERE connection=?1 AND destination=?2",
                params![connection,dest], |r| r.get(0),
            ).optional()?.map(json::<ConsentUpdate>).transpose()?;
            if previous.is_some_and(|old| old.event == update.event
                || old.observed_at_ms > update.observed_at_ms
                || old.observed_at_ms == update.observed_at_ms && (!old.eligible || update.eligible))
            {
                return Ok(false);
            }
            tx.execute(
                "INSERT INTO messaging_effect_consent (connection,destination,evidence) VALUES (?1,?2,?3)
                 ON CONFLICT(connection,destination) DO UPDATE SET evidence=excluded.evidence",
                 params![connection,dest,serde_json::to_string(update)?],
            )?;
            Ok(true)
        })
    }

    pub fn set_delivery_scope(
        &self,
        connection: &str,
        generation: &str,
        replay: Option<&ReplayScope>,
    ) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO messaging_effect_scope (connection,generation,replay) VALUES (?1,?2,?3)
                 ON CONFLICT(connection) DO UPDATE SET generation=excluded.generation,replay=excluded.replay",
                params![connection,generation,replay.map(serde_json::to_string).transpose()?],
            )?;
            Ok(())
        })
    }

    pub fn set_delivery_protection(
        &self,
        connection: &str,
        generation: &str,
        room: &Room,
        class: Option<&str>,
    ) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "INSERT INTO messaging_effect_protection (connection,destination,generation,class) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(connection,destination) DO UPDATE SET generation=excluded.generation,class=excluded.class",
                params![connection,destination(room)?,generation,class],
            )?;
            Ok(())
        })
    }

    pub fn retire_marker(&self, connection: &str, marker: &MarkerRef) -> Result<bool> {
        self.write(|tx| enqueue_retirement(tx, connection, marker))
    }

    pub fn observe_delivery(
        &self,
        connection: &str,
        update: &EffectObservation,
        now: i64,
    ) -> Result<ObservationResult> {
        self.write(|tx| {
            let dest = destination(&update.room)?;
            let (kind,value) = update.identity.key();
            let data = serde_json::to_string(update)?;
            let changed = tx.execute(
                "INSERT OR IGNORE INTO messaging_effect_observations
                 (connection,event,destination,kind,value,evidence,seen) VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![connection,update.event,dest,kind,value,data,now],
            )?;
            if changed == 0 {
                let existing: String = tx.query_row(
                    "SELECT evidence FROM messaging_effect_observations WHERE connection=?1 AND event=?2",
                    params![connection,update.event], |r| r.get(0),
                )?;
                return Ok(if existing == data { ObservationResult::Repeated } else { ObservationResult::Conflict });
            }
            // Pending early callbacks are transport evidence, not a second message inbox.
            tx.execute(
                "DELETE FROM messaging_effect_observations WHERE connection=?1 AND applied=0 AND event IN (
                    SELECT event FROM messaging_effect_observations WHERE connection=?1 AND applied=0
                    ORDER BY seen DESC, rowid DESC LIMIT -1 OFFSET 2048)",
                [connection],
            )?;
            if apply_observation(tx, connection, &dest, update, now)? {
                mark_observation(tx, connection, &update.event)?;
                return Ok(ObservationResult::Applied);
            }
            Ok(ObservationResult::Pending)
        })
    }
}

fn load_grant(tx: &Connection, connection: &str, id: &str) -> Result<Option<ReplyGrant>> {
    tx.query_row(
        "SELECT evidence FROM messaging_effect_grants WHERE connection=?1 AND id=?2",
        params![connection, id],
        |r| r.get(0),
    )
    .optional()?
    .map(json)
    .transpose()
}

fn enqueue_retirement(
    tx: &Transaction<'_>,
    connection: &str,
    marker: &MarkerRef,
) -> Result<bool> {
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT d.route,d.intent FROM messaging_effect_markers m
             JOIN messaging_deliveries d ON d.id=m.delivery
             WHERE d.connection=?1 AND m.delivery=?2 AND m.ordinal=?3 AND m.retired=0",
            params![connection, marker.delivery, marker.part],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((route_id, original)) = row else {
        return Ok(false);
    };
    let Some(route) = read_route(tx, &route_id)? else {
        return Ok(false);
    };
    let original = json::<DeliveryIntent>(original)?;
    if let DeliveryBody::Prompt { action_id, .. } = &original.body {
        let decided: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_actions WHERE id=?1 AND connection=?2 AND route=?3 AND resolved IS NOT NULL)",
            params![action_id, connection, route_id],
            |row| row.get(0),
        )?;
        if !decided {
            return Ok(false);
        }
    }
    let repair = DeliveryIntent {
        id: key(
            "marker-retire",
            &(connection, &route_id, &marker.delivery, marker.part),
        )?,
        room: route.room.clone(),
        reply_anchor: original.reply_anchor,
        edit_target: None,
        body: DeliveryBody::Retire {
            marker: marker.clone(),
        },
    };
    enqueue(tx, &route, None, &repair)?;
    Ok(true)
}

pub(super) fn retire_decided_prompt(
    tx: &Transaction<'_>,
    connection: &str,
    delivery: &str,
) -> Result<()> {
    // A prompt has one part. The same intent is queued whichever of the
    // durable decision and the provider's marker confirmation arrives last.
    let marker = MarkerRef {
        delivery: delivery.to_owned(),
        part: 0,
    };
    enqueue_retirement(tx, connection, &marker)?;
    Ok(())
}

pub(super) fn marker_receipt(
    tx: &Connection,
    connection: &str,
    route: &str,
    marker: &MarkerRef,
) -> Result<Option<EffectReceipt>> {
    tx.query_row(
        "SELECT m.receipt FROM messaging_effect_markers m
         JOIN messaging_deliveries d ON d.id=m.delivery
         WHERE d.connection=?1 AND d.route=?2 AND m.delivery=?3 AND m.ordinal=?4 AND m.retired=0",
        params![connection, route, marker.delivery, marker.part],
        |r| r.get(0),
    )
    .optional()?
    .map(json)
    .transpose()
}

fn part_receipt(
    tx: &Connection,
    delivery: &str,
    dependency: &EffectDependency,
) -> Result<Option<EffectReceipt>> {
    let receipt = tx
        .query_row(
            "SELECT receipt FROM messaging_effect_parts WHERE delivery=?1 AND ordinal=?2",
            params![delivery, dependency.part],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten()
        .map(json::<EffectReceipt>)
        .transpose()?;
    Ok(receipt.filter(|value| value.kind == dependency.kind))
}

fn replay_proof(
    tx: &Connection,
    connection: &str,
    generation: &str,
) -> Result<Option<ReplayScope>> {
    tx.query_row(
        "SELECT replay FROM messaging_effect_scope WHERE connection=?1 AND generation=?2",
        params![connection, generation],
        |r| r.get::<_, Option<String>>(0),
    )
    .optional()?
    .flatten()
    .map(json)
    .transpose()
}

fn protection_proof(
    tx: &Connection,
    connection: &str,
    generation: &str,
    room: &Room,
) -> Result<Option<String>> {
    Ok(tx.query_row(
        "SELECT class FROM messaging_effect_protection WHERE connection=?1 AND destination=?2 AND generation=?3",
        params![connection,destination(room)?,generation], |r| r.get::<_,Option<String>>(0),
    ).optional()?.flatten())
}

pub(in crate::messaging) fn eligible_consent(
    db: &Connection,
    connection: &str,
    room: &Room,
    required: bool,
) -> Result<bool> {
    let consent = db
        .query_row(
            "SELECT evidence FROM messaging_effect_consent WHERE connection=?1 AND destination=?2",
            params![connection, destination(room)?],
            |row| row.get(0),
        )
        .optional()?
        .map(json::<ConsentUpdate>)
        .transpose()?;
    Ok(consent.map_or(!required, |update| update.eligible))
}

pub(in crate::messaging) fn protection_matches<'a>(
    tx: &Connection,
    connection: &str,
    generation: &str,
    room: &Room,
    requirements: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<bool> {
    let mut requirements = requirements.into_iter().flatten().peekable();
    if requirements.peek().is_none() {
        return Ok(true);
    }
    let proof = protection_proof(tx, connection, generation, room)?;
    Ok(requirements.all(|class| proof.as_deref() == Some(class)))
}

/// Effects remain pending (or uncertain) after a policy failure so a verified renewal can unblock them.
pub(super) fn block(tx: &Transaction<'_>, id: &str, part: u32, reason: &str) -> Result<()> {
    tx.execute(
        "UPDATE messaging_parts SET reason=?3 WHERE delivery=?1 AND ordinal=?2 AND stage IN ('pending','uncertain') AND reason IS NOT ?3",
        params![id,part,reason],
    )?;
    Ok(())
}

struct AuthorizedEffect {
    dependencies: Vec<(u32, EffectReceipt)>,
    marker: Option<EffectReceipt>,
}

fn recorded_transport(
    tx: &Connection,
    id: &str,
    ordinal: u32,
    attempt: u32,
) -> Result<Option<EffectTransportPin>> {
    tx.query_row(
        "SELECT receipt FROM messaging_effect_pins WHERE delivery=?1 AND ordinal=?2 AND attempt=?3",
        params![id, ordinal, attempt],
        |row| row.get(0),
    )
    .optional()?
    .map(json)
    .transpose()
}

fn authorize_effect(
    tx: &Transaction<'_>,
    connection: &str,
    generation: &str,
    pending: &PendingDelivery,
    ordinal: u32,
    part: &DeliveryPart,
    policy: &OutboundPolicy,
) -> Result<Result<AuthorizedEffect, &'static str>> {
    let id = &pending.intent.id;
    let effect = &part.effect;
    if effect.transport.as_ref() != policy.transport.as_ref().map(|pin| &pin.requirement) {
        return Ok(Err("eligibility_transport_address"));
    }
    if effect.write_only && !policy.submitted {
        return Ok(Err("eligibility_write_only"));
    }
    if effect.visibility.notify && !policy.visibility.notify
        || effect.visibility.mention && !policy.visibility.mention
    {
        return Ok(Err("eligibility_visibility"));
    }
    let original = super::super::protection::delivery_protection(tx, connection, id)?;
    if original.as_ref().is_some_and(|pin| {
        pin.room != part.room || effect.protection.as_deref() != Some(pin.class.as_str())
    }) || !protection_matches(tx, connection, generation, &part.room, [
        effect.protection.as_deref(), policy.protection.as_deref(),
    ])? {
        return Ok(Err("eligibility_protection"));
    }
    if !eligible_consent(tx, connection, &part.room, effect.consent_required)? {
        return Ok(Err("eligibility_consent"));
    }
    if let Some(grant_id) = &effect.reply_grant {
        let Some(grant) = load_grant(tx, connection, grant_id)? else {
            return Ok(Err("eligibility_reply_grant"));
        };
        let source_event: Option<String> = tx
            .query_row(
                "SELECT event FROM messaging_effect_sources WHERE delivery=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if grant.room != part.room
            || source_event.as_deref() != Some(&grant.source_event)
            || match grant.operation {
                ReplyOperation::Publish => part.edit_target.is_some(),
                ReplyOperation::Edit => part.edit_target.is_none(),
                ReplyOperation::Either => false,
            }
        {
            return Ok(Err("eligibility_reply_grant_scope"));
        }
        if policy.now >= grant.expires_at_ms || policy.now >= grant.absolute_expires_at_ms {
            return Ok(Err("eligibility_reply_grant_expired"));
        }
        let (reserved,count): (bool,u32) = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_effect_reservations
                WHERE connection=?1 AND grant_id=?2 AND delivery=?3 AND part=?4),
             (SELECT COUNT(*) FROM messaging_effect_reservations WHERE connection=?1 AND grant_id=?2)",
            params![connection,grant.id,id,ordinal], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        if !reserved && count >= grant.slots {
            return Ok(Err("eligibility_reply_slots_exhausted"));
        }
    }
    let mut dependencies = Vec::new();
    for dependency in effect.operation.dependencies() {
        let Some(receipt) = part_receipt(tx, id, &dependency)? else {
            return Ok(Err("eligibility_dependency_unconfirmed"));
        };
        dependencies.push((dependency.part, receipt));
    }
    let marker = match &effect.operation {
        EffectOperation::Retire { marker } => {
            let Some(receipt) = marker_receipt(tx, connection, &pending.route, marker)? else {
                return Ok(Err("eligibility_marker_unavailable"));
            };
            let original = json::<DeliveryPart>(tx.query_row(
                "SELECT body FROM messaging_parts WHERE delivery = ?1 AND ordinal = ?2",
                params![marker.delivery, marker.part],
                |row| row.get(0),
            )?)?;
            if !protection_matches(tx, connection, generation, &part.room, [original.effect.protection.as_deref()])?
            {
                return Ok(Err("eligibility_protection"));
            }
            Some(receipt)
        }
        EffectOperation::Present { marker, phase, .. } if *phase != PresentationPhase::Create => {
            let Some((begin, _)) = prior_presentation(tx, id, marker, ordinal, true)? else {
                return Ok(Err("eligibility_marker_unavailable"));
            };
            let Some(marker_receipt) = marker_receipt(
                tx,
                connection,
                &pending.route,
                &MarkerRef {
                    delivery: id.clone(),
                    part: begin,
                },
            )?
            else {
                return Ok(Err("eligibility_marker_unavailable"));
            };
            let Some(previous) = prior_presentation(tx, id, marker, ordinal, false)? else {
                return Ok(Err("eligibility_dependency_unconfirmed"));
            };
            dependencies.push(previous);
            Some(marker_receipt)
        }
        _ => None,
    };
    Ok(Ok(AuthorizedEffect {
        dependencies,
        marker,
    }))
}

fn prior_presentation(
    tx: &Connection,
    delivery: &str,
    marker: &str,
    ordinal: u32,
    creation: bool,
) -> Result<Option<(u32, EffectReceipt)>> {
    let mut query = tx.prepare(
        "SELECT p.ordinal,p.body,e.receipt FROM messaging_parts p
         LEFT JOIN messaging_effect_parts e ON e.delivery=p.delivery AND e.ordinal=p.ordinal
         WHERE p.delivery=?1 AND p.ordinal<?2
         ORDER BY p.ordinal DESC",
    )?;
    let mut rows = query.query(params![delivery, ordinal])?;
    while let Some(row) = rows.next()? {
        let prior = json::<DeliveryPart>(row.get(1)?)?;
        if let EffectOperation::Present {
            marker: prior_marker,
            phase,
            ..
        } = &prior.effect.operation
            && prior_marker == marker
            && (!creation || *phase == PresentationPhase::Create)
        {
            let ordinal = row.get(0)?;
            let receipt = row.get::<_, Option<String>>(2)?.map(json).transpose()?;
            return Ok(receipt.map(|receipt| (ordinal, receipt)));
        }
    }
    Ok(None)
}

pub(super) fn operation_available(
    tx: &Transaction<'_>,
    connection: &str,
    delivery: &str,
    ordinal: u32,
    part: &DeliveryPart,
) -> Result<bool> {
    let Some(id) = part.effect.provider_operation() else {
        return Ok(true);
    };
    let reused: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_effect_identifiers WHERE connection=?1
         AND destination=?2 AND kind='operation' AND value=?3 AND (delivery!=?4 OR ordinal!=?5))",
        params![connection, destination(&part.room)?, id, delivery, ordinal],
        |r| r.get(0),
    )?;
    Ok(!reused)
}

pub(super) fn register_operation(
    tx: &Transaction<'_>,
    connection: &str,
    delivery: &str,
    ordinal: u32,
    part: &DeliveryPart,
) -> Result<()> {
    if let Some(id) = part.effect.provider_operation() {
        ensure!(
            operation_available(tx, connection, delivery, ordinal, part)?,
            "Operation key reused"
        );
        link_effect(
            tx,
            connection,
            &destination(&part.room)?,
            delivery,
            ordinal,
            "",
            "operation",
            id,
        )?;
    }
    Ok(())
}

fn link_effect(
    tx: &Transaction<'_>,
    connection: &str,
    destination: &str,
    delivery: &str,
    ordinal: u32,
    recipient: &str,
    kind: &str,
    value: &str,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_effect_identifiers
         (connection,destination,kind,value,recipient,delivery,ordinal) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![connection,destination,kind,value,recipient,delivery,ordinal],
    )?;
    Ok(())
}

fn active_claims(tx: &Connection, connection: &str, except: &str, scope: &str) -> Result<bool> {
    // Outbox already serializes a destination. This comparison covers shared transport
    // correlation scopes across distinct destinations.
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_parts p JOIN messaging_deliveries d ON d.id=p.delivery
         WHERE d.connection=?1 AND d.id!=?2 AND p.stage IN ('sending','uncertain')
         AND json_extract(p.body,'$.effect.correlation_scope')=?3)",
        params![connection,except,scope], |r| r.get(0),
    )?)
}

pub(super) fn claim(
    tx: &Transaction<'_>,
    connection: &str,
    generation: &str,
    pending: &PendingDelivery,
    policy: &OutboundPolicy,
) -> Result<Option<SendAttempt>> {
    let id = &pending.intent.id;
    let row: Option<(u32,u32,String,String)> = tx.query_row(
        "SELECT p.ordinal,p.attempt,p.stage,p.body FROM messaging_parts p
         JOIN messaging_deliveries d ON d.id=p.delivery WHERE d.connection=?1 AND d.id=?2 AND d.stage='planned'
         AND p.stage NOT IN ('sent','discarded','partial','submitted','skipped')
         AND NOT EXISTS(SELECT 1 FROM messaging_cooldowns c WHERE c.connection=d.connection AND c.until_ms>?3 AND (c.scope='' OR c.scope=d.destination))
         AND NOT EXISTS(SELECT 1 FROM messaging_deliveries earlier WHERE earlier.connection=d.connection AND earlier.destination=d.destination
             AND earlier.id!=d.id AND earlier.stage IN ('pending','planned')
             AND (EXISTS(SELECT 1 FROM messaging_parts inflight WHERE inflight.delivery=earlier.id AND inflight.stage='sending')
                  OR earlier.position<d.position AND (
                      json_type(d.intent,'$.body.retire') IS NULL
                      OR EXISTS(SELECT 1 FROM messaging_parts unknown WHERE unknown.delivery=earlier.id AND unknown.stage='uncertain')
                  )))
         ORDER BY p.ordinal LIMIT 1",
        params![connection,id,policy.now], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
    ).optional()?;
    let Some((ordinal, old_attempt, stage, data)) = row else {
        return Ok(None);
    };
    if !matches!(stage.as_str(), "pending" | "uncertain")
        || part_retry_at(tx, id, ordinal)?.is_some_and(|until| until > policy.now)
    {
        return Ok(None);
    }
    let payload = json::<DeliveryPart>(data.clone())?;
    if payload
        .effect
        .correlation_scope
        .as_deref()
        .map(|scope| active_claims(tx, connection, id, scope))
        .transpose()?
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let retransmission = stage == "uncertain";
    if retransmission {
        let Some(guarantee) = &payload.effect.replay else {
            return Ok(None);
        };
        let true_scope = replay_proof(tx, connection, generation)?;
        // A fresh verified lease can retry this exact request once. A second
        // ambiguous response must not trigger a tight retransmission loop.
        let old_data = tx
            .query_row(
                "SELECT base_request FROM messaging_effect_attempts
             WHERE delivery=?1 AND ordinal=?2 AND attempt=?3 AND lease!=?4",
                params![id, ordinal, old_attempt, generation],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        if !policy.retransmission
            || policy.now >= guarantee.expires_at_ms
            || true_scope.as_ref() != Some(&guarantee.scope)
            || old_data.as_deref() != Some(&data)
            || recorded_transport(tx, id, ordinal, old_attempt)?.as_ref()
                != policy.transport.as_ref()
        {
            return Ok(None);
        }
        // A subset retry or mixed acceptance is reconciled per recipient. A whole group
        // replay cannot be inferred from a replay guarantee for its first request.
        if !recipient_outcomes(tx, id, ordinal)?.is_empty() {
            return Ok(None);
        }
    }
    if !retransmission
        && let Some(guarantee) = &payload.effect.replay
        && (!policy.retransmission
            || guarantee.expires_at_ms <= policy.now
            || replay_proof(tx, connection, generation)?.as_ref() != Some(&guarantee.scope))
    {
        block(tx, id, ordinal, "eligibility_replay_scope")?;
        return Ok(None);
    }
    let authorized = match authorize_effect(
        tx, connection, generation, pending, ordinal, &payload, policy,
    )? {
        Ok(value) => value,
        Err(reason) => {
            block(tx, id, ordinal, reason)?;
            return Ok(None);
        }
    };
    let provision = if matches!(payload.effect.operation, EffectOperation::Provision { .. }) {
        let current = ProvisionScope::capture(tx, &pending.route, connection, &payload.room, policy.installation.as_ref())?;
        if current.is_none() || retransmission && current != ProvisionScope::for_attempt(tx, id, ordinal, old_attempt)? {
            block(tx, id, ordinal, "eligibility_provision_scope")?;
            return Ok(None);
        }
        current
    } else {
        None
    };
    let recipients = retry_recipients(tx, id, ordinal, &payload)?;
    if !payload.effect.recipients.is_empty() && recipients.is_empty() {
        return Ok(None);
    }
    if !policy.subsets && recipients != payload.effect.recipients {
        block(tx, id, ordinal, "eligibility_recipient_subset")?;
        return Ok(None);
    }
    let attempt = if retransmission {
        old_attempt
    } else {
        old_attempt
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Delivery attempts exhausted"))?
    };
    tx.execute(
        "UPDATE messaging_parts SET stage='sending',attempt=?3,generation=?4,reason=NULL WHERE delivery=?1 AND ordinal=?2",
        params![id,ordinal,attempt,generation],
    )?;
    tx.execute(
        "INSERT INTO messaging_effect_attempts (delivery,ordinal,attempt,base_request,lease,retransmission,started_at,submitted,subsets)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
         ON CONFLICT(delivery,ordinal,attempt) DO UPDATE SET lease=excluded.lease,retransmission=excluded.retransmission",
        params![id,ordinal,attempt,data,generation,retransmission,policy.now,policy.submitted,policy.subsets],
    )?;
    if let Some(scope) = provision {
        scope.record(tx, id, ordinal, attempt)?;
    }
    if let Some(pin) = &policy.transport {
        tx.execute(
            "INSERT OR IGNORE INTO messaging_effect_pins (delivery,ordinal,attempt,receipt) VALUES (?1,?2,?3,?4)",
            params![id,ordinal,attempt,serde_json::to_string(pin)?],
        )?;
    }
    if let Some(grant) = &payload.effect.reply_grant {
        tx.execute(
            "INSERT OR IGNORE INTO messaging_effect_reservations (connection,grant_id,delivery,part) VALUES (?1,?2,?3,?4)",
            params![connection,grant,id,ordinal],
        )?;
    }
    Ok(Some(SendAttempt {
        delivery_id: id.clone(),
        part: ordinal,
        attempt,
        reply_anchor: pending.intent.reply_anchor.clone(),
        payload,
        effect: ResolvedEffect {
            retransmission,
            transport: policy.transport.clone(),
            dependencies: authorized.dependencies,
            marker: authorized.marker,
            recipients,
        },
    }))
}

pub(super) fn recheck(
    tx: &Transaction<'_>,
    connection: &str,
    generation: &str,
    pending: &PendingDelivery,
    send: &SendAttempt,
    policy: &OutboundPolicy,
) -> Result<bool> {
    let evidence: Option<(String, bool)> = tx.query_row(
        "SELECT p.body,a.retransmission FROM messaging_parts p
         JOIN messaging_deliveries d ON d.id=p.delivery
         JOIN messaging_effect_attempts a ON a.delivery=p.delivery AND a.ordinal=p.ordinal AND a.attempt=p.attempt
         WHERE d.connection=?1 AND p.delivery=?2 AND p.ordinal=?3 AND p.attempt=?4
           AND p.generation=?5 AND a.lease=?5 AND p.stage='sending'",
        params![connection,send.delivery_id,send.part,send.attempt,generation],
        |r| Ok((r.get(0)?,r.get(1)?)),
    ).optional()?;
    let Some((body, retransmission)) = evidence else { return Ok(false) };
    let part = json::<DeliveryPart>(body)?;
    if part != send.payload || send.reply_anchor != pending.intent.reply_anchor
        || send.effect.retransmission != retransmission
        || send.effect.transport.as_ref() != policy.transport.as_ref()
        || recorded_transport(tx, &send.delivery_id, send.part, send.attempt)?.as_ref() != policy.transport.as_ref()
    {
        return Ok(false);
    }
    if let Some(guarantee) = &part.effect.replay
        && (!policy.retransmission
            || guarantee.expires_at_ms <= policy.now
            || replay_proof(tx, connection, generation)?.as_ref() != Some(&guarantee.scope))
    {
        return Ok(false);
    }
    if matches!(part.effect.operation, EffectOperation::Provision { .. }) {
        let current = ProvisionScope::capture(tx, &pending.route, connection, &part.room, policy.installation.as_ref())?;
        if current.is_none() || current != ProvisionScope::for_attempt(tx, &send.delivery_id, send.part, send.attempt)? {
            return Ok(false);
        }
    }
    let Ok(authorized) = authorize_effect(tx, connection, generation, pending, send.part, &part, policy)? else {
        return Ok(false);
    };
    Ok(authorized.dependencies == send.effect.dependencies
        && authorized.marker == send.effect.marker
        && retry_recipients(tx, &send.delivery_id, send.part, &part)? == send.effect.recipients)
}

pub(super) fn release_slot(tx: &Transaction<'_>, id: &str, ordinal: u32) -> Result<()> {
    tx.execute(
        "DELETE FROM messaging_effect_reservations WHERE delivery=?1 AND part=?2",
        params![id, ordinal],
    )?;
    Ok(())
}

fn recipient_outcomes(
    tx: &Connection,
    id: &str,
    ordinal: u32,
) -> Result<BTreeMap<String, RecipientOutcome>> {
    let mut query = tx.prepare(
        "SELECT recipient,outcome FROM messaging_effect_recipients WHERE delivery=?1 AND ordinal=?2",
    )?;
    let mut rows = query.query(params![id, ordinal])?;
    let mut matches = BTreeMap::new();
    while let Some(row) = rows.next()? {
        matches.insert(row.get(0)?, json(row.get(1)?)?);
    }
    Ok(matches)
}

fn retry_recipients(
    tx: &Connection,
    id: &str,
    ordinal: u32,
    part: &DeliveryPart,
) -> Result<BTreeSet<String>> {
    let prior = recipient_outcomes(tx, id, ordinal)?;
    if prior.is_empty() {
        return Ok(part.effect.recipients.clone());
    }
    Ok(prior
        .into_iter()
        .filter_map(|(key, value)| matches!(value, RecipientOutcome::NotAccepted).then_some(key))
        .collect())
}

fn mark_uncertain_recipients(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    part: &DeliveryPart,
) -> Result<()> {
    for recipient in retry_recipients(tx, id, ordinal, part)? {
        put_outcome(tx, id, ordinal, &recipient, &RecipientOutcome::Uncertain)?;
    }
    Ok(())
}

pub(super) fn recover_recipients(tx: &Transaction<'_>, connection: &str) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT p.delivery,p.ordinal,p.body FROM messaging_parts p JOIN messaging_deliveries d ON d.id=p.delivery
         WHERE d.connection=?1 AND p.stage='sending'",
    )?;
    let recovered = statement
        .query_map([connection], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, u32>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (id, ordinal, body) in recovered {
        mark_uncertain_recipients(tx, &id, ordinal, &json::<DeliveryPart>(body)?)?;
    }
    Ok(())
}

fn put_outcome(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    recipient: &str,
    outcome: &RecipientOutcome,
) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_effect_recipients (delivery,ordinal,recipient,outcome) VALUES (?1,?2,?3,?4)
         ON CONFLICT(delivery,ordinal,recipient) DO UPDATE SET outcome=excluded.outcome",
        params![id,ordinal,recipient,serde_json::to_string(outcome)?],
    )?;
    Ok(())
}

struct RecipientState {
    stage: &'static str,
    reason: Option<&'static str>,
}

fn recipient_state(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    now: i64,
) -> Result<RecipientState> {
    let outcomes = recipient_outcomes(tx, id, ordinal)?;
    if outcomes
        .values()
        .any(|v| matches!(v, RecipientOutcome::Uncertain))
    {
        return Ok(RecipientState {
            stage: "uncertain",
            reason: Some("recipient_outcome_unknown"),
        });
    }
    let retry_at = part_retry_at(tx, id, ordinal)?;
    if outcomes
        .values()
        .any(|v| matches!(v, RecipientOutcome::NotAccepted))
        && let Some(retry_at) = retry_at
    {
        let after = Duration::from_millis(retry_at.saturating_sub(now).max(1) as u64);
        cooldown(tx, id, RetryScope::Destination, after, now)?;
        return Ok(RecipientState {
            stage: "pending",
            reason: Some("recipient_retry"),
        });
    }
    let all_accepted = outcomes
        .values()
        .all(|v| matches!(v, RecipientOutcome::Accepted { .. }));
    let some_accepted = outcomes
        .values()
        .any(|v| matches!(v, RecipientOutcome::Accepted { .. }));
    Ok(if all_accepted {
        RecipientState {
            stage: "sent",
            reason: None,
        }
    } else if some_accepted {
        RecipientState {
            stage: "partial",
            reason: Some("recipient_partial_acceptance"),
        }
    } else {
        RecipientState {
            stage: "rejected",
            reason: Some("recipient_rejected"),
        }
    })
}

fn part_retry_at(tx: &Connection, delivery: &str, ordinal: u32) -> Result<Option<i64>> {
    Ok(tx.query_row(
        "SELECT retry_after_ms FROM messaging_effect_parts WHERE delivery=?1 AND ordinal=?2",
        params![delivery, ordinal], |row| row.get::<_, Option<i64>>(0),
    ).optional()?.flatten())
}

fn defer_part(
    tx: &Transaction<'_>,
    delivery: &str,
    ordinal: u32,
    now: i64,
    after: Duration,
) -> Result<()> {
    let millis = i64::try_from(after.as_nanos().div_ceil(1_000_000))
        .unwrap_or(i64::MAX)
        .max(1);
    tx.execute(
        "INSERT INTO messaging_effect_parts (delivery,ordinal,retry_after_ms) VALUES (?1,?2,?3)
         ON CONFLICT(delivery,ordinal) DO UPDATE SET retry_after_ms=MAX(COALESCE(retry_after_ms,0),excluded.retry_after_ms)",
        params![delivery, ordinal, now.saturating_add(millis)],
    )?;
    Ok(())
}

pub(super) fn cooldown(
    tx: &Transaction<'_>,
    id: &str,
    scope: RetryScope,
    after: Duration,
    now: i64,
) -> Result<()> {
    let millis = i64::try_from(after.as_millis()).unwrap_or(i64::MAX).max(1);
    tx.execute(
        "INSERT INTO messaging_cooldowns (connection,scope,until_ms)
         SELECT connection,CASE WHEN ?2 THEN destination ELSE '' END,?3 FROM messaging_deliveries WHERE id=?1
         ON CONFLICT(connection,scope) DO UPDATE SET until_ms=MAX(until_ms,excluded.until_ms)",
        params![id,scope == RetryScope::Destination,now.saturating_add(millis)],
    )?;
    Ok(())
}

use crate::messaging::{DeliveryResult, runtime::valid_id};

fn remember_receipt(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    receipt: &EffectReceipt,
) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_effect_parts (delivery,ordinal,receipt) VALUES (?1,?2,?3)
         ON CONFLICT(delivery,ordinal) DO UPDATE SET receipt=excluded.receipt",
        params![id, ordinal, serde_json::to_string(receipt)?],
    )?;
    Ok(())
}

fn remember_marker(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    receipt: &EffectReceipt,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_effect_markers (delivery,ordinal,receipt) VALUES (?1,?2,?3)",
        params![id,ordinal,serde_json::to_string(receipt)?],
    )?;
    Ok(())
}

fn status_label(state: ProviderDeliveryState) -> &'static str {
    match state {
        ProviderDeliveryState::Accepted => "accepted",
        ProviderDeliveryState::Delivered => "delivered",
        ProviderDeliveryState::Read => "read",
        ProviderDeliveryState::Played => "played",
        ProviderDeliveryState::Failed => "failed",
    }
}

fn parse_status_label(label: &str) -> Option<ProviderDeliveryState> {
    Some(match label {
        "accepted" => ProviderDeliveryState::Accepted,
        "delivered" => ProviderDeliveryState::Delivered,
        "read" => ProviderDeliveryState::Read,
        "played" => ProviderDeliveryState::Played,
        "failed" => ProviderDeliveryState::Failed,
        _ => return None,
    })
}

pub(super) fn provider_status(
    tx: &Connection,
    id: &str,
    ordinal: u32,
) -> Result<Vec<ProviderStatus>> {
    let mut statement = tx.prepare(
        "SELECT recipient,state,at_ms FROM messaging_effect_status WHERE delivery=?1 AND ordinal=?2 ORDER BY recipient",
    )?;
    let mut rows = statement.query(params![id, ordinal])?;
    let mut items = Vec::new();
    while let Some(row) = rows.next()? {
        let recipient: String = row.get(0)?;
        if let Some(state) = parse_status_label(&row.get::<_, String>(1)?) {
            items.push(ProviderStatus {
                recipient: (!recipient.is_empty()).then_some(recipient),
                state,
                observed_at_ms: row.get(2)?,
            });
        }
    }
    Ok(items)
}

pub(super) fn status_outcomes(
    tx: &Connection,
    id: &str,
    ordinal: u32,
) -> Result<BTreeMap<String, RecipientOutcome>> {
    recipient_outcomes(tx, id, ordinal)
}

fn progress_rank(state: ProviderDeliveryState) -> u8 {
    match state {
        ProviderDeliveryState::Accepted => 1,
        ProviderDeliveryState::Failed => 2,
        ProviderDeliveryState::Delivered => 3,
        ProviderDeliveryState::Read => 4,
        ProviderDeliveryState::Played => 5,
    }
}

fn store_provider_status(
    tx: &Transaction<'_>,
    id: &str,
    ordinal: u32,
    update: &EffectObservation,
) -> Result<()> {
    let recipient = update.recipient.as_deref().unwrap_or_default();
    let previous = tx.query_row(
        "SELECT state,at_ms FROM messaging_effect_status WHERE delivery=?1 AND ordinal=?2 AND recipient=?3",
        params![id,ordinal,recipient], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)),
    ).optional()?;
    if let Some((label, time)) = previous
        && let Some(known) = parse_status_label(&label)
        && (progress_rank(update.state) < progress_rank(known)
            || update.state == known && update.observed_at_ms <= time
            || update.state == ProviderDeliveryState::Failed && update.observed_at_ms < time)
    {
        return Ok(());
    }
    tx.execute(
        "INSERT INTO messaging_effect_status (delivery,ordinal,recipient,state,at_ms) VALUES (?1,?2,?3,?4,?5)
         ON CONFLICT(delivery,ordinal,recipient) DO UPDATE SET state=excluded.state,at_ms=excluded.at_ms",
        params![id,ordinal,recipient,status_label(update.state),update.observed_at_ms],
    )?;
    Ok(())
}

fn mark_observation(tx: &Transaction<'_>, connection: &str, event: &str) -> Result<()> {
    tx.execute(
        "UPDATE messaging_effect_observations SET applied=1 WHERE connection=?1 AND event=?2",
        params![connection, event],
    )?;
    Ok(())
}

fn apply_pending(
    tx: &Transaction<'_>,
    connection: &str,
    dest: &str,
    id: &str,
    ordinal: u32,
    now: i64,
) -> Result<()> {
    let mut query = tx.prepare(
        "SELECT DISTINCT o.evidence FROM messaging_effect_observations o
         JOIN messaging_effect_identifiers k ON k.connection=o.connection AND k.destination=o.destination
          AND k.kind=o.kind AND k.value=o.value
         WHERE o.applied=0 AND k.connection=?1 AND k.destination=?2 AND k.delivery=?3 AND k.ordinal=?4
         ORDER BY o.seen",
    )?;
    let pending = query
        .query_map(params![connection, dest, id, ordinal], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for payload in pending {
        let update: EffectObservation = json(payload)?;
        if apply_observation(tx, connection, dest, &update, now)? {
            mark_observation(tx, connection, &update.event)?;
        }
    }
    Ok(())
}

fn apply_observation(
    tx: &Transaction<'_>,
    connection: &str,
    dest: &str,
    update: &EffectObservation,
    now: i64,
) -> Result<bool> {
    let (kind, value) = update.identity.key();
    let mut query = tx.prepare(
        "SELECT DISTINCT p.delivery,p.ordinal,p.body,p.stage FROM messaging_effect_identifiers k
         JOIN messaging_parts p ON p.delivery=k.delivery AND p.ordinal=k.ordinal
         WHERE k.connection=?1 AND k.destination=?2 AND k.kind=?3 AND k.value=?4 AND (k.recipient='' OR k.recipient=?5)
         LIMIT 2",
    )?;
    let possible = query
        .query_map(
            params![
                connection,
                dest,
                kind,
                value,
                update.recipient.as_deref().unwrap_or_default()
            ],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, u32>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let [(id, ordinal, body, stage)] = possible.as_slice() else {
        return Ok(false);
    };
    let part: DeliveryPart = json(body.clone())?;
    if part.room != update.room
        || !matches!(
            stage.as_str(),
            "uncertain" | "sent" | "partial" | "submitted"
        )
    {
        return Ok(false);
    }
    if let Some(recipient) = &update.recipient
        && !part.effect.recipients.contains(recipient)
    {
        return Ok(false);
    }
    store_provider_status(tx, id, *ordinal, update)?;
    if stage != "uncertain" || update.state == ProviderDeliveryState::Failed {
        return Ok(true);
    }
    let receipt = update
        .receipt
        .as_deref()
        .or_else(|| match &update.identity {
            EffectIdentity::ProviderId(id) => Some(id.as_str()),
            EffectIdentity::OperationKey(_) => None,
        });
    let Some(receipt) = receipt else {
        return Ok(true);
    };
    let new_state = if let Some(recipient) = &update.recipient {
        for expected in &part.effect.recipients {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_effect_recipients (delivery,ordinal,recipient,outcome) VALUES (?1,?2,?3,?4)",
                params![id,ordinal,expected,serde_json::to_string(&RecipientOutcome::Uncertain)?],
            )?;
        }
        let previous = recipient_outcomes(tx, id, *ordinal)?;
        if matches!(previous.get(recipient), Some(RecipientOutcome::Uncertain)) {
            put_outcome(
                tx,
                id,
                *ordinal,
                recipient,
                &RecipientOutcome::Accepted {
                    receipt: receipt.to_owned(),
                },
            )?;
            link_effect(
                tx, connection, dest, id, *ordinal, recipient, "provider", receipt,
            )?;
        }
        recipient_state(tx, id, *ordinal, now)?
    } else if part.effect.recipients.is_empty()
        && part.effect.operation.receipt_kind() == Some(ReceiptKind::Message)
    {
        // An authenticated message observation can settle a message publication. Other
        // operations can settle only with their own typed receipt.
        remember_receipt(
            tx,
            id,
            *ordinal,
            &EffectReceipt {
                kind: ReceiptKind::Message,
                id: receipt.to_owned(),
            },
        )?;
        link_effect(tx, connection, dest, id, *ordinal, "", "provider", receipt)?;
        RecipientState {
            stage: "sent",
            reason: None,
        }
    } else {
        return Ok(true);
    };
    tx.execute(
        "UPDATE messaging_parts SET stage=?3,reason=?4,receipt=COALESCE(receipt,?5),generation=NULL
         WHERE delivery=?1 AND ordinal=?2 AND stage='uncertain'",
        params![id, ordinal, new_state.stage, new_state.reason, receipt],
    )?;
    super::finish_delivery(tx, id)?;
    Ok(true)
}

struct RecordedEffect {
    stage: &'static str,
    reason: Option<&'static str>,
    receipt: Option<String>,
    editable: Option<String>,
    release_slot: bool,
}

impl RecordedEffect {
    fn uncertain() -> Self {
        Self::with("uncertain", Some("send_outcome_unknown"))
    }

    fn with(stage: &'static str, reason: Option<&'static str>) -> Self {
        Self {
            stage,
            reason,
            receipt: None,
            editable: None,
            release_slot: false,
        }
    }
}

fn record_recipients(
    tx: &Transaction<'_>,
    connection: &str,
    id: &str,
    ordinal: u32,
    part: &DeliveryPart,
    result: RecipientResults,
    subsets: bool,
    now: i64,
) -> Result<RecordedEffect> {
    let requested = retry_recipients(tx, id, ordinal, part)?;
    let recipients: BTreeSet<_> = result
        .recipients
        .iter()
        .map(|entry| &entry.recipient)
        .collect();
    if requested.is_empty()
        || recipients.len() != result.recipients.len()
        || recipients.iter().any(|id| !requested.contains(*id))
        || result.receipt.as_ref().is_some_and(|id| !valid_id(id))
        || result.recipients.iter().any(
            |r| matches!(&r.outcome,RecipientOutcome::Accepted { receipt } if !valid_id(receipt)),
        )
    {
        return Ok(RecordedEffect::uncertain());
    }
    let dest = destination(&part.room)?;
    for expected in &requested {
        put_outcome(tx, id, ordinal, expected, &RecipientOutcome::Uncertain)?;
    }
    for outcome in result.recipients {
        let mut value = outcome.outcome;
        if let RecipientOutcome::Accepted { receipt } = &value {
            link_effect(
                tx,
                connection,
                &dest,
                id,
                ordinal,
                &outcome.recipient,
                "provider",
                receipt,
            )?;
        }
        if matches!(value, RecipientOutcome::NotAccepted)
            && (!subsets || result.retry_after.is_none())
        {
            value = RecipientOutcome::Rejected;
        }
        put_outcome(tx, id, ordinal, &outcome.recipient, &value)?;
    }
    if let Some(after) = result.retry_after.filter(|_| subsets) {
        let retry_at =
            now.saturating_add(i64::try_from(after.as_millis()).unwrap_or(i64::MAX).max(1));
        tx.execute(
            "INSERT INTO messaging_effect_parts (delivery,ordinal,retry_after_ms) VALUES (?1,?2,?3)
             ON CONFLICT(delivery,ordinal) DO UPDATE SET retry_after_ms=excluded.retry_after_ms",
            params![id, ordinal, retry_at],
        )?;
    }
    let state = recipient_state(tx, id, ordinal, now)?;
    let mut classified = RecordedEffect::with(state.stage, state.reason);
    classified.receipt = result.receipt;
    Ok(classified)
}

/// An authenticated create can arrive before its REST response. If that
/// evidence conflicts with a refusal, keep the alternative fenced and let
/// reconciliation apply the positive evidence to the original attempt.
fn pending_publication(
    tx: &Transaction<'_>,
    connection: &str,
    destination: &str,
    delivery: &str,
    ordinal: u32,
) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_effect_identifiers k
         JOIN messaging_effect_observations o ON o.connection=k.connection AND o.destination=k.destination
          AND o.kind=k.kind AND o.value=k.value
         WHERE k.connection=?1 AND k.destination=?2 AND k.delivery=?3 AND k.ordinal=?4
           AND json_extract(o.evidence,'$.recipient') IS NULL
           AND json_extract(o.evidence,'$.state') IN ('accepted','delivered','read','played')
           AND (o.kind='provider' OR json_type(o.evidence,'$.receipt')='text'))",
        params![connection, destination, delivery, ordinal], |row| row.get(0),
    )?)
}

pub(super) fn record(
    tx: &Transaction<'_>,
    connection: &str,
    generation: Option<&str>,
    id: &str,
    (ordinal, attempt): (u32, u32),
    result: DeliveryResult,
    prompt_cleanup: bool,
) -> Result<bool> {
    let now = crate::messaging::runtime::now();
    let row: Option<(String,String,Option<bool>,Option<bool>,Option<bool>)> = tx.query_row(
        "SELECT p.body,d.intent,a.submitted,a.subsets,a.retransmission FROM messaging_parts p
         JOIN messaging_deliveries d ON d.id=p.delivery
         LEFT JOIN messaging_effect_attempts a ON a.delivery=p.delivery AND a.ordinal=p.ordinal AND a.attempt=p.attempt
         WHERE d.connection=?1 AND p.delivery=?2 AND p.ordinal=?3 AND p.attempt=?4 AND p.generation IS ?5
         AND p.stage=CASE WHEN ?5 IS NULL THEN 'uncertain' ELSE 'sending' END",
        params![connection,id,ordinal,attempt,generation], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
    ).optional()?;
    let Some((body, intent, submitted, subsets, retransmission)) = row else {
        return Ok(false);
    };
    let mut part = json::<DeliveryPart>(body)?;
    let intent = json::<DeliveryIntent>(intent)?;
    let dest = destination(&part.room)?;
    let receipts = recipient_outcomes(tx, id, ordinal)?;
    let exact_replay = retransmission.unwrap_or(false);
    let stage = match result {
        DeliveryResult::Confirmed {
            receipt,
            editable_message,
        } if valid_id(&receipt)
            && part.effect.recipients.is_empty()
            && matches!(
                part.effect.operation,
                EffectOperation::Message
                    | EffectOperation::Publish { .. }
                    | EffectOperation::Retire { .. }
            ) =>
        {
            let mut classified = RecordedEffect::with("sent", None);
            let persisted = EffectReceipt {
                kind: ReceiptKind::Message,
                id: receipt.clone(),
            };
            remember_receipt(tx, id, ordinal, &persisted)?;
            classified.receipt = Some(receipt);
            classified.editable = editable_message.filter(|id| valid_id(id));
            if (matches!(part.body, DeliveryBody::Progress { .. })
                || prompt_cleanup && matches!(part.body, DeliveryBody::Prompt { .. }))
                && let Some(marker) = &classified.editable
            {
                remember_marker(
                    tx,
                    id,
                    ordinal,
                    &EffectReceipt {
                        kind: ReceiptKind::Presentation,
                        id: marker.clone(),
                    },
                )?;
            }
            if let Some(target) = &part.edit_target
                && matches!(part.body, DeliveryBody::Reply(_))
            {
                let progress = key("progress", &intent.id)?;
                tx.execute(
                    "UPDATE messaging_effect_markers SET retired=1 WHERE delivery=?1 AND ordinal=0
                     AND json_extract(receipt,'$.id')=?2",
                    params![progress, target],
                )?;
            }
            if let EffectOperation::Retire { marker } = &part.effect.operation {
                tx.execute(
                    "UPDATE messaging_effect_markers SET retired=1 WHERE delivery=?1 AND ordinal=?2",
                    params![marker.delivery,marker.part],
                )?;
            }
            classified
        }
        DeliveryResult::EffectConfirmed(receipt)
            if valid_id(&receipt.id)
                && part.effect.recipients.is_empty()
                && part.effect.operation.receipt_kind() == Some(receipt.kind) =>
        {
            if let EffectOperation::Present { marker, phase, .. } = &part.effect.operation {
                if *phase == PresentationPhase::Create {
                    remember_marker(tx, id, ordinal, &receipt)?;
                } else if let Some((created, _)) =
                    prior_presentation(tx, id, marker, ordinal, true)?
                {
                    if *phase == PresentationPhase::Finalize {
                        tx.execute(
                            "UPDATE messaging_effect_markers SET retired=1 WHERE delivery=?1 AND ordinal=?2",
                            params![id,created],
                        )?;
                    } else if *phase == PresentationPhase::Publish {
                        tx.execute(
                            "UPDATE messaging_effect_markers SET receipt=?3 WHERE delivery=?1 AND ordinal=?2",
                            params![id,created,serde_json::to_string(&receipt)?],
                        )?;
                    }
                }
            }
            remember_receipt(tx, id, ordinal, &receipt)?;
            let mut classified = RecordedEffect::with("sent", None);
            classified.receipt = Some(receipt.id);
            classified
        }
        DeliveryResult::Recipients(outcomes)
            if !exact_replay
                && matches!(
                    part.effect.operation,
                    EffectOperation::Message | EffectOperation::Publish { .. }
                ) =>
        {
            record_recipients(
                tx,
                connection,
                id,
                ordinal,
                &part,
                outcomes,
                subsets.unwrap_or(false),
                now,
            )?
        }
        DeliveryResult::Submitted
            if part.effect.write_only
                && submitted.unwrap_or(false)
                && generation.is_some()
                && !exact_replay
                && matches!(part.effect.operation, EffectOperation::Message)
                && part.effect.recipients.is_empty()
                && part.edit_target.is_none() =>
        {
            RecordedEffect::with("submitted", None)
        }
        DeliveryResult::Retryable { after, scope } if !exact_replay => {
            if generation.is_none() {
                for (recipient, outcome) in &receipts {
                    if matches!(outcome, RecipientOutcome::Uncertain) {
                        put_outcome(tx, id, ordinal, recipient, &RecipientOutcome::NotAccepted)?;
                    }
                }
            }
            if scope == RetryScope::Part {
                defer_part(tx, id, ordinal, now, after)?;
            } else {
                cooldown(tx, id, scope, after, now)?;
            }
            let mut classified = RecordedEffect::with("pending", Some("rate_limited"));
            classified.release_slot = receipts.is_empty();
            classified
        }
        DeliveryResult::AttachmentNotPublished
            if generation.is_some()
                && !exact_replay
                && receipts.is_empty()
                && matches!(part.effect.operation, EffectOperation::Message | EffectOperation::Publish { .. }) =>
        {
            if pending_publication(tx, connection, &dest, id, ordinal)? {
                RecordedEffect::uncertain()
            } else {
                let enabled = tx.execute(
                    "UPDATE messaging_parts SET stage='pending' WHERE delivery=?1 AND stage='standby'
                     AND json_extract(body,'$.effect.attachment_fallback_for')=?2",
                    params![id, ordinal],
                )? > 0;
                let stage = if enabled { "skipped" } else { "rejected" };
                let mut classified = RecordedEffect::with(stage, Some("attachment_not_accepted"));
                classified.release_slot = true;
                classified
            }
        }
        DeliveryResult::EditNotPublished
            if !exact_replay
                && part.edit_target.is_some()
                && part.effect.publish_after_definite_edit_refusal
                && matches!(part.effect.operation, EffectOperation::Message) =>
        {
            part.edit_target = None;
            part.effect.publish_after_definite_edit_refusal = false;
            tx.execute(
                "UPDATE messaging_parts SET body=?3 WHERE delivery=?1 AND ordinal=?2",
                params![id, ordinal, serde_json::to_string(&part)?],
            )?;
            let mut classified =
                RecordedEffect::with("pending", Some("edit_definitively_not_published"));
            classified.release_slot = true;
            classified
        }
        DeliveryResult::Rejected if !exact_replay && receipts.is_empty() => {
            let mut classified = RecordedEffect::with("rejected", Some("platform_rejected"));
            classified.release_slot = true;
            classified
        }
        DeliveryResult::Rejected if !exact_replay => {
            for recipient in retry_recipients(tx, id, ordinal, &part)? {
                put_outcome(tx, id, ordinal, &recipient, &RecipientOutcome::Rejected)?;
            }
            let result = recipient_state(tx, id, ordinal, now)?;
            RecordedEffect::with(result.stage, result.reason)
        }
        _ => RecordedEffect::uncertain(),
    };
    if stage.reason == Some("send_outcome_unknown") {
        mark_uncertain_recipients(tx, id, ordinal, &part)?;
    }
    if stage.release_slot {
        release_slot(tx, id, ordinal)?;
    }
    tx.execute(
        "UPDATE messaging_parts SET stage=?3,receipt=COALESCE(?4,receipt),reason=?5,editable_message=?6,generation=NULL
         WHERE delivery=?1 AND ordinal=?2",
        params![id,ordinal,stage.stage,stage.receipt,stage.reason,stage.editable],
    )?;
    if let Some(receipt) = &stage.receipt {
        link_effect(tx, connection, &dest, id, ordinal, "", "provider", receipt)?;
    }
    apply_pending(tx, connection, &dest, id, ordinal, now)?;
    super::finish_delivery(tx, id)?;
    if prompt_cleanup && stage.stage == "sent" && matches!(intent.body, DeliveryBody::Prompt { .. }) {
        retire_decided_prompt(tx, connection, id)?;
    }
    Ok(true)
}
