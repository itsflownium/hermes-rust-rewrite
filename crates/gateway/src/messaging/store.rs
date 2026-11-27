use super::{RecipientOutcome, types::*};

#[path = "store_effects.rs"]
mod effect_store;
pub(super) use effect_store::{OutboundPolicy, eligible_consent, protection_matches};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Serialize, de::DeserializeOwned};
use std::{path::Path, sync::Mutex};

const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_routes (
    id TEXT PRIMARY KEY, connection TEXT NOT NULL, room TEXT NOT NULL,
    session TEXT, blocked INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS messaging_checkpoints (
    connection TEXT NOT NULL, stream TEXT NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY(connection, stream)
);
CREATE TABLE IF NOT EXISTS messaging_gaps (
    connection TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS messaging_ingress (
    position INTEGER PRIMARY KEY, connection TEXT NOT NULL, event TEXT NOT NULL,
    fingerprint TEXT NOT NULL, route TEXT REFERENCES messaging_routes(id),
    disposition TEXT NOT NULL, input TEXT, control INTEGER NOT NULL,
    policy_revision INTEGER NOT NULL, stage TEXT NOT NULL, reason TEXT,
    conflict INTEGER NOT NULL DEFAULT 0, UNIQUE(connection, event)
);
CREATE INDEX IF NOT EXISTS messaging_inbox_active ON messaging_ingress(connection, control, route, position)
    WHERE stage IN ('pending','admitted','uncertain','blocked');
CREATE TABLE IF NOT EXISTS messaging_media (
    connection TEXT NOT NULL, id TEXT NOT NULL, bytes BLOB NOT NULL,
    PRIMARY KEY(connection, id)
);
CREATE TABLE IF NOT EXISTS messaging_actions (
    id TEXT PRIMARY KEY, connection TEXT NOT NULL, route TEXT NOT NULL REFERENCES messaging_routes(id),
    session TEXT NOT NULL, recipient TEXT NOT NULL, generation INTEGER NOT NULL,
    request TEXT NOT NULL, type TEXT NOT NULL, resolved TEXT
);
CREATE TABLE IF NOT EXISTS messaging_deliveries (
    position INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE,
    connection TEXT NOT NULL, destination TEXT NOT NULL,
    route TEXT NOT NULL REFERENCES messaging_routes(id), intent TEXT NOT NULL,
    sender TEXT, stage TEXT NOT NULL, reason TEXT
);
CREATE INDEX IF NOT EXISTS messaging_outbox_active ON messaging_deliveries(connection, destination, position)
    WHERE stage IN ('pending','planned');
CREATE TABLE IF NOT EXISTS messaging_parts (
    delivery TEXT NOT NULL REFERENCES messaging_deliveries(id), ordinal INTEGER NOT NULL,
    body TEXT NOT NULL, stage TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
    generation TEXT, receipt TEXT, editable_message TEXT, reason TEXT,
    PRIMARY KEY(delivery, ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_cooldowns (
    connection TEXT NOT NULL, scope TEXT NOT NULL, until_ms INTEGER NOT NULL,
    PRIMARY KEY(connection, scope)
);
";

pub(super) struct Store(pub(super) Mutex<Connection>, pub(super) super::media::ProfileAudio);

#[derive(Clone, Eq, PartialEq)]
pub struct Route {
    pub id: String,
    pub connection: String,
    pub room: Room,
    pub session: Option<String>,
    pub blocked: bool,
    pub revision: i64,
}

impl Route {
    fn from_row(row: &rusqlite::Row<'_>, start: usize) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(start)?,
            connection: row.get(start + 1)?,
            room: decode(row, start + 2)?,
            session: row.get(start + 3)?,
            blocked: row.get(start + 4)?,
            revision: row.get(start + 5)?,
        })
    }

    pub fn new(connection: &str, room: Room, participant: Option<&str>) -> Result<Self> {
        Ok(Self {
            id: key("route", &(connection, &room, participant))?,
            connection: connection.to_owned(),
            room,
            session: None,
            blocked: false,
            revision: 0,
        })
    }
}

pub struct Candidate {
    pub event: InboundEvent,
    pub decision: Result<Route, Disposition>,
    pub protection: Option<String>,
}

#[derive(Clone)]
pub(super) struct PendingInput {
    pub event: InboundEvent,
    pub route: Route,
    pub stage: String,
    pub head: bool,
}

pub(super) struct PendingDelivery {
    pub intent: DeliveryIntent,
    pub route: String,
    pub sender: Option<Sender>,
    pub planned: bool,
}

#[derive(Serialize)]
pub(super) struct DeliveryStatus {
    pub id: String,
    pub connection: String,
    pub route: String,
    pub state: String,
    pub part: Option<u32>,
    pub attempt: Option<u32>,
    pub part_state: Option<String>,
    pub error: Option<String>,
    pub provider_status: Vec<effect_store::ProviderStatus>,
    pub recipients: std::collections::BTreeMap<String, RecipientOutcome>,
}

#[derive(Serialize)]
pub(super) struct InputStatus {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<&'static str>,
    pub connection: String,
    pub route: Option<String>,
    pub session: Option<String>,
    pub state: String,
    pub reason: Option<String>,
    pub conflict: bool,
}

#[derive(Clone)]
pub struct Action {
    pub id: String,
    pub route: String,
    pub session: String,
    pub recipient: String,
    pub generation: u64,
    pub request: String,
    pub kind: String,
    pub expires: i64,
    pub tool: Option<String>,
    pub resolved: Option<Response>,
}

pub fn key(kind: &str, parts: &impl Serialize) -> Result<String> {
    use std::fmt::Write;
    let encoded = serde_json::to_vec(parts)?;
    let digest = ring::digest::digest(&ring::digest::SHA256, &encoded);
    let mut id = String::from(kind);
    id.push('-');
    for byte in digest.as_ref() {
        write!(id, "{byte:02x}")?;
    }
    Ok(id)
}

pub fn decode<T: DeserializeOwned>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<T> {
    let bytes = row.get_ref(index)?.as_str()?;
    serde_json::from_str(bytes).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

pub fn optional_decode<T: DeserializeOwned>(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Option<T>> {
    if row.get_ref(index)?.data_type() == rusqlite::types::Type::Null {
        return Ok(None);
    }
    decode(row, index).map(Some)
}

impl Store {
    #[cfg(test)]
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_with_audio(root, None)
    }

    pub(super) fn audio(&self) -> &super::media::ProfileAudio {
        &self.1
    }

    pub fn open_with_audio(
        root: &Path,
        service: Option<std::sync::Arc<dyn super::AudioService>>,
    ) -> Result<Self> {
        let connection = hermes_engine::db::open(root)?;
        connection.execute_batch(TABLES)?;
        connection.execute_batch(super::media::TABLES)?;
        connection.execute_batch(effect_store::TABLES)?;
        connection.execute_batch(super::approvals::TABLES)?;
        connection.execute_batch(super::commands::TABLES)?;
        connection.execute_batch(super::intake::TABLES)?;
        connection.execute_batch(super::prepared_threads::TABLES)?;
        connection.execute_batch(super::quotes::TABLES)?;
        connection.execute_batch(super::receiver::TABLES)?;
        connection.execute_batch(super::timed::TABLES)?;
        connection.execute_batch(super::input_aliases::TABLES)?;
        connection.execute_batch(super::producer_results::TABLES)?;
        connection.execute_batch(super::protection::TABLES)?;
        connection.execute_batch(super::profile::SCHEMA)?;
        connection.execute_batch(super::repair::TABLES)?;
        connection.execute_batch(super::routes::TABLES)?;
        connection.execute_batch(super::provision::TABLES)?;
        connection.execute_batch(super::transport::SCHEMA)?;
        connection.execute_batch(super::wake::TABLES)?;
        connection.execute("DELETE FROM messaging_enrollment", [])?;
        Ok(Self(Mutex::new(connection), super::media::ProfileAudio::new(root, service)))
    }

    pub fn write<T>(&self, change: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut connection = crate::lock(&self.0);
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = change(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn read<T>(&self, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        read(&crate::lock(&self.0))
    }

    pub fn checkpoint(&self, connection: &str, stream: &str) -> Result<Option<String>> {
        let db = crate::lock(&self.0);
        Ok(read_checkpoint(&db, connection, stream)?)
    }

    pub fn report_gap(&self, connection: &str) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_gaps (connection) VALUES (?1)",
                [connection],
            )?;
            Ok(())
        })
    }

    pub fn gap(&self, connection: &str) -> Result<bool> {
        let db = crate::lock(&self.0);
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_gaps WHERE connection = ?1)",
            [connection],
            |row| row.get(0),
        )?)
    }

    pub fn intake(
        &self,
        connection: &str,
        revision: i64,
        candidates: Vec<Candidate>,
        advance: Option<&Advance>,
    ) -> Result<Option<IntakeResult>> {
        self.write(|tx| {
            if !check_advance(tx, connection, advance)? {
                return Ok(None);
            }
            for candidate in &candidates {
                if super::repair::blocks_intake(tx, connection, &candidate.event)? {
                    return Ok(None);
                }
            }
            let mut receipts = Vec::with_capacity(candidates.len());
            for candidate in candidates {
                receipts.push(append(tx, connection, revision, candidate, false)?);
            }
            Ok(Some(IntakeResult {
                receipts,
                checkpoint: write_advance(tx, connection, advance)?,
            }))
        })
    }

    pub fn put_attachment(
        &self,
        connection: &str,
        name: String,
        bytes: &[u8],
    ) -> Result<AttachmentRef> {
        self.write(|tx| super::media::put(tx, connection, name, bytes))
    }

    pub fn attachment(&self, connection: &str, attachment: &AttachmentRef) -> Result<Vec<u8>> {
        let db = crate::lock(&self.0);
        Ok(db.query_row(
            "SELECT bytes FROM messaging_media WHERE connection = ?1 AND id = ?2",
            params![connection, attachment.id],
            |row| row.get(0),
        )?)
    }

    pub fn route(&self, id: &str) -> Result<Option<Route>> {
        let db = crate::lock(&self.0);
        Ok(read_route(&db, id)?)
    }

    pub fn bind(&self, route: &Route, session: &str) -> Result<()> {
        self.write(|tx| {
            ensure!(tx.execute(
                "UPDATE messaging_routes SET session = ?2
                 WHERE id = ?1 AND (session IS NULL OR session = ?2) AND blocked = 0 AND revision = ?3",
                params![route.id, session, route.revision])? == 1, "Messaging route changed while opening its session");
            Ok(())
        })
    }

    pub fn block_route(&self, route: &str) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE messaging_routes SET blocked = 1 WHERE id = ?1",
                [route],
            )?;
            Ok(())
        })
    }

    pub fn restore_route(&self, route: &str) -> Result<bool> {
        self.write(|tx| {
            let changed = tx.execute(
                "UPDATE messaging_routes SET blocked = 0, session = NULL, revision = revision + 1 WHERE id = ?1 AND blocked = 1",
                [route],
            )? != 0;
            if changed {
                // A prior admission might already have executed. Freshly staged input may continue.
                tx.execute(
                    "UPDATE messaging_ingress SET stage = 'done', reason = 'previous_session_missing', input = NULL
                     WHERE (route = ?1 OR route IN (SELECT destination FROM messaging_route_bindings WHERE binding = ?1))
                       AND stage IN ('admitting','admitted','uncertain','blocked')",
                    [route],
                )?;
            }
            Ok(changed)
        })
    }

    pub(super) fn inputs(&self, connection: &str) -> Result<Vec<PendingInput>> {
        let db = crate::lock(&self.0);
        let mut query = db.prepare(
            "WITH route_queue AS (
                SELECT COALESCE(link.binding,i.route) AS binding,MIN(i.position) AS head,
                    MIN(CASE WHEN i.stage IN ('pending','admitting') THEN i.position END) AS unadmitted,
                    MIN(CASE WHEN i.stage = 'preparing' THEN i.position END) AS preparing
                FROM messaging_ingress i LEFT JOIN messaging_route_bindings link ON link.destination = i.route
                WHERE i.connection = ?1 AND i.control = 0
                  AND i.stage IN ('preparing','pending','admitting','admitted','uncertain','blocked')
                GROUP BY COALESCE(link.binding,i.route)
             )
             SELECT i.input,i.stage,r.id,r.connection,r.room,r.session,r.blocked,r.revision,
                COALESCE(i.position = q.head,0)
             FROM messaging_ingress i JOIN messaging_routes r ON r.id = i.route
             LEFT JOIN messaging_route_bindings link ON link.destination = i.route
             LEFT JOIN messaging_routes parent ON parent.id = link.binding
             LEFT JOIN route_queue q ON q.binding = COALESCE(link.binding,i.route)
             WHERE i.connection = ?1 AND r.blocked = 0 AND COALESCE(parent.blocked,0) = 0 AND
                 ((i.control = 1 AND i.stage = 'pending') OR
                  (i.control = 0 AND i.stage <> 'preparing' AND
                     (i.position = q.head OR (i.position = q.unadmitted AND
                        (q.preparing IS NULL OR i.position < q.preparing)))))
             ORDER BY i.control DESC,i.position",
        )?;
        Ok(query
            .query_map([connection], |row| {
                Ok(PendingInput {
                    event: decode(row, 0)?,
                    stage: row.get(1)?,
                    route: Route::from_row(row, 2)?,
                    head: row.get(8)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn mark(
        &self,
        route: &Route,
        event: &str,
        stage: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        self.write(|tx| set_input(tx, route, event, stage, reason))
    }

    pub fn input(&self, route: &Route, id: &str) -> Result<Option<(String, InboundEvent)>> {
        let db = crate::lock(&self.0);
        Ok(db.query_row(
            "SELECT stage,input FROM messaging_ingress WHERE route = ?1 AND event = ?2 AND control = 0 AND input IS NOT NULL",
            params![route.id, id], |row| Ok((row.get(0)?, decode(row, 1)?)),
        ).optional()?)
    }

    pub fn publish_reply(
        &self,
        route: &Route,
        event: &InboundEvent,
        intent: &DeliveryIntent,
    ) -> Result<()> {
        self.write(|tx| {
            let progress = key("progress", &intent.id)?;
            tx.execute(
                "UPDATE messaging_deliveries SET stage = 'discarded', reason = 'progress_superseded'
                 WHERE id = ?1 AND route = ?2 AND stage = 'pending'",
                params![progress, route.id],
            )?;
            enqueue(tx, route, Some(&event.sender), intent)?;
            effect_store::source(tx, intent, &event.id)?;
            super::profile::link_resource_delivery(tx, &route.connection, &event.id, &intent.id)?;
            set_input(tx, route, &event.id, "done", None)
        })
    }

    pub fn publish_progress(
        &self,
        route: &Route,
        event: &InboundEvent,
        reply_id: String,
    ) -> Result<()> {
        let intent = DeliveryIntent {
            id: key("progress", &reply_id)?,
            room: route.room.clone(),
            reply_anchor: event.reply_anchor.clone(),
            edit_target: None,
            body: DeliveryBody::Progress {
                text: "Working…".into(),
                reply_id,
            },
        };
        self.write(|tx| {
            enqueue(tx, route, Some(&event.sender), &intent)?;
            effect_store::source(tx, &intent, &event.id)?;
            super::profile::link_resource_delivery(tx, &route.connection, &event.id, &intent.id)
        })
    }

    pub fn editable_progress(&self, route: &str, reply: &str) -> Result<Option<String>> {
        let progress = key("progress", &reply)?;
        let db = crate::lock(&self.0);
        Ok(db
            .query_row(
                "SELECT p.editable_message FROM messaging_deliveries d
             JOIN messaging_parts p ON p.delivery = d.id
             WHERE d.id = ?1 AND d.route = ?2 AND d.stage = 'sent' AND p.ordinal = 0
               AND NOT EXISTS (SELECT 1 FROM messaging_effect_markers m WHERE m.delivery=d.id AND m.ordinal=p.ordinal AND m.retired=1)",
                params![progress, route],
                |row| row.get(0),
            )
            .optional()?
            .flatten())
    }

    pub fn notification_text(&self, route: &str, id: &str) -> Result<Option<String>> {
        let db = crate::lock(&self.0);
        let intent: Option<DeliveryIntent> = db
            .query_row(
                "SELECT intent FROM messaging_deliveries WHERE route = ?1 AND id = ?2",
                params![route, id],
                |row| decode(row, 0),
            )
            .optional()?;
        Ok(intent.and_then(|intent| match intent.body {
            DeliveryBody::Reply(message) => Some(message.text),
            _ => None,
        }))
    }

    pub fn enqueue(&self, route: &Route, intent: &DeliveryIntent) -> Result<()> {
        self.write(|tx| enqueue(tx, route, None, intent))
    }

    pub fn prompt(
        &self,
        route: &Route,
        event: &InboundEvent,
        action: &Action,
        prompt: Prompt,
    ) -> Result<()> {
        self.write(|tx| {
            if tx.execute(
                "INSERT OR IGNORE INTO messaging_actions (id,connection,route,session,recipient,generation,request,type)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                params![action.id, route.connection, route.id, action.session, action.recipient,
                    i64::try_from(action.generation)?, action.request, action.kind],
            )? == 0 {
                return Ok(());
            }
            tx.execute(
                "INSERT INTO messaging_action_authority(id,expires,tool) VALUES (?1,?2,?3)",
                params![action.id,action.expires,action.tool],
            )?;
            let intent = DeliveryIntent {
                id: action.id.clone(),
                room: route.room.clone(),
                reply_anchor: event.reply_anchor.clone(),
                edit_target: None,
                body: DeliveryBody::Prompt { action_id: action.id.clone(), prompt },
            };
            enqueue(tx, route, Some(&event.sender), &intent)?;
            effect_store::source(tx, &intent, &event.id)?;
            super::profile::link_resource_delivery(tx, &route.connection, &event.id, &action.id)
        })
    }

    pub fn action(&self, route: &Route, id: &str) -> Result<Option<Action>> {
        let db = crate::lock(&self.0);
        Ok(db
            .query_row(
                "SELECT a.session,a.recipient,a.generation,a.request,a.type,a.resolved,COALESCE(p.expires,0),p.tool
                 FROM messaging_actions a LEFT JOIN messaging_action_authority p ON a.id = p.id
                 WHERE a.id = ?1 AND a.route = ?2 AND a.connection = ?3",
                params![id, route.id, route.connection],
                |row| {
                    Ok(Action {
                        id: id.into(),
                        route: route.id.clone(),
                        session: row.get(0)?,
                        recipient: row.get(1)?,
                        generation: row.get::<_, i64>(2)?.try_into().unwrap_or_default(),
                        request: row.get(3)?,
                        kind: row.get(4)?,
                        resolved: optional_decode(row, 5)?,
                        expires: row.get(6)?,
                        tool: row.get(7)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn settle_action(
        &self,
        route: &Route,
        event: &str,
        action: &Action,
        response: &Response,
        policy: &str,
        prompt_cleanup: bool,
    ) -> Result<()> {
        self.write(|tx| {
            ensure!(tx.execute(
                "UPDATE messaging_actions SET resolved = ?2 WHERE id = ?1 AND resolved IS NULL",
                params![action.id, serde_json::to_string(response)?],
            )? == 1, "Messaging prompt was already decided");
            super::approvals::record(tx, route, action, response, policy)?;
            super::commands::record_invocation(tx, route, event, &action.recipient, super::InvocationOutcome::Applied)?;
            if prompt_cleanup {
                effect_store::retire_decided_prompt(tx, &route.connection, &action.id)?;
            }
            set_input(tx, route, event, "done", Some("applied"))
        })
    }

    pub(super) fn delivery_for_recheck(
        &self,
        connection: &str,
        generation: &str,
        attempt: &SendAttempt,
    ) -> Result<Option<PendingDelivery>> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT d.intent,d.route,d.sender FROM messaging_deliveries d
                 JOIN messaging_parts p ON p.delivery=d.id
                 WHERE d.connection=?1 AND d.id=?2 AND d.stage='planned' AND p.ordinal=?3
                   AND p.attempt=?4 AND p.generation=?5 AND p.stage='sending'",
                params![connection, attempt.delivery_id, attempt.part, attempt.attempt, generation],
                |row| Ok(PendingDelivery {
                    intent: decode(row, 0)?,
                    route: row.get(1)?,
                    sender: optional_decode(row, 2)?,
                    planned: true,
                }),
            ).optional()?)
        })
    }

    pub(super) fn ready_deliveries(&self, connection: &str, now: i64) -> Result<Vec<PendingDelivery>> {
        let db = crate::lock(&self.0);
        let mut query = db.prepare(
            "SELECT d.intent,d.route,d.sender,d.stage FROM messaging_deliveries d
             WHERE d.connection = ?1 AND d.stage IN ('pending','planned') AND (
                 d.position IN (
                     SELECT MIN(position) FROM messaging_deliveries WHERE connection = ?1
                     AND stage IN ('pending','planned') GROUP BY destination
                 ) OR json_type(d.intent, '$.body.retire') IS NOT NULL
             )
             AND NOT EXISTS (SELECT 1 FROM messaging_cooldowns c WHERE c.connection = d.connection
                 AND c.until_ms > ?2 AND (c.scope = '' OR c.scope = d.destination))
             AND (d.stage = 'pending' OR EXISTS (
                 SELECT 1 FROM messaging_parts p WHERE p.delivery = d.id AND (p.stage = 'pending'
                   OR p.stage = 'uncertain' AND json_extract(p.body, '$.effect.replay.expires_at_ms') > ?2)
                 AND NOT EXISTS (SELECT 1 FROM messaging_effect_parts delay
                     WHERE delay.delivery = p.delivery AND delay.ordinal = p.ordinal AND delay.retry_after_ms > ?2)
                 AND NOT EXISTS (SELECT 1 FROM messaging_parts preceding WHERE preceding.delivery = p.delivery
                     AND preceding.ordinal < p.ordinal AND preceding.stage NOT IN ('sent','discarded','partial','submitted','skipped'))
             ))
             ORDER BY d.position",
        )?;
        Ok(query
            .query_map(params![connection, now], |row| {
                Ok(PendingDelivery {
                    intent: decode(row, 0)?,
                    route: row.get(1)?,
                    sender: optional_decode(row, 2)?,
                    planned: row.get::<_, String>(3)? == "planned",
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn plan(&self, connection: &str, id: &str, parts: &[DeliveryPart]) -> Result<bool> {
        self.write(|tx| install_plan(tx, connection, id, parts))
    }

    pub fn reject(&self, connection: &str, id: &str, reason: &str) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE messaging_deliveries SET stage = 'rejected',reason = ?3
                        WHERE connection = ?1 AND id = ?2 AND stage IN ('pending','planned')",
                params![connection, id, reason],
            )?;
            Ok(())
        })
    }

    pub(super) fn claim(
        &self,
        connection: &str,
        generation: &str,
        pending: &PendingDelivery,
        policy: &OutboundPolicy,
    ) -> Result<Option<SendAttempt>> {
        self.write(|tx| effect_store::claim(tx,connection,generation,pending,policy))
    }

    pub(super) fn recheck(
        &self,
        connection: &str,
        generation: &str,
        pending: &PendingDelivery,
        send: &SendAttempt,
        policy: &OutboundPolicy,
    ) -> Result<bool> {
        self.write(|tx| effect_store::recheck(tx,connection,generation,pending,send,policy))
    }

    pub fn release(&self, connection: &str, generation: &str, attempt: &SendAttempt) -> Result<()> {
        self.write(|tx| {
            let (next, reason) = if attempt.effect.retransmission {
                ("uncertain", "send_outcome_unknown")
            } else {
                ("pending", "send_cancelled_before_dispatch")
            };
            let released = tx.execute(
                "UPDATE messaging_parts SET stage = ?6,reason = ?7,generation = NULL
                 WHERE delivery = ?2 AND ordinal = ?3 AND attempt = ?4 AND generation = ?5 AND stage = 'sending'
                 AND EXISTS(SELECT 1 FROM messaging_deliveries WHERE id = ?2 AND connection = ?1)",
                params![connection, attempt.delivery_id, attempt.part, attempt.attempt, generation, next, reason],
            )?;
            if released != 0 && !attempt.effect.retransmission
                && effect_store::status_outcomes(tx,&attempt.delivery_id,attempt.part)?.is_empty()
            {
                effect_store::release_slot(tx,&attempt.delivery_id,attempt.part)?;
            }
            Ok(())
        })
    }

    pub fn record(
        &self,
        connection: &str,
        generation: Option<&str>,
        id: &str,
        attempt: (u32, u32),
        result: DeliveryResult,
        prompt_cleanup: bool,
    ) -> Result<bool> {
        self.write(|tx| {
            effect_store::record(tx, connection, generation, id, attempt, result, prompt_cleanup)
        })
    }


    pub fn reconcile(
        &self,
        connection: &str,
        installation: Option<&super::VerifiedInstallation>,
        delivery: &UncertainDelivery,
        result: DeliveryResult,
        prompt_cleanup: bool,
    ) -> Result<bool> {
        self.write(|tx| {
            let part: Option<DeliveryPart> = tx.query_row(
                "SELECT p.body FROM messaging_parts p JOIN messaging_deliveries d ON d.id=p.delivery
                 WHERE d.connection=?1 AND p.delivery=?2 AND p.ordinal=?3 AND p.attempt=?4 AND p.stage='uncertain'",
                params![connection,delivery.delivery_id,delivery.part,delivery.attempt],
                |row| decode(row,0),
            ).optional()?;
            let Some(part) = part.filter(|part| part.room == delivery.room) else {
                return Ok(false);
            };
            if matches!(part.effect.operation, super::EffectOperation::Provision { .. })
                && !super::provision::ProvisionScope::for_attempt(tx, &delivery.delivery_id, delivery.part, delivery.attempt)?
                    .is_some_and(|scope| scope.allows_installation(installation))
            {
                return Ok(false);
            }
            effect_store::record(
                tx,
                connection,
                None,
                &delivery.delivery_id,
                (delivery.part, delivery.attempt),
                result,
                prompt_cleanup,
            )
        })
    }

    pub fn uncertain_deliveries(&self, connection: &str) -> Result<Vec<UncertainDelivery>> {
        let db = crate::lock(&self.0);
        let mut query = db.prepare(
            "SELECT d.id,p.ordinal,p.attempt,p.body FROM messaging_deliveries d
             JOIN messaging_parts p ON p.delivery = d.id
             WHERE d.connection = ?1 AND p.stage = 'uncertain' ORDER BY d.position LIMIT 200",
        )?;
        Ok(query
            .query_map([connection], |row| {
                Ok(UncertainDelivery {
                    delivery_id: row.get(0)?,
                    part: row.get(1)?,
                    attempt: row.get(2)?,
                    room: decode::<DeliveryPart>(row, 3)?.room,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn recover_sends(&self, connection: &str) -> Result<()> {
        self.write(|tx| {
            effect_store::recover_recipients(tx,connection)?;
            tx.execute(
                "UPDATE messaging_parts SET stage = 'uncertain',reason = 'send_interrupted',generation = NULL
                 WHERE stage = 'sending' AND delivery IN (SELECT id FROM messaging_deliveries WHERE connection = ?1)",
                [connection],
            )?;
            Ok(())
        })
    }

    pub fn resolve(
        &self,
        connection: &str,
        delivery: &str,
        part: u32,
        attempt: u32,
        resolution: DeliveryResolution,
    ) -> Result<bool> {
        let (stage, reason) = match resolution {
            DeliveryResolution::Delivered => ("sent", "operator_confirmed"),
            DeliveryResolution::Discard => ("discarded", "operator_discarded"),
        };
        self.write(|tx| {
            let changed = tx.execute(
                "UPDATE messaging_parts SET stage = ?5,reason = ?6
                 WHERE delivery = ?2 AND ordinal = ?3 AND attempt = ?4 AND stage = 'uncertain'
                 AND EXISTS(SELECT 1 FROM messaging_deliveries WHERE id = ?2 AND connection = ?1)",
                params![connection, delivery, part, attempt, stage, reason],
            )? != 0;
            if changed {
                finish_delivery(tx, delivery)?;
            }
            Ok(changed)
        })
    }

    pub(super) fn delivery_status(&self) -> Result<Vec<DeliveryStatus>> {
        let db = crate::lock(&self.0);
        let mut query = db.prepare(
            "SELECT d.id,d.connection,d.route,d.stage,p.ordinal,p.attempt,
               CASE WHEN p.stage='pending' AND p.reason LIKE 'eligibility_%' THEN 'blocked' ELSE p.stage END,
               COALESCE(p.reason,d.reason)
             FROM messaging_deliveries d LEFT JOIN messaging_parts p ON p.delivery = d.id
             ORDER BY (p.stage = 'uncertain') DESC,d.position DESC,p.ordinal LIMIT 200",
        )?;
        let mut items = query
            .query_map([], |row| {
                Ok(DeliveryStatus {
                    id: row.get(0)?,
                    connection: row.get(1)?,
                    route: row.get(2)?,
                    state: row.get(3)?,
                    part: row.get(4)?,
                    attempt: row.get(5)?,
                    part_state: row.get(6)?,
                    error: row.get(7)?,
                    provider_status: Vec::new(),
                    recipients: Default::default(),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for status in &mut items {
            if let Some(part) = status.part {
                status.provider_status = effect_store::provider_status(&db,&status.id,part)?;
                status.recipients = effect_store::status_outcomes(&db,&status.id,part)?;
            }
        }
        Ok(items)
    }

    pub(super) fn input_status(&self) -> Result<Vec<InputStatus>> {
        let db = crate::lock(&self.0);
        let mut query = db.prepare(
            "SELECT i.event,i.connection,r.id,COALESCE(parent.session,r.session),
                CASE WHEN (r.blocked OR parent.blocked) AND i.stage NOT IN ('done','refused') THEN 'blocked' ELSE i.stage END AS state,
                COALESCE(i.reason, CASE WHEN (r.blocked OR parent.blocked) AND i.stage NOT IN ('done','refused') THEN 'route_unavailable' END),
                i.conflict
             FROM messaging_ingress i LEFT JOIN messaging_routes r ON i.route = r.id
             LEFT JOIN messaging_route_bindings link ON link.destination = r.id
             LEFT JOIN messaging_routes parent ON parent.id = link.binding
             ORDER BY (state IN ('blocked','uncertain')) DESC,i.position DESC LIMIT 200",
        )?;
        Ok(query
            .query_map([], |row| {
                let id: String = row.get(0)?;
                Ok(InputStatus {
                    provenance: super::observations::is_observation(&id).then_some("connection_observation"),
                    id,
                    connection: row.get(1)?,
                    route: row.get(2)?,
                    session: row.get(3)?,
                    state: row.get(4)?,
                    reason: row.get(5)?,
                    conflict: row.get(6)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }
}

pub fn read_route(db: &Connection, id: &str) -> rusqlite::Result<Option<Route>> {
    db.query_row(
        "SELECT id,connection,room,session,blocked,revision FROM messaging_routes WHERE id = ?1",
        [id],
        |row| Route::from_row(row, 0),
    )
    .optional()
}

pub(super) fn read_checkpoint(
    db: &Connection,
    connection: &str,
    stream: &str,
) -> rusqlite::Result<Option<String>> {
    db.query_row(
        "SELECT value FROM messaging_checkpoints WHERE connection = ?1 AND stream = ?2",
        params![connection, stream],
        |row| row.get(0),
    )
    .optional()
}

pub fn check_advance(
    db: &Connection,
    connection: &str,
    advance: Option<&Advance>,
) -> Result<bool> {
    Ok(match advance {
        Some(a) => read_checkpoint(db, connection, &a.stream)? == a.previous,
        None => true,
    })
}

pub fn write_advance(
    db: &Connection,
    connection: &str,
    advance: Option<&Advance>,
) -> Result<Option<String>> {
    let Some(a) = advance else { return Ok(None); };
    db.execute(
        "INSERT INTO messaging_checkpoints (connection,stream,value) VALUES (?1,?2,?3)
         ON CONFLICT(connection,stream) DO UPDATE SET value = excluded.value",
        params![connection, a.stream, a.checkpoint],
    )?;
    Ok(Some(a.checkpoint.clone()))
}

pub fn intake_receipt(
    db: &Connection,
    connection: &str,
    event: &str,
) -> Result<Option<IntakeReceipt>> {
    Ok(db.query_row(
        "SELECT route,disposition FROM messaging_ingress WHERE connection = ?1 AND event = ?2",
        params![connection,event],
        |row| Ok(IntakeReceipt { id: event.to_owned(), route: row.get(0)?,
            disposition: decode(row, 1)?, repeated: false }),
    ).optional()?)
}

pub(super) fn fingerprint(event: &InboundEvent) -> Result<String> {
    // Roles and mention evidence can change across a redelivery.
    key("event", &(&event.room, &event.sender.id, &event.reply_anchor, &event.body))
}

pub(super) fn append(
    tx: &Transaction<'_>,
    connection: &str,
    revision: i64,
    candidate: Candidate,
    preparing: bool,
) -> Result<IntakeReceipt> {
    let Candidate { mut event, decision, protection } = candidate;
    if let Some(id) = super::input_aliases::event_target(tx, connection, &event.id)? {
        event.id = id;
    }
    let identity = fingerprint(&event)?;
    if let Some(receipt) = ingress_replay(tx, connection, &event.id, &identity)? {
        return Ok(receipt);
    }
    let decision = if preparing {
        decision
    } else {
        attachment_decision(tx, connection, &event, decision)?
    };
    let (route, disposition, input, stage) = match decision {
        Ok(route) => {
            if let Some(class) = &protection {
                super::protection::pin_input(tx, connection, &event.id, &event.room, &event.sender.id, class)?;
            }
            tx.execute(
                "INSERT OR IGNORE INTO messaging_routes (id,connection,room) VALUES (?1,?2,?3)",
                params![route.id, connection, serde_json::to_string(&route.room)?],
            )?;
            if matches!(event.body, InboundBody::Command(super::Command::Background { .. })) {
                super::commands::record_invocation(
                    tx, &route, &event.id, &event.sender.id, super::InvocationOutcome::BackgroundQueued,
                )?;
            }
            (
                Some(route.id),
                if preparing { Disposition::Deferred } else { Disposition::Accepted },
                Some(serde_json::to_string(&event)?),
                if preparing { "preparing" } else { "pending" },
            )
        }
        Err(disposition) => (None, disposition, None, "refused"),
    };
    tx.execute(
        "INSERT INTO messaging_ingress (connection,event,fingerprint,route,disposition,input,control,policy_revision,stage)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![connection, event.id, identity, route, serde_json::to_string(&disposition)?,
            input, event.body.is_control(), revision, stage],
    )?;
    Ok(IntakeReceipt {
        id: event.id,
        route,
        disposition,
        repeated: false,
    })
}

pub fn ingress_replay(
    tx: &Connection, connection: &str, event: &str, identity: &str,
) -> Result<Option<IntakeReceipt>> {
    let existing = tx.query_row(
        "SELECT fingerprint,route,disposition FROM messaging_ingress WHERE connection = ?1 AND event = ?2",
        params![connection, event],
        |row| Ok((row.get::<_, String>(0)?, row.get(1)?, decode(row, 2)?)),
    ).optional()?;
    let Some((old, route, disposition)) = existing else {
        return Ok(None);
    };
    if old != identity {
        tx.execute(
            "UPDATE messaging_ingress SET conflict = 1 WHERE connection = ?1 AND event = ?2",
            params![connection, event],
        )?;
        return Ok(Some(IntakeReceipt {
            id: event.to_owned(),
            route: None,
            disposition: Disposition::Conflict,
            repeated: false,
        }));
    }
    Ok(Some(IntakeReceipt {
        id: event.to_owned(),
        route,
        disposition,
        repeated: true,
    }))
}

pub(super) fn attachment_decision(
    tx: &Transaction<'_>,
    connection: &str,
    event: &InboundEvent,
    decision: Result<Route, Disposition>,
) -> Result<Result<Route, Disposition>> {
    let Ok(route) = decision else { return Ok(decision) };
    if let InboundBody::Message(message) = &event.body
        && !message.attachments.is_empty()
    {
        if !super::media::attachments_authorized(tx, connection, event, &message.attachments)? {
            return Ok(Err(Disposition::Invalid));
        }
    }
    Ok(Ok(route))
}

pub(super) fn set_input(
    db: &Connection,
    route: &Route,
    event: &str,
    stage: &str,
    reason: Option<&str>,
) -> Result<()> {
    db.execute(
        "UPDATE messaging_ingress SET stage = ?3,reason = ?4,input = CASE WHEN ?3 = 'done' THEN NULL ELSE input END
         WHERE route = ?1 AND event = ?2 AND stage NOT IN ('done','refused')",
        params![route.id, event, stage, reason],
    )?;
    Ok(())
}

pub fn install_plan(
    tx: &Transaction<'_>,
    connection: &str,
    id: &str,
    parts: &[DeliveryPart],
) -> Result<bool> {
    for (index, part) in parts.iter().enumerate() {
        if !effect_store::operation_available(tx, connection, id, u32::try_from(index)?, part)? {
            return Ok(false);
        }
    }
    if tx.execute("UPDATE messaging_deliveries SET stage = 'planned' WHERE connection = ?1 AND id = ?2 AND stage = 'pending'",
        params![connection, id])? == 0 {
        return Ok(true);
    }
    for (index, part) in parts.iter().enumerate() {
        let stage = if part.effect.attachment_fallback_for.is_some() { "standby" } else { "pending" };
        tx.execute("INSERT INTO messaging_parts (delivery,ordinal,body,stage) VALUES (?1,?2,?3,?4)",
            params![id, i64::try_from(index)?, serde_json::to_string(part)?, stage])?;
        effect_store::register_operation(tx, connection, id, u32::try_from(index)?, part)?;
    }
    Ok(true)
}

pub fn enqueue(
    db: &Connection,
    route: &Route,
    sender: Option<&Sender>,
    intent: &DeliveryIntent,
) -> Result<()> {
    ensure!(
        route.room == intent.room,
        "Messaging destination does not match the route"
    );
    let body = serde_json::to_string(intent)?;
    let destination = effect_store::destination(&route.room)?;
    let inserted = db.execute(
        "INSERT OR IGNORE INTO messaging_deliveries (id,connection,destination,route,intent,sender,stage)
         VALUES (?1,?2,?3,?4,?5,?6,'pending')",
        params![intent.id, route.connection, destination, route.id, body, sender.map(serde_json::to_string).transpose()?],
    )?;
    if inserted == 0 {
        let same: bool = db.query_row(
            "SELECT route = ?2 AND intent = ?3 FROM messaging_deliveries WHERE id = ?1",
            params![intent.id, route.id, body],
            |row| row.get(0),
        )?;
        ensure!(
            same,
            "Messaging delivery identity reused for a different intent"
        );
    }
    Ok(())
}

fn finish_delivery(db: &Connection, id: &str) -> Result<()> {
    // A final primary outcome leaves any unused alternative terminal as well.
    db.execute(
        "UPDATE messaging_parts AS alternative SET stage='skipped' WHERE alternative.delivery=?1 AND alternative.stage='standby'
         AND EXISTS(SELECT 1 FROM messaging_parts primary_part WHERE primary_part.delivery=alternative.delivery
           AND primary_part.ordinal=json_extract(alternative.body,'$.effect.attachment_fallback_for')
           AND primary_part.stage IN ('sent','partial','submitted','rejected','discarded','skipped'))",
        [id],
    )?;
    db.execute(
        "UPDATE messaging_deliveries SET stage = CASE
            WHEN EXISTS (SELECT 1 FROM messaging_parts WHERE delivery = ?1 AND stage = 'rejected') THEN 'rejected'
            WHEN EXISTS (SELECT 1 FROM messaging_parts WHERE delivery = ?1 AND stage NOT IN ('sent','discarded','partial','submitted','skipped')) THEN 'planned'
            WHEN EXISTS (SELECT 1 FROM messaging_parts WHERE delivery = ?1 AND stage = 'discarded') THEN 'discarded'
            WHEN EXISTS (SELECT 1 FROM messaging_parts WHERE delivery = ?1 AND stage = 'partial') THEN 'partial'
            WHEN EXISTS (SELECT 1 FROM messaging_parts WHERE delivery = ?1 AND stage = 'submitted') THEN 'submitted'
            ELSE 'sent' END
         WHERE id = ?1 AND stage = 'planned'",
        [id],
    )?;
    Ok(())
}
