use super::{
    DeliveryBody, DeliveryIntent, DeliveryPart, Disposition, Effect, EffectOperation,
    EffectReceipt, GatewayError, InboundEvent, Ingress, MediaSource, Message, PreparationPurpose,
    ProvisionResource, ReceiptKind, Room, RoomKind, Sender, UncertainDelivery, routes,
    runtime::{State, blocking, db, now, valid_id},
    store::{self, Store},
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, params};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_prepared_threads (
    connection TEXT NOT NULL, event TEXT NOT NULL, parent TEXT NOT NULL, thread TEXT NOT NULL,
    delivery TEXT NOT NULL UNIQUE, selected INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,event),
    FOREIGN KEY(connection,event) REFERENCES messaging_preparation(connection,event),
    FOREIGN KEY(delivery) REFERENCES messaging_deliveries(id)
);
";

pub enum ThreadPreparation {
    Waiting,
    Reconcile(UncertainDelivery),
    Ready(InboundEvent),
    Closed,
}

impl Ingress {
    /// Schedule a typed provision effect for the immutable thread reserved by
    /// the handoff. Only its exact destination receipt can move the input.
    pub async fn prepare_thread(
        &self,
        event_id: String,
        token: String,
    ) -> Result<ThreadPreparation, GatewayError> {
        let scope = self.clone();
        let progress = blocking(move || scope.checked(true, |state| {
            let staged = db(scope.store.preparation(&scope.connection, &event_id))?
                .ok_or(GatewayError::Invalid)?;
            if staged.token != token {
                return Err(GatewayError::Forbidden);
            }
            let PreparationPurpose::Thread { id, inline_on_refusal } = staged.purpose else {
                return Err(GatewayError::Unsupported);
            };
            if !scope.capabilities().effects.prepare_resources || !valid_id(&id) {
                return Err(GatewayError::Unsupported);
            }
            let Some(mut event) = staged.event.filter(|_| {
                staged.receipt.disposition == Disposition::Deferred && staged.expires_ms > now()
            }) else {
                return Ok(ThreadPreparation::Closed);
            };
            let Some(route_id) = staged.receipt.route else {
                return Err(GatewayError::Invalid);
            };
            if event.room.kind != RoomKind::Group {
                return Err(GatewayError::Invalid);
            }
            let previous = db(scope.store.prepared_thread(&scope.connection, &event_id))?;
            let mut parent_event = event.clone();
            parent_event.room.thread = None;
            parent_event.reply_anchor = Some(id.clone());
            let Ok(parent) = scope.route_candidate(&state.policy, &parent_event, true)? else {
                return Err(GatewayError::Forbidden);
            };
            if parent.id != previous.as_ref().map_or(route_id.as_str(), |row| row.parent.as_str())
                || db(scope.store.route(&parent.id))?.is_none_or(|route| route.blocked)
            {
                return Err(GatewayError::Forbidden);
            }
            let mut child = parent_event.clone();
            child.room.thread = Some(id.clone());
            child.reply_anchor = None;
            if previous.as_ref().is_some_and(|row| row.selected && event.room == parent.room) {
                return Ok(ThreadPreparation::Ready(event));
            }
            let Some(destination) = scope.public_thread_route(
                state, &parent.room, &child.room, &event.sender, event.mentioned,
            )? else {
                return Err(GatewayError::Forbidden);
            };
            if db(scope.store.route(&destination.id))?.is_some_and(|route| route.blocked) {
                return Err(GatewayError::Forbidden);
            }
            let Some(previous) = previous else {
                if event.room != parent.room || event.reply_anchor.as_deref() != Some(&id) {
                    return Err(GatewayError::Invalid);
                }
                let delivery = db(store::key("prepared-thread", &(&scope.connection, &event_id)))?;
                let intent = DeliveryIntent {
                    id: delivery,
                    room: parent.room.clone(),
                    reply_anchor: Some(id.clone()),
                    edit_target: None,
                    body: DeliveryBody::Reply(Message::text("Open discussion")),
                };
                let protection = scope.delivery_protection(&state.policy, &intent.room, &intent.id)?;
                let part = DeliveryPart {
                    room: intent.room.clone(),
                    body: intent.body.clone(),
                    edit_target: None,
                    effect: Effect {
                        operation: EffectOperation::Provision { resource: ProvisionResource::Thread },
                        protection,
                        ..Effect::default()
                    },
                };
                db(scope.store.write(|tx| {
                    if !preparing(tx, &scope.connection, &event_id)? {
                        return Ok(());
                    }
                    store::enqueue(tx, &parent, Some(&event.sender), &intent)?;
                    ensure!(store::install_plan(tx, &scope.connection, &intent.id, &[part])?,
                        "Cannot reserve the thread provision effect");
                    tx.execute(
                        "INSERT INTO messaging_prepared_threads(connection,event,parent,thread,delivery)
                         VALUES (?1,?2,?3,?4,?5)",
                        params![scope.connection,event_id,parent.id,id,intent.id],
                    )?;
                    Ok(())
                }))?;
                return Ok(ThreadPreparation::Waiting);
            };
            if previous.selected {
                return if event.room == destination.room && route_id == destination.id {
                    Ok(ThreadPreparation::Ready(event))
                } else {
                    Err(GatewayError::Conflict)
                };
            }
            let target = match previous.stage.as_str() {
                "sent" if previous.receipt.as_ref().is_some_and(|proof| {
                    proof.kind == ReceiptKind::Destination && proof.id == id
                }) => {
                    scope.inherit_public_thread_membership(&parent.room, &child.room, &event.sender.id)?;
                    if !scope.route_candidate(&state.policy, &child, true)?
                        .is_ok_and(|candidate| candidate.id == destination.id)
                    {
                        return Err(GatewayError::Forbidden);
                    }
                    event = child;
                    &destination
                }
                "rejected" if inline_on_refusal => &parent,
                "rejected" | "discarded" | "sent" => {
                    db(scope.store.write(|tx| {
                        tx.execute(
                            "UPDATE messaging_ingress SET stage='refused',disposition=?3,
                             reason='thread_provision_refused',input=NULL
                             WHERE connection=?1 AND event=?2 AND stage='preparing'",
                            params![scope.connection,event_id,serde_json::to_string(&Disposition::Unsupported)?],
                        )?;
                        Ok(())
                    }))?;
                    return Ok(ThreadPreparation::Closed);
                }
                "uncertain" => return Ok(ThreadPreparation::Reconcile(UncertainDelivery {
                    delivery_id: previous.delivery,
                    part: 0,
                    attempt: previous.attempt,
                    room: parent.room,
                })),
                _ => return Ok(ThreadPreparation::Waiting),
            };
            let selected = db(scope.store.write(|tx| {
                if !preparing(tx, &scope.connection, &event_id)? {
                    return Ok(false);
                }
                routes::insert_route(tx, target)?;
                tx.execute(
                    "UPDATE messaging_ingress SET route=?3,input=?4
                     WHERE connection=?1 AND event=?2 AND stage='preparing'",
                    params![scope.connection,event_id,target.id,serde_json::to_string(&event)?],
                )?;
                tx.execute(
                    "UPDATE messaging_prepared_threads SET selected=1 WHERE connection=?1 AND event=?2",
                    params![scope.connection,event_id],
                )?;
                Ok(true)
            }))?;
            Ok(if selected { ThreadPreparation::Ready(event) } else { ThreadPreparation::Closed })
        })).await?;
        self.notify();
        Ok(progress)
    }

    pub(super) fn prepared_thread_selected(&self, event: &str) -> Result<bool, GatewayError> {
        Ok(
            db(self.store.prepared_thread(&self.connection, event))?
                .is_some_and(|row| row.selected),
        )
    }

    pub(super) fn thread_provision_allowed(
        &self,
        state: &State,
        intent: &DeliveryIntent,
        sender: Option<&Sender>,
    ) -> Result<bool, GatewayError> {
        let Some((event, parent, thread, selected)) = db(self.store.read(|db| {
            Ok(db
                .query_row(
                    "SELECT event,parent,thread,selected FROM messaging_prepared_threads
                 WHERE connection=?1 AND delivery=?2",
                    params![self.connection, intent.id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, bool>(3)?,
                        ))
                    },
                )
                .optional()?)
        }))?
        else {
            return Ok(true);
        };
        let Some(sender) = sender else {
            return Ok(false);
        };
        if selected
            || !self.capabilities().effects.prepare_resources
            || !db(self
                .store
                .read(|tx| preparing(tx, &self.connection, &event)))?
            || !self.thread_parent_allowed(state, &intent.room, sender, &parent)?
        {
            return Ok(false);
        }
        let child = Room {
            thread: Some(thread),
            ..intent.room.clone()
        };
        Ok(self
            .public_thread_route(state, &intent.room, &child, sender, true)?
            .is_some())
    }

    fn public_thread_route(
        &self,
        state: &State,
        parent: &Room,
        child: &Room,
        sender: &Sender,
        mentioned: bool,
    ) -> Result<Option<store::Route>, GatewayError> {
        let Some((actor, _)) = self.authorized_identity(&state.policy, parent, sender)? else {
            return Ok(None);
        };
        let Some(policy) = state.policy.room_policy(child) else {
            return Ok(None);
        };
        if (policy.mention_required && !mentioned)
            || state.policy.delivery_policy(child).protection.is_some()
            || !state.policy.grants_sender(child, &actor, false)
        {
            return Ok(None);
        }
        let participant = policy.per_participant.then_some(actor.id.as_str());
        db(store::Route::new(
            &self.connection,
            child.clone(),
            participant,
        ))
        .map(Some)
    }

    pub(super) fn thread_source_allowed(
        &self,
        state: &State,
        room: &Room,
        sender: &Sender,
        event_id: &str,
    ) -> Result<bool, GatewayError> {
        let Some(child) = room.thread.as_deref() else {
            return Ok(true);
        };
        let parent: Option<String> = db(self.store.read(|db| {
            Ok(db
                .query_row(
                    "SELECT parent FROM messaging_prepared_threads
                 WHERE connection=?1 AND event=?2 AND thread=?3 AND selected",
                    params![self.connection, event_id, child],
                    |row| row.get(0),
                )
                .optional()?)
        }))?;
        parent.map_or(Ok(true), |parent| {
            self.thread_parent_allowed(state, room, sender, &parent)
        })
    }

    pub fn thread_media_source_allowed(
        &self,
        source: &MediaSource,
        channel: &str,
    ) -> Result<bool, GatewayError> {
        if source.room.thread.is_none() || source.room.chat != channel {
            return Ok(false);
        }
        self.checked(true, |state| {
            let Some(staged) = db(self.store.preparation(&self.connection, &source.event_id))?
            else {
                return Ok(false);
            };
            Ok(staged.receipt.disposition == Disposition::Deferred
                && staged.expires_ms > now()
                && matches!(staged.purpose, PreparationPurpose::Thread { .. })
                && self.prepared_thread_selected(&source.event_id)?
                && staged.event.is_some_and(|event| {
                    event.room == source.room && event.sender.id == source.sender.id
                })
                && self.thread_source_allowed(
                    state,
                    &source.room,
                    &source.sender,
                    &source.event_id,
                )?)
        })
    }

    pub(super) fn thread_reply_allowed(
        &self,
        state: &State,
        intent: &DeliveryIntent,
        sender: &Sender,
    ) -> Result<bool, GatewayError> {
        if intent.room.thread.is_none() {
            return Ok(true);
        }
        let source: Option<String> = db(self.store.read(|db| {
            Ok(db
                .query_row(
                    "SELECT s.event FROM messaging_effect_sources s
                 JOIN messaging_prepared_threads t ON t.event=s.event AND t.connection=?1
                 WHERE s.delivery=?2 AND t.selected",
                    params![self.connection, intent.id],
                    |row| row.get(0),
                )
                .optional()?)
        }))?;
        source.map_or(Ok(true), |event| {
            self.thread_source_allowed(state, &intent.room, sender, &event)
        })
    }

    fn thread_parent_allowed(
        &self,
        state: &State,
        room: &Room,
        sender: &Sender,
        route: &str,
    ) -> Result<bool, GatewayError> {
        let parent = Room {
            thread: None,
            ..room.clone()
        };
        let Some(policy) = state.policy.room_policy(&parent) else {
            return Ok(false);
        };
        let Some((sender, _)) = self.authorized_identity(&state.policy, &parent, sender)? else {
            return Ok(false);
        };
        let participant = policy.per_participant.then_some(sender.id.as_str());
        Ok(
            db(store::Route::new(&self.connection, parent, participant))?.id == route
                && db(self.store.route(route))?.is_some_and(|route| !route.blocked),
        )
    }
}

struct Provision {
    delivery: String,
    parent: String,
    selected: bool,
    stage: String,
    receipt: Option<EffectReceipt>,
    attempt: u32,
}

impl Store {
    fn prepared_thread(&self, connection: &str, event: &str) -> Result<Option<Provision>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT t.delivery,t.parent,t.selected,p.stage,e.receipt,p.attempt
                 FROM messaging_prepared_threads t
                 JOIN messaging_parts p ON p.delivery=t.delivery AND p.ordinal=0
                 LEFT JOIN messaging_effect_parts e ON e.delivery=p.delivery AND e.ordinal=p.ordinal
                 WHERE t.connection=?1 AND t.event=?2",
                    params![connection, event],
                    |row| {
                        Ok(Provision {
                            delivery: row.get(0)?,
                            parent: row.get(1)?,
                            selected: row.get(2)?,
                            stage: row.get(3)?,
                            receipt: store::optional_decode(row, 4)?,
                            attempt: row.get(5)?,
                        })
                    },
                )
                .optional()?)
        })
    }
}

fn preparing(tx: &rusqlite::Connection, connection: &str, event: &str) -> Result<bool> {
    Ok(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_ingress i
         JOIN messaging_preparation p USING(connection,event)
         WHERE i.connection=?1 AND i.event=?2 AND i.stage='preparing' AND p.expires>?3)",
        params![connection, event, now()],
        |row| row.get(0),
    )?)
}

pub(super) fn retire_unstarted(tx: &Transaction<'_>, connection: &str) -> Result<()> {
    tx.execute(
        "UPDATE messaging_deliveries AS d SET stage='rejected', reason='source_preparation_closed'
         WHERE d.connection=?1 AND d.stage='planned' AND EXISTS (
             SELECT 1 FROM messaging_prepared_threads t
             JOIN messaging_ingress i USING(connection,event)
             JOIN messaging_parts p ON p.delivery=t.delivery AND p.ordinal=0
             WHERE t.delivery=d.id AND i.stage<>'preparing' AND p.stage='pending'
         )",
        [connection],
    )?;
    Ok(())
}
