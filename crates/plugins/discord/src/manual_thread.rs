use super::{
    DiscordAdapter, Ingress,
    channels::{ChannelRoute, User, snowflake},
    client::DiscordClient,
    delivery,
    settings::{
        CREATE_PUBLIC_THREADS, READ_MESSAGE_HISTORY, SEE_CHANNEL, SEND_IN_THREADS, SEND_MESSAGES,
        thread_permissions,
    },
};
use crate::messaging::{
    ActorKind, Advance, Command, DeliveryBody, DeliveryIntent, DeliveryPart, DeliveryResult,
    Disposition, Effect, EffectDependency, EffectOperation, EffectReceipt, GatewayError,
    InboundBody, InboundEvent, Message, ProvisionResource, ReceiptKind, Room, RoomKind,
    SendAttempt, Sender, UncertainDelivery, routes,
    runtime::{State, blocking, db, now, valid_advance, valid_input_id},
    storage as store,
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_manual_threads (
    connection TEXT NOT NULL, event TEXT NOT NULL, delivery TEXT NOT NULL UNIQUE,
    installation TEXT NOT NULL, request TEXT NOT NULL, probe_after_ms INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,event),
    FOREIGN KEY(connection,event) REFERENCES messaging_ingress(connection,event),
    FOREIGN KEY(delivery) REFERENCES messaging_deliveries(id)
);
CREATE INDEX IF NOT EXISTS messaging_discord_manual_threads_recovery
    ON messaging_discord_manual_threads(connection,installation,probe_after_ms);
";

const ARCHIVE_DEFAULT: u32 = 1440;
const COMMAND_TTL_MS: i64 = 15 * 60 * 1000;
const BOT_PERMISSIONS: u64 =
    SEE_CHANNEL | SEND_MESSAGES | READ_MESSAGE_HISTORY | CREATE_PUBLIC_THREADS;
const ACTOR_PERMISSIONS: u64 = SEE_CHANNEL | CREATE_PUBLIC_THREADS;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct ThreadSpec {
    pub name: String,
    pub message: Option<String>,
    pub archive: u32,
}

impl ThreadSpec {
    pub(super) fn new(name: &str, message: Option<&str>, archive: Option<u64>) -> Option<Self> {
        let name = name.trim();
        let archive = u32::try_from(archive.unwrap_or(ARCHIVE_DEFAULT as u64)).ok()?;
        let message = message.map(str::trim).filter(|text| !text.is_empty());
        if name.is_empty()
            || name.encode_utf16().count() > 100
            || name.chars().any(char::is_control)
            || message
                .is_some_and(|text| text.encode_utf16().count() > 2_000 || text.contains('\0'))
            || !matches!(archive, 60 | 1440 | 4320 | 10080)
        {
            return None;
        }
        Some(Self {
            name: name.into(),
            message: message.map(str::to_owned),
            archive,
        })
    }

    pub(super) fn parse(data: &serde_json::Value) -> Option<Self> {
        let mut name = None;
        let mut message = None;
        let mut archive = None;
        for option in data.get("options")?.as_array()? {
            let value = option.get("value")?;
            match option.get("name")?.as_str()? {
                "name" if name.is_none() => name = Some(value.as_str()?),
                "message" if message.is_none() => message = Some(value.as_str()?),
                "auto_archive_duration" if archive.is_none() => archive = Some(value.as_u64()?),
                _ => return None,
            }
        }
        Self::new(name?, message, archive)
    }

    pub(super) fn bot_permissions(&self) -> u64 {
        BOT_PERMISSIONS
            | if self.message.is_some() {
                SEND_IN_THREADS
            } else {
                0
            }
    }

    pub(super) fn actor_permissions(&self) -> u64 {
        ACTOR_PERMISSIONS
            | if self.message.is_some() {
                SEND_IN_THREADS
            } else {
                0
            }
    }
}

pub(super) fn has_permissions(bits: Option<&str>, required: u64) -> bool {
    bits.and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|bits| bits & 8 != 0 || bits & required == required)
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ManualThread {
    pub event: String,
    pub guild: String,
    pub room: Room,
    pub sender: Sender,
    pub spec: ThreadSpec,
    pub created_ms: i64,
}

#[derive(Clone)]
pub(super) struct Reservation {
    pub request: ManualThread,
    pub delivery: String,
    installation: String,
}

pub(super) enum ThreadOutcome {
    Pending,
    Created { id: String, starter_failed: bool },
    Refused,
}

impl ManualThread {
    fn identity(&self, installation: &str) -> Result<String> {
        store::key(
            "discord-manual-thread",
            &(
                installation,
                &self.guild,
                &self.room,
                &self.sender.id,
                &self.spec,
                self.created_ms,
            ),
        )
    }

    fn event(&self) -> InboundEvent {
        InboundEvent {
            id: self.event.clone(),
            room: self.room.clone(),
            sender: self.sender.clone(),
            reply_anchor: None,
            mentioned: true,
            // The admission policy is shared with conversation commands.
            // This transient probe is never put on the native input queue.
            body: InboundBody::Command(Command::Status),
        }
    }

    fn plan(&self, delivery: &str, protection: Option<String>) -> Vec<DeliveryPart> {
        let part = |ordinal, text: &str, operation| DeliveryPart {
            room: self.room.clone(),
            edit_target: None,
            body: DeliveryBody::Reply(Message::text(text)),
            effect: Effect {
                operation_key: (ordinal != 1).then(|| delivery::manual_nonce(delivery, ordinal)),
                operation,
                protection: protection.clone(),
                ..Effect::default()
            },
        };
        let mut plan = vec![
            part(0, "Discussion requested.", EffectOperation::Message),
            part(
                1,
                &self.spec.name,
                EffectOperation::Provision {
                    resource: ProvisionResource::ThreadFromMessage { starter: 0 },
                },
            ),
        ];
        if let Some(message) = &self.spec.message {
            plan.push(part(
                2,
                message,
                EffectOperation::Publish {
                    using: vec![EffectDependency {
                        part: 1,
                        kind: ReceiptKind::Destination,
                    }],
                },
            ));
        }
        plan
    }

    fn current_actor(
        &self,
        ingress: &Ingress,
        state: &State,
    ) -> Result<Option<Sender>, GatewayError> {
        Ok(ingress
            .authorized_identity(&state.policy, &self.room, &self.sender)?
            .and_then(|(actor, kind)| (kind == ActorKind::Person).then_some(actor)))
    }
}

#[async_trait::async_trait]
pub(super) trait ManualThreadIngress {
    async fn commit_manual_thread(
        &self, request: ManualThread, advance: Advance, pin_first_message: bool,
    ) -> Result<Disposition, GatewayError>;
    fn manual_thread_delivery(&self, delivery: &str) -> Result<Option<Reservation>, GatewayError>;
    async fn manual_thread_outcome(&self, request: &ManualThread, expires_ms: i64)
        -> Result<ThreadOutcome, GatewayError>;
}

#[async_trait::async_trait]
impl ManualThreadIngress for Ingress {
    async fn commit_manual_thread(
        &self,
        mut request: ManualThread,
        advance: Advance,
        pin_first_message: bool,
    ) -> Result<Disposition, GatewayError> {
        if !valid_input_id(&request.event)
            || !valid_advance(Some(&advance))
            || request.room.kind != RoomKind::Group
            || request.room.thread.is_some()
            || !snowflake(&request.room.chat)
            || !snowflake(&request.guild)
            || !snowflake(&request.sender.id)
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let accepted = blocking(move || scope.checked(true, |state| {
            let installation = scope.transport_scope()?;
            let actor = request.current_actor(&scope, state)?;
            let parent = scope.route_event(&state.policy, &request.event())?;
            if let Some(actor) = actor.as_ref() {
                request.sender = actor.clone();
            }
            let identity = db(request.identity(&installation))?;
            let delivery = db(store::key("discord-manual-thread-delivery", &(scope.connection_name(), &request.event)))?;
            let advance = scope.scoped_advance(Some(advance))?;
            let protection = scope.delivery_protection(&state.policy, &request.room, &delivery)?;
            let route = parent.ok().filter(|parent| actor.is_some()
                && parent.room.thread.is_none()
                && scope.capabilities().effects.prepare_resources);
            let outcome = db(scope.storage().write(|tx| {
                if !store::check_advance(tx, scope.connection_name(), advance.as_ref())? {
                    return Ok(Err(GatewayError::CheckpointChanged));
                }
                let disposition = if let Some(previous) = store::ingress_replay(
                    tx, scope.connection_name(), &request.event, &identity,
                )? {
                    previous.disposition
                } else {
                    let available = match route.as_ref() {
                        Some(route) => store::read_route(tx, &route.id)?.is_none_or(|row| !row.blocked),
                        None => false,
                    };
                    let accepted = route.filter(|_| available);
                    let disposition = if accepted.is_some() { Disposition::Accepted } else { Disposition::Forbidden };
                    if let Some(route) = accepted.as_ref() {
                        routes::insert_route(tx, route)?;
                    }
                    tx.execute(
                        "INSERT INTO messaging_ingress
                         (connection,event,fingerprint,route,disposition,control,policy_revision,stage,reason)
                         VALUES (?1,?2,?3,?4,?5,1,?6,?7,'discord_manual_thread')",
                        params![scope.connection_name(), request.event, identity,
                            accepted.as_ref().map(|route| &route.id), serde_json::to_string(&disposition)?,
                            state.revision(), if accepted.is_some() { "done" } else { "refused" }],
                    )?;
                    if let Some(route) = accepted {
                        let plan = request.plan(&delivery, protection);
                        let intent = DeliveryIntent {
                            id: delivery.clone(),
                            room: request.room.clone(),
                            reply_anchor: None,
                            edit_target: None,
                            body: plan[0].body.clone(),
                        };
                        store::enqueue(tx, &route, Some(&request.sender), &intent)?;
                        ensure!(store::install_plan(tx, scope.connection_name(), &delivery, &plan)?,
                            "Cannot reserve Discord thread creation");
                        tx.execute(
                            "INSERT INTO messaging_discord_manual_threads(connection,event,delivery,installation,request)
                             VALUES (?1,?2,?3,?4,?5)",
                            params![scope.connection_name(), request.event, delivery, installation, serde_json::to_string(&request)?],
                        )?;
                        if pin_first_message && request.spec.message.is_some() {
                            delivery::thread_updates::reserve_starter_pin(
                                tx, scope.connection_name(), &delivery, &installation, &request.guild,
                            )?;
                        }
                    }
                    disposition
                };
                store::write_advance(tx, scope.connection_name(), advance.as_ref())?;
                Ok(Ok(disposition))
            }))?;
            outcome
        })).await?;
        self.notify();
        Ok(accepted)
    }

    fn manual_thread_delivery(
        &self,
        delivery: &str,
    ) -> Result<Option<Reservation>, GatewayError> {
        db(self.storage().read(|db| {
            Ok(db
                .query_row(
                    "SELECT request,delivery,installation FROM messaging_discord_manual_threads
                 WHERE connection=?1 AND delivery=?2",
                    params![self.connection_name(), delivery],
                    read_reservation,
                )
                .optional()?)
        }))
    }

    async fn manual_thread_outcome(
        &self,
        request: &ManualThread,
        expires_ms: i64,
    ) -> Result<ThreadOutcome, GatewayError> {
        let scope = self.clone();
        let request = request.clone();
        blocking(move || scope.checked(true, |state| {
            if expires_ms <= now() {
                return Err(GatewayError::Forbidden);
            }
            let actor = request.current_actor(&scope, state)?.ok_or(GatewayError::Forbidden)?;
            let installation = scope.transport_scope()?;
            let mut exact = request.clone();
            exact.sender = actor;
            let fingerprint = db(exact.identity(&installation))?;
            db(scope.storage().read(|db| {
                let row: Option<(String, String)> = db.query_row(
                    "SELECT t.delivery,d.stage FROM messaging_discord_manual_threads t
                     JOIN messaging_ingress i USING(connection,event)
                     JOIN messaging_deliveries d ON d.id=t.delivery
                     WHERE t.connection=?1 AND t.event=?2 AND i.fingerprint=?3 AND t.installation=?4",
                    params![scope.connection_name(), exact.event, fingerprint, installation],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).optional()?;
                let Some((delivery, stage)) = row else {
                    return Ok(ThreadOutcome::Refused);
                };
                let starter = receipt(db, &delivery, 0, ReceiptKind::Message)?;
                let thread = receipt(db, &delivery, 1, ReceiptKind::Destination)?;
                let confirmed = thread.filter(|thread| Some(thread) == starter.as_ref());
                if let Some(id) = confirmed {
                    return Ok(match stage.as_str() {
                        "sent" => ThreadOutcome::Created { id, starter_failed: false },
                        "rejected" | "discarded" => ThreadOutcome::Created { id, starter_failed: true },
                        _ => ThreadOutcome::Pending,
                    });
                }
                Ok(if matches!(stage.as_str(), "rejected" | "discarded" | "sent") {
                    ThreadOutcome::Refused
                } else {
                    ThreadOutcome::Pending
                })
            }))
        })).await
    }
}

fn read_reservation(row: &rusqlite::Row<'_>) -> rusqlite::Result<Reservation> {
    Ok(Reservation {
        request: store::decode(row, 0)?,
        delivery: row.get(1)?,
        installation: row.get(2)?,
    })
}

pub(super) fn receipt(
    db: &rusqlite::Connection,
    delivery: &str,
    ordinal: u32,
    kind: ReceiptKind,
) -> Result<Option<String>> {
    let value = db
        .query_row(
            "SELECT e.receipt FROM messaging_parts p
         JOIN messaging_effect_parts e USING(delivery,ordinal)
         WHERE p.delivery=?1 AND p.ordinal=?2 AND p.stage='sent'",
            params![delivery, ordinal],
            |row| store::optional_decode::<EffectReceipt>(row, 0),
        )
        .optional()?
        .flatten();
    Ok(value
        .filter(|proof| proof.kind == kind && snowflake(&proof.id))
        .map(|proof| proof.id))
}

impl DiscordAdapter {
    pub(super) async fn recheck_manual_thread(
        &self,
        ingress: &Ingress,
        attempt: &SendAttempt,
        destination: &Room,
        reservation: &Reservation,
    ) -> Result<(), DeliveryResult> {
        let request = &reservation.request;
        if attempt.payload.room != request.room
            || ingress.transport_scope().ok().as_ref() != Some(&reservation.installation)
            || now() >= request.created_ms.saturating_add(COMMAND_TTL_MS)
        {
            return Err(DeliveryResult::Rejected);
        }
        let retry = DeliveryResult::Retryable {
            after: Duration::from_millis(500),
            scope: crate::messaging::RetryScope::Part,
        };
        let Some(roles) = thread_permissions(
            &self.client,
            &request.guild,
            &request.room.chat,
            &self.bot.bot_id,
            &request.sender.id,
            request.spec.bot_permissions(),
            request.spec.actor_permissions(),
        )
        .await
        .map_err(|_| retry.clone())?
        else {
            return Err(DeliveryResult::Rejected);
        };
        if !self.settings.allows_surface(
            Some(&request.guild),
            &destination.chat,
            destination.thread.as_deref(),
        ) || !self
            .settings
            .allows_command(&request.sender.id, Some(&request.guild), &roles)
        {
            return Err(DeliveryResult::Rejected);
        }
        let route = ChannelRoute {
            room: request.room.clone(),
            guild: Some(request.guild.clone()),
        };
        let user = User {
            id: request.sender.id.clone(),
            bot: false,
            system: false,
        };
        self.record_sender(ingress, &route, &user, &roles)
            .await
            .map_err(|_| retry)?;
        if let Some(thread) = &destination.thread {
            let actual: serde_json::Value =
                self.client
                    .get(&["channels", thread])
                    .await
                    .map_err(|error| match error {
                        super::client::RestFailure::Missing
                        | super::client::RestFailure::Forbidden => DeliveryResult::Rejected,
                        _ => DeliveryResult::Retryable {
                            after: Duration::from_millis(500),
                            scope: crate::messaging::RetryScope::Part,
                        },
                    })?;
            if !verified_thread(&actual, request, thread, &self.bot.bot_id) {
                return Err(DeliveryResult::Rejected);
            }
            ingress
                .checked(true, |_| {
                    ingress.inherit_public_thread_membership(
                        &request.room,
                        destination,
                        &request.sender.id,
                    )
                })
                .map_err(|_| DeliveryResult::Rejected)?;
        }
        Ok(())
    }

    pub(super) fn maintain_threads(&self, ingress: &Ingress, cancel: &CancellationToken) {
        let Ok(worker) = self.manual_recovery.clone().try_acquire_owned() else {
            return;
        };
        let client = self.client.clone();
        let bot = self.bot.bot_id.clone();
        let ingress = ingress.clone();
        let cancel = cancel.clone();
        let delivery = self.settings.delivery.clone();
        tokio::spawn(async move {
            let _worker = worker;
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = async {
                    recover_threads(client.clone(), bot.clone(), ingress.clone()).await;
                    delivery::thread_updates::maintain(&client, &bot, &ingress, &delivery).await;
                } => {},
            }
        });
    }
}

async fn recover_threads(client: DiscordClient, bot: String, ingress: Ingress) {
    let scope = ingress.clone();
    let rows = blocking(move || scope.checked(true, |_| {
        let installation = scope.transport_scope()?;
        let before = now();
        db(scope.storage().write(|tx| {
            let mut query = tx.prepare(
                "SELECT t.request,t.delivery,t.installation,p.attempt FROM messaging_discord_manual_threads t
                 JOIN messaging_parts p ON p.delivery=t.delivery AND p.ordinal=1 AND p.stage='uncertain'
                 WHERE t.connection=?1 AND t.installation=?2 AND t.probe_after_ms<=?3
                 ORDER BY t.probe_after_ms,t.event LIMIT 4",
            )?;
            let rows = query.query_map(params![scope.connection_name(), installation, before], |row| {
                Ok((read_reservation(row)?, row.get::<_,u32>(3)?))
            })?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut candidates = Vec::new();
            for (reservation, attempt) in rows {
                tx.execute(
                    "UPDATE messaging_discord_manual_threads SET probe_after_ms=?1
                     WHERE connection=?2 AND delivery=?3",
                    params![before.saturating_add(4000), scope.connection_name(), reservation.delivery],
                )?;
                if let Some(id) = receipt(tx, &reservation.delivery, 0, ReceiptKind::Message)? {
                    candidates.push((reservation, attempt, id));
                }
            }
            Ok(candidates)
        }))
    })).await;
    let Ok(rows) = rows else { return };
    for (reservation, attempt, id) in rows {
        let request = &reservation.request;
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            client.get::<serde_json::Value>(&["channels", &id]),
        )
        .await;
        if matches!(response, Ok(Ok(value)) if verified_thread(&value, request, &id, &bot)) {
            let _ = ingress
                .reconcile_delivery(
                    UncertainDelivery {
                        delivery_id: reservation.delivery,
                        part: 1,
                        attempt,
                        room: request.room.clone(),
                    },
                    DeliveryResult::EffectConfirmed(EffectReceipt {
                        kind: ReceiptKind::Destination,
                        id,
                    }),
                )
                .await;
        }
    }
}

pub(super) fn verified_thread(
    value: &serde_json::Value,
    request: &ManualThread,
    id: &str,
    bot: &str,
) -> bool {
    value["id"].as_str() == Some(id)
        && value["parent_id"].as_str() == Some(&request.room.chat)
        && value["guild_id"].as_str() == Some(&request.guild)
        && value["owner_id"].as_str() == Some(bot)
        && matches!(value["type"].as_u64(), Some(10 | 11))
}
