//! Optional updates derived from confirmed deliveries in threads created by this bot.
use super::super::{
    DiscordAdapter, DiscordClient,
    channels::snowflake,
    client::RestFailure,
    manual_thread::receipt,
    settings::{
        DiscordDeliverySettings, PIN_MESSAGES, READ_MESSAGE_HISTORY, SEE_CHANNEL, map_roles,
        thread_permissions,
    },
};
use super::{AUTO_THREAD_NAME, Call, buckets, execute_http, own_message};
use crate::messaging::{
    ActorKind, DeliveryBody, DeliveryIntent, DeliveryResult, EffectReceipt, GatewayError, Ingress,
    ReceiptKind, Room, RoomKind, Sender,
    runtime::{db, now},
    storage as store,
};
use anyhow::Result;
use reqwest::Method;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use unicode_segmentation::UnicodeSegmentation;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_thread_updates (
    connection TEXT NOT NULL, installation TEXT NOT NULL,
    delivery TEXT NOT NULL REFERENCES messaging_deliveries(id), action TEXT NOT NULL,
    request TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
    next_ms INTEGER NOT NULL DEFAULT 0, expires_ms INTEGER NOT NULL,
    PRIMARY KEY(delivery,action)
);
CREATE INDEX IF NOT EXISTS messaging_discord_thread_updates_ready
    ON messaging_discord_thread_updates(connection,installation,next_ms)
    WHERE state IN ('pending','issued');
";

const BATCH: u32 = 4;
const EXPIRES_MS: i64 = 15 * 60 * 1000;
const READ_RETRY_MS: i64 = 5_000;
const RECHECK_ISSUED_MS: i64 = 30_000;

#[derive(Clone, Serialize, Deserialize)]
enum Action {
    Rename(String),
    PinReply,
    PinStarter,
}

impl Action {
    fn key(&self) -> &'static str {
        match self {
            Self::Rename(_) => "rename",
            Self::PinReply | Self::PinStarter => "pin",
        }
    }

    fn enabled(&self, delivery: &DiscordDeliverySettings) -> bool {
        match self {
            Self::Rename(_) => delivery.rename_auto_threads,
            Self::PinReply | Self::PinStarter => delivery.pin_first_thread_message,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Request {
    guild: String,
    action: Action,
}

fn reserve(
    tx: &Connection,
    connection: &str,
    installation: &str,
    delivery: &str,
    request: &Request,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_discord_thread_updates
         (connection,installation,delivery,action,request,expires_ms)
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![
            connection,
            installation,
            delivery,
            request.action.key(),
            serde_json::to_string(request)?,
            now().saturating_add(EXPIRES_MS)
        ],
    )?;
    Ok(())
}

pub(crate) fn reserve_starter_pin(
    tx: &Connection,
    connection: &str,
    delivery: &str,
    installation: &str,
    guild: &str,
) -> Result<()> {
    reserve(
        tx,
        connection,
        installation,
        delivery,
        &Request {
            guild: guild.to_owned(),
            action: Action::PinStarter,
        },
    )
}

impl DiscordAdapter {
    pub(crate) fn reserve_auto_thread_updates(
        &self,
        ingress: &Ingress,
        intent: &DeliveryIntent,
    ) -> Result<(), GatewayError> {
        if !self.settings.delivery.rename_auto_threads
            && !self.settings.delivery.pin_first_thread_message
        {
            return Ok(());
        }
        let (Some(thread), DeliveryBody::Reply(reply)) = (&intent.room.thread, &intent.body) else {
            return Ok(());
        };
        let Some(guild) = self
            .settings
            .guilds
            .iter()
            .find(|(_, guild)| guild.channels.contains_key(&intent.room.chat))
            .map(|(id, _)| id)
        else {
            return Ok(());
        };
        ingress.checked(true, |_| {
            let installation = ingress.transport_scope()?;
            db(ingress.storage().write(|tx| {
                let original: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_effect_sources s
                     JOIN messaging_deliveries d ON d.id=s.delivery
                     JOIN messaging_ingress i ON i.connection=d.connection AND i.event=s.event
                     JOIN messaging_prepared_threads t ON t.connection=d.connection AND t.event=s.event
                     WHERE s.delivery=?1 AND d.connection=?2 AND t.thread=?3 AND t.selected
                       AND d.route=i.route AND d.sender IS NOT NULL)",
                    params![intent.id, ingress.connection_name(), thread],
                    |row| row.get(0),
                )?;
                if original {
                    if self.settings.delivery.rename_auto_threads
                        && let Some(title) = reply_title(&reply.text)
                    {
                        reserve(tx, ingress.connection_name(), &installation, &intent.id,
                            &Request { guild: guild.clone(), action: Action::Rename(title) })?;
                    }
                    if self.settings.delivery.pin_first_thread_message {
                        reserve(tx, ingress.connection_name(), &installation, &intent.id,
                            &Request { guild: guild.clone(), action: Action::PinReply })?;
                    }
                }
                Ok(())
            }))
        })
    }
}

fn reply_title(text: &str) -> Option<String> {
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let first = lines.next()?;
    if first.starts_with("```") || first.starts_with("~~~") {
        return None;
    }
    let line = first.trim_start_matches(['#', '>', '*', '-', ' ']);
    let title = line
        .split_whitespace()
        .filter(|word| {
            !word.starts_with("<@")
                && !word.starts_with("<#")
                && !matches!(*word, "@everyone" | "@here")
        })
        .collect::<Vec<_>>()
        .join(" ");
    let mut units = 0;
    let title: String = title
        .graphemes(true)
        .take_while(|grapheme| {
            units += grapheme.encode_utf16().count();
            units <= 80
        })
        .collect();
    let title = title.trim_end_matches([' ', '#', '*']);
    (!title.is_empty() && title != AUTO_THREAD_NAME).then(|| title.to_owned())
}

struct Job {
    intent: DeliveryIntent,
    sender: Sender,
    route: String,
    install: String,
    request: Request,
    issued: bool,
}

struct Target {
    room: Room,
    message: Option<String>,
}

impl Job {
    fn target(&self, conn: &Connection) -> Result<Option<Target>> {
        let mut room = self.intent.room.clone();
        let message = match &self.request.action {
            Action::Rename(_) => None,
            Action::PinReply => {
                let mut read = conn.prepare(
                    "SELECT e.receipt FROM messaging_parts p
                     JOIN messaging_effect_parts e USING(delivery,ordinal)
                     WHERE p.delivery=?1 AND p.stage='sent' AND e.receipt IS NOT NULL
                       AND json_type(p.body,'$.body.reply')='object'
                     ORDER BY p.ordinal",
                )?;
                let rows = read.query_map([&self.intent.id], |row| {
                    store::decode::<EffectReceipt>(row, 0)
                })?;
                let mut confirmed = None;
                for proof in rows {
                    let proof = proof?;
                    if proof.kind == ReceiptKind::Message && snowflake(&proof.id) {
                        confirmed = Some(proof.id);
                        break;
                    }
                }
                confirmed
            }
            Action::PinStarter => {
                let child = receipt(conn, &self.intent.id, 1, ReceiptKind::Destination)?;
                let marker = receipt(conn, &self.intent.id, 0, ReceiptKind::Message)?;
                if child.is_none() || child != marker {
                    return Ok(None);
                }
                room.thread = child;
                receipt(conn, &self.intent.id, 2, ReceiptKind::Message)?
            }
        };
        Ok((room.kind == RoomKind::Group
            && snowflake(&room.chat)
            && room.thread.as_deref().is_some_and(snowflake)
            && (matches!(self.request.action, Action::Rename(_)) || message.is_some()))
        .then_some(Target { room, message }))
    }

    fn move_to(&self, ingress: &Ingress, state: &str, delay: i64) -> Result<bool, GatewayError> {
        ingress.checked(true, |_| {
            if self.install != ingress.transport_scope()? {
                return Ok(false);
            }
            db(ingress.storage().write(|tx| {
                Ok(tx.execute(
                    "UPDATE messaging_discord_thread_updates SET state=?5,next_ms=?6
                     WHERE connection=?1 AND installation=?2 AND delivery=?3 AND action=?4
                       AND state=?7 AND expires_ms>?8",
                    params![
                        ingress.connection_name(),
                        self.install,
                        self.intent.id,
                        self.request.action.key(),
                        state,
                        now().saturating_add(delay),
                        self.state(),
                        now()
                    ],
                )? == 1)
            }))
        })
    }

    fn state(&self) -> &'static str {
        if self.issued { "issued" } else { "pending" }
    }

    fn authorized(
        &self,
        ingress: &Ingress,
        room: &Room,
        roles: Option<&BTreeSet<String>>,
    ) -> Result<bool, GatewayError> {
        let parent = Room {
            thread: None,
            ..room.clone()
        };
        let manual = matches!(self.request.action, Action::PinStarter);
        ingress.checked(true, |state| {
            if self.install != ingress.transport_scope()?
                || db(ingress.storage().route(&self.route))?.is_none_or(|route| {
                    route.connection != ingress.connection_name()
                        || route.blocked
                        || route.room != self.intent.room
                })
            {
                return Ok(false);
            }
            let Some((_, ActorKind::Person)) =
                ingress.authorized_identity(&state.policy, &parent, &self.sender)?
            else {
                return Ok(false);
            };
            if (!manual && !ingress.authorize_sender(&state.policy, room, &self.sender)?)
                || (manual
                    && !ingress.authorize_scoped_command(&state.policy, &parent, &self.sender)?)
            {
                return Ok(false);
            }
            Ok(roles.is_none_or(|roles| {
                let fresh = Sender {
                    id: self.sender.id.clone(),
                    roles: map_roles(Some(&self.request.guild), roles),
                };
                state.policy.grants_sender(&parent, &fresh, false)
                    && state.policy.grants_sender(room, &fresh, false)
                    && (!manual || state.policy.allows_scoped_command(&parent, &fresh))
            }))
        })
    }
}

pub(crate) async fn maintain(
    client: &DiscordClient,
    bot: &str,
    ingress: &Ingress,
    delivery: &DiscordDeliverySettings,
) {
    if !delivery.rename_auto_threads && !delivery.pin_first_thread_message {
        return;
    }
    let jobs = ingress.checked(true, |_| {
        let install = ingress.transport_scope()?;
        db(ingress.storage().read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT d.intent,d.sender,d.route,j.request,j.state
                 FROM messaging_discord_thread_updates j
                 JOIN messaging_deliveries d ON d.id=j.delivery AND d.connection=j.connection
                 WHERE j.connection=?1 AND j.installation=?2 AND j.state IN ('pending','issued')
                   AND j.next_ms<=?3 AND j.expires_ms>?3 AND d.stage='sent'
                 ORDER BY j.next_ms,d.position,j.action LIMIT ?4",
            )?;
            let rows =
                stmt.query_map(params![ingress.connection_name(), install, now(), BATCH], |row| {
                    Ok(Job {
                        intent: store::decode(row, 0)?,
                        sender: store::decode(row, 1)?,
                        route: row.get(2)?,
                        install: install.clone(),
                        request: store::decode(row, 3)?,
                        issued: row.get::<_, String>(4)? == "issued",
                    })
                })?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        }))
    });
    let Ok(jobs) = jobs else { return };
    for job in jobs {
        if apply(client, bot, ingress, delivery, job).await.is_err() {
            break;
        }
    }
}

enum Observation {
    Ready,
    AlreadyApplied,
    Changed,
}

async fn observe(
    client: &DiscordClient,
    bot: &str,
    job: &Job,
    target: &Target,
    thread: &str,
) -> Result<Observation, RestFailure> {
    let actual: Value = client.get(&["channels", thread]).await?;
    if actual["id"].as_str() != Some(thread)
        || actual["parent_id"].as_str() != Some(&target.room.chat)
        || actual["guild_id"].as_str() != Some(&job.request.guild)
        || actual["owner_id"].as_str() != Some(bot)
        || !matches!(actual["type"].as_u64(), Some(10 | 11))
    {
        return Ok(Observation::Changed);
    }
    let observation = match &job.request.action {
        Action::Rename(title) => match actual["name"].as_str() {
            Some(name) if name == title => Observation::AlreadyApplied,
            Some(AUTO_THREAD_NAME) => Observation::Ready,
            _ => Observation::Changed,
        },
        Action::PinReply | Action::PinStarter => {
            let Some(id) = target.message.as_deref() else {
                return Ok(Observation::Changed);
            };
            let message: Value = client.get(&["channels", thread, "messages", id]).await?;
            if own_message(&message, bot, thread) != Some(id) {
                return Ok(Observation::Changed);
            }
            match message["pinned"].as_bool() {
                Some(true) => Observation::AlreadyApplied,
                Some(false) => Observation::Ready,
                None => Observation::Changed,
            }
        }
    };
    if matches!(observation, Observation::Ready)
        && (actual["thread_metadata"]["archived"] == true
            || actual["thread_metadata"]["locked"] == true)
    {
        return Ok(Observation::Changed);
    }
    Ok(observation)
}

async fn apply(
    client: &DiscordClient,
    bot: &str,
    ingress: &Ingress,
    delivery: &DiscordDeliverySettings,
    mut job: Job,
) -> Result<(), GatewayError> {
    let target = db(ingress.storage().read(|conn| job.target(conn)))?;
    let Some(target) = target.filter(|_| job.request.action.enabled(delivery)) else {
        job.move_to(ingress, "skipped", 0)?;
        return Ok(());
    };
    if !job.authorized(ingress, &target.room, None)? {
        job.move_to(ingress, "skipped", 0)?;
        return Ok(());
    }
    if let Some((delay, _)) = buckets::wait(ingress, None).await? {
        job.move_to(ingress, job.state(), delay_ms(delay))?;
        return Ok(());
    }
    let roles = if job.issued {
        None
    } else {
        let required = SEE_CHANNEL | READ_MESSAGE_HISTORY;
        let pin = if target.message.is_some() {
            PIN_MESSAGES
        } else {
            0
        };
        match thread_permissions(
            client,
            &job.request.guild,
            &target.room.chat,
            bot,
            &job.sender.id,
            required | pin,
            required,
        )
        .await
        {
            Ok(Some(roles)) => Some(roles),
            Ok(None) => {
                job.move_to(ingress, "skipped", 0)?;
                return Ok(());
            }
            Err(failure) => return defer_read(ingress, &job, failure),
        }
    };
    let Some(thread) = target.room.thread.as_deref() else {
        return Ok(());
    };
    match observe(client, bot, &job, &target, thread).await {
        Ok(Observation::AlreadyApplied) => {
            job.move_to(ingress, "applied", 0)?;
            return Ok(());
        }
        Ok(Observation::Changed) => {
            job.move_to(ingress, "skipped", 0)?;
            return Ok(());
        }
        Ok(Observation::Ready) => {}
        Err(failure) => return defer_read(ingress, &job, failure),
    }
    let Some(roles) = roles.as_ref() else {
        // After a lost response, another attempt could override a human edit.
        job.move_to(ingress, "issued", RECHECK_ISSUED_MS)?;
        return Ok(());
    };
    if !job.authorized(ingress, &target.room, Some(roles))? {
        job.move_to(ingress, "skipped", 0)?;
        return Ok(());
    }
    let request = match (&job.request.action, target.message.as_deref()) {
        (Action::Rename(name), _) => client
            .bot(Method::PATCH, &["channels", thread])
            .json(&json!({"name":name})),
        (_, Some(id)) => client.bot(Method::PUT, &["channels", thread, "messages", "pins", id]),
        _ => return Ok(()),
    };
    let (client, request) = request.build_split();
    let Ok(request) = request else {
        job.move_to(ingress, "skipped", 0)?;
        return Ok(());
    };
    let route = buckets::Route::from_request(&request);
    if let Some((delay, _)) = buckets::wait(ingress, route.as_ref()).await? {
        job.move_to(ingress, "pending", delay_ms(delay))?;
        return Ok(());
    }
    if !job.move_to(ingress, "issued", READ_RETRY_MS)? {
        return Ok(());
    }
    job.issued = true;
    if !job.authorized(ingress, &target.room, Some(roles))? {
        job.move_to(ingress, "skipped", 0)?;
        return Ok(());
    }
    match execute_http(ingress, route.as_ref(), client, request, Call::Mutation, 1).await {
        Ok(reply) if reply.status.is_success() => {
            // Confirm the exact state with a scoped read, including after reopen.
            job.move_to(ingress, "issued", 0)?;
        }
        Err(DeliveryResult::Retryable { after, .. }) => {
            job.move_to(ingress, "pending", delay_ms(after))?;
        }
        Err(DeliveryResult::Rejected) => {
            job.move_to(ingress, "skipped", 0)?;
        }
        Ok(reply) if matches!(reply.failure(Call::Mutation), DeliveryResult::Rejected) => {
            job.move_to(ingress, "skipped", 0)?;
        }
        _ => {}
    }
    Ok(())
}

fn delay_ms(delay: Duration) -> i64 {
    delay.as_nanos().div_ceil(1_000_000).min(i64::MAX as u128) as i64
}

fn defer_read(ingress: &Ingress, job: &Job, failure: RestFailure) -> Result<(), GatewayError> {
    match failure {
        RestFailure::RateLimited => {
            job.move_to(ingress, job.state(), 60_000)?;
        }
        RestFailure::Unavailable => {
            job.move_to(ingress, job.state(), READ_RETRY_MS)?;
        }
        _ => {
            job.move_to(ingress, "skipped", 0)?;
        }
    }
    Ok(())
}
