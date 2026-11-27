use super::{
    Connection, IntakeMode, Telegram, address::Destination, connection_failure, delivery, lock,
    media, updates,
};
use hermes_gateway_core::messaging::{
    ActorKind, Advance, Command, ConnectionFailure, Control, Disposition, GatewayError,
    InboundBody, InboundEvent, Ingress, InputAlias, MemberEvidence, Message, Response, Room,
    RoomKind, Sender,
    runtime::{db, now},
    storage::{self, Candidate},
};
use hermes_telegram::{ErrorKind, Method, Store as PreviousStore};
use ring::rand::{SecureRandom, SystemRandom};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt::Write, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_telegram_webhook (
    connection TEXT PRIMARY KEY, confirmed TEXT, pending TEXT
);
";

const STREAM: &str = "telegram-updates";
const RESET_AFTER: i64 = 7 * 24 * 60 * 60 * 1_000;
const EVIDENCE_MS: i64 = 30 * 60 * 1_000;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Cursor {
    epoch: u64,
    next_poll: i64,
    last_seen: i64,
}

impl Cursor {
    fn idle(&self, observed: i64) -> bool {
        self.last_seen > 0 && observed.saturating_sub(self.last_seen) >= RESET_AFTER
    }

    fn poll_offset(&self) -> i64 {
        if self.idle(now()) { 0 } else { self.next_poll }
    }

    fn record(&mut self, id: i64, polling: bool, observed: i64) {
        if self.idle(observed) {
            self.epoch = self.epoch.saturating_add(1);
            self.next_poll = 0;
        }
        if polling {
            self.next_poll = self.next_poll.max(id + 1);
        }
        self.last_seen = self.last_seen.max(observed);
    }
}

async fn cursor(ingress: &Ingress) -> Result<(Option<String>, Cursor), GatewayError> {
    let raw = ingress.checkpoint(STREAM.into()).await?;
    let parsed = raw
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| GatewayError::Invalid)?
        .unwrap_or_default();
    Ok((raw, parsed))
}

impl Telegram {
    pub(super) async fn run(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> Result<ConnectionFailure, ConnectionFailure> {
        // Only the active adapter holds the lease. Credential verification can
        // run beside the existing generation during owner-controlled rotation.
        let root = self.0.root.clone();
        let bot = self.0.bot.identity.id;
        let predecessor = tokio::task::spawn_blocking(move || {
            let store = PreviousStore::new(&root);
            let lease = store.claim(bot).map_err(|_| "account_in_use")?;
            if store
                .has_native_state(bot)
                .map_err(|_| "predecessor_unreadable")?
            {
                return Err("migration_required");
            }
            Ok(lease)
        })
        .await
        .map_err(|_| ConnectionFailure::Retryable)?;
        let predecessor_lease = predecessor.map_err(|issue| {
            lock(&self.0.verification)["issue"] = json!(issue);
            if issue == "account_in_use" {
                ConnectionFailure::Retryable
            } else {
                ConnectionFailure::Configuration
            }
        })?;
        self.0
            .predecessor_lease
            .set(predecessor_lease)
            .map_err(|_| ConnectionFailure::Configuration)?;
        lock(&self.0.verification)["issue"] = Value::Null;

        let webhook = self
            .0
            .client
            .request(Method::GetWebhookInfo, json!({}), &cancel)
            .await
            .map_err(connection_failure)?;
        let current = webhook["url"]
            .as_str()
            .ok_or(ConnectionFailure::Configuration)?;
        if !current.is_empty() {
            if !owns_webhook(&ingress, current).map_err(gateway_failure)? {
                lock(&self.0.verification)["issue"] = json!("external_webhook");
                return Err(ConnectionFailure::Configuration);
            }
            // A previous registration can succeed while its response is lost.
            // Keep that observed URL as the fallback before planning another.
            confirm_webhook(&ingress, current).map_err(gateway_failure)?;
        }

        match &self.0.settings.intake {
            IntakeMode::Polling => {
                if !current.is_empty() {
                    if !self
                        .0
                        .client
                        .request(
                            Method::DeleteWebhook,
                            json!({"drop_pending_updates":false}),
                            &cancel,
                        )
                        .await
                        .map_err(connection_failure)?
                        .as_bool()
                        .unwrap_or(false)
                    {
                        return Err(ConnectionFailure::Configuration);
                    }
                    let status = self
                        .0
                        .client
                        .request(Method::GetWebhookInfo, json!({}), &cancel)
                        .await
                        .map_err(connection_failure)?;
                    if status["url"].as_str() != Some("") {
                        return Err(ConnectionFailure::Configuration);
                    }
                }
                self.verify_home(&ingress, &cancel).await;
                ingress
                    .report_transport_ready(true)
                    .map_err(gateway_failure)?;
                self.sync_menu(&cancel).await;
                tokio::select! {
                    outcome = self.poll(&ingress, &cancel) => Ok(outcome),
                    _ = media::resume(&self.0, &ingress, &cancel) => Ok(ConnectionFailure::Stopped),
                }
            }
            IntakeMode::Webhook { .. } => {
                let origin = self
                    .0
                    .settings
                    .webhook_origin()
                    .map_err(|_| ConnectionFailure::Configuration)?
                    .ok_or(ConnectionFailure::Configuration)?;
                let secret = webhook_secret().ok_or(ConnectionFailure::Retryable)?;
                let handler = self.clone();
                let webhook_cancel = cancel.clone();
                let path = ingress
                    .register_webhook(
                        "x-telegram-bot-api-secret-token"
                            .parse()
                            .expect("valid header name"),
                        secret.clone(),
                        Arc::new(move |ingress, body| {
                            let handler = handler.clone();
                            let cancel = webhook_cancel.clone();
                            Box::pin(async move {
                                let update = serde_json::from_slice(&body)
                                    .map_err(|_| GatewayError::Invalid)?;
                                handler.process(&ingress, &update, false, &cancel).await
                            })
                        }),
                    )
                    .map_err(gateway_failure)?;
                let url = origin
                    .join(&path)
                    .map_err(|_| ConnectionFailure::Configuration)?;
                // Persist the URL we are about to manage before the call can
                // change remote state or lose its acknowledgement.
                plan_webhook(&ingress, url.as_str()).map_err(gateway_failure)?;
                let registered = self
                    .0
                    .client
                    .request(
                        Method::SetWebhook,
                        json!({
                            "url":url.as_str(), "secret_token":secret, "drop_pending_updates":false,
                            "allowed_updates":updates::UPDATE_TYPES,
                        }),
                        &cancel,
                    )
                    .await
                    .map_err(connection_failure)?;
                let observed = self
                    .0
                    .client
                    .request(Method::GetWebhookInfo, json!({}), &cancel)
                    .await
                    .map_err(connection_failure)?;
                if registered.as_bool() != Some(true)
                    || observed["url"].as_str() != Some(url.as_str())
                {
                    return Err(ConnectionFailure::Configuration);
                }
                confirm_webhook(&ingress, url.as_str()).map_err(gateway_failure)?;
                {
                    let mut verification = lock(&self.0.verification);
                    verification["intake_health"] = json!("webhook");
                    verification["pending_updates_at_registration"] =
                        json!(observed["pending_update_count"].as_u64());
                }
                self.verify_home(&ingress, &cancel).await;
                ingress
                    .report_transport_ready(true)
                    .map_err(gateway_failure)?;
                self.sync_menu(&cancel).await;
                media::resume(&self.0, &ingress, &cancel).await;
                Ok(ConnectionFailure::Stopped)
            }
        }
    }

    async fn verify_home(&self, ingress: &Ingress, cancel: &CancellationToken) {
        let Some(room) = &self.0.settings.home else {
            return;
        };
        let Some(destination) =
            Destination::from_room(room).filter(|destination| destination.topic.is_none())
        else {
            return;
        };
        let chat = self
            .0
            .client
            .request(Method::GetChat, json!({"chat_id":destination.chat}), cancel)
            .await;
        let route = if let Ok(chat) = chat
            && chat["id"].as_i64() == Some(destination.chat)
            && self.can_send_home(room, &chat, cancel).await
        {
            ingress
                .verify_destination(room.clone(), now().saturating_add(EVIDENCE_MS))
                .await
                .ok()
        } else {
            None
        };
        lock(&self.0.verification)["home_route"] = json!(route);
    }

    async fn can_send_home(&self, room: &Room, chat: &Value, cancel: &CancellationToken) -> bool {
        let kind = chat["type"].as_str();
        if room.kind == RoomKind::Direct {
            return kind == Some("private");
        }
        let bot = self.0.bot.identity.id;
        let Ok(member) = self
            .0
            .client
            .request(
                Method::GetChatMember,
                json!({"chat_id":chat["id"],"user_id":bot}),
                cancel,
            )
            .await
        else {
            return false;
        };
        if member.pointer("/user/id").and_then(Value::as_i64) != Some(bot) {
            return false;
        }
        match (kind, member["status"].as_str()) {
            (Some("channel"), Some("creator")) => true,
            (Some("channel"), Some("administrator")) => member["can_post_messages"] == true,
            (Some("group" | "supergroup"), Some("creator" | "administrator")) => true,
            (Some("group" | "supergroup"), Some("member")) => {
                chat.pointer("/permissions/can_send_messages") == Some(&Value::Bool(true))
            }
            (Some("group" | "supergroup"), Some("restricted")) => {
                member["is_member"] == true && member["can_send_messages"] == true
            }
            _ => false,
        }
    }

    async fn sync_menu(&self, cancel: &CancellationToken) {
        if !self.0.settings.sync_commands {
            return;
        }
        let menu: Vec<_> = [
            ("status", "Show conversation status"),
            ("queue", "Show queued work"),
            ("history", "Show recent turns"),
            ("model", "Choose a model"),
            ("voice", "Set spoken replies"),
            ("bg", "Start a background turn"),
            ("sethome", "Set this conversation as the home destination"),
            ("new", "Start a new session"),
            ("skill", "Run an available skill"),
            ("stop", "Stop the active turn"),
            ("steer", "Correct the active turn"),
            ("redirect", "Redirect the active turn"),
        ]
        .into_iter()
        .map(|(command, description)| json!({"command":command,"description":description}))
        .collect();
        let result = self
            .0
            .client
            .request(Method::SetMyCommands, json!({"commands":menu}), cancel)
            .await;
        lock(&self.0.verification)["command_menu"] = json!(if result.is_ok() {
            "registered"
        } else {
            "unavailable"
        });
    }

    async fn poll(&self, ingress: &Ingress, cancel: &CancellationToken) -> ConnectionFailure {
        loop {
            let (_, checkpoint) = match cursor(ingress).await {
                Ok(cursor) => cursor,
                Err(error) => return gateway_failure(error),
            };
            let updates = self
                .0
                .client
                .request(
                    Method::GetUpdates,
                    json!({
                        "offset":checkpoint.poll_offset(), "limit":100, "timeout":20,
                        "allowed_updates":updates::UPDATE_TYPES,
                    }),
                    cancel,
                )
                .await;
            let updates = match updates {
                Ok(Value::Array(updates)) => {
                    lock(&self.0.verification)["intake_health"] = json!("polling");
                    updates
                }
                Ok(_) => return ConnectionFailure::Retryable,
                Err(error)
                    if matches!(
                        error.kind,
                        ErrorKind::Conflict | ErrorKind::RateLimited | ErrorKind::Retryable
                    ) =>
                {
                    lock(&self.0.verification)["intake_health"] = json!(match error.kind {
                        ErrorKind::Conflict => "poller_conflict",
                        ErrorKind::RateLimited => "rate_limited",
                        _ => "backoff",
                    });
                    let delay = Duration::from_secs(error.retry_after.unwrap_or(2));
                    tokio::select! {
                        _ = cancel.cancelled() => return ConnectionFailure::Stopped,
                        _ = tokio::time::sleep(delay) => continue,
                    }
                }
                Err(error) => return connection_failure(error),
            };
            // A malformed identity breaks the proven prefix. Its successors
            // must wait for another delivery of the batch.
            let mut delivered = None;
            for update in updates {
                let Some(id) = updates::id(&update)
                    .filter(|id| delivered.is_none_or(|previous| *id >= previous))
                else {
                    return ConnectionFailure::Retryable;
                };
                if let Err(error) = self.process(ingress, &update, true, cancel).await {
                    return gateway_failure(error);
                }
                delivered = Some(id);
            }
            if delivered.is_none() {
                tokio::select! {
                    _ = cancel.cancelled() => return ConnectionFailure::Stopped,
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
        }
    }

    async fn process(
        &self,
        ingress: &Ingress,
        update: &Value,
        polling: bool,
        cancel: &CancellationToken,
    ) -> Result<(), GatewayError> {
        let update_id = updates::id(update).ok_or(GatewayError::Invalid)?;
        let _serial = self.0.intake.lock().await;
        let (previous, mut checkpoint) = cursor(ingress).await?;
        checkpoint.record(update_id, polling, now());
        let event_id = format!("telegram-update-{}-{update_id}", checkpoint.epoch);
        let advance = Advance {
            stream: STREAM.into(),
            previous,
            checkpoint: db(serde_json::to_string(&checkpoint).map_err(Into::into))?,
        };
        if let Some(tap) = updates::callback(update, &self.0.bot) {
            return delivery::callback(&self.0, ingress, tap, event_id, advance, cancel).await;
        }
        let Some(mut incoming) = updates::message(update, &event_id, &self.0.bot, &self.0.patterns)
        else {
            return ignore(ingress, event_id, update, advance).await;
        };
        if [
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
        ]
        .into_iter()
        .filter_map(|kind| update.get(kind))
        .any(|message| updates::other_command(message, &self.0.bot))
        {
            return refuse(ingress, incoming.event, Disposition::Forbidden, advance).await;
        }

        incoming.event.sender.roles = observe_actor(
            &self.0,
            ingress,
            &incoming.event.room,
            &incoming.event.sender.id,
            incoming.kind,
            cancel,
        )
        .await?;
        if let InboundBody::Control(Control::ReplaceQueued { event_id, .. }) =
            &mut incoming.event.body
        {
            let provider_id = format!("telegram-message:{event_id}");
            let Some(original) = ingress.storage().read(|conn| {
                Ok(conn.query_row("SELECT event FROM messaging_input_aliases WHERE connection=?1 AND namespace='event' AND provider_id=?2",
                    params![ingress.connection_name(), provider_id], |r| r.get::<_, String>(0)).optional()?)
            }).map_err(|_| GatewayError::TemporarilyUnavailable)? else {
                return refuse(ingress, incoming.event, Disposition::Unsupported, advance).await;
            };
            *event_id = original;
        }
        if matches!(
            incoming.event.body,
            InboundBody::Command(Command::Background { .. })
        ) {
            incoming.event.reply_anchor = None;
        }

        if self.0.settings.typed_clarification
            && !incoming.edited
            && incoming.files.is_empty()
            && let InboundBody::Message(message) = &incoming.event.body
            && let Some(reply) = incoming.reply_to_bot
            && let Some(action_id) = delivery::clarification(
                ingress,
                &incoming.event.room,
                &incoming.event.sender,
                reply,
            )?
        {
            incoming.event.body = InboundBody::Control(Control::Respond {
                action_id,
                response: Response::Clarify(message.text.clone()),
            });
        }
        if !incoming.edited
            && incoming.files.is_empty()
            && incoming.kind == ActorKind::Person
            && let InboundBody::Message(message) = &incoming.event.body
            && let Some(control) =
                updates::busy_control(&message.text, &self.0.bot.identity.username)
        {
            let generation = match ingress
                .active_execution(&incoming.event.room, &incoming.event.sender)
                .await
            {
                Ok(Some(generation)) => generation,
                Ok(None) => {
                    return refuse(ingress, incoming.event, Disposition::Unsupported, advance)
                        .await;
                }
                Err(GatewayError::Forbidden) => {
                    return refuse(ingress, incoming.event, Disposition::Forbidden, advance).await;
                }
                Err(error) => return Err(error),
            };
            incoming.event.body = InboundBody::Control(control.against(generation));
        }

        if !incoming.files.is_empty()
            && !incoming.edited
            && matches!(incoming.event.body, InboundBody::Message(_))
        {
            return media::stage(ingress, incoming, advance).await;
        }
        match incoming.event.body {
            InboundBody::Message(_) => {
                let dest =
                    Destination::from_room(&incoming.event.room).ok_or(GatewayError::Invalid)?;
                let alias = InputAlias {
                    namespace: "event".into(),
                    id: format!("telegram-message:{}", dest.message_key(incoming.message_id)),
                };
                ingress
                    .submit_aliased(incoming.event, vec![alias], Some(advance))
                    .await?;
            }
            _ => {
                ingress.submit(vec![incoming.event], Some(advance)).await?;
            }
        }
        Ok(())
    }
}

pub(super) async fn observe_actor(
    connection: &Connection,
    ingress: &Ingress,
    room: &Room,
    sender: &str,
    kind: ActorKind,
    cancel: &CancellationToken,
) -> Result<BTreeSet<String>, GatewayError> {
    let mut roles = BTreeSet::new();
    if room.kind == RoomKind::Group && kind == ActorKind::Person {
        let chat = room
            .chat
            .parse::<i64>()
            .map_err(|_| GatewayError::Invalid)?;
        let user = sender.parse::<i64>().map_err(|_| GatewayError::Invalid)?;
        let role = format!("telegram-admin:{chat}");
        let operator = connection.settings.access.operator_roles.contains(&role);
        if operator {
            let member = connection
                .client
                .request(
                    Method::GetChatMember,
                    json!({"chat_id":chat,"user_id":user}),
                    cancel,
                )
                .await;
            match member {
                Ok(member)
                    if member.pointer("/user/id").and_then(Value::as_i64) == Some(user)
                        && matches!(
                            member["status"].as_str(),
                            Some("creator" | "administrator")
                        ) =>
                {
                    roles.insert(role);
                }
                Err(error)
                    if matches!(
                        error.kind,
                        ErrorKind::Cancelled | ErrorKind::Retryable | ErrorKind::RateLimited
                    ) =>
                {
                    return Err(GatewayError::TemporarilyUnavailable);
                }
                _ => {}
            }
        }
    }
    ingress
        .observe_membership(
            sender.to_owned(),
            kind,
            MemberEvidence {
                room: room.clone(),
                roles: roles.clone(),
            },
            now().saturating_add(EVIDENCE_MS),
        )
        .await?;
    Ok(roles)
}

fn owns_webhook(ingress: &Ingress, url: &str) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_telegram_webhook
             WHERE connection=?1 AND (confirmed=?2 OR pending=?2))",
                params![ingress.connection_name(), url],
                |row| row.get(0),
            )?)
        }))
    })
}

fn plan_webhook(ingress: &Ingress, url: &str) -> Result<(), GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "INSERT INTO messaging_telegram_webhook(connection,pending) VALUES(?1,?2)
            ON CONFLICT(connection) DO UPDATE SET pending=excluded.pending",
                params![ingress.connection_name(), url],
            )?;
            Ok(())
        }))
    })
}

fn confirm_webhook(ingress: &Ingress, url: &str) -> Result<(), GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_telegram_webhook SET confirmed=?2,pending=NULL
            WHERE connection=?1 AND pending=?2",
                params![ingress.connection_name(), url],
            )?;
            Ok(())
        }))
    })
}

fn webhook_secret() -> Option<String> {
    let mut secret = [0u8; 32];
    SystemRandom::new().fill(&mut secret).ok()?;
    let mut token = String::with_capacity(secret.len() * 2);
    for byte in secret {
        write!(token, "{byte:02x}").ok()?;
    }
    Some(token)
}

pub(super) async fn refuse(
    ingress: &Ingress,
    event: InboundEvent,
    reason: Disposition,
    advance: Advance,
) -> Result<(), GatewayError> {
    let scope = ingress.clone();
    hermes_gateway_core::messaging::runtime::blocking(move || {
        scope.checked(true, |state| {
            let advance = scope.scoped_advance(Some(advance))?;
            db(scope.storage().intake(
                state.revision(),
                vec![Candidate {
                    event,
                    decision: Err(reason),
                    protection: None,
                }],
                advance.as_ref(),
            ))?
            .ok_or(GatewayError::CheckpointChanged)?;
            Ok(())
        })
    })
    .await
}

pub(super) async fn ignore(
    ingress: &Ingress,
    id: String,
    update: &Value,
    advance: Advance,
) -> Result<(), GatewayError> {
    let fingerprint = db(storage::key("telegram-update", update))?;
    let event = InboundEvent {
        id,
        room: Room {
            chat: "unrouted".into(),
            kind: RoomKind::Direct,
            thread: None,
        },
        sender: Sender {
            id: "telegram-system".into(),
            roles: BTreeSet::new(),
        },
        reply_anchor: None,
        mentioned: false,
        body: InboundBody::Message(Message::text(fingerprint)),
    };
    refuse(ingress, event, Disposition::Unsupported, advance).await
}

fn gateway_failure(error: GatewayError) -> ConnectionFailure {
    match error {
        GatewayError::StaleConnection => ConnectionFailure::Stopped,
        _ => ConnectionFailure::Retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_poll_reopens_the_epoch_without_skipping_an_out_of_order_webhook() {
        let before = now() - RESET_AFTER - 1;
        let mut cursor = Cursor {
            epoch: 3,
            next_poll: 90_001,
            last_seen: before,
        };
        assert_eq!(cursor.poll_offset(), 0);
        cursor.record(501, false, now());
        cursor.record(500, false, now());
        assert_eq!(cursor.epoch, 4);
        assert_eq!(cursor.poll_offset(), 0);
        cursor.record(500, true, now());
        assert_eq!(cursor.poll_offset(), 501);
        cursor.record(501, true, now());
        assert_eq!(cursor.poll_offset(), 502);
    }
}
