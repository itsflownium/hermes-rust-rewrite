use super::{ChatMessage, Identity, Inbox, MAX_INBOX_BYTES, Store, Token, private_json};
use crate::telegram::{Incoming, Receipt};
use hermes_engine::db;
use ring::digest;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    fmt::Display,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_INBOX_MESSAGES: i64 = 10_000;
// Telegram retains updates for at most 24 hours, and can restart update IDs
// at a lower value after a week of inactivity.
const CURSOR_IDLE_SECONDS: i64 = 24 * 60 * 60;

// Telegram owns these additive tables in the profile database. The inbox row is
// also the migration marker. The JSON inbox is imported only if that row is absent.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS telegram_inboxes (
    bot_id INTEGER PRIMARY KEY,
    cursor INTEGER NOT NULL,
    last_seen INTEGER,
    revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS telegram_messages (
    id INTEGER PRIMARY KEY,
    bot_id INTEGER NOT NULL REFERENCES telegram_inboxes(bot_id),
    update_id INTEGER,
    message_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    sender TEXT NOT NULL,
    text TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    outgoing INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS telegram_messages_inbox ON telegram_messages(bot_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS telegram_messages_sent
    ON telegram_messages(bot_id, user_id, message_id) WHERE outgoing = 1;
CREATE TABLE IF NOT EXISTS telegram_routes (
    bot_id INTEGER NOT NULL REFERENCES telegram_inboxes(bot_id),
    chat_id INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    PRIMARY KEY (bot_id, chat_id)
);
CREATE TABLE IF NOT EXISTS telegram_updates (
    ingress_order INTEGER PRIMARY KEY,
    bot_id INTEGER NOT NULL REFERENCES telegram_inboxes(bot_id),
    update_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    fingerprint BLOB NOT NULL,
    conflict INTEGER NOT NULL DEFAULT 0,
    stage TEXT NOT NULL,
    text TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    retry_at INTEGER,
    reason TEXT,
    sent_message_id INTEGER,
    UNIQUE (bot_id, update_id)
);
CREATE INDEX IF NOT EXISTS telegram_updates_outstanding
    ON telegram_updates(bot_id, chat_id, ingress_order)
    WHERE stage IN ('received','turn_unknown','blocked','pending','sending','unknown');
";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkStage {
    Received,
    TurnUnknown,
    Pending,
}

pub struct Work {
    pub update_id: i64,
    pub chat_id: i64,
    pub stage: WorkStage,
    pub text: String,
}

pub enum TurnOutcome {
    Reply(String),
    Unknown,
    Blocked(&'static str),
    Failed(&'static str),
}

pub enum DeliveryOutcome {
    Sent(Receipt),
    RateLimited { retry_at: i64 },
    Failed,
    Unknown,
}

#[derive(Clone, Copy)]
pub enum DeliveryResolution {
    Delivered,
    Discard,
}

impl Store {
    pub(super) fn has_native_inbox(&self, bot_id: i64) -> Result<bool, String> {
        let Some(sql) = db::open_existing(&self.profile).map_err(storage)? else {
            return Ok(false);
        };
        let installed: bool = sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='telegram_inboxes')",
            [], |row| row.get(0),
        ).map_err(storage)?;
        if !installed {
            return Ok(false);
        }
        sql.query_row(
            "SELECT EXISTS(SELECT 1 FROM telegram_inboxes WHERE bot_id=?1)",
            [bot_id], |row| row.get(0),
        ).map_err(storage)
    }

    fn open_inbox(&self, bot: &Identity) -> Result<Connection, String> {
        if bot.id <= 0 {
            return Err("Telegram bot identity is invalid".into());
        }
        let mut sql = db::open(&self.profile).map_err(storage)?;
        sql.execute_batch(SCHEMA).map_err(storage)?;
        let exists = |sql: &Connection| {
            sql.query_row(
                "SELECT EXISTS(SELECT 1 FROM telegram_inboxes WHERE bot_id = ?1)",
                [bot.id],
                |row| row.get::<_, bool>(0),
            )
            .map_err(storage)
        };
        if exists(&sql)? {
            return Ok(sql);
        }
        let _lock = self.lock(&format!("bot-{}.inbox.lock", bot.id))?;
        if exists(&sql)? {
            return Ok(sql);
        }

        let path = self.root.join(format!("bot-{}.inbox.json", bot.id));
        let old: Option<Inbox> = if path.try_exists().map_err(storage)? {
            let inbox: Inbox = private_json(&path, MAX_INBOX_BYTES)?;
            if inbox.version != 1 || inbox.bot.id != bot.id || inbox.cursor < 0 {
                return Err("Native Telegram inbox identity or version is invalid".into());
            }
            Some(inbox)
        } else {
            None
        };
        let cursor = old.as_ref().map_or(0, |inbox| inbox.cursor);
        let last_seen = (cursor > 0).then(unix_time);
        let revision =
            i64::try_from(old.as_ref().map_or(0, |inbox| inbox.revision)).map_err(storage)?;
        let tx = sql
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        tx.execute(
            "INSERT INTO telegram_inboxes (bot_id,cursor,last_seen,revision) VALUES (?1,?2,?3,?4)",
            params![bot.id, cursor, last_seen, revision],
        )
        .map_err(storage)?;
        if let Some(old) = &old {
            for message in &old.messages {
                append_message(&tx, bot.id, message).map_err(storage)?;
                if let Some(id) = message.update_id {
                    let hash = fingerprint(message.user_id, message.message_id, &message.text);
                    tx.execute(
                        "INSERT OR IGNORE INTO telegram_updates (bot_id,update_id,chat_id,fingerprint,stage)
                         VALUES (?1,?2,?3,?4,'manual')",
                        params![bot.id, id, message.user_id, hash.as_ref()],
                    ).map_err(storage)?;
                }
            }
        }
        tx.commit().map_err(storage)?;
        if old.is_some() {
            db::retire(&path);
        }
        Ok(sql)
    }

    fn write_inbox<T>(
        &self,
        bot: &Identity,
        action: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, String> {
        let mut sql = self.open_inbox(bot)?;
        let tx = sql
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage)?;
        let result = action(&tx).map_err(storage)?;
        tx.commit().map_err(storage)?;
        Ok(result)
    }

    pub fn load_inbox(&self, bot: &Identity) -> Result<Arc<Inbox>, String> {
        let mut sql = self.open_inbox(bot)?;
        let read = sql.transaction().map_err(storage)?;
        let (cursor, revision): (i64, i64) = read
            .query_row(
                "SELECT cursor,revision FROM telegram_inboxes WHERE bot_id = ?1",
                [bot.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        let mut messages = read
            .prepare(
                "SELECT update_id,message_id,user_id,sender,text,timestamp,outgoing
             FROM telegram_messages WHERE bot_id = ?1 ORDER BY id DESC LIMIT ?2",
            )
            .and_then(|mut query| {
                query
                    .query_map(params![bot.id, MAX_INBOX_MESSAGES], |row| {
                        Ok(ChatMessage {
                            update_id: row.get(0)?,
                            message_id: row.get(1)?,
                            user_id: row.get(2)?,
                            sender: row.get(3)?,
                            text: row.get(4)?,
                            timestamp: row.get(5)?,
                            outgoing: row.get(6)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(storage)?;
        messages.reverse();
        Ok(Arc::new(Inbox {
            version: 1,
            bot: bot.clone(),
            cursor,
            revision: u64::try_from(revision).map_err(storage)?,
            messages,
        }))
    }

    pub fn poll_offset(&self, bot: &Identity, now: i64) -> Result<i64, String> {
        let sql = self.open_inbox(bot)?;
        let (cursor, last_seen): (i64, Option<i64>) = sql
            .query_row(
                "SELECT cursor,last_seen FROM telegram_inboxes WHERE bot_id = ?1",
                [bot.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(storage)?;
        Ok(
            if last_seen.is_some_and(|seen| now.saturating_sub(seen) < CURSOR_IDLE_SECONDS) {
                cursor
            } else {
                0
            },
        )
    }

    /// Accepted work, display messages, and Telegram's next acknowledgement
    /// offset commit together before the connector polls again.
    pub fn commit_poll(
        &self,
        bot: &Identity,
        token: &Token,
        messages: &[Incoming],
        offset: i64,
        next_offset: i64,
        now: i64,
    ) -> Result<(), String> {
        let _settings = self.lock("settings.lock")?;
        let settings = self.connection_settings(bot, token)?;
        if next_offset <= offset && messages.is_empty() {
            return Ok(());
        }
        self.write_inbox(bot, |tx| {
            let mut messages: Vec<_> = messages.iter().collect();
            messages.sort_by_key(|message| message.update_id);
            for message in messages {
                if message.bot_id != bot.id || message.chat_id != message.user_id
                    || message.user_id <= 0 || !settings.allowed.contains(&message.user_id)
                    || message.update_id >= next_offset
                {
                    continue;
                }
                let hash = fingerprint(message.chat_id, message.message_id, &message.text);
                let stage = if settings.agent_replies { "received" } else { "manual" };
                let text = settings.agent_replies.then_some(message.text.as_str());
                let inserted = if message.update_id >= offset {
                    tx.execute(
                        "INSERT OR IGNORE INTO telegram_updates (bot_id,update_id,chat_id,fingerprint,stage,text)
                         VALUES (?1,?2,?3,?4,?5,?6)",
                        params![bot.id, message.update_id, message.chat_id, hash.as_ref(), stage, text],
                    )?
                } else {
                    0 // An acknowledged update can reveal a conflict but cannot start work.
                };
                if inserted == 0 {
                    tx.execute(
                        "UPDATE telegram_updates SET conflict = 1
                         WHERE bot_id = ?1 AND update_id = ?2 AND fingerprint <> ?3 AND conflict = 0",
                        params![bot.id, message.update_id, hash.as_ref()],
                    )?;
                } else {
                    append_message(tx, bot.id, &ChatMessage {
                        update_id: Some(message.update_id),
                        message_id: message.message_id,
                        user_id: message.user_id,
                        sender: message.sender.clone(),
                        text: message.text.clone(),
                        timestamp: message.timestamp,
                        outgoing: false,
                    })?;
                }
            }
            tx.execute(
                "UPDATE telegram_inboxes SET cursor = ?2,last_seen = ?3,revision = revision + 1
                 WHERE bot_id = ?1 AND ?2 > ?4 AND (?4 = 0 OR cursor <= ?4)",
                params![bot.id, next_offset, now, offset],
            )?;
            Ok(())
        })
    }

    pub fn record_sent(
        &self,
        bot: &Identity,
        user_id: i64,
        text: String,
        receipt: Receipt,
    ) -> Result<(), String> {
        self.write_inbox(bot, |tx| record_echo(tx, bot.id, user_id, text, receipt))
    }

    pub fn route(&self, bot: &Identity, chat_id: i64) -> Result<Option<String>, String> {
        let sql = self.open_inbox(bot)?;
        sql.query_row(
            "SELECT session_id FROM telegram_routes WHERE bot_id = ?1 AND chat_id = ?2",
            params![bot.id, chat_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage)
    }

    pub fn save_route(&self, bot: &Identity, chat_id: i64, session_id: &str) -> Result<(), String> {
        let saved = self.write_inbox(bot, |tx| {
            tx.execute(
                "INSERT OR IGNORE INTO telegram_routes (bot_id,chat_id,session_id) VALUES (?1,?2,?3)",
                params![bot.id, chat_id, session_id],
            )?;
            tx.query_row(
                "SELECT session_id FROM telegram_routes WHERE bot_id = ?1 AND chat_id = ?2",
                params![bot.id, chat_id], |row| row.get::<_, String>(0),
            )
        })?;
        if saved != session_id {
            return Err("Telegram chat already has a different conversation mapping".into());
        }
        Ok(())
    }

    /// Only the oldest outstanding item for each chat can progress. Unknown
    /// sends and missing routes fence that chat even if it has more input.
    pub fn ready(&self, bot: &Identity, now: i64) -> Result<Vec<Work>, String> {
        let sql = self.open_inbox(bot)?;
        sql.prepare(
            "WITH heads AS (
                 SELECT MIN(ingress_order) AS id FROM telegram_updates
                 WHERE bot_id = ?1 AND stage IN ('received','turn_unknown','blocked','pending','sending','unknown')
                 GROUP BY chat_id
             )
             SELECT update_id,chat_id,stage,text FROM telegram_updates JOIN heads ON ingress_order = heads.id
             WHERE stage IN ('received','turn_unknown','pending') AND (retry_at IS NULL OR retry_at <= ?2)
             ORDER BY ingress_order",
        ).and_then(|mut query| {
            query.query_map(params![bot.id, now], |row| {
                let stage = match row.get_ref(2)?.as_str()? {
                    "received" => WorkStage::Received,
                    "turn_unknown" => WorkStage::TurnUnknown,
                    "pending" => WorkStage::Pending,
                    _ => return Err(rusqlite::Error::InvalidQuery),
                };
                Ok(Work { update_id: row.get(0)?, chat_id: row.get(1)?, stage, text: row.get(3)? })
            })?.collect()
        }).map_err(storage)
    }

    pub fn record_turn(
        &self,
        bot: &Identity,
        update_id: i64,
        outcome: TurnOutcome,
    ) -> Result<(), String> {
        let (stage, text, reason) = match outcome {
            TurnOutcome::Reply(text) => ("pending", Some(text), None),
            TurnOutcome::Unknown => ("turn_unknown", None, Some("execution_unknown")),
            TurnOutcome::Blocked(reason) => ("blocked", None, Some(reason)),
            TurnOutcome::Failed(reason) => ("failed", None, Some(reason)),
        };
        self.write_inbox(bot, |tx| {
            tx.execute(
                "UPDATE telegram_updates SET stage = ?3,text = COALESCE(?4, CASE WHEN ?3 = 'turn_unknown' THEN text END),reason = ?5
                 WHERE bot_id = ?1 AND update_id = ?2
                     AND (stage IN ('received','turn_unknown') OR (stage = 'pending' AND ?3 = 'blocked'))",
                params![bot.id, update_id, stage, text, reason],
            )?;
            Ok(())
        })
    }

    pub fn recover_sends(&self, bot: &Identity) -> Result<(), String> {
        self.write_inbox(bot, |tx| {
            tx.execute(
                "UPDATE telegram_updates SET stage = 'unknown',reason = 'send_interrupted'
                 WHERE bot_id = ?1 AND stage = 'sending'",
                [bot.id],
            )?;
            Ok(())
        })
    }

    /// The request may leave the process once, after this attempt is durable.
    pub fn begin_send(
        &self,
        bot: &Identity,
        token: &Token,
        work: &Work,
        now: i64,
    ) -> Result<Option<i64>, String> {
        self.with_agent_policy(bot, token, work.chat_id, || self.write_inbox(bot, |tx| {
            tx.query_row(
                "UPDATE telegram_updates SET stage = 'sending',attempt = attempt + 1,retry_at = NULL,reason = NULL
                 WHERE bot_id = ?1 AND update_id = ?2 AND chat_id = ?3 AND stage = 'pending'
                     AND (retry_at IS NULL OR retry_at <= ?4)
                 RETURNING attempt",
                params![bot.id, work.update_id, work.chat_id, now], |row| row.get(0),
            ).optional()
        })).map(Option::flatten)
    }

    pub fn record_delivery(
        &self,
        bot: &Identity,
        work: &Work,
        attempt: i64,
        outcome: DeliveryOutcome,
    ) -> Result<(), String> {
        self.write_inbox(bot, |tx| {
            let (stage, retry_at, reason, receipt) = match outcome {
                DeliveryOutcome::Sent(receipt) => ("sent", None, None, Some(receipt)),
                DeliveryOutcome::RateLimited { retry_at } => ("pending", Some(retry_at), Some("rate_limited"), None),
                DeliveryOutcome::Failed => ("failed", None, Some("send_rejected"), None),
                DeliveryOutcome::Unknown => ("unknown", None, Some("send_outcome_unknown"), None),
            };
            let message_id = receipt.as_ref().map(|receipt| receipt.message_id);
            let changed = tx.execute(
                "UPDATE telegram_updates SET stage = ?4,retry_at = ?5,reason = ?6,sent_message_id = ?7,
                     text = CASE WHEN ?4 IN ('pending','unknown') THEN text END
                 WHERE bot_id = ?1 AND update_id = ?2 AND attempt = ?3 AND stage = 'sending'",
                params![bot.id, work.update_id, attempt, stage, retry_at, reason, message_id],
            )?;
            if changed != 0 && let Some(receipt) = receipt {
                record_echo(tx, bot.id, work.chat_id, work.text.clone(), receipt)?;
            }
            Ok(())
        })
    }

    pub fn resolve_delivery(
        &self,
        bot: &Identity,
        update_id: i64,
        attempt: i64,
        resolution: DeliveryResolution,
    ) -> Result<bool, String> {
        let (stage, reason) = match resolution {
            DeliveryResolution::Delivered => ("sent", "operator_confirmed_delivery"),
            DeliveryResolution::Discard => ("discarded", "operator_discarded_delivery"),
        };
        self.write_inbox(bot, |tx| {
            tx.execute(
                "UPDATE telegram_updates SET stage = ?4,reason = ?5,text = NULL
                 WHERE bot_id = ?1 AND update_id = ?2 AND attempt = ?3 AND stage = 'unknown'",
                params![bot.id, update_id, attempt, stage, reason],
            )
            .map(|changed| changed != 0)
        })
    }

    pub fn work_status(&self, bot: &Identity) -> Result<Value, String> {
        let mut sql = self.open_inbox(bot)?;
        let read = sql.transaction().map_err(storage)?;
        let counts = read.prepare(
            "SELECT stage,count(*) FROM telegram_updates WHERE bot_id = ?1 AND stage <> 'manual' GROUP BY stage",
        ).and_then(|mut query| {
            query.query_map([bot.id], |row| Ok((row.get::<_, String>(0)?, Value::from(row.get::<_, i64>(1)?))))?
                .collect::<rusqlite::Result<serde_json::Map<String, Value>>>()
        }).map_err(storage)?;
        let conflicts: i64 = read
            .query_row(
                "SELECT count(*) FROM telegram_updates WHERE bot_id = ?1 AND conflict = 1",
                [bot.id],
                |row| row.get(0),
            )
            .map_err(storage)?;
        let issues = read.prepare(
            "SELECT u.update_id,u.chat_id,u.stage,u.conflict,u.attempt,u.retry_at,u.reason,r.session_id
             FROM telegram_updates u LEFT JOIN telegram_routes r ON u.bot_id = r.bot_id AND u.chat_id = r.chat_id
             WHERE u.bot_id = ?1 AND (u.conflict = 1 OR u.reason IS NOT NULL)
             ORDER BY (u.stage IN ('unknown','turn_unknown','blocked')) DESC,u.ingress_order DESC LIMIT 50",
        ).and_then(|mut query| query.query_map([bot.id], status_row)?.collect::<rusqlite::Result<Vec<_>>>())
            .map_err(storage)?;
        Ok(json!({"counts":counts,"conflicts":conflicts,"issues":issues}))
    }
}

fn status_row(row: &Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "update_id": row.get::<_, i64>(0)?,
        "chat_id": row.get::<_, i64>(1)?,
        "stage": row.get::<_, String>(2)?,
        "conflict": row.get::<_, bool>(3)?,
        "attempt": row.get::<_, i64>(4)?,
        "retry_at": row.get::<_, Option<i64>>(5)?,
        "reason": row.get::<_, Option<String>>(6)?,
        "session_id": row.get::<_, Option<String>>(7)?,
    }))
}

fn record_echo(
    tx: &Transaction<'_>,
    bot_id: i64,
    user_id: i64,
    text: String,
    receipt: Receipt,
) -> rusqlite::Result<()> {
    if append_message(
        tx,
        bot_id,
        &ChatMessage {
            update_id: None,
            message_id: receipt.message_id,
            user_id,
            sender: "You".into(),
            text,
            timestamp: receipt.timestamp,
            outgoing: true,
        },
    )? {
        tx.execute(
            "UPDATE telegram_inboxes SET revision = revision + 1 WHERE bot_id = ?1",
            [bot_id],
        )?;
    }
    Ok(())
}

fn append_message(
    tx: &Transaction<'_>,
    bot_id: i64,
    message: &ChatMessage,
) -> rusqlite::Result<bool> {
    tx.execute(
        "INSERT OR IGNORE INTO telegram_messages (bot_id,update_id,message_id,user_id,sender,text,timestamp,outgoing)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![bot_id, message.update_id, message.message_id, message.user_id, message.sender,
            message.text, message.timestamp, message.outgoing],
    ).map(|changed| changed != 0)
}

fn fingerprint(chat_id: i64, message_id: i64, text: &str) -> digest::Digest {
    let mut hash = digest::Context::new(&digest::SHA256);
    hash.update(&chat_id.to_be_bytes());
    hash.update(&message_id.to_be_bytes());
    hash.update(text.as_bytes());
    hash.finish()
}

fn storage(error: impl Display) -> String {
    format!("Telegram inbox storage: {error}")
}

fn unix_time() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Settings;

    fn setup(root: &std::path::Path) -> (Store, Identity, Token) {
        let bot = Identity {
            id: 123456,
            username: "fixture_bot".into(),
            name: "Fixture".into(),
        };
        let token = Token::parse("123456:abcdefghijklmnopqrstuvwxyz1234567890").unwrap();
        let store = Store::new(root);
        store
            .save_settings(&Settings {
                version: 1,
                token: Some(token.clone()),
                allowed: [42].into_iter().collect(),
                bot: Some(bot.clone()),
                enabled: true,
                agent_replies: true,
            })
            .unwrap();
        (store, bot, token)
    }

    fn dm(bot: &Identity, id: i64, text: &str) -> Incoming {
        Incoming {
            bot_id: bot.id,
            update_id: id,
            message_id: id,
            chat_id: 42,
            user_id: 42,
            sender: "Fixture".into(),
            text: text.into(),
            timestamp: 10,
        }
    }

    #[test]
    fn migrates_existing_inbox_once_without_queuing_history_and_accepts_a_lower_cursor() {
        let root = tempfile::tempdir().unwrap();
        let (store, bot, token) = setup(root.path());
        let legacy = Inbox {
            version: 1,
            bot: bot.clone(),
            cursor: 901,
            revision: 6,
            messages: vec![
                ChatMessage {
                    update_id: Some(900),
                    message_id: 900,
                    user_id: 42,
                    sender: "Fixture".into(),
                    text: "Old message".into(),
                    timestamp: 10,
                    outgoing: false,
                },
                ChatMessage {
                    update_id: None,
                    message_id: 901,
                    user_id: 42,
                    sender: "You".into(),
                    text: "Old reply".into(),
                    timestamp: 11,
                    outgoing: true,
                },
            ],
        };
        let legacy_path = store.root.join(format!("bot-{}.inbox.json", bot.id));
        super::super::atomic_json(&legacy_path, &legacy).unwrap();
        let imported = store.load_inbox(&bot).unwrap();
        assert_eq!((imported.cursor, imported.revision), (901, 6));
        assert_eq!(imported.messages.len(), 2);
        assert!(store.ready(&bot, unix_time()).unwrap().is_empty());

        // The database remains authoritative even if an old file reappears.
        super::super::atomic_json(
            &legacy_path,
            &Inbox {
                cursor: 999,
                ..legacy
            },
        )
        .unwrap();
        let reopened = Store::new(root.path());
        assert_eq!(reopened.poll_offset(&bot, unix_time()).unwrap(), 901);
        let later = unix_time() + CURSOR_IDLE_SECONDS + 1;
        assert_eq!(reopened.poll_offset(&bot, later).unwrap(), 0);
        reopened
            .commit_poll(&bot, &token, &[], 0, 0, later)
            .unwrap();
        assert_eq!(reopened.load_inbox(&bot).unwrap().cursor, 901);
        reopened
            .commit_poll(&bot, &token, &[dm(&bot, 900, "Old message")], 0, 901, later)
            .unwrap();
        assert!(reopened.ready(&bot, later).unwrap().is_empty());
        let idle_again = later + CURSOR_IDLE_SECONDS + 1;
        reopened
            .commit_poll(&bot, &token, &[dm(&bot, 5, "New epoch")], 0, 6, idle_again)
            .unwrap();
        assert_eq!(reopened.poll_offset(&bot, idle_again).unwrap(), 6);
        assert_eq!(reopened.ready(&bot, idle_again).unwrap()[0].update_id, 5);
        assert_eq!(reopened.load_inbox(&bot).unwrap().messages.len(), 3);
    }

    #[test]
    fn a_bot_lease_covers_profiles_and_a_revoked_chat_cannot_claim_a_send() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("profiles").join("agent");
        std::fs::create_dir_all(&profile).unwrap();
        let (store, bot, token) = setup(&profile);
        let default = Store::new(root.path());
        let lease = store.claim(bot.id).unwrap();
        assert!(default.claim(bot.id).is_err());
        drop(lease);
        assert!(default.claim(bot.id).is_ok());

        store
            .commit_poll(&bot, &token, &[dm(&bot, 10, "accepted")], 0, 11, 100)
            .unwrap();
        store
            .record_turn(&bot, 10, TurnOutcome::Reply("A committed answer".into()))
            .unwrap();
        let head = store.ready(&bot, 100).unwrap().pop().unwrap();
        let mut settings = store.load_settings().unwrap().unwrap();
        settings.allowed = [43].into_iter().collect();
        store.save_settings(&settings).unwrap();
        assert_eq!(store.begin_send(&bot, &token, &head, 100).unwrap(), None);
        assert_eq!(store.work_status(&bot).unwrap()["counts"]["pending"], 1);
    }
}
