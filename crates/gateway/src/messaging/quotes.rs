use super::{
    Advance, Disposition, GatewayError, InboundBody, InboundEvent, IntakeReceipt, Message, Room,
    intake::{PreparationPurpose, Staged},
    runtime::{Ingress, State, blocking, db, valid_advance, valid_id},
    store::{self, Route, Store},
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};

const MAX_QUOTE_BYTES: usize = 8 * 1024;
pub const MAX_SCROLLBACK_BYTES: usize = 12 * 1024;
pub const MAX_SCROLLBACK_MESSAGE_BYTES: usize = 3 * 1024;
const MAX_SCROLLBACK_MESSAGES: u8 = 20;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_quoted_context (
    connection TEXT NOT NULL, event TEXT NOT NULL, route TEXT NOT NULL REFERENCES messaging_routes(id),
    source_transport TEXT NOT NULL, context TEXT,
    PRIMARY KEY(connection,event),
    FOREIGN KEY(connection,event) REFERENCES messaging_preparation(connection,event)
);
CREATE INDEX IF NOT EXISTS messaging_quoted_context_audience ON messaging_quoted_context(route)
    WHERE context IS NOT NULL;
CREATE TABLE IF NOT EXISTS messaging_quote_checkpoints (
    connection TEXT NOT NULL, stream TEXT NOT NULL, value TEXT NOT NULL,
    PRIMARY KEY(connection,stream)
);
";

/// An authenticated provider reference. A quote travels only within the actual
/// destination that can read the source, even when sessions can be shared.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuoteReference {
    pub room: Room,
    pub message_id: String,
}

impl QuoteReference {
    pub(super) fn valid(&self) -> bool {
        valid_id(&self.message_id)
            && valid_id(&self.room.chat)
            && self.room.thread.as_deref().is_none_or(valid_id)
    }
}

/// Read older content relative to this exact trigger and destination.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScrollbackReference {
    pub before: QuoteReference,
    pub reply: Option<QuoteReference>,
    pub count: u8,
}

impl ScrollbackReference {
    pub(super) fn valid(&self, event: &InboundEvent) -> bool {
        self.before.valid()
            && self.before.room == event.room
            && event.reply_anchor.as_deref() == Some(self.before.message_id.as_str())
            && (1..=MAX_SCROLLBACK_MESSAGES).contains(&self.count)
            && self.reply.as_ref().is_none_or(|reply| {
                reply.valid()
                    && reply.room == self.before.room
                    && reply.message_id != self.before.message_id
            })
    }
}

pub struct ScrollbackQuote {
    pub source: QuoteReference,
    pub text: String,
}

/// Result of the adapter's scoped provider lookup. The returned source must
/// come from the authenticated lookup, rather than echoing request parameters.
pub enum QuoteLookup {
    Found {
        source: QuoteReference,
        text: String,
    },
    Unavailable,
    Scrollback {
        messages: Vec<hermes_engine::ChannelHistoryMessage>,
        quote: Option<ScrollbackQuote>,
    },
}

/// Quotation and any contextual cursor observed during the same provider read.
pub struct QuoteHydration {
    /// Supply a body only if the original handoff contained just a header.
    pub message: Option<Message>,
    pub quote: QuoteLookup,
    pub advance: Option<Advance>,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
struct QuoteBinding {
    route: String,
    route_revision: i64,
    policy_revision: i64,
    transport: String,
}

#[derive(Serialize, Deserialize)]
struct QuoteContext {
    source: QuoteReference,
    binding: QuoteBinding,
    /// None is an explicit provider report of unavailable context.
    text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scrollback: Option<StoredScrollback>,
}

#[derive(Serialize, Deserialize)]
struct StoredScrollback {
    reply: Option<String>,
    messages: Vec<hermes_engine::ChannelHistoryMessage>,
}

pub(super) struct QuoteCommit {
    context: QuoteContext,
    advance: Option<Advance>,
}

fn context_payload(
    source: &QuoteReference,
    expected: Option<ScrollbackReference>,
    result: QuoteLookup,
) -> Result<(Option<String>, Option<StoredScrollback>), GatewayError> {
    let valid_text = |text: &str| text.len() <= MAX_QUOTE_BYTES && !text.contains('\0');
    match (expected, result) {
        (
            None,
            QuoteLookup::Found {
                source: actual,
                text,
            },
        ) if actual == *source && valid_text(&text) => Ok((Some(text), None)),
        (None, QuoteLookup::Unavailable) => Ok((None, None)),
        (Some(expected), QuoteLookup::Scrollback { messages, quote }) => {
            let mut ids = std::collections::BTreeSet::new();
            if messages.len() > usize::from(expected.count)
                || messages.iter().any(|entry| {
                    !valid_id(&entry.message_id)
                        || !valid_id(&entry.author_id)
                        || entry.message_id == source.message_id
                        || !ids.insert(&entry.message_id)
                        || entry.text.len() > MAX_SCROLLBACK_MESSAGE_BYTES
                        || entry.text.contains('\0')
                })
                || messages.iter().map(|entry| entry.text.len()).sum::<usize>()
                    > MAX_SCROLLBACK_BYTES
            {
                return Err(GatewayError::Invalid);
            }
            let text = match quote {
                Some(quote)
                    if expected.reply.as_ref() == Some(&quote.source)
                        && valid_text(&quote.text) =>
                {
                    Some(quote.text)
                }
                None => None,
                _ => return Err(GatewayError::Invalid),
            };
            let reply = expected.reply.map(|reply| reply.message_id);
            Ok((text, Some(StoredScrollback { reply, messages })))
        }
        _ => Err(GatewayError::Invalid),
    }
}

impl QuoteBinding {
    fn position(&self, stream: &str) -> Result<String, GatewayError> {
        db(store::key("quote-checkpoint", &(self, stream)))
    }
}

struct QuoteRead {
    event: InboundEvent,
    source: QuoteReference,
    binding: QuoteBinding,
    scrollback: Option<ScrollbackReference>,
}

enum Readiness {
    Complete(IntakeReceipt),
    Read(QuoteRead),
}

impl Store {
    fn quote_destination_is_isolated(&self, route: &str) -> Result<bool> {
        self.read(|db| quote_destination_is_isolated(db, route, None))
    }

    fn quote_context(&self, connection: &str, event: &str) -> Result<Option<QuoteContext>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT context FROM messaging_quoted_context
                 WHERE connection = ?1 AND event = ?2 AND context IS NOT NULL",
                    params![connection, event],
                    |row| store::decode(row, 0),
                )
                .optional()?)
        })
    }

    fn delivery_quote(&self, connection: &str, delivery: &str) -> Result<Option<QuoteContext>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT q.context FROM messaging_quoted_context q
                 JOIN messaging_effect_sources s ON s.event = q.event
                 WHERE q.connection = ?1 AND s.delivery = ?2 AND q.context IS NOT NULL",
                    params![connection, delivery],
                    |row| store::decode(row, 0),
                )
                .optional()?)
        })
    }

    pub(super) fn refuse_quoted_input(&self, route: &Route, event: &str) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE messaging_ingress
                 SET stage = 'refused', disposition = ?3, reason = 'quote_authorization_changed', input = NULL
                 WHERE route = ?1 AND event = ?2 AND stage NOT IN ('done','refused')",
                params![route.id, event, serde_json::to_string(&Disposition::Forbidden)?],
            )?;
            Ok(())
        })
    }

    fn quote_transport_matches(
        &self,
        connection: &str,
        event: &str,
        transport: &str,
    ) -> Result<bool> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_quoted_context
                 WHERE connection = ?1 AND event = ?2 AND source_transport = ?3)",
                params![connection, event, transport],
                |row| row.get(0),
            )?)
        })
    }
}

pub(super) fn stage_quote(
    tx: &Connection,
    connection: &str,
    event: &str,
    route: &str,
    transport: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_quoted_context (connection,event,route,source_transport)
         VALUES (?1,?2,?3,?4)",
        params![connection, event, route, transport],
    )?;
    Ok(())
}

fn read_checkpoint(db: &Connection, connection: &str, stream: &str) -> Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT value FROM messaging_quote_checkpoints WHERE connection = ?1 AND stream = ?2",
            params![connection, stream],
            |row| row.get(0),
        )
        .optional()?)
}

fn quote_destination_is_isolated(
    db: &Connection,
    route: &str,
    revision: Option<i64>,
) -> Result<bool> {
    Ok(db.query_row(
        "SELECT NOT EXISTS(SELECT 1 FROM messaging_route_bindings
           WHERE destination = ?1 OR binding = ?1)
         AND NOT EXISTS(SELECT 1 FROM messaging_routes WHERE id = ?1 AND blocked)
         AND (?2 IS NULL OR EXISTS(SELECT 1 FROM messaging_routes
           WHERE id = ?1 AND revision = ?2))",
        params![route, revision],
        |row| row.get(0),
    )?)
}

impl QuoteCommit {
    pub(super) fn binding_current(&self, tx: &Connection) -> Result<bool> {
        let binding = &self.context.binding;
        quote_destination_is_isolated(tx, &binding.route, Some(binding.route_revision))
    }

    pub(super) fn checkpoint_current(&self, tx: &Connection, connection: &str) -> Result<bool> {
        match &self.advance {
            Some(advance) => {
                Ok(read_checkpoint(tx, connection, &advance.stream)? == advance.previous)
            }
            None => Ok(true),
        }
    }

    pub(super) fn persist(&self, tx: &Connection, connection: &str, event: &str) -> Result<()> {
        let changed = tx.execute(
            "UPDATE messaging_quoted_context SET context = ?3
             WHERE connection = ?1 AND event = ?2 AND source_transport = ?4 AND context IS NULL",
            params![
                connection,
                event,
                serde_json::to_string(&self.context)?,
                self.context.binding.transport
            ],
        )?;
        anyhow::ensure!(changed == 1, "Quote preparation changed before commit");
        if let Some(advance) = &self.advance {
            tx.execute(
                "INSERT INTO messaging_quote_checkpoints (connection,stream,value) VALUES (?1,?2,?3)
                 ON CONFLICT(connection,stream) DO UPDATE SET value = excluded.value",
                params![connection, advance.stream, advance.checkpoint],
            )?;
        }
        Ok(())
    }
}

impl Ingress {
    pub(super) fn quote_transport(&self) -> Result<String, GatewayError> {
        db(store::key(
            "quote-transport",
            &(&self.connection, self.installation()),
        ))
    }

    pub fn allows_quote(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &str,
        source: &QuoteReference,
    ) -> Result<bool, GatewayError> {
        Ok(source.room == event.room
            && matches!(&event.body, InboundBody::Message(_))
            && self
                .authorized_identity(&state.policy, &event.room, &event.sender)?
                .is_some_and(|(_, kind)| kind == super::ActorKind::Person)
            && db(self.store.quote_destination_is_isolated(route))?)
    }

    pub(super) fn quote_preparation_allowed(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &str,
        source: &QuoteReference,
    ) -> Result<bool, GatewayError> {
        Ok(self.allows_quote(state, event, route, source)?
            && self
                .route_candidate(&state.policy, event, true)?
                .is_ok_and(|candidate| candidate.id == route)
            && db(self.store.quote_transport_matches(
                &self.connection,
                &event.id,
                &self.quote_transport()?,
            ))?)
    }

    fn quote_binding(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &str,
        source: &QuoteReference,
    ) -> Result<Option<QuoteBinding>, GatewayError> {
        if !self.quote_preparation_allowed(state, event, route, source)? {
            return Ok(None);
        }
        self.check_input_protection(&state.policy, event)?;
        let current = db(self.store.route(route))?.ok_or(GatewayError::Forbidden)?;
        Ok(Some(QuoteBinding {
            route: route.to_owned(),
            route_revision: current.revision,
            policy_revision: state.revision,
            transport: self.quote_transport()?,
        }))
    }

    fn pending_quote(
        &self,
        state: &State,
        staged: Staged,
        token: &str,
    ) -> Result<Readiness, GatewayError> {
        if staged.token != token {
            return Err(GatewayError::Forbidden);
        }
        let (source, scrollback) = match staged.purpose {
            PreparationPurpose::Quote(source) => (source, None),
            PreparationPurpose::Scrollback(scrollback) => {
                (scrollback.before.clone(), Some(scrollback))
            }
            _ => return Err(GatewayError::Unsupported),
        };
        if staged.receipt.disposition != Disposition::Deferred {
            if !db(self.store.quote_transport_matches(
                &self.connection,
                &staged.receipt.id,
                &self.quote_transport()?,
            ))? {
                return Err(GatewayError::Forbidden);
            }
            return Ok(Readiness::Complete(IntakeReceipt {
                repeated: true,
                ..staged.receipt
            }));
        }
        let event = staged.event.ok_or(GatewayError::Forbidden)?;
        let route = staged
            .receipt
            .route
            .as_deref()
            .ok_or(GatewayError::Forbidden)?;
        if staged.expires_ms <= chrono::Utc::now().timestamp_millis() {
            return Err(GatewayError::Forbidden);
        }
        let binding = self
            .quote_binding(state, &event, route, &source)?
            .ok_or(GatewayError::Forbidden)?;
        Ok(Readiness::Read(QuoteRead {
            event,
            source,
            binding,
            scrollback,
        }))
    }

    /// Read the committed contextual cursor for this staged quote's exact
    /// binding. Context cursors occupy a separate scoped stream from intake.
    pub async fn quote_checkpoint(
        &self,
        event_id: &str,
        token: &str,
        stream: &str,
    ) -> Result<Option<String>, GatewayError> {
        if !valid_id(event_id) || !valid_id(token) || !valid_id(stream) {
            return Err(GatewayError::Invalid);
        }
        let (event, token, stream) = (event_id.to_owned(), token.to_owned(), stream.to_owned());
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let staged = db(scope.store.preparation(&scope.connection, &event))?
                    .ok_or(GatewayError::Forbidden)?;
                let Readiness::Read(read) = scope.pending_quote(state, staged, &token)? else {
                    return Err(GatewayError::Forbidden);
                };
                let stream = read.binding.position(&stream)?;
                db(scope
                    .store
                    .read(|tx| read_checkpoint(tx, &scope.connection, &stream)))
            })
        })
        .await
    }

    /// Hydrate the source declared in the durable handoff. Provider access
    /// occurs only after authorization, and completion checks authorization
    /// again, before advancing the optional contextual cursor.
    pub async fn hydrate_quote<F, Fut>(
        &self,
        event_id: &str,
        token: &str,
        lookup: F,
    ) -> Result<IntakeReceipt, GatewayError>
    where
        F: FnOnce(QuoteReference) -> Fut,
        Fut: Future<Output = Result<QuoteHydration, GatewayError>>,
    {
        if !valid_id(event_id) || !valid_id(token) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let (event, read_token) = (event_id.to_owned(), token.to_owned());
        let read = blocking(move || {
            scope.checked(true, |state| {
                let staged = db(scope.store.preparation(&scope.connection, &event))?
                    .ok_or(GatewayError::Forbidden)?;
                scope.pending_quote(state, staged, &read_token)
            })
        })
        .await?;
        let read = match read {
            Readiness::Complete(receipt) => return Ok(receipt),
            Readiness::Read(read) => read,
        };
        let looked_up = lookup(read.source.clone()).await;
        let scope = self.clone();
        let token = token.to_owned();
        let receipt = blocking(move || {
            scope.checked(true, |state| {
                let mut event = read.event;
                let staged = db(scope.store.preparation(&scope.connection, &event.id))?
                    .ok_or(GatewayError::Forbidden)?;
                if staged.token != token {
                    return Err(GatewayError::Forbidden);
                }
                if staged.receipt.disposition != Disposition::Deferred {
                    return Ok(IntakeReceipt {
                        repeated: true,
                        ..staged.receipt
                    });
                }
                let mut decision = if staged.expires_ms <= chrono::Utc::now().timestamp_millis() {
                    Err(Disposition::Expired)
                } else if scope
                    .quote_binding(state, &event, &read.binding.route, &read.source)?
                    .as_ref()
                    != Some(&read.binding)
                {
                    Err(Disposition::Forbidden)
                } else {
                    match &looked_up {
                        Err(GatewayError::Forbidden) => Err(Disposition::Forbidden),
                        Err(GatewayError::Invalid) => Err(Disposition::Invalid),
                        Err(GatewayError::Unsupported) => Err(Disposition::Unsupported),
                        _ => scope.route_candidate(&state.policy, &event, true)?,
                    }
                };
                let quote = if decision.is_ok() {
                    let QuoteHydration {
                        message,
                        quote,
                        mut advance,
                    } = looked_up?;
                    if let Some(message) = message {
                        if !matches!(&event.body, InboundBody::Message(original)
                            if original.attachments.is_empty()
                                && (original.text.is_empty() || message.text == original.text
                                    || message.text.strip_prefix(&original.text)
                                        .is_some_and(|more| more.starts_with('\n'))))
                        {
                            return Err(GatewayError::Conflict);
                        }
                        event.body = InboundBody::Message(message);
                    }
                    decision = scope.route_event(&state.policy, &event)?;
                    if !valid_advance(advance.as_ref()) {
                        return Err(GatewayError::Invalid);
                    }
                    if let Some(advance) = &mut advance {
                        advance.stream = read.binding.position(&advance.stream)?;
                    }
                    let (text, scrollback) = context_payload(&read.source, read.scrollback, quote)?;
                    Some(QuoteCommit {
                        context: QuoteContext {
                            source: read.source,
                            binding: read.binding,
                            text,
                            scrollback,
                        },
                        advance,
                    })
                } else {
                    None
                };
                let fingerprint = db(store::key(
                    "prepared-quote",
                    &quote.as_ref().map(|q| &q.context),
                ))?;
                let id = event.id.clone();
                let candidate = scope.intake_candidate(&state.policy, event, decision, None)?;
                db(scope.store.finish_preparation(
                    &scope.connection,
                    &id,
                    &token,
                    &fingerprint,
                    candidate,
                    quote,
                ))?
            })
        })
        .await?;
        self.notify();
        Ok(receipt)
    }

    fn checked_quote(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &Route,
    ) -> Result<Option<QuoteContext>, GatewayError> {
        let Some(quote) = db(self.store.quote_context(&self.connection, &event.id))? else {
            return Ok(None);
        };
        if state.revision != quote.binding.policy_revision
            || self
                .quote_binding(state, event, &route.id, &quote.source)?
                .as_ref()
                != Some(&quote.binding)
        {
            return Err(GatewayError::Forbidden);
        }
        Ok(Some(quote))
    }

    pub(super) fn recheck_quote(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &Route,
    ) -> Result<bool, GatewayError> {
        match self.checked_quote(state, event, route) {
            Ok(_) => Ok(true),
            Err(GatewayError::Forbidden) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn recheck_quote_delivery(
        &self,
        state: &State,
        route: &Route,
        delivery: &str,
    ) -> Result<bool, GatewayError> {
        let Some(quote) = db(self.store.delivery_quote(&self.connection, delivery))? else {
            return Ok(true);
        };
        Ok(quote.source.room == route.room
            && quote.binding.route == route.id
            && quote.binding.route_revision == route.revision
            && quote.binding.policy_revision == state.revision
            && quote.binding.transport == self.quote_transport()?
            && db(self.store.quote_destination_is_isolated(&route.id))?)
    }

    /// Pin the logical input, not its content, across scheduling. The engine
    /// rechecks just before the provider sees the ephemeral context.
    pub(super) fn prepared_context_source(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &Route,
    ) -> Result<Option<hermes_engine::ChannelContextSource>, GatewayError> {
        if self.checked_quote(state, event, route)?.is_none() {
            return Ok(None);
        }
        let (scope, event, route) = (self.clone(), event.clone(), route.clone());
        Ok(Some(Arc::new(move || {
            let quote = scope.checked(true, |state| {
                scope
                    .checked_quote(state, &event, &route)?
                    .ok_or(GatewayError::Forbidden)
            })?;
            let (reply, scrollback) = match quote.scrollback {
                Some(history) => (history.reply, history.messages),
                None => (Some(quote.source.message_id), Vec::new()),
            };
            Ok(hermes_engine::ChannelContext {
                quote: reply.map(|message_id| hermes_engine::ChannelQuote {
                    message_id,
                    text: quote.text,
                }),
                scrollback,
            })
        })))
    }
}
