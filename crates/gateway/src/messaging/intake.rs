use super::{
    quotes::{QuoteCommit, QuoteReference, ScrollbackReference, stage_quote},
    runtime::{Ingress, MAX_BATCH, State, blocking, db, now, valid_advance, valid_id},
    store::{self, Candidate, Store},
    types::*,
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_handoffs (
    connection TEXT NOT NULL, id TEXT NOT NULL,
    fingerprint TEXT NOT NULL, receipts TEXT NOT NULL,
    PRIMARY KEY(connection, id)
);
CREATE TABLE IF NOT EXISTS messaging_preparation (
    connection TEXT NOT NULL, event TEXT NOT NULL, token TEXT NOT NULL,
    purpose TEXT NOT NULL, expires INTEGER NOT NULL, finished TEXT,
    PRIMARY KEY(connection, event)
);
CREATE INDEX IF NOT EXISTS messaging_preparation_deadline ON messaging_preparation(connection, expires)
    WHERE finished IS NULL;
";

/// A bridge envelope has its own replay identity. The event IDs within it
/// identify logical input independently of the bridge or poll checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Handoff {
    pub id: String,
    pub items: Vec<HandoffItem>,
    pub advance: Option<Advance>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HandoffItem {
    pub event: InboundEvent,
    pub preparation: Option<Preparation>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Preparation {
    pub token: String,
    pub purpose: PreparationPurpose,
    pub expires_ms: i64,
    /// The protection class of a delayed input. This contains no key material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protection: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationPurpose {
    Body,
    Media,
    ProtectedContent,
    Context,
    Burst,
    Quote(QuoteReference),
    Scrollback(ScrollbackReference),
    Thread { id: String, inline_on_refusal: bool },
}

impl PreparationPurpose {
    pub fn context_reference(&self) -> Option<&QuoteReference> {
        match self {
            Self::Quote(source) => Some(source),
            Self::Scrollback(request) => Some(&request.before),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeferredInput {
    pub event: InboundEvent,
    pub preparation: Preparation,
}

impl HandoffItem {
    pub(super) fn valid(&self) -> bool {
        super::runtime::valid_input_id(&self.event.id)
            && !matches!(self.event.body, InboundBody::Wake)
            && self.preparation.as_ref().is_none_or(|preparation| {
                valid_id(&preparation.token)
                    && preparation.expires_ms > 0
                    && preparation.protection.as_deref().is_none_or(valid_id)
                    && matches!(self.event.body, InboundBody::Message(_))
                    && preparation.purpose.context_reference().is_none_or(QuoteReference::valid)
                    && !matches!(&preparation.purpose, PreparationPurpose::Scrollback(request)
                        if !request.valid(&self.event))
                    && !matches!(&preparation.purpose, PreparationPurpose::Thread { id, .. }
                        if !valid_id(id) || self.event.room.kind != RoomKind::Group
                            || self.event.room.thread.is_some() || self.event.reply_anchor.as_ref() != Some(id))
            })
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum HandoffMode {
    Independent,
    TextBurst,
    AddressedBurst,
}

impl Ingress {
    /// Commit all dispositions before an acknowledgement or an SDK-owned
    /// checkpoint can advance. The bridge can replay this call after restart.
    pub async fn stage_handoff(&self, handoff: Handoff) -> Result<IntakeResult, GatewayError> {
        self.stage(handoff, HandoffMode::Independent).await
    }

    /// Merge a compatible text burst at durable admission. Every original ID
    /// retains a disposition even when several inputs form one turn.
    pub async fn stage_text_burst(&self, handoff: Handoff) -> Result<IntakeResult, GatewayError> {
        self.stage_burst(handoff, HandoffMode::TextBurst).await
    }

    /// Coalesce continuations of a newly admitted addressed message. Keep
    /// each original anchor in its receipt and only the first in the turn.
    pub async fn stage_addressed_burst(&self, handoff: Handoff) -> Result<IntakeResult, GatewayError> {
        self.stage_burst(handoff, HandoffMode::AddressedBurst).await
    }

    async fn stage_burst(&self, handoff: Handoff, mode: HandoffMode) -> Result<IntakeResult, GatewayError> {
        let first = handoff.items.first().ok_or(GatewayError::Invalid)?;
        let addressed = mode == HandoffMode::AddressedBurst;
        let mut identities = std::collections::BTreeSet::new();
        if !handoff.items.iter().enumerate().all(|(index, item)| {
            item.event.room == first.event.room
                && item.event.sender.id == first.event.sender.id
                && matches!(&item.event.body, InboundBody::Message(message) if message.attachments.is_empty())
                && if addressed {
                    item.event.mentioned && (index == 0 || item.preparation.is_none())
                        && identities.insert(&item.event.id)
                } else {
                    item.preparation.is_none() && item.event.reply_anchor == first.event.reply_anchor
                }
        }) {
            return Err(GatewayError::Invalid);
        }
        self.stage(handoff, mode).await
    }

    async fn stage(&self, handoff: Handoff, mode: HandoffMode) -> Result<IntakeResult, GatewayError> {
        if !valid_id(&handoff.id)
            || handoff.items.is_empty()
            || handoff.items.len() > MAX_BATCH
            || !valid_advance(handoff.advance.as_ref())
            || handoff.items.iter().any(|item| !item.valid())
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let result = blocking(move || {
            scope.checked(true, |state| {
                let Handoff { id, items, advance } = handoff;
                let id = scope.scoped_transport_key("transport-handoff", &id)?;
                let advance = scope.scoped_advance(advance)?;
                let mut candidates = Vec::with_capacity(items.len());
                for item in items {
                    let decision = scope.route_candidate(
                        &state.policy,
                        &item.event,
                        item.preparation.is_some(),
                    )?;
                    let declared = item.preparation.as_ref().and_then(|p| p.protection.as_deref());
                    let mut candidate = scope.intake_candidate(&state.policy, item.event, decision, declared)?;
                    if let Some(preparation) = &item.preparation
                        && (preparation.purpose == PreparationPurpose::ProtectedContent
                            || candidate.protection.is_some())
                        && (candidate.protection.is_none()
                            || !matches!(&candidate.event.body, InboundBody::Message(message)
                                if message.text.is_empty() && message.attachments.is_empty()))
                    {
                        candidate.decision = candidate.decision.and(Err(Disposition::Invalid));
                    }
                    if let (Some(source), Ok(route)) =
                        (item.preparation.as_ref().and_then(|p| p.purpose.context_reference()), &candidate.decision)
                        && !scope.allows_quote(state, &candidate.event, &route.id, source)?
                    {
                        candidate.decision = Err(Disposition::Forbidden);
                    }
                    if matches!(item.preparation.as_ref().map(|p| &p.purpose), Some(PreparationPurpose::Thread { .. }))
                        && (!scope.capabilities().effects.prepare_resources || !scope.capabilities().threads
                            || candidate.protection.is_some())
                    {
                        candidate.decision = candidate.decision.and(Err(Disposition::Unsupported));
                    }
                    candidates.push((candidate, item.preparation));
                }
                db(scope.store.handoff(
                    &scope.connection,
                    state.revision,
                    &id,
                    candidates,
                    advance.as_ref(),
                    mode,
                    &scope.quote_transport()?,
                ))?
            })
        })
        .await?;
        self.notify();
        Ok(result)
    }

    pub async fn pending_preparations(&self) -> Result<Vec<DeferredInput>, GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let mut pending = Vec::new();
                for (route, input) in db(scope.store.preparations(&scope.connection))? {
                    if scope.allows_preparation(state, &input.event, &route, &input.preparation.purpose)? {
                        pending.push(input);
                    }
                }
                Ok(pending)
            })
        })
        .await
    }

    /// Recheck the staged header immediately before opening the protected body.
    pub async fn authorize_preparation(
        &self,
        event: String,
        token: String,
    ) -> Result<(), GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let staged = db(scope.store.preparation(&scope.connection, &event))?;
                if let Some(staged) = staged
                    && staged.token == token
                    && staged.purpose.context_reference().is_none()
                    && (!matches!(staged.purpose, PreparationPurpose::Thread { .. })
                        || scope.prepared_thread_selected(&event)?)
                    && scope.pending_authorized(state, &staged)?
                    && let Some(event) = &staged.event
                {
                    return scope.check_input_protection(&state.policy, event);
                }
                Err(GatewayError::Forbidden)
            })
        })
        .await
    }

    pub(super) fn can_prepare(&self, state: &State, event: &str) -> Result<bool, GatewayError> {
        if let Some(staged) = db(self.store.preparation(&self.connection, event))? {
            return self.pending_authorized(state, &staged);
        }
        Ok(false)
    }

    fn pending_authorized(&self, state: &State, staged: &Staged) -> Result<bool, GatewayError> {
        if staged.expires_ms > now()
            && staged.receipt.disposition == Disposition::Deferred
            && let (Some(event), Some(route)) = (&staged.event, &staged.receipt.route)
        {
            return self.allows_preparation(state, event, route, &staged.purpose);
        }
        Ok(false)
    }

    fn allows_preparation(
        &self,
        state: &State,
        event: &InboundEvent,
        route: &str,
        purpose: &PreparationPurpose,
    ) -> Result<bool, GatewayError> {
        if let Some(source) = purpose.context_reference() {
            return self.quote_preparation_allowed(state, event, route, source);
        }
        Ok(self
            .route_candidate(&state.policy, event, true)?
            .is_ok_and(|current| current.id == route)
            && self.thread_source_allowed(state, &event.room, &event.sender, &event.id)?)
    }

    /// Complete a durable staged input. The adapter supplies only hydrated
    /// content, never a new source actor, destination or logical identity.
    pub async fn complete_preparation(
        &self,
        event_id: String,
        token: String,
        message: Message,
    ) -> Result<IntakeReceipt, GatewayError> {
        if !valid_id(&event_id) || !valid_id(&token) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let result = blocking(move || {
            scope.checked(true, |state| {
                let content = db(store::key("prepared-content", &message))?;
                let staged = db(scope.store.preparation(&scope.connection, &event_id))?
                    .ok_or(GatewayError::Invalid)?;
                if token != staged.token {
                    return Err(GatewayError::Forbidden);
                }
                if staged.purpose.context_reference().is_some() {
                    return Err(GatewayError::Unsupported);
                }
                if let Some(finished) = staged.finished {
                    return if finished == content {
                        Ok(IntakeReceipt {
                            repeated: true,
                            ..staged.receipt
                        })
                    } else {
                        Err(GatewayError::Conflict)
                    };
                }
                if staged.receipt.disposition == Disposition::Expired {
                    return Ok(IntakeReceipt {
                        repeated: true,
                        ..staged.receipt
                    });
                }
                if matches!(staged.purpose, PreparationPurpose::Thread { .. })
                    && !scope.prepared_thread_selected(&event_id)?
                {
                    return Err(GatewayError::TemporarilyUnavailable);
                }
                let mut event = staged.event.ok_or(GatewayError::Invalid)?;
                if !scope.thread_source_allowed(state, &event.room, &event.sender, &event_id)? {
                    return Err(GatewayError::Forbidden);
                }
                let expired = staged.expires_ms <= chrono::Utc::now().timestamp_millis();
                if !expired {
                    scope.check_input_protection(&state.policy, &event)?;
                }
                event.body = InboundBody::Message(message);
                let decision = if expired { Err(Disposition::Expired) }
                    else { scope.route_event(&state.policy, &event)? };
                let candidate = scope.intake_candidate(&state.policy, event, decision, None)?;
                db(scope.store.finish_preparation(
                    &scope.connection, &event_id, &token, &content, candidate, None,
                ))?
            })
        })
        .await?;
        self.notify();
        Ok(result)
    }
}

pub struct Staged {
    pub(super) token: String,
    pub(super) purpose: PreparationPurpose,
    pub(super) expires_ms: i64,
    finished: Option<String>,
    pub(super) receipt: IntakeReceipt,
    pub(super) event: Option<InboundEvent>,
}

impl Staged {
    pub fn into_event(self) -> Option<InboundEvent> {
        self.event
    }
}

impl Store {
    fn handoff(
        &self,
        connection: &str,
        revision: i64,
        id: &str,
        candidates: Vec<(Candidate, Option<Preparation>)>,
        advance: Option<&Advance>,
        mode: HandoffMode,
        quote_transport: &str,
    ) -> Result<Result<IntakeResult, GatewayError>> {
        let identities = candidates
            .iter()
            .map(|(candidate, preparation)| {
                Ok((
                    &candidate.event.id,
                    store::fingerprint(&candidate.event)?,
                    preparation,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let addressed = mode == HandoffMode::AddressedBurst;
        let merge = mode != HandoffMode::Independent;
        let kind = if addressed { "addressed-handoff" } else { "handoff" };
        let fingerprint = store::key(kind, &(identities, advance, merge))?;
        self.write(|tx| {
            let old = tx.query_row(
                "SELECT fingerprint, receipts FROM messaging_handoffs WHERE connection = ?1 AND id = ?2",
                params![connection, id],
                |row| Ok((row.get::<_,String>(0)?, store::decode::<Vec<IntakeReceipt>>(row, 1)?)),
            ).optional()?;
            if let Some((stored, mut receipts)) = old {
                if stored != fingerprint {
                    return Ok(Err(GatewayError::Conflict));
                }
                for receipt in &mut receipts {
                    if let Some(current) = store::intake_receipt(tx, connection, &receipt.id)?
                        && receipt.disposition != Disposition::Conflict
                    {
                        *receipt = current;
                    }
                    receipt.repeated = true;
                }
                let checkpoint = advance.map(|a| store::read_checkpoint(tx, connection, &a.stream))
                    .transpose()?.flatten();
                return Ok(Ok(IntakeResult { receipts, checkpoint }));
            }
            if !store::check_advance(tx, connection, advance)? {
                return Ok(Err(GatewayError::CheckpointChanged));
            }
            for (candidate, _) in &candidates {
                if super::repair::blocks_intake(tx, connection, &candidate.event)? {
                    return Ok(Err(GatewayError::TemporarilyUnavailable));
                }
            }
            let merge_event = if merge && candidates.len() > 1
                && candidates.iter().all(|(candidate, _)| candidate.protection == candidates[0].0.protection)
            {
                let mut event = candidates[0].0.event.clone();
                let merged = candidates.iter().filter_map(|(candidate,_)| match &candidate.event.body {
                    InboundBody::Message(message) => Some(message.text.as_str()),
                    _ => None,
                }).collect::<Vec<_>>().join("\n");
                if merged.len() > super::runtime::MAX_TEXT {
                    return Ok(Err(GatewayError::Invalid));
                }
                event.body = InboundBody::Message(Message::text(merged));
                Some(event)
            } else {
                None
            };
            let mut receipts: Vec<IntakeReceipt> = Vec::with_capacity(candidates.len());
            for (index, (mut candidate, preparation)) in candidates.into_iter().enumerate() {
                if addressed && index > 0 && !receipts.first().is_some_and(|first| {
                    !first.repeated && matches!(first.disposition, Disposition::Accepted | Disposition::Deferred)
                }) {
                    candidate.decision = Err(Disposition::Forbidden);
                }
                let prepare = preparation.is_some()
                    && preparation.as_ref().is_some_and(|p| p.expires_ms > chrono::Utc::now().timestamp_millis());
                let candidate = if preparation.is_some() && !prepare {
                    Candidate { decision: Err(Disposition::Expired), ..candidate }
                } else {
                    candidate
                };
                let receipt = store::append(tx, connection, revision, candidate, prepare)?;
                if let Some(preparation) = preparation
                    && receipt.disposition == Disposition::Deferred && !receipt.repeated
                {
                    tx.execute(
                        "INSERT INTO messaging_preparation(connection,event,token,purpose,expires)
                         VALUES (?1,?2,?3,?4,?5)",
                        params![connection, receipt.id, preparation.token,
                            serde_json::to_string(&preparation.purpose)?, preparation.expires_ms],
                    )?;
                    if preparation.purpose.context_reference().is_some() {
                        let route = receipt.route.as_deref()
                            .ok_or_else(|| anyhow::anyhow!("Quote preparation has no route"))?;
                        stage_quote(tx, connection, &receipt.id, route, quote_transport)?;
                    }
                }
                receipts.push(receipt);
            }
            if let Some(event) = merge_event
                && receipts.iter().enumerate().all(|(index, receipt)| !receipt.repeated
                    && (receipt.disposition == Disposition::Accepted || addressed && index == 0
                        && receipt.disposition == Disposition::Deferred)
                    && receipt.route == receipts[0].route)
            {
                tx.execute(
                    "UPDATE messaging_ingress SET input = ?3 WHERE connection = ?1 AND event = ?2",
                    params![connection,event.id,serde_json::to_string(&event)?],
                )?;
                let disposition = serde_json::to_string(&Disposition::Coalesced)?;
                for receipt in &mut receipts[1..] {
                    tx.execute(
                        "UPDATE messaging_ingress SET input = NULL,stage = 'done',disposition = ?3,reason = ?4
                         WHERE connection = ?1 AND event = ?2",
                        params![connection,receipt.id,disposition,event.id],
                    )?;
                    receipt.disposition = Disposition::Coalesced;
                }
            } else if addressed {
                let refused = serde_json::to_string(&Disposition::Forbidden)?;
                for receipt in receipts.iter_mut().skip(1)
                    .filter(|receipt| !receipt.repeated && receipt.disposition == Disposition::Accepted)
                {
                    tx.execute(
                        "UPDATE messaging_ingress SET input=NULL,route=NULL,stage='refused',disposition=?3,
                         reason='burst_not_admitted' WHERE connection=?1 AND event=?2",
                        params![connection,receipt.id,refused],
                    )?;
                    receipt.route = None;
                    receipt.disposition = Disposition::Forbidden;
                }
            }
            let checkpoint = store::write_advance(tx, connection, advance)?;
            tx.execute(
                "INSERT INTO messaging_handoffs(connection,id,fingerprint,receipts) VALUES (?1,?2,?3,?4)",
                params![connection, id, fingerprint, serde_json::to_string(&receipts)?],
            )?;
            Ok(Ok(IntakeResult { receipts, checkpoint }))
        })
    }

    fn preparations(&self, connection: &str) -> Result<Vec<(String, DeferredInput)>> {
        self.read(|db| {
            let mut query = db.prepare(
                "SELECT i.input,p.token,p.purpose,p.expires,i.route,protection.class FROM messaging_preparation p
                 JOIN messaging_ingress i USING(connection,event)
                 LEFT JOIN messaging_input_protection protection USING(connection,event)
                 WHERE p.connection = ?1 AND i.stage = 'preparing' AND p.expires > ?2
                 ORDER BY i.position LIMIT ?3",
            )?;
            Ok(query
                .query_map(
                    params![
                        connection,
                        chrono::Utc::now().timestamp_millis(),
                        MAX_BATCH as i64
                    ],
                    |row| {
                        Ok((
                            row.get(4)?,
                            DeferredInput {
                                event: store::decode(row, 0)?,
                                preparation: Preparation {
                                    token: row.get(1)?,
                                    purpose: store::decode(row, 2)?,
                                    expires_ms: row.get(3)?,
                                    protection: row.get(5)?,
                                },
                            },
                        ))
                    },
                )?
                .collect::<rusqlite::Result<_>>()?)
        })
    }

    pub fn preparation(&self, connection: &str, event: &str) -> Result<Option<Staged>> {
        self.read(|db| Ok(db.query_row(
            "SELECT p.token,p.expires,p.finished,i.route,i.disposition,i.input,p.purpose FROM messaging_preparation p
             JOIN messaging_ingress i USING(connection,event) WHERE p.connection = ?1 AND p.event = ?2",
            params![connection,event],
            |row| Ok(Staged { token: row.get(0)?, expires_ms: row.get(1)?, finished: row.get(2)?,
                receipt: IntakeReceipt { id: event.to_owned(), route: row.get(3)?,
                    disposition: store::decode(row, 4)?, repeated: false },
                event: store::optional_decode(row, 5)?,
                purpose: store::decode(row, 6)?,
            }),
        ).optional()?))
    }

    pub(super) fn finish_preparation(
        &self,
        connection: &str,
        id: &str,
        token: &str,
        fingerprint: &str,
        candidate: Candidate,
        quote: Option<QuoteCommit>,
    ) -> Result<Result<IntakeReceipt, GatewayError>> {
        self.write(|tx| {
            let old = store::intake_receipt(tx, connection, id)?
                .ok_or_else(|| anyhow::anyhow!("Deferred input disappeared"))?;
            if old.disposition != Disposition::Deferred {
                return Ok(Ok(old));
            }
            let mut decision =
                store::attachment_decision(tx, connection, &candidate.event, candidate.decision)?;
            if decision.is_ok()
                && let Some(quote) = &quote
                && !quote.binding_current(tx)?
            {
                decision = Err(Disposition::Forbidden);
            }
            let (disposition, body, stage) = match decision {
                Ok(route) if old.route.as_deref() == Some(route.id.as_str()) => {
                    let event = &candidate.event;
                    if let Some(class) = &candidate.protection {
                        super::protection::pin_input(tx, connection, id, &event.room, &event.sender.id, class)?;
                    }
                    (Disposition::Accepted, Some(serde_json::to_string(event)?), "pending")
                }
                Ok(_) => (Disposition::Forbidden, None, "refused"),
                Err(disposition) => (disposition, None, "refused"),
            };
            if disposition == Disposition::Accepted
                && let Some(quote) = &quote
                && !quote.checkpoint_current(tx, connection)?
            {
                return Ok(Err(GatewayError::CheckpointChanged));
            }
            let finished = tx.execute(
                "UPDATE messaging_preparation SET finished = ?4
                 WHERE connection = ?1 AND event = ?2 AND token = ?3 AND finished IS NULL",
                params![connection, id, token, fingerprint],
            )?;
            ensure!(finished == 1, "Preparation changed before commit");
            tx.execute(
                "UPDATE messaging_ingress SET stage = ?3, disposition = ?4, input = ?5
                 WHERE connection = ?1 AND event = ?2 AND stage = 'preparing'",
                params![
                    connection,
                    id,
                    stage,
                    serde_json::to_string(&disposition)?,
                    body
                ],
            )?;
            if disposition == Disposition::Accepted && let Some(quote) = quote {
                quote.persist(tx, connection, id)?;
            }
            Ok(Ok(IntakeReceipt { disposition, ..old }))
        })
    }

    pub(super) fn expire_preparations(&self, connection: &str, now: i64) -> Result<()> {
        self.write(|tx| {
            tx.execute(
                "UPDATE messaging_ingress SET stage = 'refused', reason = 'preparation_expired', input = NULL, disposition = ?3
                 WHERE connection = ?1 AND stage = 'preparing' AND event IN
                   (SELECT event FROM messaging_preparation WHERE connection = ?1 AND expires <= ?2)",
                params![connection, now, serde_json::to_string(&Disposition::Expired)?],
            )?;
            super::prepared_threads::retire_unstarted(tx, connection)
        })
    }
}
