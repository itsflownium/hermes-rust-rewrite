use crate::{
    common::{self, Media, sender},
    outbound,
};
use hermes_gateway_core::messaging::{
    AdapterFuture, AttachmentManifest, Command, Control, DeferredInput, Disposition, GatewayError,
    Handoff, HandoffItem, InboundBody, InboundEvent, Ingress, MediaPurpose, MediaSource, Message,
    Preparation, PreparationPurpose, QuoteHydration, QuoteLookup, QuoteReference, Room,
    media::{MAX_ADAPTER_FILE, MAX_ADAPTER_INPUT_BYTES, PreparedAudio, valid_name},
    runtime::{MAX_TEXT, blocking, db, now},
    storage, voice_codec,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{pin::Pin, time::Duration};
use tokio::io::AsyncRead;
use tokio_util::sync::CancellationToken;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_whatsapp_sources (
    connection TEXT NOT NULL, event TEXT NOT NULL, room TEXT NOT NULL, anchor TEXT NOT NULL,
    actor TEXT NOT NULL, fingerprint TEXT NOT NULL, data TEXT,
    expires INTEGER NOT NULL, PRIMARY KEY(connection,event)
);
CREATE INDEX IF NOT EXISTS messaging_whatsapp_quote
    ON messaging_whatsapp_sources(connection,room,anchor);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_refusals (
    connection TEXT NOT NULL, occurrence TEXT NOT NULL, reason TEXT NOT NULL,
    PRIMARY KEY(connection,occurrence)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_voice (
    connection TEXT NOT NULL, event TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_bursts (
    connection TEXT NOT NULL, token TEXT NOT NULL, members TEXT NOT NULL,
    PRIMARY KEY(connection,token)
);
";

const PREPARATION_MS: i64 = 15 * 60 * 1000;
const CONTEXT_MS: i64 = 2 * 24 * 60 * 60 * 1000;
pub(crate) const MAX_FILES: usize = 8;
pub(crate) type MediaStream = Pin<Box<dyn AsyncRead + Send>>;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct Source {
    pub event: String,
    pub room: Room,
    pub actor: String,
    pub anchor: String,
    pub provider_chat: String,
    pub provider_actor: Option<String>,
    pub text: String,
    pub media: Vec<Media>,
    pub quote: Option<String>,
    pub choice: Option<String>,
    pub mentioned: bool,
    pub owner: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub speech_mentions: Vec<String>,
}

impl Source {
    fn event(&self, body: InboundBody) -> InboundEvent {
        let background = matches!(body, InboundBody::Command(Command::Background { .. }));
        InboundEvent {
            id: self.event.clone(),
            room: self.room.clone(),
            sender: sender(&self.actor),
            reply_anchor: (!background).then(|| self.anchor.clone()),
            mentioned: self.mentioned,
            body,
        }
    }
}

pub(crate) struct MediaDescription {
    pub name: String,
    pub size: usize,
    pub endpoint: String,
}

pub(crate) trait MediaReader: Sync {
    fn signal<'a>(
        &'a self,
        _ingress: &'a Ingress,
        _source: &'a Source,
        _cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async {})
    }

    fn describe<'a>(
        &'a self,
        ingress: &'a Ingress,
        media: &'a Media,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaDescription, GatewayError>>;

    fn open<'a>(
        &'a self,
        media: &'a Media,
        address: &'a MediaDescription,
        limit: usize,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaStream, GatewayError>>;
}

pub(crate) fn refuse(
    ingress: &Ingress,
    occurrence: &str,
    reason: &str,
) -> Result<(), GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_whatsapp_refusals VALUES(?1,?2,?3)",
                params![ingress.connection_name(), occurrence, reason],
            )?;
            Ok(())
        }))
    })
}

pub(crate) async fn stage(ingress: &Ingress, source: Source) -> Result<bool, GatewayError> {
    if source.text.len() > MAX_TEXT || source.text.contains('\0') || source.media.len() > MAX_FILES
    {
        refuse(ingress, &source.event, "invalid_content")?;
        return Ok(false);
    }
    common::observe(ingress, &source.room, &source.actor).await?;
    if source.text == "/pair" && source.choice.is_none() {
        let replay = ingress.checked(true, |_| {
            db(ingress.storage().read(|conn| {
                Ok(conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM messaging_whatsapp_refusals
                WHERE connection=?1 AND occurrence=?2)",
                    params![ingress.connection_name(), source.event],
                    |row| row.get::<_, bool>(0),
                )?)
            }))
        })?;
        if !replay {
            let outcome = match ingress
                .begin_pairing(source.room.clone(), sender(&source.actor))
                .await
            {
                Ok(_) => "pairing_requested",
                Err(GatewayError::Forbidden | GatewayError::Invalid) => "pairing_unavailable",
                Err(error) => return Err(error),
            };
            refuse(ingress, &source.event, outcome)?;
        }
        return Ok(false);
    }
    if !common::permits(ingress, &source.room, &source.actor)? {
        refuse(ingress, &source.event, "actor_forbidden")?;
        return Ok(false);
    }
    let scope = ingress.clone();
    let persisted = source.clone();
    let prepared = blocking(move || save_source(&scope, &persisted)).await?;
    let Some(expires_ms) = prepared else {
        refuse(ingress, &source.event, "conflicting_source")?;
        return Ok(false);
    };
    let start = source.event(InboundBody::Message(Message::text("")));
    let command = source
        .speech_mentions
        .is_empty()
        .then(|| common::action(&source.text))
        .flatten();
    let answer = if source.choice.is_some() || source.quote.is_some() {
        outbound::answer(ingress, &source)?
    } else {
        None
    };
    let callback = source.choice.is_some();
    let interrupt = if source.text == "/stop" && source.speech_mentions.is_empty() {
        ingress
            .active_execution(&source.room, &sender(&source.actor))
            .await?
            .map(|execution_generation| Control::Interrupt {
                execution_generation,
            })
    } else {
        None
    };
    if let Some(value) = answer.or(interrupt) {
        return submit_control(ingress, source.event(InboundBody::Control(value))).await;
    }
    if callback
        || source.text == "/stop"
        || source
            .quote
            .as_deref()
            .map(|anchor| outbound::is_prompt(ingress, &source.room, anchor))
            .transpose()?
            .unwrap_or(false)
    {
        refuse(ingress, &source.event, "stale_control")?;
        return Ok(false);
    }
    if let Some(command) = command {
        return submit_control(ingress, source.event(InboundBody::Command(command))).await;
    }
    let quote = source.quote.as_ref().map(|id| QuoteReference {
        room: source.room.clone(),
        message_id: id.clone(),
    });
    let token = db(storage::key("whatsapp-prepare", &source.event))?;
    let purpose = quote.map_or(PreparationPurpose::Body, PreparationPurpose::Quote);
    let staged = ingress
        .stage_handoff(Handoff {
            id: token.clone(),
            items: vec![HandoffItem {
                event: start,
                preparation: Some(Preparation {
                    token,
                    purpose,
                    expires_ms,
                    protection: None,
                }),
            }],
            advance: None,
        })
        .await?;
    Ok(staged
        .receipts
        .first()
        .is_some_and(|r| matches!(r.disposition, Disposition::Accepted | Disposition::Deferred)))
}

pub(crate) async fn stage_burst(
    ingress: &Ingress,
    sources: &[Source],
) -> Result<bool, GatewayError> {
    let Some(first) = sources
        .first()
        .filter(|first| sources.len() > 1 && first.mentioned)
    else {
        return Ok(false);
    };
    let files = sources
        .iter()
        .map(|source| source.media.len())
        .sum::<usize>();
    let bytes = sources
        .iter()
        .map(|source| source.text.len().saturating_add(21))
        .sum::<usize>();
    if files > MAX_FILES || bytes > MAX_TEXT {
        return Ok(false);
    }
    if sources.iter().any(|source| {
        source.actor != first.actor
            || source.room != first.room
            || (source.text.trim().is_empty() && source.media.is_empty())
            || source.text.contains('\0')
            || source.text.starts_with('/')
            || source.quote.is_some()
            || source.choice.is_some()
            || !source.speech_mentions.is_empty()
    }) {
        return Ok(false);
    }
    common::observe(ingress, &first.room, &first.actor).await?;
    if !common::permits(ingress, &first.room, &first.actor)? {
        return Ok(false);
    }
    let mut items = Vec::with_capacity(sources.len());
    let mut deadline = None;
    for source in sources {
        let scope = ingress.clone();
        let saved = source.clone();
        let Some(expires) = blocking(move || save_source(&scope, &saved)).await? else {
            return Ok(false);
        };
        deadline.get_or_insert(expires);
        let text = if source.text.trim().is_empty() {
            "WhatsApp attachment"
        } else {
            &source.text
        };
        let mut event = source.event(InboundBody::Message(Message::text(text)));
        event.mentioned = true;
        items.push(HandoffItem {
            event,
            preparation: None,
        });
    }
    let id = db(storage::key("whatsapp-burst", &items))?;
    if files > 0 {
        let members: Vec<_> = sources
            .iter()
            .skip(1)
            .map(|source| source.event.as_str())
            .collect();
        ingress.checked(true, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_whatsapp_bursts VALUES(?1,?2,?3)",
                    params![
                        ingress.connection_name(),
                        id,
                        serde_json::to_string(&members)?
                    ],
                )?;
                Ok(())
            }))
        })?;
        items[0].preparation = Some(Preparation {
            token: id.clone(),
            purpose: PreparationPurpose::Burst,
            expires_ms: deadline.ok_or(GatewayError::Invalid)?,
            protection: None,
        });
    }
    ingress
        .stage_addressed_burst(Handoff {
            id,
            items,
            advance: None,
        })
        .await?;
    Ok(true)
}

async fn submit_control(ingress: &Ingress, event: InboundEvent) -> Result<bool, GatewayError> {
    let result = ingress.submit(vec![event], None).await?;
    Ok(result
        .receipts
        .first()
        .is_some_and(|receipt| receipt.disposition == Disposition::Accepted))
}

fn save_source(ingress: &Ingress, source: &Source) -> Result<Option<i64>, GatewayError> {
    let fingerprint = db(storage::key("whatsapp-source", source))?;
    let serialized = db(serde_json::to_string(source).map_err(anyhow::Error::from))?;
    let protected = db(hermes_engine::secret::seal(ingress.root(), &serialized))?;
    ingress.checked(true, |_| db(ingress.storage().write(|tx| {
        let scope = ingress.connection_name();
        tx.execute(
            "INSERT OR IGNORE INTO messaging_whatsapp_sources VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![scope,source.event,serde_json::to_string(&source.room)?,source.anchor,source.actor,
                fingerprint,protected,now().saturating_add(CONTEXT_MS)],
        )?;
        let (saved, expires): (String, i64) = tx.query_row(
            "SELECT fingerprint,expires FROM messaging_whatsapp_sources WHERE connection=?1 AND event=?2",
            params![scope,source.event], |r| Ok((r.get(0)?,r.get(1)?)),
        )?;
        let prepared = expires.saturating_sub(CONTEXT_MS).saturating_add(PREPARATION_MS);
        Ok((saved == fingerprint).then_some(prepared))
    })))
}

fn decode_source(protected: String) -> Result<Source, GatewayError> {
    let json = db(hermes_engine::secret::unseal(&protected))?;
    serde_json::from_str(&json).map_err(|_| GatewayError::Invalid)
}

fn pending_source(ingress: &Ingress, event: &str) -> Result<Source, GatewayError> {
    let saved = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row(
            "SELECT data FROM messaging_whatsapp_sources WHERE connection=?1 AND event=?2 AND expires>?3",
            params![ingress.connection_name(),event,now()], |row| row.get::<_,String>(0),
        ).optional()?)
    })))?.ok_or(GatewayError::Invalid)?;
    decode_source(saved)
}

pub(crate) fn source_at(
    ingress: &Ingress,
    room: &Room,
    anchor: &str,
) -> Result<Option<Source>, GatewayError> {
    let sources = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        let mut query = conn.prepare("SELECT data FROM messaging_whatsapp_sources
            WHERE connection=?1 AND room=?2 AND anchor=?3 AND expires>?4 AND data IS NOT NULL LIMIT 2")?;
        let rows = query.query_map(params![ingress.connection_name(),serde_json::to_string(room)?,anchor,now()],
            |row| row.get::<_,String>(0))?;
        Ok(rows.collect::<Result<Vec<_>,_>>()?)
    })))?;
    let mut sources = sources.into_iter();
    let first = sources.next();
    if sources.next().is_some() {
        return Ok(None);
    }
    first.map(decode_source).transpose()
}

pub(crate) async fn resume<M: MediaReader>(
    ingress: &Ingress,
    client: &M,
    cancel: &CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = async {
                if let Ok(items) = ingress.pending_preparations().await {
                    for item in items {
                        if let Err(GatewayError::StaleConnection) = hydrate(ingress, client, item, cancel).await {
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            } => {}
        }
    }
}

async fn hydrate<M: MediaReader>(
    ingress: &Ingress,
    client: &M,
    item: DeferredInput,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let id = &item.event.id;
    let token = &item.preparation.token;
    let mut saved = pending_source(ingress, id)?;
    if matches!(item.preparation.purpose, PreparationPurpose::Quote(_)) {
        ingress
            .hydrate_quote(id, token, |reference| async {
                client.signal(ingress, &saved, cancel).await;
                let history = source_at(ingress, &reference.room, &reference.message_id)?;
                let quote = match &history {
                    Some(source) if !source.text.is_empty() => Some(source.text.clone()),
                    _ => outbound::confirmed_text(ingress, &reference.room, &reference.message_id)?,
                }
                .map_or(QuoteLookup::Unavailable, |text| QuoteLookup::Found {
                    source: reference,
                    text: text.chars().take(3_500).collect(),
                });
                let message = read_media(ingress, client, &item, saved, history, cancel).await?;
                Ok(QuoteHydration {
                    message: Some(message),
                    quote,
                    advance: None,
                })
            })
            .await?;
        return Ok(());
    }
    ingress
        .authorize_preparation(id.clone(), token.clone())
        .await?;
    if item.preparation.purpose == PreparationPurpose::Burst {
        complete_burst_source(ingress, &item, &mut saved)?;
    }
    client.signal(ingress, &saved, cancel).await;
    let message = read_media(ingress, client, &item, saved, None, cancel).await?;
    ingress
        .complete_preparation(id.clone(), token.clone(), message)
        .await?;
    Ok(())
}

fn complete_burst_source(
    ingress: &Ingress,
    item: &DeferredInput,
    saved: &mut Source,
) -> Result<(), GatewayError> {
    let members = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        let scope = ingress.connection_name();
        let Some(members) = conn.query_row(
            "SELECT members FROM messaging_whatsapp_bursts WHERE connection=?1 AND token=?2",
            params![scope,item.preparation.token], |row| storage::decode::<Vec<String>>(row,0),
        ).optional()? else { return Ok(Vec::new()) };
        for member in &members {
            let merged = conn.query_row("SELECT EXISTS(SELECT 1 FROM messaging_ingress
                WHERE connection=?1 AND event=?2 AND reason=?3 AND json_extract(disposition,'$')='coalesced')",
                params![scope,member,item.event.id], |row| row.get::<_,bool>(0))?;
            if !merged { return Ok(Vec::new()); }
        }
        Ok(members)
    })))?;
    for member in members {
        let part = pending_source(ingress, &member)?;
        if part.room != saved.room || part.actor != saved.actor {
            return Err(GatewayError::Forbidden);
        }
        if !part.text.trim().is_empty() {
            if !saved.text.is_empty() {
                saved.text.push('\n');
            }
            saved.text.push_str(&part.text);
        }
        saved.media.extend(part.media);
    }
    Ok(())
}

async fn read_media<M: MediaReader>(
    ingress: &Ingress,
    client: &M,
    item: &DeferredInput,
    source: Source,
    quote: Option<Source>,
    cancel: &CancellationToken,
) -> Result<Message, GatewayError> {
    let mut message = Message::text(source.text);
    let mut addressed = source.speech_mentions.is_empty();
    let mut budget = MAX_ADAPTER_INPUT_BYTES;
    let tagged = source.media.into_iter().map(|media| (media, false));
    let historical = quote
        .into_iter()
        .flat_map(|source| source.media)
        .map(|media| (media, true));
    for (file, historical) in tagged.chain(historical).take(MAX_FILES) {
        let result = stage_file(ingress, client, item, &file, historical, budget, cancel).await;
        match result {
            Ok((PreparedAudio::Attachment(attachment), size)) => {
                message.attachments.push(attachment);
                budget -= size;
            }
            Ok((PreparedAudio::Transcript(transcript), size)) => {
                if !addressed {
                    let lower = transcript.to_lowercase();
                    addressed = source
                        .speech_mentions
                        .iter()
                        .any(|pattern| lower.contains(&pattern.to_lowercase()));
                }
                if message.text.len() + transcript.len() + 32 < MAX_TEXT {
                    if !message.text.is_empty() {
                        message.text.push('\n');
                    }
                    message.text.push_str("WhatsApp voice note\n");
                    message.text.push_str(&transcript);
                }
                budget -= size;
            }
            Err(GatewayError::Invalid | GatewayError::Unsupported | GatewayError::Forbidden) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(if addressed {
        message
    } else {
        Message::text("")
    })
}

async fn stage_file<M: MediaReader>(
    ingress: &Ingress,
    client: &M,
    item: &DeferredInput,
    file: &Media,
    historical: bool,
    budget: usize,
    cancel: &CancellationToken,
) -> Result<(PreparedAudio, usize), GatewayError> {
    let limit = budget.min(MAX_ADAPTER_FILE);
    let description = client.describe(ingress, file, cancel).await?;
    if description.size == 0 || description.size > limit || !valid_name(&description.name) {
        return Err(GatewayError::Invalid);
    }
    let voice = file.voice && !historical;
    let purpose = if voice {
        MediaPurpose::VoiceNote
    } else {
        MediaPurpose::Attachment
    };
    let grant = ingress
        .authorize_adapter_media(
            MediaSource::from(&item.event),
            AttachmentManifest {
                name: description.name.clone(),
                size: Some(description.size),
                sha256: None,
            },
            purpose,
            Duration::from_secs(90),
            limit,
        )
        .await?;
    let open = || client.open(file, &description, limit, cancel);
    let artifact = if voice {
        ingress
            .prepare_adapter_audio(
                grant,
                Some(voice_source),
                open,
                voice_codec::decode_voice_note,
            )
            .await?
    } else {
        PreparedAudio::Attachment(ingress.stage_attachment(grant, open).await?)
    };
    Ok((artifact, description.size))
}

fn voice_source(
    tx: &rusqlite::Transaction<'_>,
    scope: &str,
    source: &MediaSource,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_whatsapp_voice VALUES(?1,?2)",
        params![scope, source.event_id],
    )?;
    Ok(())
}
