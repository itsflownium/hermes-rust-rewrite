use super::{
    Connection, intake,
    updates::{File, Incoming},
};
use hermes_gateway_core::messaging::{
    Advance, AttachmentManifest, DeferredInput, Disposition, GatewayError, Handoff, HandoffItem,
    InboundBody, Ingress, MediaPurpose, MediaSource, Message, Preparation, PreparationPurpose,
    media::{MAX_ADAPTER_FILE, MAX_ADAPTER_INPUT_BYTES, MAX_FILE, PreparedAudio, valid_name},
    runtime::{db, now},
    storage,
};
use hermes_telegram::{Error, ErrorKind, Method, telegram::FileStream};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::{path::Path, time::Duration};
use tokio_util::sync::CancellationToken;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_telegram_media (
    connection TEXT NOT NULL, event TEXT NOT NULL, files TEXT NOT NULL, expires INTEGER NOT NULL,
    PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_telegram_voice (
    connection TEXT NOT NULL, event TEXT NOT NULL, room TEXT NOT NULL, actor TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_telegram_albums (
    connection TEXT NOT NULL, album TEXT NOT NULL, leader TEXT NOT NULL,
    close_after INTEGER NOT NULL, sealed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,album), UNIQUE(connection,leader)
);
CREATE TABLE IF NOT EXISTS messaging_telegram_album_parts (
    connection TEXT NOT NULL, album TEXT NOT NULL, event TEXT NOT NULL,
    message INTEGER NOT NULL, caption TEXT NOT NULL, files TEXT NOT NULL,
    PRIMARY KEY(connection,event),
    FOREIGN KEY(connection,album) REFERENCES messaging_telegram_albums(connection,album)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS messaging_telegram_album_order
    ON messaging_telegram_album_parts(connection,album,message);
";

const CLOUD_DOWNLOAD: usize = 20 * 1024 * 1024;
const PREPARATION_MS: i64 = 10 * 60 * 1_000;
const MEDIA_GRANT: Duration = Duration::from_secs(90);
const ALBUM_WINDOW_MS: i64 = 700;
const ALBUM_MAX: i64 = 10;

enum AlbumPart {
    Leader,
    Follower,
    Individual,
    Conflict,
}

pub(super) async fn stage(
    ingress: &Ingress,
    incoming: Incoming,
    advance: Advance,
) -> Result<(), GatewayError> {
    let allowed = ingress.checked(true, |state| {
        if !ingress.authorize_sender(&state.policy, &incoming.event.room, &incoming.event.sender)? {
            return Ok(Err(Disposition::Forbidden));
        }
        if state
            .policy
            .delivery_policy(&incoming.event.room)
            .protection
            .is_some()
        {
            return Ok(Err(Disposition::ProtectionUnavailable));
        }
        Ok(Ok(()))
    })?;
    if let Err(reason) = allowed {
        return intake::refuse(ingress, incoming.event, reason, advance).await;
    }
    if incoming.files.len() > 8 || incoming.files.iter().any(|file| !valid_name(&file.name)) {
        return intake::refuse(ingress, incoming.event, Disposition::Invalid, advance).await;
    }
    if let Some(album) = &incoming.album {
        match stage_album(ingress, &incoming, album)? {
            AlbumPart::Follower => {
                return intake::refuse(ingress, incoming.event, Disposition::Coalesced, advance)
                    .await;
            }
            AlbumPart::Conflict => {
                return intake::refuse(ingress, incoming.event, Disposition::Conflict, advance)
                    .await;
            }
            AlbumPart::Leader | AlbumPart::Individual => {}
        }
    }
    let scope = ingress.clone();
    let event = incoming.event.id.clone();
    let serialized = serde_json::to_string(&incoming.files).map_err(|_| GatewayError::Invalid)?;
    let (matches, expires_ms) = hermes_gateway_core::messaging::runtime::blocking(move || {
        scope.checked(true, |_| db(scope.storage().write(|tx| {
            let time = now();
            tx.execute("DELETE FROM messaging_telegram_media WHERE connection=?1 AND expires<?2",
                params![scope.connection_name(),time.saturating_sub(24 * 60 * 60 * 1_000)])?;
            tx.execute("INSERT OR IGNORE INTO messaging_telegram_media VALUES(?1,?2,?3,?4)",
                params![scope.connection_name(),event,serialized,time.saturating_add(PREPARATION_MS)])?;
            let (files, expires): (String, i64) = tx.query_row(
                "SELECT files,expires FROM messaging_telegram_media WHERE connection=?1 AND event=?2",
                params![scope.connection_name(),event], |r| Ok((r.get(0)?,r.get(1)?)))?;
            Ok((files == serialized, expires))
        })))
    }).await?;
    if !matches {
        return intake::refuse(ingress, incoming.event, Disposition::Conflict, advance).await;
    }
    let token = db(storage::key("telegram-media", &incoming.event.id))?;
    // Handoffs pin their whole envelope. A provider retry can bring the same
    // event with a new checkpoint, so only that event's identity stays fixed.
    let id = db(storage::key(
        "telegram-media-handoff",
        &(&incoming.event.id, &advance),
    ))?;
    ingress
        .stage_handoff(Handoff {
            id,
            items: vec![HandoffItem {
                event: incoming.event,
                preparation: Some(Preparation {
                    token,
                    purpose: PreparationPurpose::Media,
                    expires_ms,
                    protection: None,
                }),
            }],
            advance: Some(advance),
        })
        .await?;
    Ok(())
}

fn stage_album(
    ingress: &Ingress,
    incoming: &Incoming,
    album: &str,
) -> Result<AlbumPart, GatewayError> {
    let event = &incoming.event;
    let scope = db(storage::key(
        "telegram-album",
        &(&event.room, &event.sender.id, album),
    ))?;
    let serialized = serde_json::to_string(&incoming.files).map_err(|_| GatewayError::Invalid)?;
    let InboundBody::Message(message) = &event.body else {
        return Err(GatewayError::Invalid);
    };
    ingress.checked(true, |state| {
        if !ingress.authorize_sender(&state.policy, &event.room, &event.sender)? {
            return Ok(AlbumPart::Individual);
        }
        db(ingress.storage().write(|tx| {
            let time = now();
            let connection = ingress.connection_name();
            let old = tx.query_row(
                "SELECT a.album,a.leader,p.message,p.caption,p.files FROM messaging_telegram_album_parts p
                 JOIN messaging_telegram_albums a USING(connection,album) WHERE p.connection=?1 AND p.event=?2",
                params![connection,event.id],
                |row| Ok((row.get::<_,String>(0)?,row.get::<_,String>(1)?,row.get::<_,i64>(2)?,
                    row.get::<_,String>(3)?,row.get::<_,String>(4)?)),
            ).optional()?;
            if let Some((previous, leader, id, caption, files)) = old {
                return Ok(if previous != scope || id != incoming.message_id || caption != message.text || files != serialized {
                    AlbumPart::Conflict
                } else if leader == event.id {
                    AlbumPart::Leader
                } else {
                    AlbumPart::Follower
                });
            }
            // If this provider update already had its own admission, it stays
            // individual even if an album with the same ID arrives later.
            if storage::intake_receipt(tx, connection, &event.id)?.is_some() {
                return Ok(AlbumPart::Individual);
            }
            tx.execute("DELETE FROM messaging_telegram_albums WHERE connection=?1 AND close_after<?2",
                params![connection,time.saturating_sub(24 * 60 * 60 * 1_000)])?;
            let leader = tx.query_row(
                "SELECT leader, close_after, sealed FROM messaging_telegram_albums WHERE connection=?1 AND album=?2",
                params![connection,scope], |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?,row.get::<_,bool>(2)?)),
            ).optional()?;
            let part = if let Some((_, deadline, sealed)) = leader {
                if sealed || deadline <= time {
                    return Ok(AlbumPart::Individual);
                }
                let count: i64 = tx.query_row(
                    "SELECT count(*) FROM messaging_telegram_album_parts WHERE connection=?1 AND album=?2",
                    params![connection,scope], |row| row.get(0))?;
                if count >= ALBUM_MAX {
                    return Ok(AlbumPart::Individual);
                }
                AlbumPart::Follower
            } else {
                tx.execute("INSERT INTO messaging_telegram_albums(connection,album,leader,close_after) VALUES(?1,?2,?3,?4)",
                    params![connection,scope,event.id,time.saturating_add(ALBUM_WINDOW_MS)])?;
                AlbumPart::Leader
            };
            tx.execute("INSERT INTO messaging_telegram_album_parts VALUES(?1,?2,?3,?4,?5,?6)",
                params![connection,scope,event.id,incoming.message_id,message.text,serialized])?;
            Ok(part)
        }))
    })
}

fn close_album(ingress: &Ingress, id: &str) -> Result<Option<Vec<(String, String)>>, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().write(|tx| {
        let connection = ingress.connection_name();
        let album = tx.query_row(
            "SELECT album,close_after,sealed FROM messaging_telegram_albums WHERE connection=?1 AND leader=?2",
            params![connection,id], |row| Ok((row.get::<_,String>(0)?,row.get::<_,i64>(1)?,row.get::<_,bool>(2)?)),
        ).optional()?;
        let Some((album, deadline, sealed)) = album else { return Ok(Some(Vec::new())) };
        if !sealed && deadline > now() {
            return Ok(None);
        }
        tx.execute("UPDATE messaging_telegram_albums SET sealed=1 WHERE connection=?1 AND album=?2",
            params![connection,album])?;
        let mut parts = tx.prepare("SELECT caption,files FROM messaging_telegram_album_parts
            WHERE connection=?1 AND album=?2 ORDER BY message,event")?;
        let parts = parts.query_map(params![connection,album], |row| Ok((row.get(0)?,row.get(1)?)))?;
        Ok(Some(parts.collect::<rusqlite::Result<_>>()?))
    })))
}

pub(super) async fn resume(connection: &Connection, ingress: &Ingress, cancel: &CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = ingress.pending_preparations() => {
                if let Ok(items) = result {
                    for item in items {
                        if item.preparation.purpose == PreparationPurpose::Media {
                            tokio::select! {
                                _ = cancel.cancelled() => return,
                                _ = hydrate(connection, ingress, item, cancel) => {}
                            }
                        }
                    }
                }
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
}

async fn hydrate(
    connection: &Connection,
    ingress: &Ingress,
    item: DeferredInput,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let source = MediaSource::from(&item.event);
    let id = item.event.id;
    let token = item.preparation.token;
    let InboundBody::Message(mut message) = item.event.body else {
        return Err(GatewayError::Invalid);
    };
    ingress
        .authorize_preparation(id.clone(), token.clone())
        .await?;
    let album = {
        let _serial = connection.intake.lock().await;
        close_album(ingress, &id)?
    };
    let Some(album) = album else { return Ok(()) };
    let mut files: Vec<File> = Vec::new();
    if album.is_empty() {
        let manifest = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
            Ok(conn.query_row("SELECT files FROM messaging_telegram_media WHERE connection=?1 AND event=?2",
                params![ingress.connection_name(),id], |r| r.get::<_, String>(0)).optional()?)
        })))?.ok_or(GatewayError::Invalid)?;
        files = serde_json::from_str(&manifest).map_err(|_| GatewayError::Invalid)?;
    } else {
        message.text.clear();
        for (caption, manifest) in album {
            if !caption.is_empty() {
                append(&mut message, &caption);
            }
            files.extend(
                serde_json::from_str::<Vec<File>>(&manifest).map_err(|_| GatewayError::Invalid)?,
            );
        }
    }
    let limit = if connection.settings.local_files.is_some() {
        MAX_ADAPTER_FILE
    } else {
        CLOUD_DOWNLOAD
    };
    let mut budget = MAX_ADAPTER_INPUT_BYTES;
    for file in &files {
        ingress
            .authorize_preparation(id.clone(), token.clone())
            .await?;
        let size = file.size.unwrap_or(MAX_FILE);
        let max_file = limit.min(budget);
        budget = budget.saturating_sub(size);
        match receive(connection, ingress, &source, file, max_file, cancel).await {
            Ok(PreparedAudio::Attachment(handle)) => {
                if file.voice {
                    ingress.checked(true, |_| {
                        db(ingress
                            .storage()
                            .write(|tx| record_voice(tx, ingress.connection_name(), &source)))
                    })?;
                }
                message.attachments.push(handle);
            }
            Ok(PreparedAudio::Transcript(text)) => {
                append(&mut message, &format!("Telegram voice note: {text}"))
            }
            Err(GatewayError::Invalid | GatewayError::Unsupported) => {
                append(
                    &mut message,
                    &format!("Telegram attachment unavailable: {}", file.name),
                );
            }
            Err(error) => return Err(error),
        }
    }
    ingress.complete_preparation(id, token, message).await?;
    Ok(())
}

fn append(message: &mut Message, content: &str) {
    if message.text.len() + content.len() < hermes_gateway_core::messaging::runtime::MAX_TEXT {
        if !message.text.is_empty() {
            message.text.push('\n');
        }
        message.text.push_str(content);
    }
}

async fn receive(
    connection: &Connection,
    ingress: &Ingress,
    source: &MediaSource,
    file: &File,
    limit: usize,
    cancel: &CancellationToken,
) -> Result<PreparedAudio, GatewayError> {
    if file.size.unwrap_or(MAX_FILE) > limit {
        return Err(GatewayError::Invalid);
    }
    let manifest = AttachmentManifest {
        name: file.name.clone(),
        size: file.size,
        sha256: None,
    };
    let voice = file.voice && file.size.is_some();
    let purpose = if voice {
        MediaPurpose::VoiceNote
    } else {
        MediaPurpose::Attachment
    };
    let grant = if file.size.is_some() {
        ingress
            .authorize_adapter_media(source.clone(), manifest, purpose, MEDIA_GRANT, limit)
            .await?
    } else {
        ingress
            .authorize_media(source.clone(), manifest, purpose, MEDIA_GRANT)
            .await?
    };
    let open = || open_file(connection, file, cancel);
    if voice {
        ingress
            .prepare_adapter_audio(
                grant,
                Some(record_voice),
                open,
                hermes_gateway_core::messaging::voice_codec::decode_voice_note,
            )
            .await
    } else {
        ingress
            .stage_attachment(grant, open)
            .await
            .map(PreparedAudio::Attachment)
    }
}

fn record_voice(
    tx: &rusqlite::Transaction<'_>,
    connection: &str,
    source: &MediaSource,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_telegram_voice VALUES(?1,?2,?3,?4)",
        params![
            connection,
            source.event_id,
            serde_json::to_string(&source.room)?,
            source.sender.id
        ],
    )?;
    Ok(())
}

async fn open_file(
    connection: &Connection,
    file: &File,
    cancel: &CancellationToken,
) -> Result<FileStream, GatewayError> {
    let result = connection
        .client
        .request(Method::GetFile, json!({"file_id":file.id}), cancel)
        .await
        .map_err(read_failure)?;
    if result["file_id"].as_str() != Some(&file.id)
        || file
            .size
            .zip(result["file_size"].as_u64())
            .is_some_and(|(expected, reported)| expected as u64 != reported)
    {
        return Err(GatewayError::Invalid);
    }
    let path = result["file_path"]
        .as_str()
        .filter(|path| !path.is_empty())
        .ok_or(GatewayError::Invalid)?;
    if Path::new(path).is_absolute() {
        let root = connection
            .settings
            .local_files
            .as_ref()
            .ok_or(GatewayError::Invalid)?;
        let root = tokio::fs::canonicalize(root)
            .await
            .map_err(|_| GatewayError::Invalid)?;
        let path = tokio::fs::canonicalize(path)
            .await
            .map_err(|_| GatewayError::Invalid)?;
        if !path.starts_with(root) {
            return Err(GatewayError::Invalid);
        }
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|_| GatewayError::Invalid)?;
        if !metadata.is_file() || file.size.is_some_and(|size| size as u64 != metadata.len()) {
            return Err(GatewayError::Invalid);
        }
        let reader = tokio::fs::File::open(path)
            .await
            .map_err(|_| GatewayError::Invalid)?;
        return Ok(Box::pin(reader));
    }
    connection
        .client
        .download(path, cancel)
        .await
        .map_err(read_failure)
}

fn read_failure(error: Error) -> GatewayError {
    match error.kind {
        ErrorKind::Invalid | ErrorKind::Rejected => GatewayError::Invalid,
        ErrorKind::Credentials => GatewayError::Forbidden,
        ErrorKind::Cancelled => GatewayError::StaleConnection,
        _ => GatewayError::TemporarilyUnavailable,
    }
}
