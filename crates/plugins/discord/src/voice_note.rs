use crate::messaging::{GatewayError, Ingress, MediaSource, Room, runtime::db, storage as store};
use crate::messaging::voice_codec::{MAX_ENCODED_BYTES as MAX_BYTES, MAX_ENCODED_SECONDS as MAX_SECONDS};
use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::{Value, json};

pub(super) const FLAG: u64 = 1 << 13;
pub(super) const MIME: &str = "audio/ogg";
pub(super) const FILENAME: &str = "voice-message.ogg";
const RATE: usize = 48_000;
const WAVEFORM_POINTS: usize = 256;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_voice_sources (
    connection TEXT NOT NULL, event TEXT NOT NULL, room TEXT NOT NULL, actor TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
";

pub(crate) fn record_source(
    tx: &Transaction<'_>,
    connection: &str,
    source: &MediaSource,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_discord_voice_sources (connection,event,room,actor)
         VALUES (?1,?2,?3,?4)",
        params![
            connection,
            source.event_id,
            serde_json::to_string(&source.room)?,
            source.sender.id
        ],
    )?;
    Ok(())
}

pub(super) fn channel_reply(
    ingress: &Ingress,
    delivery: &str,
    room: &Room,
) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|tx| {
            Ok(tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_deliveries d
                JOIN messaging_effect_sources s ON s.delivery=d.id
                WHERE d.connection=?1 AND d.id=?2 AND d.sender IS NOT NULL
                  AND json_extract(d.intent,'$.room')=json(?3))",
                params![ingress.connection_name(), delivery, serde_json::to_string(room)?],
                |row| row.get(0),
            )?)
        }))
    })
}

pub(super) fn spoken_source(
    ingress: &Ingress,
    delivery: &str,
    room: &Room,
) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| {
        let source = db(ingress.storage().read(|tx| {
            Ok(tx
                .query_row(
                    "SELECT v.room FROM messaging_discord_voice_sources v
                 JOIN messaging_effect_sources s ON s.event=v.event
                 JOIN messaging_deliveries d ON d.id=s.delivery AND d.connection=v.connection
                 JOIN messaging_ingress i ON i.connection=v.connection AND i.event=v.event
                 JOIN messaging_routes r ON r.id=i.route AND r.room=v.room
                 WHERE v.connection=?1 AND d.id=?2 AND json_extract(d.sender,'$.id')=v.actor",
                    params![ingress.connection_name(), delivery],
                    |row| store::decode::<Room>(row, 0),
                )
                .optional()?)
        }))?;
        Ok(source.as_ref() == Some(room))
    })
}

pub(super) fn is_voice_message(value: &Value) -> bool {
    value["flags"]
        .as_u64()
        .is_some_and(|flags| flags & FLAG != 0)
}

pub(super) fn accepted_note(value: &Value) -> bool {
    let Some(files) = value["attachments"].as_array() else {
        return false;
    };
    let [file] = files.as_slice() else {
        return false;
    };
    is_voice_message(value)
        && (value["content"].is_null() || value["content"].as_str() == Some(""))
        && file["filename"].as_str() == Some(FILENAME)
        && file["content_type"]
            .as_str()
            .is_some_and(|kind| kind.starts_with("audio/"))
        && file["size"]
            .as_u64()
            .is_some_and(|bytes| (1..=MAX_BYTES as u64).contains(&bytes))
        && file["duration_secs"].as_f64().is_some_and(|duration| {
            duration.is_finite() && duration > 0.0 && duration <= MAX_SECONDS as f64
        })
        && file["waveform"].as_str().is_some_and(|waveform| {
            waveform.len() <= 4 * WAVEFORM_POINTS.div_ceil(3)
                && STANDARD
                    .decode(waveform)
                    .is_ok_and(|data| (1..=WAVEFORM_POINTS).contains(&data.len()))
        })
}

pub(super) struct VoiceNote {
    pub bytes: Vec<u8>,
    duration_secs: f64,
    waveform: String,
}

impl VoiceNote {
    pub(super) fn attachment(&self) -> Value {
        json!({
            "id": 0, "filename": FILENAME, "duration_secs": self.duration_secs,
            "waveform": self.waveform,
        })
    }
}

pub(super) use crate::messaging::voice_codec::wav_pcm_format;

pub(super) fn encode(wav: &[u8]) -> Result<VoiceNote, GatewayError> {
    let crate::messaging::voice_codec::EncodedVoiceNote { bytes, samples } =
        crate::messaging::voice_codec::encode_voice_note(wav)?;
    let window = (RATE / 10).max(samples.len().div_ceil(WAVEFORM_POINTS));
    let waveform = STANDARD.encode(
        samples
            .chunks(window)
            .map(|chunk| {
                let level = chunk
                    .iter()
                    .map(|sample| f64::from(*sample).powi(2))
                    .sum::<f64>()
                    / chunk.len() as f64;
                (level.sqrt() * 255.0 / 32768.0).round() as u8
            })
            .collect::<Vec<_>>(),
    );
    Ok(VoiceNote {
        bytes,
        duration_secs: samples.len() as f64 / RATE as f64,
        waveform,
    })
}
