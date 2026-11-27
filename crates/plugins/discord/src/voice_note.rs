use crate::messaging::{GatewayError, Ingress, MediaSource, Room, runtime::db, storage as store};
use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use ogg::{PacketWriteEndInfo, PacketWriter};
use opus2::{Application, Bitrate, Channels, Encoder};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::io::Cursor;

pub(super) const FLAG: u64 = 1 << 13;
pub(super) const MIME: &str = "audio/ogg";
pub(super) const FILENAME: &str = "voice-message.ogg";
const RATE: usize = 48_000;
const FRAME: usize = RATE / 50;
const MAX_SECONDS: usize = 90;
const WAVEFORM_POINTS: usize = 256;
const MAX_BYTES: usize = 1024 * 1024;

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

/// Read the mono 16-bit PCM WAV emitted by the profile speech service.
pub(super) fn wav_pcm(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let (rate, channels, samples) = wav_pcm_format(bytes)?;
    (channels == 1).then_some((rate, samples))
}

pub(super) fn wav_pcm_format(bytes: &[u8]) -> Option<(usize, u32, &[u8])> {
    if bytes.get(..4)? != b"RIFF" || bytes.get(8..12)? != b"WAVE" {
        return None;
    }
    let mut chunks = bytes.get(12..)?;
    let mut format = None;
    let mut samples = None;
    while !chunks.is_empty() {
        let header = chunks.get(..8)?;
        let size = u32::from_le_bytes(header[4..8].try_into().ok()?) as usize;
        let end = 8usize.checked_add(size)?;
        let data = chunks.get(8..end)?;
        match &header[..4] {
            b"fmt " if data.get(..2)? == [1, 0] && data.get(14..16)? == [16, 0] => {
                let channels = u16::from_le_bytes(data.get(2..4)?.try_into().ok()?);
                let rate = u32::from_le_bytes(data.get(4..8)?.try_into().ok()?) as usize;
                format = Some((rate, u32::from(channels)));
            }
            b"data" if !data.is_empty() && data.len().is_multiple_of(2) => samples = Some(data),
            _ => {}
        }
        chunks = chunks.get(end + size % 2..)?;
    }
    let (rate, channels) = format?;
    let samples = samples?;
    ((8_000..=192_000).contains(&rate)
        && (1..=2).contains(&channels)
        && samples.len().is_multiple_of(channels as usize * 2))
        .then_some((rate, channels, samples))
}

pub(super) fn encode(wav: &[u8]) -> Result<VoiceNote, GatewayError> {
    let invalid = GatewayError::Invalid;
    let (source_rate, pcm) = wav_pcm(wav).ok_or(invalid)?;
    let source_count = pcm.len() / 2;
    if source_count > source_rate * MAX_SECONDS {
        return Err(GatewayError::Unsupported);
    }
    let count = (source_count as u64 * RATE as u64).div_ceil(source_rate as u64);
    let sample = |index: usize| i16::from_le_bytes([pcm[index * 2], pcm[index * 2 + 1]]);
    let samples: Vec<i16> = (0..count)
        .map(|index| {
            let position = index * source_rate as u64;
            let (left, weight) = (
                (position / RATE as u64) as usize,
                (position % RATE as u64) as i64,
            );
            let a = i64::from(sample(left));
            let b = i64::from(sample((left + 1).min(source_count - 1)));
            (a + (b - a) * weight / RATE as i64) as i16
        })
        .collect();

    let mut codec =
        Encoder::new(RATE as u32, Channels::Mono, Application::Voip).map_err(|_| invalid)?;
    codec
        .set_bitrate(Bitrate::Bits(32_000))
        .map_err(|_| invalid)?;
    let lookahead =
        u16::try_from(codec.get_lookahead().map_err(|_| invalid)?).map_err(|_| invalid)?;

    let mut writer = PacketWriter::new(Cursor::new(Vec::new()));
    let mut head = b"OpusHead\x01\x01".to_vec();
    head.extend_from_slice(&lookahead.to_le_bytes());
    head.extend_from_slice(&(RATE as u32).to_le_bytes());
    head.extend_from_slice(&[0, 0, 0]); // Zero gain and mapping family 0 (mono).
    writer
        .write_packet(head, 1, PacketWriteEndInfo::EndPage, 0)
        .map_err(|_| invalid)?;
    writer
        .write_packet(
            b"OpusTags\x07\x00\x00\x00Mercury\0\0\0\0".as_slice(),
            1,
            PacketWriteEndInfo::EndPage,
            0,
        )
        .map_err(|_| invalid)?;

    let final_granule = samples.len() + usize::from(lookahead);
    let frames = final_granule.div_ceil(FRAME);
    let mut frame = [0i16; FRAME];
    let mut output = [0u8; 1275];
    for index in 0..frames {
        frame.fill(0);
        let offset = index * FRAME;
        if offset < samples.len() {
            let remaining = FRAME.min(samples.len() - offset);
            frame[..remaining].copy_from_slice(&samples[offset..offset + remaining]);
        }
        let len = codec.encode(&frame, &mut output).map_err(|_| invalid)?;
        let last = index + 1 == frames;
        let state = if last {
            PacketWriteEndInfo::EndStream
        } else {
            PacketWriteEndInfo::NormalPacket
        };
        let granule = if last { final_granule } else { offset + FRAME };
        writer
            .write_packet(output[..len].to_vec(), 1, state, granule as u64)
            .map_err(|_| invalid)?;
    }

    let bytes = writer.into_inner().into_inner();
    if bytes.len() > MAX_BYTES {
        return Err(GatewayError::Unsupported);
    }
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
