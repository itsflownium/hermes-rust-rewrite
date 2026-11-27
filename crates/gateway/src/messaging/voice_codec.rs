//! Convert between Ogg Opus platform voice notes and the profile speech format.

use super::{GatewayError, media::MAX_FILE};
use ogg::{PacketWriteEndInfo, PacketWriter};
use opus2::{Application, Bitrate, Channels, Encoder};
use std::io;

const RATE: usize = 48_000;
const FRAME: usize = RATE / 50;
pub const MAX_ENCODED_SECONDS: usize = 90;
pub const MAX_ENCODED_BYTES: usize = 1024 * 1024;

pub struct EncodedVoiceNote {
    pub bytes: Vec<u8>,
    pub samples: Vec<i16>,
}

const SAMPLE_RATE: usize = 16_000;
const MAX_VOICE_SAMPLES: usize = 120 * SAMPLE_RATE;

/// Voice notes are Ogg Opus. Apply pre-skip and the ending granule at
/// the output sample rate and bound decoded PCM independently of file size.
pub fn decode_voice_note(bytes: &[u8]) -> Result<Vec<u8>, GatewayError> {
    if bytes.starts_with(b"RIFF") {
        return if bytes.len() <= MAX_FILE {
            Ok(bytes.to_vec())
        } else {
            Err(GatewayError::Unsupported)
        };
    }
    let mut reader = ogg::PacketReader::new(io::Cursor::new(bytes));
    let head = reader
        .read_packet()
        .map_err(|_| GatewayError::Invalid)?
        .ok_or(GatewayError::Invalid)?;
    let data = &head.data;
    if data.len() != 19
        || !data.starts_with(b"OpusHead\x01")
        || data[18] != 0
        || !head.first_in_stream()
    {
        return Err(GatewayError::Unsupported);
    }
    let pre_skip = usize::from(u16::from_le_bytes([data[10], data[11]]));
    let skip_output = pre_skip.div_ceil(3);
    let channels = match data[9] {
        1 | 2 => opus2::Channels::Mono,
        _ => return Err(GatewayError::Unsupported),
    };
    let tags = reader
        .read_packet()
        .map_err(|_| GatewayError::Invalid)?
        .ok_or(GatewayError::Invalid)?;
    if tags.stream_serial() != head.stream_serial() || !tags.data.starts_with(b"OpusTags") {
        return Err(GatewayError::Invalid);
    }
    let mut decoder =
        opus2::Decoder::new(SAMPLE_RATE as u32, channels).map_err(|_| GatewayError::Unsupported)?;
    let mut pcm = Vec::new();
    let mut frame = [0; 120 * SAMPLE_RATE / 1000];
    while let Some(packet) = reader.read_packet().map_err(|_| GatewayError::Invalid)? {
        if packet.stream_serial() != head.stream_serial()
            || packet.data.is_empty()
            || packet.data.len() > 1275
        {
            return Err(GatewayError::Invalid);
        }
        let samples = decoder
            .decode(&packet.data, &mut frame, false)
            .map_err(|_| GatewayError::Invalid)?;
        if pcm.len() + samples > MAX_VOICE_SAMPLES + skip_output {
            return Err(GatewayError::Unsupported);
        }
        pcm.extend_from_slice(&frame[..samples]);
        if packet.last_in_stream() {
            let end =
                usize::try_from(packet.absgp_page() / 3).map_err(|_| GatewayError::Invalid)?;
            if reader
                .read_packet()
                .map_err(|_| GatewayError::Invalid)?
                .is_some()
                || end > pcm.len()
                || end <= skip_output
            {
                return Err(GatewayError::Invalid);
            }
            return Ok(pcm_wave(&pcm[skip_output..end]));
        }
    }
    Err(GatewayError::Invalid)
}

pub fn pcm_wave(samples: &[i16]) -> Vec<u8> {
    let sample_bytes = (samples.len() * size_of::<i16>()) as u32;
    let mut bytes = Vec::with_capacity(44 + sample_bytes as usize);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + sample_bytes).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16u32.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&(SAMPLE_RATE as u32).to_le_bytes());
    bytes.extend_from_slice(&(2 * SAMPLE_RATE as u32).to_le_bytes());
    bytes.extend_from_slice(&2u16.to_le_bytes());
    bytes.extend_from_slice(&16u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&sample_bytes.to_le_bytes());
    for sample in samples {
        bytes.extend_from_slice(&sample.to_le_bytes());
    }
    bytes
}

/// Read the mono 16-bit PCM WAV emitted by the profile speech service.
pub fn wav_pcm(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let (rate, channels, samples) = wav_pcm_format(bytes)?;
    (channels == 1).then_some((rate, samples))
}

pub fn wav_pcm_format(bytes: &[u8]) -> Option<(usize, u32, &[u8])> {
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

pub fn encode_voice_note(wav: &[u8]) -> Result<EncodedVoiceNote, GatewayError> {
    let invalid = GatewayError::Invalid;
    let (source_rate, pcm) = wav_pcm(wav).ok_or(invalid)?;
    let source_count = pcm.len() / 2;
    if source_count > source_rate * MAX_ENCODED_SECONDS {
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

    let mut writer = PacketWriter::new(io::Cursor::new(Vec::new()));
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
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(GatewayError::Unsupported);
    }
    Ok(EncodedVoiceNote { bytes, samples })
}
