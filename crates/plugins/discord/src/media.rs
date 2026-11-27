use super::client::{DiscordClient, RestFailure};
use crate::messaging::{
    AttachmentManifest, GatewayError, Ingress, MediaPurpose, MediaSource, Message,
    media::{MAX_ADAPTER_FILE, MAX_ADAPTER_INPUT_BYTES, PreparedAudio, valid_name},
};
pub(super) use crate::messaging::voice_codec::{decode_voice_note, pcm_wave};

use futures_util::TryStreamExt;
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, io, time::Duration};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

#[cfg(test)]
mod artifact_tests;
#[cfg(test)]
pub(super) mod fixtures;
#[cfg(test)]
mod tests;

const MAX_ATTACHMENTS: usize = 8;
const MEDIA_LIFETIME: Duration = Duration::from_secs(90);

/// Immutable evidence retained with the prepared message. Discord signs
/// access URLs again when an authorized reader retrieves the message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct AttachmentSource {
    id: String,
    filename: String,
    size: usize,
    voice: bool,
}

fn snowflake(input: &str) -> bool {
    input.bytes().all(|c| c.is_ascii_digit()) && input.parse::<u64>().is_ok_and(|id| id != 0)
}

pub(super) fn attachment_sources(message: &Value) -> Result<Vec<AttachmentSource>, GatewayError> {
    let Some(items) = message.get("attachments") else {
        return Ok(Vec::new());
    };
    let items = items.as_array().ok_or(GatewayError::Invalid)?;
    if items.len() > MAX_ATTACHMENTS {
        return Err(GatewayError::Invalid);
    }
    let voice_note = super::voice_note::is_voice_message(message);
    let mut ids = HashSet::with_capacity(items.len());
    items
        .iter()
        .map(|item| {
            let id = item["id"]
                .as_str()
                .filter(|id| snowflake(id))
                .ok_or(GatewayError::Invalid)?;
            if !ids.insert(id) {
                return Err(GatewayError::Invalid);
            }
            let filename = item["filename"]
                .as_str()
                .filter(|name| valid_name(name))
                .ok_or(GatewayError::Invalid)?;
            let size = item["size"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(GatewayError::Invalid)?;
            let voice = voice_note
                || item["content_type"]
                    .as_str()
                    .is_some_and(|kind| kind.starts_with("audio/"));
            Ok(AttachmentSource {
                id: id.to_owned(),
                filename: filename.to_owned(),
                size,
                voice,
            })
        })
        .collect()
}

pub(super) fn within_inbound_limit(attachments: &[AttachmentSource], max_per_file: u64) -> bool {
    attachments.iter().all(|file| {
        file.size != 0 && file.size as u64 <= max_per_file && file.size <= MAX_ADAPTER_FILE
    }) && attachments
        .iter()
        .try_fold(0usize, |total, file| total.checked_add(file.size))
        .is_some_and(|total| total <= MAX_ADAPTER_INPUT_BYTES)
}

fn gateway_failure(error: RestFailure) -> GatewayError {
    match error {
        RestFailure::Credentials | RestFailure::Forbidden | RestFailure::Missing => {
            GatewayError::Forbidden
        }
        RestFailure::RateLimited | RestFailure::Unavailable => GatewayError::TemporarilyUnavailable,
        RestFailure::Invalid => GatewayError::Invalid,
    }
}

fn verified_cdn_url(url: &str, channel: &str, artifact: &str) -> Result<Url, GatewayError> {
    let url = Url::parse(url).map_err(|_| GatewayError::Invalid)?;
    if url.scheme() != "https"
        || !matches!(
            url.host_str(),
            Some("cdn.discordapp.com" | "media.discordapp.net")
        )
        || url.port_or_known_default() != Some(443)
        || !attachment_path(&url, channel, artifact)
    {
        return Err(GatewayError::Forbidden);
    }
    Ok(url)
}

fn attachment_path(url: &Url, channel: &str, artifact: &str) -> bool {
    let Some(mut path) = url.path_segments() else { return false };
    url.username().is_empty() && url.password().is_none() && url.fragment().is_none()
        && path.next() == Some("attachments") && path.next() == Some(channel)
        && path.next() == Some(artifact) && path.next().is_some_and(|name| !name.is_empty())
        && path.next().is_none()
}

fn attachment_url(
    _client: &DiscordClient, url: &str, channel: &str, artifact: &str,
) -> Result<Url, GatewayError> {
    #[cfg(test)]
    if let Ok(candidate) = Url::parse(url)
        && _client.fixture_media_url(&candidate) && attachment_path(&candidate, channel, artifact)
    {
        return Ok(candidate);
    }
    verified_cdn_url(url, channel, artifact)
}

pub(super) async fn hydrate_attachments(
    client: &DiscordClient,
    ingress: &Ingress,
    source: &MediaSource,
    channel: &str,
    message: &str,
    body: Message,
    expected: &[AttachmentSource],
    max_inbound_bytes: u64,
) -> Result<Message, GatewayError> {
    hydrate_with(
        client,
        ingress,
        source,
        channel,
        message,
        body,
        expected,
        max_inbound_bytes,
        |url, size| open_cdn(client, url, size),
    )
    .await
}

async fn hydrate_with<S, Open, Opened>(
    client: &DiscordClient,
    ingress: &Ingress,
    source: &MediaSource,
    channel: &str,
    message: &str,
    mut body: Message,
    expected: &[AttachmentSource],
    max_inbound_bytes: u64,
    open: Open,
) -> Result<Message, GatewayError>
where
    S: AsyncRead + Unpin,
    Open: Fn(Url, usize) -> Opened,
    Opened: Future<Output = Result<S, GatewayError>>,
{
    if !snowflake(channel) || !snowflake(message) || !body.attachments.is_empty() {
        return Err(GatewayError::Invalid);
    }
    if source.room.thread.as_deref().unwrap_or(&source.room.chat) != channel
        && !ingress.thread_media_source_allowed(source, channel)?
    {
        return Err(GatewayError::Forbidden);
    }
    if expected.is_empty() {
        return Ok(body);
    }
    if !within_inbound_limit(expected, max_inbound_bytes) {
        return Err(GatewayError::Unsupported);
    }

    let mut grants = Vec::with_capacity(expected.len());
    for attachment in expected {
        grants.push(
            ingress
                .authorize_adapter_media(
                    source.clone(),
                    AttachmentManifest {
                        name: attachment.filename.clone(),
                        size: Some(attachment.size),
                        sha256: None,
                    },
                    if attachment.voice {
                        MediaPurpose::VoiceNote
                    } else {
                        MediaPurpose::Attachment
                    },
                    MEDIA_LIFETIME,
                    usize::try_from(max_inbound_bytes).map_err(|_| GatewayError::Invalid)?,
                )
                .await?,
        );
    }
    let metadata: Value = client
        .get(&["channels", channel, "messages", message])
        .await
        .map_err(gateway_failure)?;
    if metadata["id"].as_str() != Some(message)
        || metadata["channel_id"].as_str() != Some(channel)
        || metadata["author"]["id"].as_str() != Some(&source.sender.id)
        || attachment_sources(&metadata)? != expected
    {
        return Err(GatewayError::Conflict);
    }

    let urls = metadata["attachments"]
        .as_array()
        .ok_or(GatewayError::Invalid)?
        .iter()
        .zip(expected)
        .map(|(item, attachment)| {
            attachment_url(
                client,
                item["url"].as_str().ok_or(GatewayError::Invalid)?,
                channel,
                &attachment.id,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    for ((attachment, grant), url) in expected.iter().zip(grants).zip(urls) {
        if attachment.voice {
            match ingress
                .prepare_adapter_audio(
                    grant,
                    if super::voice_note::is_voice_message(&metadata) {
                        Some(super::voice_note::record_source)
                    } else {
                        None
                    },
                    || open(url, attachment.size), decode_voice_note,
                )
                .await?
            {
                PreparedAudio::Transcript(spoken) => {
                    if !body.text.is_empty() {
                        body.text.push_str("\n\nVoice note:\n");
                    }
                    body.text.push_str(&spoken);
                    if body.text.len() > crate::messaging::runtime::MAX_TEXT {
                        return Err(GatewayError::Invalid);
                    }
                }
                PreparedAudio::Attachment(file) => body.attachments.push(file),
            }
        } else {
            body.attachments.push(
                ingress
                    .stage_attachment(grant, || open(url, attachment.size))
                    .await?,
            );
        }
    }
    Ok(body)
}

async fn open_cdn(
    client: &DiscordClient,
    url: Url,
    size: usize,
) -> Result<impl AsyncRead + Unpin, GatewayError> {
    let response = client
        .public_get(url)
        .timeout(MEDIA_LIFETIME)
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)?;
    if response.status().is_redirection()
        || matches!(
            response.status(),
            StatusCode::UNAUTHORIZED
                | StatusCode::FORBIDDEN
                | StatusCode::NOT_FOUND
                | StatusCode::GONE
        )
    {
        return Err(GatewayError::Forbidden);
    }
    if !response.status().is_success() {
        return Err(GatewayError::TemporarilyUnavailable);
    }
    if response
        .content_length()
        .is_some_and(|declared| declared != size as u64)
    {
        return Err(GatewayError::Invalid);
    }
    Ok(StreamReader::new(
        response.bytes_stream().map_err(io::Error::other),
    ))
}
