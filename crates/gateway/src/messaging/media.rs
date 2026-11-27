//! Profile-owned attachment preparation and delivery capabilities.

use super::{
    runtime::{Ingress, State, blocking, db, valid_id},
    store::{Store, key},
    transport::TransportUse,
    types::*,
};
use anyhow::{Result, ensure};
use ring::digest::{SHA256, digest};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::{
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt};

pub const MAX_FILE: usize = 4 * 1024 * 1024;
const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ADAPTER_FILE: usize = 100 * 1024 * 1024;
pub const MAX_ADAPTER_INPUT_BYTES: usize = 128 * 1024 * 1024;
const MAX_PROFILE_BYTES: i64 = 256 * 1024 * 1024;
const MAX_PENDING_GRANTS: i64 = 64;
const MAX_FETCH_GRANTS_PER_EFFECT: i64 = 32;
const MAX_READ_LIFETIME: Duration = Duration::from_secs(120);
pub const MAX_FEEDBACK_TEXT: usize = 256;
const MAX_FEEDBACK_WAV: usize = 2 * 1024 * 1024;
const FEEDBACK_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_FETCH_LIFETIME: Duration = Duration::from_secs(900);
const DOWNLOAD_OPERATION: &str = "download_media";

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_media_reads (
    token TEXT PRIMARY KEY, connection TEXT NOT NULL, generation TEXT NOT NULL,
    expires_ms INTEGER NOT NULL, claimed INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS messaging_media_read_expiry ON messaging_media_reads(expires_ms);
CREATE TABLE IF NOT EXISTS messaging_input_media (
    connection TEXT NOT NULL, event TEXT NOT NULL, sender TEXT NOT NULL, room TEXT NOT NULL,
    artifact TEXT NOT NULL, name TEXT NOT NULL,
    PRIMARY KEY(connection,event,sender,room,artifact,name),
    FOREIGN KEY(connection,artifact) REFERENCES messaging_media(connection,id)
);
CREATE TABLE IF NOT EXISTS messaging_scoped_input_media (
    connection TEXT NOT NULL, event TEXT NOT NULL, sender TEXT NOT NULL, room TEXT NOT NULL,
    artifact TEXT NOT NULL, name TEXT NOT NULL, provider_inline INTEGER NOT NULL,
    PRIMARY KEY(connection,event,sender,room,artifact,name),
    FOREIGN KEY(connection,event,sender,room,artifact,name)
        REFERENCES messaging_input_media(connection,event,sender,room,artifact,name)
        ON UPDATE CASCADE ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS messaging_spoken_addressing (
    connection TEXT NOT NULL, event TEXT NOT NULL, sender TEXT NOT NULL, room TEXT NOT NULL,
    revision INTEGER NOT NULL, expires_ms INTEGER NOT NULL,
    PRIMARY KEY(connection,event,sender,room)
);
CREATE TABLE IF NOT EXISTS messaging_artifact_fetch (
    token TEXT PRIMARY KEY, connection TEXT NOT NULL, delivery TEXT NOT NULL,
    ordinal INTEGER NOT NULL, attempt INTEGER NOT NULL, artifact TEXT NOT NULL,
    name TEXT NOT NULL, expires_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS messaging_artifact_fetch_expiry ON messaging_artifact_fetch(expires_ms);
CREATE TABLE IF NOT EXISTS messaging_spoken_replies (
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, connection TEXT NOT NULL,
    artifact TEXT NOT NULL, name TEXT NOT NULL,
    PRIMARY KEY(delivery,ordinal),
    FOREIGN KEY(connection,artifact) REFERENCES messaging_media(connection,id)
);
";

/// The normalized source the adapter verified before reading remote content.
/// Its event ID must be the same logical ID later submitted to the inbox.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaSource {
    pub event_id: String,
    pub room: Room,
    pub sender: Sender,
    pub mentioned: bool,
}

impl From<&InboundEvent> for MediaSource {
    fn from(event: &InboundEvent) -> Self {
        Self {
            event_id: event.id.clone(),
            room: event.room.clone(),
            sender: event.sender.clone(),
            mentioned: event.mentioned,
        }
    }
}

/// Provider size or hash may be unavailable. If present, they must match
/// before the result can be used. The host always computes its own content ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentManifest {
    pub name: String,
    pub size: Option<usize>,
    pub sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaPurpose {
    Attachment,
    VoiceNote,
    SpokenAddressing,
}

/// A single-use permission to open one protected read. It is never a portable
/// credential, and carries no copy of the provider endpoint or bearer token.
pub struct MediaRead {
    token: String,
    revision: i64,
    source: MediaSource,
    manifest: AttachmentManifest,
    purpose: MediaPurpose,
    expires_ms: i64,
    protection: Option<String>,
    max_file: usize,
    allow_artifact: bool,
}

/// Called in the gateway's transaction after the audio grant is rechecked.
pub type AudioSourceRecorder = fn(&rusqlite::Transaction<'_>, &str, &MediaSource) -> anyhow::Result<()>;

pub enum PreparedAudio {
    Transcript(String),
    Attachment(AttachmentRef),
}

/// A profile's speech service. Adapters normalize voice data to 16 kHz mono
/// PCM WAV. The host applies source or effect policy before invoking the service.
pub trait AudioService: Send + Sync {
    fn transcribe<'a>(
        &'a self,
        profile: &'a Path,
        wav: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>>;

    fn synthesize<'a>(
        &'a self,
        _profile: &'a Path,
        _text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async { Err(GatewayError::Unsupported) })
    }
}

struct LocalAudio;

impl AudioService for LocalAudio {
    fn transcribe<'a>(
        &'a self,
        profile: &'a Path,
        wav: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            if !hermes_engine::transcribe::available() {
                return Err(GatewayError::Unsupported);
            }
            let mut recording = db(tempfile::Builder::new()
                .suffix(".wav")
                .tempfile_in(profile)
                .map_err(Into::into))?;
            db(recording.write_all(wav).map_err(Into::into))?;
            db(hermes_engine::transcribe::transcribe(profile, recording.path(), None).await)
        })
    }
    fn synthesize<'a>(
        &'a self,
        profile: &'a Path,
        text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(super::system_audio::synthesize(profile, text))
    }
}

pub(super) struct ProfileAudio {
    root: PathBuf,
    service: Arc<dyn AudioService>,
    active: tokio::sync::Mutex<()>,
}

impl ProfileAudio {
    pub(super) fn new(root: &Path, service: Option<Arc<dyn AudioService>>) -> Self {
        Self {
            root: root.to_owned(),
            service: service.unwrap_or_else(|| Arc::new(LocalAudio)),
            active: tokio::sync::Mutex::new(()),
        }
    }
}

/// This bearer handle authorizes repeat reads of the one object for a
/// limited period, including after a reconnect to the same account.
pub struct ArtifactFetch {
    token: String,
    pub expires_ms: i64,
}

impl ArtifactFetch {
    pub fn token(&self) -> &str {
        &self.token
    }
}

struct ArtifactUse {
    delivery: String,
    ordinal: u32,
    attempt: u32,
}

struct ScopedEffect {
    room: Room,
    sender: Option<Sender>,
    blocked: bool,
    body: DeliveryBody,
    protection: Option<String>,
}

impl Ingress {
    /// Commit read authorization before the adapter opens a file, requests a
    /// media URL, or uses platform credentials on behalf of this source.
    pub async fn authorize_media(
        &self,
        source: MediaSource,
        manifest: AttachmentManifest,
        purpose: MediaPurpose,
        lifetime: Duration,
    ) -> Result<MediaRead, GatewayError> {
        self.authorize_media_with_limit(source, manifest, purpose, lifetime, MAX_FILE, false).await
    }

    /// Authorize a bounded known-size artifact for a stock adapter.
    pub async fn authorize_adapter_media(
        &self,
        source: MediaSource,
        manifest: AttachmentManifest,
        purpose: MediaPurpose,
        lifetime: Duration,
        max_file: usize,
    ) -> Result<MediaRead, GatewayError> {
        if max_file == 0 || max_file > MAX_ADAPTER_FILE || manifest.size.is_none() {
            return Err(GatewayError::Invalid);
        }
        self.authorize_media_with_limit(
            source, manifest, purpose, lifetime, max_file, true,
        ).await
    }

    async fn authorize_media_with_limit(
        &self,
        source: MediaSource,
        manifest: AttachmentManifest,
        purpose: MediaPurpose,
        lifetime: Duration,
        max_file: usize,
        allow_artifact: bool,
    ) -> Result<MediaRead, GatewayError> {
        if !self.capabilities().attachments {
            return Err(GatewayError::Unsupported);
        }
        if !valid_source(&source)
            || !valid_name(&manifest.name)
            || manifest
                .size
                .is_some_and(|size| size == 0 || size > max_file)
        {
            return Err(GatewayError::Invalid);
        }
        let expires_ms = expires(lifetime, MAX_READ_LIFETIME)?;
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let mut source = source;
                let (verified, _) = scope.effective_sender(&state.policy, &source.room, &source.sender)?
                    .ok_or(GatewayError::Forbidden)?;
                source.sender = verified;
                let protection = scope.media_allowed(state, &source, purpose)?;
                let token = crate::new_id();
                let now = now();
                let reserved = db(scope.store.write(|tx| {
                    tx.execute("DELETE FROM messaging_media_reads WHERE expires_ms <= ?1", [now])?;
                    let n: i64 = tx.query_row(
                        "SELECT count(*) FROM messaging_media_reads WHERE connection = ?1 AND generation = ?2",
                        params![scope.connection, scope.generation],
                        |row| row.get(0),
                    )?;
                    if n >= MAX_PENDING_GRANTS {
                        return Ok(false);
                    }
                    tx.execute(
                        "INSERT INTO messaging_media_reads (token,connection,generation,expires_ms) VALUES (?1,?2,?3,?4)",
                        params![token, scope.connection, scope.generation, expires_ms],
                    )?;
                    Ok(true)
                }))?;
                if !reserved {
                    return Err(GatewayError::TemporarilyUnavailable);
                }
                Ok(MediaRead {
                    token,
                    revision: state.revision,
                    source,
                    manifest,
                    purpose,
                    expires_ms,
                    protection,
                    max_file,
                    allow_artifact,
                })
            })
        })
        .await
    }

    /// Open a protected stream only after claiming a current host read.
    /// The callback is never called for an expired, revoked, or reused grant.
    pub async fn stage_attachment<R, Open, Opened>(
        &self,
        grant: MediaRead,
        open: Open,
    ) -> Result<AttachmentRef, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        if grant.purpose != MediaPurpose::Attachment {
            return Err(GatewayError::Invalid);
        }
        let bytes = self.read_media(&grant, open).await?;
        self.finish_attachment(grant, bytes).await
    }

    /// Receive a provider asset through a previously verified HTTPS address.
    /// The address, actor, operation and room are checked before issuing the
    /// request. Redirects and implicit proxy settings cannot forward its credential.
    pub async fn download_attachment(
        &self,
        source: MediaSource,
        manifest: AttachmentManifest,
        locator: TransportUse,
        lifetime: Duration,
    ) -> Result<AttachmentRef, GatewayError> {
        if locator.operation != DOWNLOAD_OPERATION || locator.room.as_ref() != Some(&source.room) {
            return Err(GatewayError::Forbidden);
        }
        let grant = self
            .authorize_media(source, manifest, MediaPurpose::Attachment, lifetime)
            .await?;
        if locator.actor.as_deref() != Some(grant.source.sender.id.as_str()) {
            return Err(GatewayError::Forbidden);
        }
        self.claim_media_read(&grant)?;
        let mut response = tokio::time::timeout(remaining(grant.expires_ms)?, async {
            let address = self.transport_authority(&locator).await?;
            self.checked(true, |state| self.recheck_media(state, &grant))?;
            let client = db(reqwest::Client::builder()
                .https_only(true)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(remaining(grant.expires_ms)?)
                .build()
                .map_err(Into::into))?;
            let mut request = client
                .get(address.endpoint.clone())
                .header(reqwest::header::ACCEPT_ENCODING, "identity");
            if let Some(bearer) = address.credential() {
                request = request.bearer_auth(bearer);
            }
            request
                .send()
                .await
                .map_err(|_| GatewayError::TemporarilyUnavailable)
        })
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)??;
        let status = response.status();
        if status.is_redirection()
            || status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
            || status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::GONE
        {
            return Err(GatewayError::Forbidden);
        }
        if !status.is_success() {
            return Err(GatewayError::TemporarilyUnavailable);
        }
        let limit = grant.manifest.size.unwrap_or(grant.max_file);
        if response.content_length().is_some_and(|n| n > limit as u64) {
            return Err(GatewayError::Invalid);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| GatewayError::TemporarilyUnavailable)?
        {
            self.checked(true, |state| self.recheck_media(state, &grant))?;
            if chunk.len() > limit - bytes.len() {
                return Err(GatewayError::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        verify_bytes(&grant, &bytes)?;
        self.transport_authority(&locator).await?;
        self.finish_attachment(grant, bytes).await
    }

    async fn finish_attachment(
        &self,
        grant: MediaRead,
        bytes: Vec<u8>,
    ) -> Result<AttachmentRef, GatewayError> {
        let preview = hermes_engine::Attachment::validate_bytes(&grant.manifest.name, &bytes).is_ok();
        if !preview && !grant.allow_artifact {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                scope.recheck_media(state, &grant)?;
                db(scope.store.write(|tx| {
                    put_inbound(tx, &scope.connection, &grant, &bytes, preview)
                }))?.ok_or(GatewayError::Invalid)
            })
        })
        .await
    }

    /// Transcribe an authorized voice note. Only the returned text is
    /// admitted to the model. No recording is retained by this operation.
    pub async fn transcribe_voice<R, Open, Opened>(
        &self,
        grant: MediaRead,
        open: Open,
    ) -> Result<String, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        if grant.purpose != MediaPurpose::VoiceNote {
            return Err(GatewayError::Invalid);
        }
        self.transcribe(&grant, open).await
    }

    /// Authorize only the voice portion to check a spoken wake phrase. A
    /// rejected recording makes no transcript available to the adapter or turn.
    pub async fn check_spoken_addressing<R, Open, Opened>(
        &self,
        grant: MediaRead,
        open: Open,
    ) -> Result<Option<String>, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        if grant.purpose != MediaPurpose::SpokenAddressing {
            return Err(GatewayError::Invalid);
        }
        let text = self.transcribe(&grant, open).await?;
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                scope.recheck_media(state, &grant)?;
                let policy = state
                    .policy
                    .room_policy(&grant.source.room)
                    .ok_or(GatewayError::Forbidden)?;
                if !policy
                    .spoken_addressing
                    .iter()
                    .any(|phrase| starts_with_phrase(&text, phrase))
                {
                    return Ok(None);
                }
                db(scope.store.write(|tx| {
                    tx.execute(
                        "DELETE FROM messaging_spoken_addressing WHERE expires_ms <= ?1",
                        [now()],
                    )?;
                    tx.execute(
                        "INSERT INTO messaging_spoken_addressing VALUES (?1,?2,?3,?4,?5,?6)
                         ON CONFLICT(connection,event,sender,room) DO UPDATE SET
                             revision = excluded.revision, expires_ms = excluded.expires_ms",
                        params![
                            scope.connection,
                            grant.source.event_id,
                            grant.source.sender.id,
                            serde_json::to_string(&grant.source.room)?,
                            grant.revision,
                            grant.expires_ms
                        ],
                    )?;
                    Ok(())
                }))?;
                Ok(Some(text))
            })
        })
        .await
    }

    async fn transcribe<R, Open, Opened>(
        &self,
        grant: &MediaRead,
        open: Open,
    ) -> Result<String, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        let bytes = self.read_media(grant, open).await?;
        self.transcribe_bytes(grant, bytes).await
    }

    /// Decode an authorized platform audio format before invoking profile STT.
    /// The source manifest is checked against the original bytes.
    pub async fn prepare_adapter_audio<R, Open, Opened>(
        &self,
        grant: MediaRead,
        record_source: Option<AudioSourceRecorder>,
        open: Open,
        decode: fn(&[u8]) -> Result<Vec<u8>, GatewayError>,
    ) -> Result<PreparedAudio, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        if grant.purpose != MediaPurpose::VoiceNote || !grant.allow_artifact {
            return Err(GatewayError::Invalid);
        }
        let original = self.read_media(&grant, open).await?;
        self.checked(true, |state| self.recheck_media(state, &grant))?;
        let (original, decoded) = blocking(move || {
            let decoded = decode(&original);
            Ok((original, decoded))
        }).await?;
        let transcript = match decoded {
            Ok(wav) => self.transcribe_bytes(&grant, wav).await,
            Err(error) => Err(error),
        };
        match transcript {
            Ok(text) => {
                if let Some(record_source) = record_source {
                    self.checked(true, |state| {
                        self.recheck_media(state, &grant)?;
                        db(self.store.write(|tx| {
                            record_source(tx, &self.connection, &grant.source)
                        }))
                    })?;
                }
                Ok(PreparedAudio::Transcript(text))
            }
            Err(GatewayError::Invalid | GatewayError::Unsupported) => self
                .finish_attachment(grant, original).await.map(PreparedAudio::Attachment),
            Err(error) => Err(error),
        }
    }

    async fn transcribe_bytes(&self, grant: &MediaRead, bytes: Vec<u8>) -> Result<String, GatewayError> {
        validate_wav(&bytes, Some(16_000))?;
        let audio = self.store.audio();
        let _guard = tokio::time::timeout(remaining(grant.expires_ms)?, audio.active.lock())
            .await
            .map_err(|_| GatewayError::TemporarilyUnavailable)?;
        self.checked(true, |state| self.recheck_media(state, grant))?;
        let text = tokio::time::timeout(
            remaining(grant.expires_ms)?,
            audio.service.transcribe(&audio.root, &bytes),
        )
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)??;
        self.checked(true, |state| self.recheck_media(state, grant))?;
        let text = text.trim();
        if text.is_empty() || text.len() > super::runtime::MAX_TEXT || text.contains('\0') {
            return Err(GatewayError::Invalid);
        }
        Ok(text.to_owned())
    }

    async fn read_media<R, Open, Opened>(
        &self,
        grant: &MediaRead,
        open: Open,
    ) -> Result<Vec<u8>, GatewayError>
    where
        R: AsyncRead + Unpin,
        Open: FnOnce() -> Opened,
        Opened: Future<Output = Result<R, GatewayError>>,
    {
        self.claim_media_read(grant)?;
        let bytes = tokio::time::timeout(remaining(grant.expires_ms)?, async {
            let mut reader = open().await?;
            let mut data = Vec::with_capacity(grant.manifest.size.unwrap_or_default());
            let limit = grant.manifest.size.unwrap_or(grant.max_file);
            (&mut reader)
                .take((limit + 1) as u64)
                .read_to_end(&mut data)
                .await
                .map_err(|_| GatewayError::TemporarilyUnavailable)?;
            verify_bytes(grant, &data)?;
            Ok::<_, GatewayError>(data)
        })
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)??;
        Ok(bytes)
    }

    fn claim_media_read(&self, grant: &MediaRead) -> Result<(), GatewayError> {
        self.checked(true, |state| {
            self.recheck_media(state, grant)?;
            let consumed = db(self.store.write(|tx| {
                let claimed = tx.execute(
                    "UPDATE messaging_media_reads SET claimed = 1
                     WHERE token = ?1 AND connection = ?2 AND generation = ?3 AND expires_ms > ?4 AND claimed = 0",
                    params![grant.token, self.connection, self.generation, now()],
                )? == 1;
                if claimed && let Some(class) = &grant.protection {
                    let source = &grant.source;
                    super::protection::pin_input(
                        tx, &self.connection, &source.event_id, &source.room, &source.sender.id, class,
                    )?;
                }
                Ok(claimed)
            }))?;
            if consumed { Ok(()) } else { Err(GatewayError::Forbidden) }
        })
    }

    pub fn spoken_addressed(
        &self,
        state: &State,
        source: &MediaSource,
    ) -> Result<bool, GatewayError> {
        db(self
            .store
            .spoken_addressed(&self.connection, source, state.revision))
    }

    fn media_allowed(
        &self,
        state: &State,
        source: &MediaSource,
        purpose: MediaPurpose,
    ) -> Result<Option<String>, GatewayError> {
        let policy = &state.policy;
        let room = policy
            .room_policy(&source.room)
            .ok_or(GatewayError::Forbidden)?;
        if source.room.thread.is_some() && !self.capabilities().threads {
            return Err(GatewayError::Unsupported);
        }
        if !self.authorize_sender(policy, &source.room, &source.sender)?
            || !self.thread_source_allowed(state, &source.room, &source.sender, &source.event_id)?
        {
            return Err(GatewayError::Forbidden);
        }
        if purpose == MediaPurpose::SpokenAddressing {
            if !room.mention_required || room.spoken_addressing.is_empty() {
                return Err(GatewayError::Forbidden);
            }
        } else if room.mention_required
            && !source.mentioned
            && !self.spoken_addressed(state, source)?
        {
            return Err(GatewayError::Forbidden);
        }
        self.check_source_protection(policy, &source.room, &source.sender.id, &source.event_id)
    }

    fn recheck_media(&self, state: &State, grant: &MediaRead) -> Result<(), GatewayError> {
        if grant.revision != state.revision {
            return Err(GatewayError::Forbidden);
        }
        remaining(grant.expires_ms)?;
        if self.media_allowed(state, &grant.source, grant.purpose)? != grant.protection {
            return Err(GatewayError::ProtectionUnavailable);
        }
        Ok(())
    }

    /// Read the exact attachment named in the active prepared send. Recheck
    /// the current route and actor before releasing any bytes.
    pub async fn reply_artifact(
        &self,
        attempt: &SendAttempt,
        artifact: &AttachmentRef,
    ) -> Result<Vec<u8>, GatewayError> {
        let (scope, attempt, artifact) = (self.clone(), attempt.clone(), artifact.clone());
        blocking(move || {
            scope.checked(false, |state| {
                let identity = ArtifactUse::from(&attempt);
                let effect = scope.check_effect(state, &identity, Some(&attempt))?;
                scope.artifact_in_effect(state, &identity, &effect, &artifact)?;
                db(scope.store.attachment(&scope.connection, &artifact))
            })
        })
        .await
    }

    /// Synthesize a configured live cue for an authorized tool-enabled actor.
    /// Cues use available local speech capacity and have no artifact or receipt.
    pub async fn synthesize_voice_feedback(
        &self,
        room: &Room,
        actor: &Sender,
        phrase: &str,
    ) -> Result<Vec<u8>, GatewayError> {
        if phrase.trim().is_empty() || phrase.len() > MAX_FEEDBACK_TEXT {
            return Err(GatewayError::Invalid);
        }
        let check = || self.checked(true, |state| {
            if state.policy.delivery_policy(room).protection.is_some()
                || !state.policy.room_policy(room).is_some_and(|room| room.tools)
                || !self.authorized_identity(&state.policy, room, actor)?
                    .is_some_and(|(_, kind)| kind == super::ActorKind::Person)
            {
                return Err(GatewayError::Forbidden);
            }
            Ok(())
        });
        check()?;
        let audio = self.store.audio();
        let _permit = audio.active.try_lock().map_err(|_| GatewayError::TemporarilyUnavailable)?;
        let bytes = tokio::time::timeout(FEEDBACK_TIMEOUT, audio.service.synthesize(&audio.root, phrase))
            .await.map_err(|_| GatewayError::TemporarilyUnavailable)??;
        check()?;
        if bytes.len() > MAX_FEEDBACK_WAV {
            return Err(GatewayError::Invalid);
        }
        validate_wav(&bytes, None)?;
        Ok(bytes)
    }

    /// Render speech from this verified reply's exact committed text. The
    /// result is kept for the same effect across retry and owner restart.
    pub async fn synthesize_reply(
        &self,
        attempt: &SendAttempt,
    ) -> Result<AttachmentRef, GatewayError> {
        if !self.capabilities().attachments {
            return Err(GatewayError::Unsupported);
        }
        let identity = ArtifactUse::from(attempt);
        let check = || {
            self.checked(false, |state| {
                let effect = self.check_effect(state, &identity, Some(attempt))?;
                if !state
                    .policy
                    .room_policy(&effect.room)
                    .is_some_and(|room| room.spoken_replies)
                {
                    return Err(GatewayError::Forbidden);
                }
                let DeliveryBody::Reply(message) = effect.body else {
                    return Err(GatewayError::Unsupported);
                };
                if message.text.trim().is_empty() {
                    return Err(GatewayError::Invalid);
                }
                Ok(message.text)
            })
        };
        let text = check()?;
        let audio = self.store.audio();
        let _guard = tokio::time::timeout(MAX_READ_LIFETIME, audio.active.lock())
            .await
            .map_err(|_| GatewayError::TemporarilyUnavailable)?;
        check()?;
        if let Some(existing) = db(self.store.spoken_reply(&self.connection, &identity))? {
            return Ok(existing);
        }
        let bytes = tokio::time::timeout(
            MAX_READ_LIFETIME,
            audio.service.synthesize(&audio.root, &text),
        )
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)??;
        validate_wav(&bytes, None)?;
        self.checked(false, |state| {
            let effect = self.check_effect(state, &identity, Some(attempt))?;
            if !state
                .policy
                .room_policy(&effect.room)
                .is_some_and(|room| room.spoken_replies)
            {
                return Err(GatewayError::Forbidden);
            }
            db(self.store.write(|tx| {
                let attachment = put(tx, &self.connection, "reply.wav".into(), &bytes)?;
                tx.execute(
                    "INSERT INTO messaging_spoken_replies VALUES (?1,?2,?3,?4,?5)",
                    params![
                        identity.delivery,
                        identity.ordinal,
                        self.connection,
                        attachment.id,
                        attachment.name
                    ],
                )?;
                Ok(attachment)
            }))
        })
    }

    /// Mint a separate read capability for a provider fetching an artifact
    /// instead of receiving its bytes inline. Only an active effect can mint it.
    pub async fn authorize_artifact_fetch(
        &self,
        attempt: &SendAttempt,
        artifact: &AttachmentRef,
        lifetime: Duration,
    ) -> Result<ArtifactFetch, GatewayError> {
        let expires_ms = expires(lifetime, MAX_FETCH_LIFETIME)?;
        let (scope, attempt, artifact) = (self.clone(), attempt.clone(), artifact.clone());
        blocking(move || {
            scope.checked(false, |state| {
                let identity = ArtifactUse::from(&attempt);
                let effect = scope.check_effect(state, &identity, Some(&attempt))?;
                scope.artifact_in_effect(state, &identity, &effect, &artifact)?;
                let token = crate::new_id();
                let hashed = db(key("artifact-fetch", &token))?;
                db(scope.store.write(|tx| {
                    tx.execute(
                        "DELETE FROM messaging_artifact_fetch WHERE expires_ms <= ?1",
                        [now()],
                    )?;
                    let active: i64 = tx.query_row(
                        "SELECT count(*) FROM messaging_artifact_fetch
                         WHERE connection = ?1 AND delivery = ?2 AND ordinal = ?3 AND attempt = ?4",
                        params![
                            scope.connection,
                            identity.delivery,
                            identity.ordinal,
                            identity.attempt
                        ],
                        |row| row.get(0),
                    )?;
                    ensure!(
                        active < MAX_FETCH_GRANTS_PER_EFFECT,
                        "Artifact fetch quota reached"
                    );
                    tx.execute(
                        "INSERT INTO messaging_artifact_fetch VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                        params![
                            hashed,
                            scope.connection,
                            identity.delivery,
                            identity.ordinal,
                            identity.attempt,
                            artifact.id,
                            artifact.name,
                            expires_ms
                        ],
                    )?;
                    Ok(())
                }))?;
                Ok(ArtifactFetch { token, expires_ms })
            })
        })
        .await
    }

    /// Read the originally granted object, with current recipient policy and
    /// the exact provider attempt still in force. A confirmed effect can
    /// serve subsequent reads until the capability expires.
    pub async fn read_provider_artifact(
        &self,
        token: String,
        object: String,
    ) -> Result<Vec<u8>, GatewayError> {
        if !valid_id(&token) || !valid_id(&object) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let hashed = db(key("artifact-fetch", &token))?;
                let (identity, artifact) =
                    db(scope
                        .store
                        .provider_artifact(&scope.connection, &hashed, &object))?
                    .ok_or(GatewayError::Forbidden)?;
                let effect = scope.check_effect(state, &identity, None)?;
                scope.artifact_in_effect(state, &identity, &effect, &artifact)?;
                db(scope.store.attachment(&scope.connection, &artifact))
            })
        })
        .await
    }

    pub async fn revoke_artifact_fetch(&self, token: String) -> Result<bool, GatewayError> {
        if !valid_id(&token) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                let hashed = db(key("artifact-fetch", &token))?;
                db(scope.store.write(|tx| {
                    Ok(tx.execute(
                        "DELETE FROM messaging_artifact_fetch WHERE token = ?1 AND connection = ?2",
                        params![hashed, scope.connection],
                    )? == 1)
                }))
            })
        })
        .await
    }

    fn check_effect(
        &self,
        state: &State,
        identity: &ArtifactUse,
        active: Option<&SendAttempt>,
    ) -> Result<ScopedEffect, GatewayError> {
        let effect =
            db(self
                .store
                .scoped_effect(&self.connection, &self.generation, identity, active))?
            .ok_or(GatewayError::Forbidden)?;
        let room = &effect.room;
        if effect.blocked || state.policy.room_policy(room).is_none() {
            return Err(GatewayError::Forbidden);
        }
        if let Some(sender) = &effect.sender
            && !self.authorize_sender(&state.policy, room, sender)?
        {
            return Err(GatewayError::Forbidden);
        }
        self.check_delivery_protection(
            &state.policy, room, &identity.delivery, effect.protection.as_deref(),
        )?;
        Ok(effect)
    }

    fn artifact_in_effect(
        &self,
        state: &State,
        identity: &ArtifactUse,
        effect: &ScopedEffect,
        artifact: &AttachmentRef,
    ) -> Result<(), GatewayError> {
        let DeliveryBody::Reply(message) = &effect.body else {
            return Err(GatewayError::Forbidden);
        };
        if message.attachments.contains(artifact)
            || state
                .policy
                .room_policy(&effect.room)
                .is_some_and(|room| room.spoken_replies)
                && db(self.store.spoken_reply(&self.connection, identity))?.as_ref()
                    == Some(artifact)
        {
            Ok(())
        } else {
            Err(GatewayError::Forbidden)
        }
    }
}

impl From<&SendAttempt> for ArtifactUse {
    fn from(attempt: &SendAttempt) -> Self {
        Self {
            delivery: attempt.delivery_id.clone(),
            ordinal: attempt.part,
            attempt: attempt.attempt,
        }
    }
}

impl Store {
    fn spoken_addressed(
        &self,
        connection: &str,
        source: &MediaSource,
        revision: i64,
    ) -> Result<bool> {
        let db = crate::lock(&self.0);
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_spoken_addressing
                WHERE connection = ?1 AND event = ?2 AND sender = ?3 AND room = ?4
                    AND revision = ?5 AND expires_ms > ?6)",
            params![
                connection,
                source.event_id,
                source.sender.id,
                serde_json::to_string(&source.room)?,
                revision,
                now()
            ],
            |row| row.get(0),
        )?)
    }

    fn scoped_effect(
        &self,
        connection: &str,
        generation: &str,
        identity: &ArtifactUse,
        active: Option<&SendAttempt>,
    ) -> Result<Option<ScopedEffect>> {
        let db = crate::lock(&self.0);
        let stored: Option<(DeliveryPart, Option<String>, bool, String, Option<String>)> = db
            .query_row(
                "SELECT p.body,d.sender,r.blocked,p.stage,p.generation
                 FROM messaging_parts p JOIN messaging_deliveries d ON d.id = p.delivery
                 JOIN messaging_routes r ON r.id = d.route
                 WHERE d.connection = ?1 AND r.connection = ?1 AND d.id = ?2
                     AND p.ordinal = ?3 AND p.attempt = ?4",
                params![
                    connection,
                    identity.delivery,
                    identity.ordinal,
                    identity.attempt
                ],
                |row| {
                    Ok((
                        super::store::decode(row, 0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((part, sender, blocked, stage, lease)) = stored else {
            return Ok(None);
        };
        if let Some(attempt) = active {
            if stage != "sending" || lease.as_deref() != Some(generation) || attempt.payload != part
            {
                return Ok(None);
            }
        } else if !matches!(stage.as_str(), "sending" | "uncertain" | "sent") {
            return Ok(None);
        }
        let sender = sender.map(|json| serde_json::from_str(&json)).transpose()?;
        Ok(Some(ScopedEffect {
            room: part.room,
            sender,
            blocked,
            body: part.body,
            protection: part.effect.protection,
        }))
    }

    fn provider_artifact(
        &self,
        connection: &str,
        token: &str,
        object: &str,
    ) -> Result<Option<(ArtifactUse, AttachmentRef)>> {
        let db = crate::lock(&self.0);
        Ok(db
            .query_row(
                "SELECT delivery,ordinal,attempt,artifact,name FROM messaging_artifact_fetch
             WHERE connection = ?1 AND token = ?2 AND artifact = ?3 AND expires_ms > ?4",
                params![connection, token, object, now()],
                |row| {
                    Ok((
                        ArtifactUse {
                            delivery: row.get(0)?,
                            ordinal: row.get(1)?,
                            attempt: row.get(2)?,
                        },
                        AttachmentRef {
                            id: row.get(3)?,
                            name: row.get(4)?,
                        },
                    ))
                },
            )
            .optional()?)
    }

    fn spoken_reply(
        &self,
        connection: &str,
        effect: &ArtifactUse,
    ) -> Result<Option<AttachmentRef>> {
        let db = crate::lock(&self.0);
        Ok(db.query_row(
            "SELECT artifact,name FROM messaging_spoken_replies WHERE delivery = ?1 AND ordinal = ?2 AND connection = ?3",
            params![effect.delivery, effect.ordinal, connection],
            |row| Ok(AttachmentRef { id: row.get(0)?, name: row.get(1)? }),
        ).optional()?)
    }
}

pub(super) fn put(
    tx: &Transaction<'_>,
    connection: &str,
    name: String,
    bytes: &[u8],
) -> Result<AttachmentRef> {
    ensure!(
        valid_name(&name) && !bytes.is_empty() && bytes.len() <= MAX_FILE,
        "Invalid messaging media"
    );
    let hash = digest(&SHA256, bytes);
    let id = key("media", &(connection, hash.as_ref()))?;
    put_blob(tx, connection, id, name, bytes)
}

fn put_blob(
    tx: &Transaction<'_>,
    connection: &str,
    id: String,
    name: String,
    bytes: &[u8],
) -> Result<AttachmentRef> {
    let cached: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_media WHERE connection = ?1 AND id = ?2)",
        params![connection, id],
        |row| row.get(0),
    )?;
    if !cached {
        let used: i64 = tx.query_row(
            "SELECT coalesce(sum(length(bytes)),0) FROM messaging_media",
            [],
            |row| row.get(0),
        )?;
        ensure!(
            used + bytes.len() as i64 <= MAX_PROFILE_BYTES,
            "Messaging media quota reached"
        );
        tx.execute(
            "INSERT INTO messaging_media VALUES (?1,?2,?3)",
            params![connection, id, bytes],
        )?;
    }
    Ok(AttachmentRef { id, name })
}

fn put_inbound(
    tx: &Transaction<'_>,
    connection: &str,
    grant: &MediaRead,
    bytes: &[u8],
    preview: bool,
) -> Result<Option<AttachmentRef>> {
    let source = &grant.source;
    let name = &grant.manifest.name;
    let room = serde_json::to_string(&source.room)?;
    let kind = if grant.allow_artifact { "scoped-media" } else { "media" };
    let id = key(kind, &(connection, digest(&SHA256, bytes).as_ref()))?;
    if grant.allow_artifact {
        let staged: i64 = tx.query_row(
            "SELECT coalesce(sum(length(m.bytes)), 0)
             FROM messaging_input_media i JOIN messaging_media m
               ON i.connection=m.connection AND i.artifact=m.id
             WHERE i.connection=?1 AND i.event=?2 AND i.sender=?3 AND i.room=?4
               AND (i.artifact != ?5 OR i.name != ?6)",
            params![connection, source.event_id, source.sender.id, room, id, name],
            |row| row.get(0),
        )?;
        if staged + bytes.len() as i64 > MAX_ADAPTER_INPUT_BYTES as i64 {
            return Ok(None);
        }
    }
    let attachment = put_blob(tx, connection, id, name.clone(), bytes)?;
    tx.execute(
        "INSERT OR IGNORE INTO messaging_input_media VALUES (?1,?2,?3,?4,?5,?6)",
        params![connection, source.event_id, source.sender.id, room, attachment.id, attachment.name],
    )?;
    if grant.allow_artifact {
        tx.execute(
            "INSERT OR IGNORE INTO messaging_scoped_input_media VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![connection, source.event_id, source.sender.id, room, attachment.id, attachment.name, preview],
        )?;
    }
    Ok(Some(attachment))
}

const BOUND_INPUT_MEDIA: &str = "
    FROM messaging_input_media i JOIN messaging_media m
        ON m.connection=i.connection AND m.id=i.artifact
    LEFT JOIN messaging_scoped_input_media s
        ON s.connection=i.connection AND s.event=i.event AND s.sender=i.sender AND s.room=i.room
        AND s.artifact=i.artifact AND s.name=i.name
    WHERE i.connection=?1 AND i.event=?2 AND i.sender=?3 AND i.room=?4
        AND i.artifact=?5 AND i.name=?6";

pub(super) struct InputMedia {
    pub file: AttachmentRef,
    pub size: usize,
    pub preview: bool,
}

pub(super) fn attachments_authorized(
    tx: &Transaction<'_>,
    connection: &str,
    event: &InboundEvent,
    attachments: &[AttachmentRef],
) -> Result<bool> {
    Ok(input_media(tx, connection, event, attachments)?.is_some())
}

pub(super) fn input_media(
    db: &Connection,
    connection: &str,
    event: &InboundEvent,
    attachments: &[AttachmentRef],
) -> Result<Option<Vec<InputMedia>>> {
    let room = serde_json::to_string(&event.room)?;
    let mut query = db.prepare(&format!("SELECT length(m.bytes),s.provider_inline {BOUND_INPUT_MEDIA}"))?;
    let mut total = 0;
    let mut preview_bytes = 0;
    let mut extended = true;
    let mut refs = Vec::with_capacity(attachments.len());
    for file in attachments {
        let Some((size, inline)) = query.query_row(
            params![connection, event.id, event.sender.id, room, file.id, file.name],
            |row| Ok((row.get::<_,i64>(0)?,row.get::<_,Option<bool>>(1)?)),
        ).optional()? else { return Ok(None) };
        let size = usize::try_from(size)?;
        total += size;
        extended &= inline.is_some();
        let preview = inline.unwrap_or(true) && preview_bytes + size <= MAX_INPUT_BYTES;
        if preview {
            preview_bytes += size;
        }
        refs.push(InputMedia { file: file.clone(), size, preview });
    }
    let limit = if extended { MAX_ADAPTER_INPUT_BYTES } else { MAX_INPUT_BYTES };
    Ok((total <= limit).then_some(refs))
}

pub fn read_input_artifact(
    db: &Connection,
    connection: &str,
    event: &InboundEvent,
    file: &AttachmentRef,
    offset: usize,
    limit: usize,
) -> Result<Option<Vec<u8>>> {
    let room = serde_json::to_string(&event.room)?;
    let position = i64::try_from(offset.checked_add(1).ok_or_else(|| anyhow::anyhow!("Invalid attachment offset"))?)?;
    let limit = i64::try_from(limit)?;
    db.query_row(
        &format!("SELECT substr(m.bytes,?7,?8) {BOUND_INPUT_MEDIA} AND s.artifact IS NOT NULL"),
        params![connection,event.id,event.sender.id,room,file.id,file.name,position,limit],
        |row| row.get(0),
    ).optional().map_err(Into::into)
}

fn verify_bytes(grant: &MediaRead, data: &[u8]) -> Result<(), GatewayError> {
    let manifest = &grant.manifest;
    if data.is_empty()
        || data.len() > grant.max_file
        || manifest.size.is_some_and(|n| n != data.len())
        || manifest
            .sha256
            .is_some_and(|hash| digest(&SHA256, data).as_ref() != hash)
    {
        return Err(GatewayError::Invalid);
    }
    Ok(())
}

fn valid_source(source: &MediaSource) -> bool {
    valid_id(&source.event_id)
        && valid_id(&source.room.chat)
        && source.room.thread.as_ref().is_none_or(|id| valid_id(id))
        && valid_id(&source.sender.id)
        && source.sender.roles.len() <= 128
        && source.sender.roles.iter().all(|id| valid_id(id))
}

pub fn valid_name(name: &str) -> bool {
    valid_id(name) && name != "." && name != ".." && !name.contains(['/', '\\', ':'])
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn expires(lifetime: Duration, max: Duration) -> Result<i64, GatewayError> {
    if lifetime.as_millis() == 0 || lifetime > max {
        return Err(GatewayError::Invalid);
    }
    Ok(now() + lifetime.as_millis() as i64)
}

fn remaining(deadline: i64) -> Result<Duration, GatewayError> {
    let millis = deadline - now();
    if millis <= 0 {
        return Err(GatewayError::Forbidden);
    }
    Ok(Duration::from_millis(millis as u64))
}

fn starts_with_phrase(text: &str, phrase: &str) -> bool {
    fn words(value: &str) -> impl Iterator<Item = String> + '_ {
        value
            .split(|ch: char| !ch.is_alphanumeric())
            .filter(|word| !word.is_empty())
            .map(str::to_lowercase)
    }
    let mut text = words(text);
    let mut phrase = words(phrase).peekable();
    phrase.peek().is_some() && phrase.all(|word| text.next().as_deref() == Some(&word))
}

fn validate_wav(bytes: &[u8], sample_rate: Option<u32>) -> Result<(), GatewayError> {
    let invalid = GatewayError::Invalid;
    if bytes.len() < 12
        || bytes.len() > MAX_FILE
        || bytes.get(..4) != Some(b"RIFF")
        || bytes.get(8..12) != Some(b"WAVE")
    {
        return Err(invalid);
    }
    let declared_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if declared_len.checked_add(8) != Some(bytes.len()) {
        return Err(invalid);
    }
    let mut rest = &bytes[12..];
    let mut format = false;
    let mut data = false;
    while !rest.is_empty() {
        let header = rest.get(..8).ok_or(invalid)?;
        let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let end = 8usize.checked_add(size).ok_or(invalid)?;
        let chunk = rest.get(8..end).ok_or(invalid)?;
        match &header[..4] {
            b"fmt " if !format => {
                let description = chunk.get(..16).ok_or(invalid)?;
                let rate = u32::from_le_bytes(description[4..8].try_into().unwrap());
                format = description[..2] == 1u16.to_le_bytes()
                    && description[2..4] == 1u16.to_le_bytes()
                    && (8_000..=192_000).contains(&rate)
                    && sample_rate.is_none_or(|wanted| rate == wanted)
                    && description[8..12] == (rate * 2).to_le_bytes()
                    && description[12..14] == 2u16.to_le_bytes()
                    && description[14..16] == 16u16.to_le_bytes();
                if !format {
                    return Err(invalid);
                }
            }
            b"data" if !data => {
                data = size > 0 && size % 2 == 0;
            }
            _ => {}
        }
        rest = rest.get(end + size % 2..).ok_or(invalid)?;
    }
    if format && data { Ok(()) } else { Err(invalid) }
}
