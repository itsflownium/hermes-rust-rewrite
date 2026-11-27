use super::{
    Arc, AtomicBool, AtomicU64, Binding, CancellationToken, Channels, Config, ConnectionInfo,
    CoreEvent, DecodeConfig, DecodeMode, Driver, Event, EventContext, EventHandler, Ingress,
    JOIN_TIMEOUT, Mutex, Ordering, SAMPLE_INTERVAL, SampleRate, VOICE_ORIGIN, Voice, authorized,
    cues::AmbientTrack, mpsc,
};
use crate::messaging::{
    AttachmentManifest, InboundBody, InboundEvent, MediaPurpose, MediaRead, MediaSource, Message,
    Sender,
};
use std::{
    collections::HashMap,
    io::Cursor,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[cfg(test)]
mod tests;

const MAX_SPEAKERS: usize = 12;
const MAX_UTTERANCE: usize = 16_000 * 20;
const MIN_UTTERANCE: usize = 16_000 / 5;
const SILENCE: Duration = Duration::from_millis(650);
const GRANT_LIFETIME: Duration = Duration::from_secs(90);
const MAX_FRAME: usize = 16_000 * 120 / 1000;
const SPEECH_ECHO_TAIL: Duration = Duration::from_millis(180);

pub(super) struct AudioLink {
    pub(super) driver: Mutex<Driver>,
    pub(super) stopped: CancellationToken,
    pub(super) ready: AtomicBool,
    pub(super) epoch: AtomicU64,
    pub(super) playing: AtomicBool,
    pub(super) ambient: Mutex<Option<AmbientTrack>>,
    pub(super) ack: Mutex<Option<CancellationToken>>,
    pub(super) last_mic: Mutex<Option<Instant>>,
    quiet_until: Mutex<Instant>,
    transport_epoch: AtomicU64,
    last_progress: Mutex<Instant>,
    pub(super) playback: tokio::sync::Mutex<()>,
}

impl AudioLink {
    fn new(driver: Driver) -> Self {
        Self {
            driver: Mutex::new(driver),
            stopped: CancellationToken::new(),
            ready: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            playing: AtomicBool::new(false),
            ambient: Mutex::new(None),
            ack: Mutex::new(None),
            last_mic: Mutex::new(None),
            quiet_until: Mutex::new(Instant::now()),
            transport_epoch: AtomicU64::new(0),
            last_progress: Mutex::new(Instant::now()),
            playback: tokio::sync::Mutex::new(()),
        }
    }

    pub(super) fn start(
        voice: Voice,
        binding: Binding,
        ingress: Ingress,
        info: ConnectionInfo,
    ) -> Arc<Self> {
        let driver = Driver::new(Config::default().decode_mode(DecodeMode::Decode(
            DecodeConfig::new(Channels::Mono, SampleRate::Hz16000),
        )));
        let link = Arc::new(Self::new(driver));
        let (signals, receiver) = mpsc::channel(96);
        let handler = DriverEvents {
            link: Arc::downgrade(&link),
            signals,
            bot: voice.bot,
        };
        let connect = {
            let mut driver = crate::lock(&link.driver);
            for event in [
                CoreEvent::DriverConnect,
                CoreEvent::DriverReconnect,
                CoreEvent::DriverDisconnect,
                CoreEvent::SpeakingStateUpdate,
                CoreEvent::ClientDisconnect,
                CoreEvent::VoiceTick,
            ] {
                driver.add_global_event(event.into(), handler.clone());
            }
            driver.connect(info)
        };
        let active = link.clone();
        tokio::spawn(async move {
            let connected = tokio::select! {
                _ = active.stopped.cancelled() => false,
                result = tokio::time::timeout(JOIN_TIMEOUT, connect) => matches!(result, Ok(Ok(()))),
            };
            if connected && !active.stopped.is_cancelled() {
                active.run(voice, binding, ingress, receiver).await;
            } else {
                active.stopped.cancel();
                crate::lock(&active.driver).leave();
            }
        });
        link
    }

    async fn run(
        self: Arc<Self>,
        voice: Voice,
        binding: Binding,
        ingress: Ingress,
        receiver: mpsc::Receiver<Signal>,
    ) {
        tokio::join!(
            receive(
                voice.clone(),
                binding.clone(),
                ingress.clone(),
                self.clone(),
                receiver
            ),
            voice.ambient(&binding, &ingress, &self),
        );
    }

    pub(super) fn valid(&self, epoch: u64) -> bool {
        self.ready.load(Ordering::Acquire)
            && !self.stopped.is_cancelled()
            && self.epoch.load(Ordering::Acquire) == epoch
    }

    pub(super) fn last_progress(&self) -> Instant {
        *crate::lock(&self.last_progress)
    }

    pub(super) fn end_speech(&self) {
        *crate::lock(&self.quiet_until) = Instant::now() + SPEECH_ECHO_TAIL;
        self.playing.store(false, Ordering::Release);
    }

    fn capture_blocked(&self) -> bool {
        self.playing.load(Ordering::Acquire) || Instant::now() < *crate::lock(&self.quiet_until)
    }

    fn refresh(&self, ready: bool) {
        self.ready.store(false, Ordering::Release);
        *crate::lock(&self.last_progress) = Instant::now();
        self.transport_epoch.fetch_add(1, Ordering::AcqRel);
        self.invalidate_output();
        self.ready.store(ready, Ordering::Release);
    }
}

#[derive(Clone)]
struct DriverEvents {
    link: std::sync::Weak<AudioLink>,
    signals: mpsc::Sender<Signal>,
    bot: u64,
}

enum Signal {
    Speaker {
        transport: u64,
        ssrc: u32,
        user: u64,
        microphone: bool,
    },
    Disconnected(u64),
    Frame {
        epoch: u64,
        ssrc: u32,
        pcm: Vec<i16>,
    },
}

#[async_trait::async_trait]
impl EventHandler for DriverEvents {
    async fn act(&self, context: &EventContext<'_>) -> Option<Event> {
        let link = self.link.upgrade()?;
        let epoch = link.epoch.load(Ordering::Acquire);
        match context {
            EventContext::DriverConnect(_) => link.refresh(true),
            EventContext::DriverReconnect(_) => link.refresh(true),
            EventContext::DriverDisconnect(_) => link.refresh(false),
            EventContext::SpeakingStateUpdate(update) => {
                if let Some(user) = update.user_id {
                    let signal = Signal::Speaker {
                        transport: link.transport_epoch.load(Ordering::Acquire),
                        ssrc: update.ssrc,
                        user: user.0,
                        microphone: user.0 != self.bot
                            && update
                                .speaking
                                .contains(songbird::model::SpeakingState::MICROPHONE),
                    };
                    if self.signals.try_send(signal).is_err() {
                        link.epoch.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            EventContext::ClientDisconnect(user) => {
                if self
                    .signals
                    .try_send(Signal::Disconnected(user.user_id.0))
                    .is_err()
                {
                    link.epoch.fetch_add(1, Ordering::AcqRel);
                }
            }
            EventContext::VoiceTick(tick) if link.valid(epoch) && !link.capture_blocked() => {
                for (ssrc, audio) in tick.speaking.iter().take(MAX_SPEAKERS) {
                    if audio.packet.is_some()
                        && let Some(pcm) = &audio.decoded_voice
                        && !pcm.is_empty()
                        && pcm.len() <= MAX_FRAME
                    {
                        let _ = self.signals.try_send(Signal::Frame {
                            epoch,
                            ssrc: *ssrc,
                            pcm: pcm.clone(),
                        });
                    }
                }
            }
            _ => {}
        }
        None
    }
}

struct Utterance {
    actor: u64,
    source: MediaSource,
    media: MediaRead,
    pcm: Vec<i16>,
    last: Instant,
}

async fn receive(
    voice: Voice,
    binding: Binding,
    ingress: Ingress,
    link: Arc<AudioLink>,
    mut signals: mpsc::Receiver<Signal>,
) {
    let mut speakers = HashMap::new();
    let mut captures: HashMap<u32, Utterance> = HashMap::new();
    let mut timer = tokio::time::interval(SAMPLE_INTERVAL);
    let mut epoch = link.epoch.load(Ordering::Acquire);
    let mut transport = link.transport_epoch.load(Ordering::Acquire);
    let max_transcripts = Arc::new(Semaphore::new(4));
    loop {
        let current = link.epoch.load(Ordering::Acquire);
        if current != epoch {
            captures.clear();
            epoch = current;
        }
        let current_transport = link.transport_epoch.load(Ordering::Acquire);
        if current_transport != transport {
            speakers.clear();
            transport = current_transport;
        }
        tokio::select! {
            biased;
            _ = link.stopped.cancelled() => break,
            _ = timer.tick() => {
                if !link.valid(epoch) || link.capture_blocked() {
                    captures.clear();
                    continue;
                }
                let complete = captures.iter().filter_map(|(ssrc, audio)|
                    (audio.last.elapsed() >= SILENCE || audio.pcm.len() >= MAX_UTTERANCE).then_some(*ssrc)
                ).collect::<Vec<_>>();
                for ssrc in complete {
                    let Some(audio) = captures.remove(&ssrc) else { continue };
                    if audio.pcm.len() < MIN_UTTERANCE || !member_eligible(&voice, &binding, audio.actor) {
                        continue;
                    }
                    if let Ok(permit) = max_transcripts.clone().try_acquire_owned() {
                        let (voice, binding, ingress, link) =
                            (voice.clone(), binding.clone(), ingress.clone(), link.clone());
                        tokio::spawn(async move {
                            let _permit = permit;
                            submit(voice, binding, ingress, link, epoch, audio).await;
                        });
                    }
                }
            }
            Some(signal) = signals.recv() => match signal {
                Signal::Speaker { transport: current, ssrc, user, microphone } if current == transport => {
                    if microphone && member_eligible(&voice, &binding, user)
                        && (speakers.len() < MAX_SPEAKERS || speakers.contains_key(&ssrc))
                    {
                        if speakers.insert(ssrc, user).is_some_and(|old| old != user) {
                            captures.remove(&ssrc);
                        }
                    } else if speakers.get(&ssrc).is_some_and(|old| *old != user) {
                        speakers.remove(&ssrc);
                        captures.remove(&ssrc);
                    }
                }
                Signal::Disconnected(user) => {
                    speakers.retain(|_, actor| *actor != user);
                    captures.retain(|_, audio| audio.actor != user);
                }
                Signal::Frame { epoch: current, ssrc, pcm } if current == epoch && link.valid(epoch) => {
                    let Some(&actor) = speakers.get(&ssrc) else { continue };
                    if link.capture_blocked() || !member_eligible(&voice, &binding, actor) {
                        captures.remove(&ssrc);
                        continue;
                    }
                    // Avoid indefinitely extending a segment with Discord silence packets.
                    if !pcm.iter().any(|sample| sample.unsigned_abs() >= 128) {
                        continue;
                    }
                    let capture = if let Some(capture) = captures.get_mut(&ssrc) {
                        capture
                    } else {
                        let Some(capture) = begin(&binding, &ingress, actor).await else { continue };
                        if !link.valid(epoch) || !member_eligible(&voice, &binding, actor) {
                            continue;
                        }
                        captures.entry(ssrc).or_insert(capture)
                    };
                    link.note_microphone();
                    let remaining = MAX_UTTERANCE - capture.pcm.len();
                    capture.pcm.extend_from_slice(&pcm[..pcm.len().min(remaining)]);
                    capture.last = Instant::now();
                    if let Some(call) = crate::lock(&voice.state).calls.get_mut(&binding.guild) {
                        call.last_activity = capture.last;
                    }
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
pub(crate) struct FixtureCall {
    link: Arc<AudioLink>,
    signals: mpsc::Sender<Signal>,
}

#[cfg(test)]
impl FixtureCall {
    pub(crate) fn attach(
        voice: &Voice,
        ingress: &Ingress,
        guild: u64,
    ) -> Option<Self> {
        let (signals, receiver) = mpsc::channel(96);
        let link = Arc::new(AudioLink::new(Driver::new(Config::default())));
        link.refresh(true);
        let binding = {
            let mut state = crate::lock(&voice.state);
            let call = state
                .calls
                .get_mut(&guild)
                .filter(|call| call.session.is_some() && call.link.is_none())?;
            call.link = Some(link.clone());
            call.binding.clone()
        };
        tokio::spawn(
            link.clone()
                .run(voice.clone(), binding, ingress.clone(), receiver),
        );
        Some(Self { link, signals })
    }

    pub(crate) async fn utterance(&self, user: u64, ssrc: u32) {
        self.signals
            .send(Signal::Speaker {
                transport: self.link.transport_epoch.load(Ordering::Acquire),
                ssrc,
                user,
                microphone: true,
            })
            .await
            .unwrap();
        for _ in 0..14 {
            self.signals
                .send(Signal::Frame {
                    epoch: self.link.epoch.load(Ordering::Acquire),
                    ssrc,
                    pcm: vec![1000; 320],
                })
                .await
                .unwrap();
        }
    }

    pub(crate) fn speaking(&self) -> bool {
        self.link.playing.load(Ordering::Acquire)
    }
}

fn member_eligible(voice: &Voice, binding: &Binding, actor: u64) -> bool {
    let state = crate::lock(&voice.state);
    actor != voice.bot
        && state.channels.contains(&(binding.guild, binding.channel))
        && state
            .calls
            .get(&binding.guild)
            .is_some_and(|call| call.binding.id == binding.id)
        && state
            .members
            .get(&(binding.guild, voice.bot))
            .is_some_and(|member| member.channel == binding.channel && !member.deafened)
        && state
            .members
            .get(&(binding.guild, actor))
            .is_some_and(|member| member.channel == binding.channel && !member.muted && !member.bot)
}

async fn begin(binding: &Binding, ingress: &Ingress, actor: u64) -> Option<Utterance> {
    let sender = Sender {
        id: actor.to_string(),
        roles: Default::default(),
    };
    if !authorized(ingress, &binding.room, &sender).ok()? {
        return None;
    }
    let event_id = format!("{VOICE_ORIGIN}{}-{}", binding.id, crate::new_id());
    let source = MediaSource {
        event_id,
        room: binding.room.clone(),
        sender,
        mentioned: true,
    };
    let media = ingress
        .authorize_media(
            source.clone(),
            AttachmentManifest {
                name: "discord-utterance.wav".into(),
                size: None,
                sha256: None,
            },
            MediaPurpose::VoiceNote,
            GRANT_LIFETIME,
        )
        .await
        .ok()?;
    Some(Utterance {
        actor,
        source,
        media,
        pcm: Vec::new(),
        last: Instant::now(),
    })
}

async fn submit(
    voice: Voice,
    binding: Binding,
    ingress: Ingress,
    link: Arc<AudioLink>,
    epoch: u64,
    audio: Utterance,
) {
    if !link.valid(epoch) || !member_eligible(&voice, &binding, audio.actor) {
        return;
    }
    let wav = super::super::media::pcm_wave(&audio.pcm);
    let text = tokio::select! {
        _ = link.stopped.cancelled() => return,
        text = ingress.transcribe_voice(audio.media, || async { Ok(Cursor::new(wav)) }) => text,
    };
    let Ok(text) = text else { return };
    if link.valid(epoch) && member_eligible(&voice, &binding, audio.actor) {
        let submit = ingress.submit(
            vec![InboundEvent {
                id: audio.source.event_id.clone(),
                room: audio.source.room,
                sender: audio.source.sender,
                reply_anchor: Some(audio.source.event_id),
                mentioned: audio.source.mentioned,
                body: InboundBody::Message(Message::text(text)),
            }],
            None,
        );
        let _ = submit.await;
    }
}
