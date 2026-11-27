use super::{
    ActivePlayback, AudioLink, Binding, CancellationToken, Ingress, TrackFinished, VOICE_ORIGIN,
    Voice, authorized, playback_completed,
};
use crate::{
    media::decode_voice_note, settings::DiscordVoiceFxSettings, voice_note::wav_pcm_format,
};
use crate::messaging::{FirstToolFeedback, InboundBody, InboundEvent, Sender};
use songbird::{
    events::{EventData, TrackEvent},
    input::{Input, RawAdapter},
    tracks::{LoopState, Track, TrackHandle},
};
use std::{
    fs::File,
    io::{Cursor, Read},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

const MAX_CUE_CLIP: Duration = Duration::from_secs(6);
const CUE_PLAYBACK_LIMIT: Duration = Duration::from_secs(8);
const MAX_AMBIENT_CLIP: Duration = Duration::from_secs(30);
const MAX_AMBIENT_BYTES: usize = 8 * 1024 * 1024;
const AMBIENT_TICK: Duration = Duration::from_millis(80);
const DUCK_RELEASE: Duration = Duration::from_millis(400);
const MIC_TAIL: Duration = Duration::from_millis(220);
const DEFAULT_ACKS: &[&str] = &["One moment.", "I'll take a look.", "I'm working on it."];

pub(super) struct VoiceCues {
    pub(super) settings: DiscordVoiceFxSettings,
    next_phrase: AtomicUsize,
}

impl VoiceCues {
    pub(super) fn new(settings: DiscordVoiceFxSettings) -> Self {
        Self {
            settings,
            next_phrase: AtomicUsize::new(0),
        }
    }

    pub(super) fn lead_ms(&self) -> u16 {
        if self.settings.enabled {
            self.settings.lead_silence_ms
        } else {
            0
        }
    }

    fn speech_gain(&self) -> f32 {
        if self.settings.enabled {
            self.settings.speech_gain.amplitude()
        } else {
            1.0
        }
    }

    fn phrase(&self) -> &str {
        let index = self.next_phrase.fetch_add(1, Ordering::Relaxed);
        self.settings
            .ack_phrases
            .as_ref()
            .filter(|phrases| !phrases.is_empty())
            .map_or_else(
                || DEFAULT_ACKS[index % DEFAULT_ACKS.len()],
                |phrases| phrases[index % phrases.len()].as_str(),
            )
    }

    fn duck(&self) -> f32 {
        self.settings
            .duck_gain
            .amplitude()
            .min(self.settings.ambient_gain.amplitude())
    }
}

impl Voice {
    pub(crate) fn first_tool_feedback(
        &self,
        ingress: Ingress,
        event: &InboundEvent,
    ) -> Option<FirstToolFeedback> {
        if !self.cues.settings.enabled
            || !self.cues.settings.ack_enabled
            || !matches!(event.body, InboundBody::Message(_))
        {
            return None;
        }
        let actor = event.sender.clone();
        let user = actor.id.parse::<u64>().ok()?;
        let (binding, link, epoch) = {
            let state = crate::lock(&self.state);
            let call = state.calls.values().find(|call| {
                call.binding.room == event.room
                    && state
                        .members
                        .get(&(call.binding.guild, user))
                        .is_some_and(|member| member.channel == call.binding.channel && !member.bot)
            })?;
            if event.id.starts_with(VOICE_ORIGIN)
                && !event
                    .id
                    .starts_with(&format!("{VOICE_ORIGIN}{}-", call.binding.id))
            {
                return None;
            }
            let link = call.link.as_ref()?;
            let epoch = link.epoch.load(Ordering::Acquire);
            if !link.valid(epoch) {
                return None;
            }
            (call.binding.clone(), link.clone(), epoch)
        };

        let voice = self.clone();
        Some(Arc::new(move |turn| {
            let (voice, ingress, binding, actor, link) = (
                voice.clone(),
                ingress.clone(),
                binding.clone(),
                actor.clone(),
                link.clone(),
            );
            tokio::spawn(async move {
                voice
                    .speak_ack(&ingress, &binding, &actor, &link, epoch, turn)
                    .await;
            });
        }))
    }

    async fn speak_ack(
        &self,
        ingress: &Ingress,
        binding: &Binding,
        actor: &Sender,
        link: &AudioLink,
        epoch: u64,
        turn: CancellationToken,
    ) {
        let allowed = || {
            link.valid(epoch)
                && self.audience_allowed(ingress, binding)
                && authorized(ingress, &binding.room, actor).unwrap_or_default()
        };
        if turn.is_cancelled() || !allowed() || link.playback.try_lock().is_err() {
            return;
        }
        let Some(ack) = AckLease::reserve(link, &turn) else {
            return;
        };
        let speech = tokio::select! {
            biased;
            _ = ack.stop.cancelled() => return,
            _ = link.stopped.cancelled() => return,
            result = ingress.synthesize_voice_feedback(&binding.room, actor, self.cues.phrase()) => result,
        };
        let Ok(bytes) = speech else { return };
        let Some(input) = speech_input(&bytes, MAX_CUE_CLIP, self.cues.lead_ms()) else {
            return;
        };
        let Ok(_speech) = link.playback.try_lock() else {
            return;
        };
        if ack.stop.is_cancelled() || !allowed() {
            return;
        }
        let (_playing, completed) = link.start_speech(input, &self.cues);
        tokio::select! {
            biased;
            _ = ack.stop.cancelled() => {}
            _ = tokio::time::sleep(CUE_PLAYBACK_LIMIT) => {}
            _ = playback_completed(&link.stopped, completed, allowed) => {}
        }
    }

    pub(super) async fn ambient(&self, binding: &Binding, ingress: &Ingress, link: &AudioLink) {
        let settings = &self.cues.settings;
        if !settings.enabled
            || !settings.ambient_enabled
            || settings.ambient_gain.amplitude() == 0.0
        {
            return;
        }
        let path = settings
            .ambient_path
            .as_ref()
            .map(|path| ingress.root().join(path));
        let clip = tokio::select! {
            _ = link.stopped.cancelled() => return,
            result = tokio::task::spawn_blocking(move || ambient_clip(path.as_deref())) => {
                let Ok(clip) = result else { return };
                clip
            }
        };
        let _cleanup = AmbientCleanup(link);
        let mut tick = tokio::time::interval(AMBIENT_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = link.stopped.cancelled() => return,
                _ = tick.tick() => {}
            }
            let epoch = link.epoch.load(Ordering::Acquire);
            let allowed = link.valid(epoch) && self.audience_allowed(ingress, binding);
            let mut ambient = crate::lock(&link.ambient);
            if !allowed || !link.valid(epoch) {
                ambient.take();
                continue;
            }
            if ambient.as_ref().is_some_and(|track| track.epoch != epoch) {
                ambient.take();
            }
            let mic_active = link.mic_active();
            let speaking = link.playing.load(Ordering::Acquire);
            let idle = settings.ambient_gain.amplitude();
            let target = if mic_active {
                0.0
            } else if speaking {
                self.cues.duck()
            } else {
                idle
            };
            if let Some(track) = ambient.as_mut() {
                track.approach(target, idle);
            } else {
                let handle = crate::lock(&link.driver).play(
                    Track::new(clip.input())
                        .loops(LoopState::Infinite)
                        .volume(target),
                );
                *ambient = Some(AmbientTrack {
                    handle,
                    epoch,
                    gain: target,
                    updated: Instant::now(),
                });
            }
        }
    }
}

struct AckLease<'a> {
    pending: &'a Mutex<Option<CancellationToken>>,
    stop: CancellationToken,
}

impl<'a> AckLease<'a> {
    fn reserve(link: &'a AudioLink, turn: &CancellationToken) -> Option<Self> {
        let mut pending = crate::lock(&link.ack);
        if pending.is_some() {
            return None;
        }
        let stop = turn.child_token();
        *pending = Some(stop.clone());
        Some(Self {
            pending: &link.ack,
            stop,
        })
    }
}

impl Drop for AckLease<'_> {
    fn drop(&mut self) {
        crate::lock(self.pending).take();
    }
}

impl AudioLink {
    pub(super) fn start_speech(
        &self,
        input: Input,
        cues: &VoiceCues,
    ) -> (ActivePlayback<'_>, oneshot::Receiver<bool>) {
        let (done, finished) = oneshot::channel();
        let completion = Arc::new(Mutex::new(Some(done)));
        let mut track = Track::new(input).volume(cues.speech_gain());
        for event in [TrackEvent::End, TrackEvent::Error] {
            track.events.add_event(
                EventData::new(event.into(), TrackFinished(completion.clone())),
                Duration::ZERO,
            );
        }
        self.playing.store(true, Ordering::Release);
        if let Some(ambient) = crate::lock(&self.ambient).as_mut() {
            ambient.lower(cues.duck());
        }
        let playback = ActivePlayback {
            link: self,
            track: crate::lock(&self.driver).play(track),
        };
        (playback, finished)
    }

    pub(super) fn cancel_ack(&self) {
        if let Some(stop) = crate::lock(&self.ack).as_ref() {
            stop.cancel();
        }
    }

    pub(super) fn invalidate_output(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.cancel_ack();
        crate::lock(&self.ambient).take();
    }

    pub(super) fn note_microphone(&self) {
        *crate::lock(&self.last_mic) = Some(Instant::now());
    }

    fn mic_active(&self) -> bool {
        crate::lock(&self.last_mic).is_some_and(|last| last.elapsed() < MIC_TAIL)
    }
}

pub(super) struct AmbientTrack {
    handle: TrackHandle,
    epoch: u64,
    gain: f32,
    updated: Instant,
}

impl AmbientTrack {
    #[cfg(test)]
    pub(super) fn gain(&self) -> f32 {
        self.gain
    }

    fn lower(&mut self, target: f32) {
        self.set(self.gain.min(target));
    }

    fn approach(&mut self, target: f32, full_gain: f32) {
        let next = eased_gain(self.gain, target, full_gain, self.updated.elapsed());
        self.set(next);
    }

    fn set(&mut self, gain: f32) {
        if self.gain != gain {
            let _ = self.handle.set_volume(gain);
            self.gain = gain;
        }
        self.updated = Instant::now();
    }
}

impl Drop for AmbientTrack {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

struct AmbientCleanup<'a>(&'a AudioLink);

impl Drop for AmbientCleanup<'_> {
    fn drop(&mut self) {
        crate::lock(&self.0.ambient).take();
    }
}

fn eased_gain(current: f32, target: f32, full_gain: f32, elapsed: Duration) -> f32 {
    (current + full_gain * elapsed.as_secs_f32() / DUCK_RELEASE.as_secs_f32()).min(target)
}

struct Clip {
    pcm: Arc<[u8]>,
    rate: u32,
    channels: u32,
}

impl Clip {
    fn from_wav(wav: &[u8], limit: Duration, lead_ms: u16) -> Option<Self> {
        let (rate, channels, samples) = wav_pcm_format(wav)?;
        let samples_per_second = rate * channels as usize;
        if samples.len() / 2 > samples_per_second * limit.as_secs() as usize {
            return None;
        }
        let lead_frames = rate * usize::from(lead_ms) / 1000;
        let lead_bytes = lead_frames * channels as usize * size_of::<f32>();
        let mut pcm = vec![0; lead_bytes];
        pcm.reserve(samples.len() * 2);
        for sample in samples.chunks_exact(2) {
            let value = f32::from(i16::from_le_bytes([sample[0], sample[1]])) / 32768.0;
            pcm.extend(value.to_le_bytes());
        }
        Some(Self {
            pcm: pcm.into(),
            rate: rate as u32,
            channels,
        })
    }

    fn input(&self) -> Input {
        RawAdapter::new(Cursor::new(self.pcm.clone()), self.rate, self.channels).into()
    }
}

pub(super) fn speech_input(wav: &[u8], limit: Duration, lead_ms: u16) -> Option<Input> {
    Some(Clip::from_wav(wav, limit, lead_ms)?.input())
}

fn ambient_clip(path: Option<&Path>) -> Clip {
    path.and_then(load_ambient)
        .unwrap_or_else(synthetic_ambient)
}

fn load_ambient(path: &Path) -> Option<Clip> {
    let meta = path.metadata().ok()?;
    if !meta.is_file() || meta.len() > MAX_AMBIENT_BYTES as u64 {
        return None;
    }
    let mut bytes = Vec::new();
    File::open(path)
        .ok()?
        .take((MAX_AMBIENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() > MAX_AMBIENT_BYTES {
        return None;
    }
    if bytes.starts_with(b"OggS") {
        bytes = decode_voice_note(&bytes).ok()?;
    }
    Clip::from_wav(&bytes, MAX_AMBIENT_CLIP, 0)
}

#[cfg(test)]
#[test]
fn lead_silence_and_pcm_stay_aligned_to_stereo_frames() {
    let mut wave = crate::media::pcm_wave(&[1000; 4]);
    wave[22..24].copy_from_slice(&2_u16.to_le_bytes());
    wave[24..28].copy_from_slice(&44_100_u32.to_le_bytes());
    wave[28..32].copy_from_slice(&176_400_u32.to_le_bytes());
    wave[32..34].copy_from_slice(&4_u16.to_le_bytes());

    let clip = Clip::from_wav(&wave, Duration::from_secs(1), 15).unwrap();
    assert_eq!(clip.pcm.len(), (661 * 2 + 4) * size_of::<f32>());
    assert!(
        clip.pcm[..661 * 2 * size_of::<f32>()]
            .iter()
            .all(|byte| *byte == 0)
    );
    wave[40..44].copy_from_slice(&6_u32.to_le_bytes());
    wave.truncate(50);
    assert!(Clip::from_wav(&wave, Duration::from_secs(1), 15).is_none());
}

#[cfg(test)]
#[test]
fn configured_ambient_clips_are_bounded_and_missing_files_use_a_loop() {
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("ambient.wav");
    std::fs::write(
        &local,
        crate::media::pcm_wave(&[2000; 320]),
    )
    .unwrap();
    let clip = ambient_clip(Some(&local));
    assert_eq!(clip.rate, 16_000);
    assert_eq!(clip.pcm.len(), 320 * size_of::<f32>());
    assert_eq!(
        f32::from_le_bytes(clip.pcm[..4].try_into().unwrap()),
        2000.0 / 32768.0
    );

    let malformed = dir.path().join("not-audio");
    std::fs::write(&malformed, b"malformed audio").unwrap();
    let fallback = ambient_clip(Some(&malformed));
    assert_eq!(fallback.pcm.len(), 16_000 * 4 * size_of::<f32>());
    assert!(fallback.pcm.iter().any(|sample| *sample != 0));
    assert_eq!(fallback.pcm, ambient_clip(Some(dir.path())).pcm);
}

fn synthetic_ambient() -> Clip {
    const RATE: u32 = 16_000;
    let mut pcm = Vec::with_capacity(RATE as usize * 4 * size_of::<f32>());
    for index in 0..(RATE * 4) {
        let phase = index as f32 / RATE as f32 * std::f32::consts::TAU;
        let sample = ((phase * 98.25).sin() * 0.24 + (phase * 147.5).sin() * 0.08)
            * (0.91 + 0.09 * (phase * 0.5).cos());
        pcm.extend(sample.to_le_bytes());
    }
    Clip {
        pcm: pcm.into(),
        rate: RATE,
        channels: 1,
    }
}
