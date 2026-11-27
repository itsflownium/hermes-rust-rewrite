//! Scoped leases over Discord's independent voice transport.
use super::settings::DiscordVoiceFxSettings;
use crate::messaging::{
    ActorKind, DeliveryIntent, DeliveryResult, GatewayError, Ingress, Room, RoomKind, SendAttempt,
    Sender,
};
use capture::AudioLink;
use cues::VoiceCues;
use reqwest::Url;
use serde_json::{Value, json};
use songbird::{
    Config, ConnectionInfo, CoreEvent, Driver, Event, EventContext, EventHandler,
    driver::{Channels, DecodeConfig, DecodeMode, SampleRate},
    tracks::PlayMode,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod capture;
mod cues;
#[cfg(test)]
pub(super) use capture::FixtureCall;

const MAX_CALLS: usize = 4;
const MAX_STATES: usize = 8192;
const JOIN_TIMEOUT: Duration = Duration::from_secs(20);
const VOICE_IDLE: Duration = Duration::from_secs(600);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(120);
const PLAY_LIMIT: Duration = Duration::from_secs(90);
const VOICE_ORIGIN: &str = "discord-voice-";
const PLAYBACK: &str = "discord-voice-playback-";

#[derive(Clone)]
pub(super) struct Voice {
    bot: u64,
    state: Arc<Mutex<VoiceState>>,
    cues: Arc<VoiceCues>,
}

#[derive(Default)]
struct VoiceState {
    channels: HashSet<(u64, u64)>,
    members: HashMap<(u64, u64), Member>,
    complete_rosters: HashSet<u64>,
    calls: HashMap<u64, Call>,
    leaving: HashSet<u64>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct Member {
    channel: u64,
    muted: bool,
    deafened: bool,
    bot: bool,
}

#[derive(Clone)]
struct Binding {
    id: String,
    room: Room,
    guild: u64,
    channel: u64,
    requester: Sender,
}

struct Call {
    binding: Binding,
    session: Option<String>,
    server: Option<Server>,
    link: Option<Arc<AudioLink>>,
    last_activity: Instant,
}

#[derive(Eq, PartialEq)]
struct Server {
    endpoint: String,
    token: String,
}

fn id(value: &Value) -> Option<u64> {
    value.as_str()?.parse::<u64>().ok().filter(|id| *id != 0)
}

fn authority(value: &str) -> Option<String> {
    let endpoint = Url::parse(&format!("wss://{value}")).ok()?;
    let host = endpoint.host_str()?;
    let label = host.strip_suffix(".discord.media")?;
    (!label.is_empty()
        && label
            .bytes()
            .all(|char| char.is_ascii_alphanumeric() || char == b'-')
        && endpoint.port_or_known_default() == Some(443)
        && endpoint.path() == "/"
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && endpoint.username().is_empty()
        && endpoint.password().is_none())
    .then(|| host.to_owned())
}

fn state_command(guild: u64, channel: Option<u64>) -> Value {
    json!({"op": 4, "d": {
        "guild_id": guild.to_string(),
        "channel_id": channel.map(|channel| channel.to_string()),
        "self_mute": false,
        "self_deaf": false,
    }})
}

impl Voice {
    pub(super) fn new(bot: u64, settings: DiscordVoiceFxSettings) -> Self {
        Self {
            bot,
            state: Arc::new(Mutex::new(VoiceState::default())),
            cues: Arc::new(VoiceCues::new(settings)),
        }
    }

    pub(super) fn join(
        &self,
        ingress: &Ingress,
        actor: &Sender,
        room: &Room,
        guild: &str,
    ) -> Result<Value, GatewayError> {
        let guild = guild
            .parse()
            .ok()
            .filter(|guild| *guild != 0)
            .ok_or(GatewayError::Invalid)?;
        let user = actor.id.parse().map_err(|_| GatewayError::Invalid)?;
        if room.kind != RoomKind::Group || !authorized(ingress, room, actor)? {
            return Err(GatewayError::Forbidden);
        }
        let mut state = crate::lock(&self.state);
        let member = state
            .members
            .get(&(guild, user))
            .filter(|member| !member.bot)
            .ok_or(GatewayError::Forbidden)?;
        let channel = member.channel;
        if !state.channels.contains(&(guild, channel)) {
            return Err(GatewayError::Unsupported);
        }
        if state.calls.contains_key(&guild)
            || state.leaving.contains(&guild)
            || state.members.contains_key(&(guild, self.bot))
        {
            return Err(GatewayError::Conflict);
        }
        if state.calls.len() >= MAX_CALLS {
            return Err(GatewayError::TemporarilyUnavailable);
        }
        state.calls.insert(
            guild,
            Call {
                binding: Binding {
                    id: crate::new_id(),
                    room: room.clone(),
                    guild,
                    channel,
                    requester: actor.clone(),
                },
                session: None,
                server: None,
                link: None,
                last_activity: Instant::now(),
            },
        );
        Ok(state_command(guild, Some(channel)))
    }

    /// Call from the authorized main Gateway stream. No voice I/O runs on its heartbeat path.
    pub(super) fn observe(&self, kind: &str, payload: &Value, ingress: &Ingress) {
        let mut state = crate::lock(&self.state);
        let Some(guild) = id(&payload["guild_id"]).or_else(|| {
            (kind == "GUILD_CREATE" || kind == "GUILD_DELETE")
                .then(|| id(&payload["id"]))
                .flatten()
        }) else {
            return;
        };
        match kind {
            "GUILD_CREATE" => {
                state.channels.retain(|(current, _)| *current != guild);
                state.members.retain(|(current, _), _| *current != guild);
                if let Some(channels) = payload["channels"].as_array() {
                    for channel in channels.iter().take(MAX_STATES) {
                        state.update_channel(guild, channel);
                    }
                }
                state.complete_rosters.remove(&guild);
                if let Some(members) = payload["voice_states"].as_array() {
                    if members.len() <= MAX_STATES - state.members.len()
                        && members.iter().all(|member| {
                            id(&member["user_id"]).is_some() && id(&member["channel_id"]).is_some()
                        })
                    {
                        state.complete_rosters.insert(guild);
                    }
                    for member in members.iter().take(MAX_STATES) {
                        state.update_member(self.bot, guild, member);
                    }
                }
            }
            "GUILD_DELETE" => {
                state.channels.retain(|(current, _)| *current != guild);
                state.members.retain(|(current, _), _| *current != guild);
                state.calls.remove(&guild);
                state.complete_rosters.remove(&guild);
                state.leaving.remove(&guild);
            }
            "CHANNEL_CREATE" | "CHANNEL_UPDATE" => state.update_channel(guild, payload),
            "CHANNEL_DELETE" => {
                if let Some(channel) = id(&payload["id"]) {
                    state.channels.remove(&(guild, channel));
                }
            }
            "VOICE_STATE_UPDATE" => state.update_member(self.bot, guild, payload),
            "VOICE_SERVER_UPDATE" => {
                let Some(call) = state.calls.get_mut(&guild) else {
                    return;
                };
                let server =
                    payload["endpoint"]
                        .as_str()
                        .and_then(authority)
                        .and_then(|endpoint| {
                            let token = payload["token"].as_str()?.to_owned();
                            (!token.is_empty()
                                && token.len() <= 512
                                && !token.chars().any(char::is_control))
                            .then_some(Server { endpoint, token })
                        });
                if call.server != server {
                    call.end_link();
                    call.server = server;
                    call.last_activity = Instant::now();
                }
            }
            _ => return,
        }
        let Some(call) = state.calls.get_mut(&guild) else {
            return;
        };
        if call.link.is_none()
            && let Some(info) = call.connection_info(self.bot)
        {
            let binding = call.binding.clone();
            let link = AudioLink::start(self.clone(), binding, ingress.clone(), info);
            call.link = Some(link);
        }
    }

    #[cfg(test)]
    pub(super) fn leave(&self, guild: &str) -> Option<Value> {
        let guild = guild.parse().ok()?;
        let mut state = crate::lock(&self.state);
        state.discard_call(guild)
    }

    pub(super) fn leave_bound(
        &self,
        ingress: &Ingress,
        actor: &Sender,
        room: &Room,
        guild: &str,
    ) -> Result<Option<Value>, GatewayError> {
        let guild = guild.parse().map_err(|_| GatewayError::Invalid)?;
        ingress.checked(true, |state| {
            let mut voice = crate::lock(&self.state);
            let Some(call) = voice.calls.get(&guild) else {
                return Ok(None);
            };
            if call.binding.room != *room
                || !ingress.authorize_sender(&state.policy, room, actor)?
                || (call.binding.requester.id != actor.id
                    && !ingress.authorize_operator(&state.policy, room, actor)?)
            {
                return Err(GatewayError::Forbidden);
            }
            Ok(voice.discard_call(guild))
        })
    }

    pub(super) fn reset(&self) {
        *crate::lock(&self.state) = VoiceState::default();
    }

    /// Expire a pending handshake or inactive/revoked binding. The caller
    /// sends the returned voice-state updates through the main Gateway.
    pub(super) fn housekeeping(&self, ingress: &Ingress) -> Vec<Value> {
        let snapshots = {
            let state = crate::lock(&self.state);
            state
                .calls
                .values()
                .map(|call| {
                    (
                        call.binding.clone(),
                        call.link.as_ref().map_or(call.last_activity, |link| {
                            call.last_activity.max(link.last_progress())
                        }),
                        call.link
                            .as_ref()
                            .is_some_and(|link| link.ready.load(Ordering::Acquire)),
                    )
                })
                .collect::<Vec<_>>()
        };
        snapshots
            .into_iter()
            .filter_map(|(binding, activity, ready)| {
                let limit = if ready { VOICE_IDLE } else { JOIN_TIMEOUT };
                if activity.elapsed() < limit
                    && authorized(ingress, &binding.room, &binding.requester).unwrap_or_default()
                    && self.requester_present(&binding)
                {
                    return None;
                }
                let mut state = crate::lock(&self.state);
                if state
                    .calls
                    .get(&binding.guild)
                    .is_some_and(|call| call.binding.id == binding.id)
                {
                    return state.discard_call(binding.guild);
                }
                None
            })
            .collect()
    }

    fn requester_present(&self, binding: &Binding) -> bool {
        let Ok(requester) = binding.requester.id.parse() else {
            return false;
        };
        let state = crate::lock(&self.state);
        state.channels.contains(&(binding.guild, binding.channel))
            && state
                .members
                .get(&(binding.guild, requester))
                .is_some_and(|member| member.channel == binding.channel)
    }

    pub(super) fn playback_key(&self, intent: &DeliveryIntent) -> Option<String> {
        let anchor = intent.reply_anchor.as_ref()?;
        let state = crate::lock(&self.state);
        let call = state.calls.values().find(|call| {
            &call.binding.room == &intent.room
                && anchor.starts_with(&format!("{VOICE_ORIGIN}{}-", call.binding.id))
        })?;
        call.link
            .as_ref()
            .filter(|link| link.ready.load(Ordering::Acquire))?;
        Some(format!("{PLAYBACK}{anchor}"))
    }

    pub(super) fn playback(attempt: &SendAttempt) -> bool {
        attempt
            .payload
            .effect
            .operation_key
            .as_deref()
            .is_some_and(|key| key.starts_with(PLAYBACK))
    }

    pub(super) async fn play(&self, ingress: &Ingress, attempt: &SendAttempt) -> DeliveryResult {
        let Some((binding, link, epoch)) = self.playback_link(attempt) else {
            return DeliveryResult::Rejected;
        };
        if !self.audience_allowed(ingress, &binding) {
            return DeliveryResult::Rejected;
        }
        link.cancel_ack();
        let Ok(_exclusivity) = link.playback.try_lock() else {
            return DeliveryResult::Retryable {
                after: Duration::from_secs(1),
                scope: crate::messaging::RetryScope::Destination,
            };
        };
        let Ok(artifact) = ingress.synthesize_reply(attempt).await else {
            return DeliveryResult::Rejected;
        };
        let Ok(bytes) = ingress.reply_artifact(attempt, &artifact).await else {
            return DeliveryResult::Rejected;
        };
        let Some(input) = cues::speech_input(&bytes, PLAY_LIMIT, self.cues.lead_ms()) else {
            return DeliveryResult::Rejected;
        };
        if !link.valid(epoch) || !self.audience_allowed(ingress, &binding) {
            return DeliveryResult::Rejected;
        }
        let (playback, finished) = link.start_speech(input, &self.cues);
        let confirmed = playback_completed(&link.stopped, finished, || {
            link.valid(epoch) && self.audience_allowed(ingress, &binding)
        })
        .await;
        if confirmed {
            DeliveryResult::Confirmed {
                receipt: playback.track.uuid().to_string(),
                editable_message: None,
            }
        } else {
            DeliveryResult::Uncertain
        }
    }

    fn playback_link(&self, attempt: &SendAttempt) -> Option<(Binding, Arc<AudioLink>, u64)> {
        let key = attempt
            .payload
            .effect
            .operation_key
            .as_deref()?
            .strip_prefix(PLAYBACK)?;
        let state = crate::lock(&self.state);
        let call = state.calls.values().find(|call| {
            call.binding.room == attempt.payload.room
                && key.starts_with(&format!("{VOICE_ORIGIN}{}-", call.binding.id))
        })?;
        let link = call.link.as_ref()?;
        let epoch = link.epoch.load(Ordering::Acquire);
        link.valid(epoch)
            .then(|| (call.binding.clone(), link.clone(), epoch))
    }

    fn audience_allowed(&self, ingress: &Ingress, binding: &Binding) -> bool {
        if !self.requester_present(binding) {
            return false;
        }
        let listeners = {
            let state = crate::lock(&self.state);
            if !state.complete_rosters.contains(&binding.guild)
                || state
                    .calls
                    .get(&binding.guild)
                    .is_none_or(|call| call.binding.id != binding.id)
                || state
                    .members
                    .get(&(binding.guild, self.bot))
                    .is_none_or(|bot| bot.channel != binding.channel || bot.muted)
            {
                return false;
            }
            state
                .members
                .iter()
                .filter_map(|(&(guild, user), member)| {
                    (guild == binding.guild
                        && member.channel == binding.channel
                        && user != self.bot)
                        .then_some(Sender {
                            id: user.to_string(),
                            roles: Default::default(),
                        })
                })
                .collect::<Vec<_>>()
        };
        !listeners.is_empty()
            && listeners
                .iter()
                .all(|actor| authorized(ingress, &binding.room, actor).unwrap_or_default())
    }
}

impl VoiceState {
    fn discard_call(&mut self, guild: u64) -> Option<Value> {
        self.calls.remove(&guild)?;
        self.leaving.insert(guild);
        Some(state_command(guild, None))
    }

    fn update_channel(&mut self, guild: u64, data: &Value) {
        if let Some(channel) = id(&data["id"]) {
            if data["type"].as_u64() == Some(2) && self.channels.len() < MAX_STATES {
                self.channels.insert((guild, channel));
            } else {
                self.channels.remove(&(guild, channel));
            }
        }
    }

    fn update_member(&mut self, bot: u64, guild: u64, payload: &Value) {
        let Some(user) = id(&payload["user_id"]) else {
            return;
        };
        if !payload
            .as_object()
            .is_some_and(|fields| fields.contains_key("channel_id"))
        {
            return;
        }
        let mut channel = id(&payload["channel_id"]);
        let old = self.members.get(&(guild, user)).copied();
        if self.members.len() >= MAX_STATES && old.is_none() && channel.is_some() {
            self.complete_rosters.remove(&guild);
            channel = None;
        }
        let member = channel.map(|channel| Member {
            channel,
            muted: ["mute", "self_mute", "suppress"]
                .iter()
                .any(|flag| payload[flag].as_bool() == Some(true)),
            deafened: ["deaf", "self_deaf"]
                .iter()
                .any(|flag| payload[flag].as_bool() == Some(true)),
            bot: user == bot
                || payload["member"]["user"]["bot"]
                    .as_bool()
                    .unwrap_or(old.is_some_and(|member| member.bot)),
        });
        if let Some(member) = member {
            self.members.insert((guild, user), member);
        } else {
            self.members.remove(&(guild, user));
        }
        if user == bot && channel.is_none() {
            self.leaving.remove(&guild);
        }
        let Some(call) = self.calls.get_mut(&guild) else {
            return;
        };
        if user == bot {
            if channel != Some(call.binding.channel) {
                self.calls.remove(&guild);
                return;
            }
            let session = payload["session_id"].as_str().filter(|session| {
                !session.is_empty()
                    && session.len() <= 128
                    && !session.chars().any(char::is_control)
            });
            if call.session.as_deref() != session {
                call.end_link();
                if call.session.is_some() {
                    call.server = None;
                }
                call.session = session.map(str::to_owned);
            }
        }
        if old != member
            && [old, member]
                .into_iter()
                .flatten()
                .any(|member| member.channel == call.binding.channel)
            && let Some(link) = &call.link
        {
            // A changed call audience invalidates audio already captured or playing.
            link.invalidate_output();
        }
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        self.end_link();
    }
}

impl Call {
    fn end_link(&mut self) {
        if let Some(link) = self.link.take() {
            link.stopped.cancel();
            crate::lock(&link.driver).leave();
        }
    }

    fn connection_info(&self, bot: u64) -> Option<ConnectionInfo> {
        let server = self.server.as_ref()?;
        Some(ConnectionInfo {
            channel_id: songbird::id::ChannelId(self.binding.channel.try_into().ok()?),
            guild_id: songbird::id::GuildId(self.binding.guild.try_into().ok()?),
            user_id: songbird::id::UserId(bot.try_into().ok()?),
            endpoint: server.endpoint.clone(),
            session_id: self.session.clone()?,
            token: server.token.clone(),
        })
    }
}

fn authorized(ingress: &Ingress, room: &Room, actor: &Sender) -> Result<bool, GatewayError> {
    ingress.checked(true, |state| {
        // The Songbird transport-ready event carries no negotiated DAVE state.
        // A profile protection requirement needs independent evidence.
        if state.policy.delivery_policy(room).protection.is_some() {
            return Err(GatewayError::ProtectionUnavailable);
        }
        Ok(ingress
            .authorized_identity(&state.policy, room, actor)?
            .is_some_and(|(_, kind)| kind == ActorKind::Person))
    })
}

async fn playback_completed(
    stopped: &CancellationToken,
    mut finished: oneshot::Receiver<bool>,
    allowed: impl Fn() -> bool,
) -> bool {
    let mut check = tokio::time::interval(SAMPLE_INTERVAL);
    let timeout = tokio::time::sleep(PLAY_LIMIT);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            biased;
            _ = stopped.cancelled() => return false,
            _ = &mut timeout => return false,
            _ = check.tick() => {
                if !allowed() {
                    return false;
                }
            }
            result = &mut finished => return result == Ok(true) && allowed(),
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn interrupted_or_revoked_playback_does_not_get_a_completion_receipt() {
    let running = CancellationToken::new();
    let (done, finished) = oneshot::channel();
    done.send(true).unwrap();
    assert!(playback_completed(&running, finished, || true).await);

    let stopped = CancellationToken::new();
    stopped.cancel();
    let (done, finished) = oneshot::channel();
    done.send(true).unwrap();
    assert!(!playback_completed(&stopped, finished, || true).await);

    let (done, finished) = oneshot::channel();
    done.send(true).unwrap();
    assert!(!playback_completed(&running, finished, || false).await);
}

struct ActivePlayback<'a> {
    link: &'a AudioLink,
    track: songbird::tracks::TrackHandle,
}

impl Drop for ActivePlayback<'_> {
    fn drop(&mut self) {
        let _ = self.track.stop();
        self.link.end_speech();
    }
}

struct TrackFinished(Arc<Mutex<Option<oneshot::Sender<bool>>>>);

#[async_trait::async_trait]
impl EventHandler for TrackFinished {
    async fn act(&self, event: &EventContext<'_>) -> Option<Event> {
        if let EventContext::Track(tracks) = event
            && let Some((state, _)) = tracks.first()
            && let Some(done) = crate::lock(&self.0).take()
        {
            let _ = done.send(matches!(state.playing, PlayMode::End));
        }
        None
    }
}
