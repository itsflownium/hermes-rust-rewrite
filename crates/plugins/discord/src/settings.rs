pub use super::delivery::ReplyMode;
use super::{
    DiscordAdapter,
    client::{DiscordClient, RestFailure, parse_proxy},
};
use crate::{
    lock,
    messaging::{
        AccessPolicy, AdapterFactory, AdapterFuture, CONTRACT_VERSION, Capabilities,
        ConnectionFailure, DeliveryPolicy, Descriptor, EffectCapabilities,
        EnrollmentCredential, RequesterCommandGrant, RoomGrant, RoomRules, SpokenReplyMode, TurnPolicy,
        VerifiedAdapter, VerifiedInstallation,
    },
};
use anyhow::{Context, Result, ensure};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

pub(crate) const PLATFORM: &str = "discord";
const GUILDS: u64 = 1 << 0;
const GUILD_MEMBERS: u64 = 1 << 1;
const GUILD_VOICE_STATES: u64 = 1 << 7;
const GUILD_MESSAGES: u64 = 1 << 9;
const DIRECT_MESSAGES: u64 = 1 << 12;
const MESSAGE_CONTENT: u64 = 1 << 15;
const MEMBERS_FLAGS: u64 = (1 << 14) | (1 << 15);
const CONTENT_FLAGS: u64 = (1 << 18) | (1 << 19);
const ADD_REACTIONS: u64 = 1 << 6;
pub(super) const SEE_CHANNEL: u64 = 1 << 10;
pub(super) const SEND_MESSAGES: u64 = 1 << 11;
const ATTACH_FILES: u64 = 1 << 15;
const SEND_VOICE_MESSAGES: u64 = 1 << 46;
pub(super) const PIN_MESSAGES: u64 = 1 << 51;
pub(super) const READ_MESSAGE_HISTORY: u64 = 1 << 16;
const CONNECT_AND_SPEAK: u64 = (1 << 20) | (1 << 21);
pub(super) const CREATE_PUBLIC_THREADS: u64 = 1 << 35;
pub(super) const SEND_IN_THREADS: u64 = 1 << 38;
const MAX_DEPLOYMENT_SECRET_BYTES: u64 = 8192;

pub(crate) fn configure(root: &Path, settings: serde_json::Value) -> Result<crate::messaging::StockConfiguration> {
    let settings: DiscordSettings = serde_json::from_value(settings)?;
    let policy = settings.host_policy();
    let document = serde_json::to_value(&settings)?;
    let factory = Arc::new(DiscordFactory::new(root.to_owned(), settings)?);
    Ok(crate::messaging::StockConfiguration { settings: document, policy, factory })
}

pub(crate) fn replace_credential(previous: &serde_json::Value, next: &serde_json::Value) -> bool {
    let requested = next["deployment_secret"].as_str();
    requested.is_some() && requested != previous["deployment_secret"].as_str()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordSettings {
    pub proxy: Option<String>,
    pub grants: DiscordGrants,
    pub operators: BTreeSet<String>,
    pub trusted_bots: BTreeSet<String>,
    pub direct: DiscordDirectSettings,
    pub dm_role_guild: Option<String>,
    pub guilds: BTreeMap<String, DiscordGuildSettings>,
    pub ignored_channels: BTreeSet<String>,
    pub suppress_other_mentions: bool,
    pub participated_thread_exemption: bool,
    pub auto_thread: bool,
    pub auto_thread_inline_fallback: bool,
    pub voice_enabled: bool,
    pub voice_fx: DiscordVoiceFxSettings,
    pub dispatch_silence_secs: u64,
    pub recovery: DiscordRecoverySettings,
    pub commands: DiscordCommandSettings,
    pub delivery: DiscordDeliverySettings,
    pub media: DiscordMediaSettings,
    pub default_delivery_target: Option<DiscordDestination>,
    /// Optional named secret in the owner's profile under messaging-secrets.
    pub deployment_secret: Option<String>,
}

impl Default for DiscordSettings {
    fn default() -> Self {
        Self {
            proxy: None,
            grants: DiscordGrants::default(),
            operators: BTreeSet::new(),
            trusted_bots: BTreeSet::new(),
            direct: DiscordDirectSettings::default(),
            dm_role_guild: None,
            guilds: BTreeMap::new(),
            ignored_channels: BTreeSet::new(),
            suppress_other_mentions: true,
            participated_thread_exemption: true,
            auto_thread: false,
            auto_thread_inline_fallback: false,
            voice_enabled: false,
            voice_fx: DiscordVoiceFxSettings::default(),
            dispatch_silence_secs: 300,
            recovery: DiscordRecoverySettings::default(),
            commands: DiscordCommandSettings::default(),
            delivery: DiscordDeliverySettings::default(),
            media: DiscordMediaSettings::default(),
            default_delivery_target: None,
            deployment_secret: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordVoiceFxSettings {
    pub enabled: bool,
    pub ambient_enabled: bool,
    /// Optional local PCM WAV or Ogg Opus loop. Relative paths are profile-relative.
    pub ambient_path: Option<PathBuf>,
    pub ambient_gain: DiscordVoiceGain,
    pub duck_gain: DiscordVoiceGain,
    pub speech_gain: DiscordVoiceGain,
    pub lead_silence_ms: u16,
    pub ack_enabled: bool,
    pub ack_phrases: Option<Vec<String>>,
}

impl Default for DiscordVoiceFxSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            ambient_enabled: true,
            ambient_path: None,
            ambient_gain: DiscordVoiceGain(180),
            duck_gain: DiscordVoiceGain(60),
            speech_gain: DiscordVoiceGain(1000),
            lead_silence_ms: 200,
            ack_enabled: true,
            ack_phrases: None,
        }
    }
}

impl DiscordVoiceFxSettings {
    fn validate(&self) -> Result<()> {
        ensure!(self.lead_silence_ms <= 1000, "Invalid Discord voice cue lead silence");
        ensure!(
            self.ack_phrases.as_ref().is_none_or(|phrases| {
                phrases.len() <= 16 && phrases.iter().all(|phrase| {
                    !phrase.trim().is_empty()
                        && phrase.len() <= crate::messaging::media::MAX_FEEDBACK_TEXT
                        && !phrase.chars().any(char::is_control)
                })
            }),
            "Invalid Discord voice acknowledgement phrases"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct DiscordVoiceGain(u16);

impl TryFrom<f64> for DiscordVoiceGain {
    type Error = &'static str;

    fn try_from(gain: f64) -> Result<Self, Self::Error> {
        if !(0.0..=1.0).contains(&gain) {
            return Err("Discord voice gains must be between 0 and 1");
        }
        Ok(Self((gain * 1000.0).round() as u16))
    }
}

impl From<DiscordVoiceGain> for f64 {
    fn from(gain: DiscordVoiceGain) -> Self {
        f64::from(gain.0) / 1000.0
    }
}

impl DiscordVoiceGain {
    pub(super) fn amplitude(self) -> f32 {
        f32::from(self.0) / 1000.0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordGrants {
    pub open: bool,
    pub users: BTreeSet<String>,
    pub roles: BTreeSet<String>,
}

impl DiscordGrants {
    fn validate(&self, has_guild: bool) -> Result<()> {
        validate_ids(self.users.iter().chain(&self.roles))?;
        ensure!(
            has_guild || self.roles.is_empty(),
            "Discord role grants require a guild"
        );
        Ok(())
    }

    fn for_guild(&self, guild: Option<&str>, paired: bool) -> RoomGrant {
        RoomGrant {
            open: self.open,
            paired,
            users: self.users.clone(),
            roles: map_roles(guild, &self.roles),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordDirectSettings {
    pub rules: DiscordRoomSettings,
    pub pairing: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordGuildSettings {
    pub rules: DiscordRoomSettings,
    pub channels: BTreeMap<String, DiscordChannelSettings>,
    pub operator_roles: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordChannelSettings {
    pub rules: DiscordRoomSettings,
    pub threads: BTreeMap<String, DiscordRoomSettings>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordRoomSettings {
    pub enabled: bool,
    pub grants: Option<DiscordGrants>,
    pub denied_users: BTreeSet<String>,
    pub denied_roles: BTreeSet<String>,
    pub mention_required: Option<bool>,
    pub shared_session: Option<bool>,
    pub turn: TurnPolicy,
    pub delivery: Option<DeliveryPolicy>,
}

impl Default for DiscordRoomSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            grants: None,
            denied_users: BTreeSet::new(),
            denied_roles: BTreeSet::new(),
            mention_required: None,
            shared_session: None,
            turn: TurnPolicy::default(),
            delivery: None,
        }
    }
}

impl DiscordRoomSettings {
    fn validate(&self, has_guild: bool) -> Result<()> {
        validate_ids(self.denied_users.iter().chain(&self.denied_roles))?;
        ensure!(
            has_guild || self.denied_roles.is_empty(),
            "Discord role denials require a guild"
        );
        if let Some(grants) = &self.grants {
            grants.validate(has_guild)?;
        }
        Ok(())
    }

    fn uses_roles(&self) -> bool {
        !self.denied_roles.is_empty() || self.grants.as_ref().is_some_and(|g| !g.roles.is_empty())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordRecoverySettings {
    pub history_window_secs: u64,
    pub max_surfaces: usize,
    pub max_messages: usize,
    pub context_messages: usize,
}
impl Default for DiscordRecoverySettings {
    fn default() -> Self {
        Self {
            history_window_secs: 0,
            max_surfaces: 16,
            max_messages: 100,
            context_messages: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordCommandSettings {
    pub enabled: bool,
    pub sync: bool,
    pub test_guild: Option<String>,
    pub users: Option<BTreeSet<String>>,
    pub guild_roles: BTreeMap<String, BTreeSet<String>>,
}

impl DiscordCommandSettings {
    pub(super) fn has_explicit_grants(&self) -> bool {
        self.users.is_some() || !self.guild_roles.is_empty()
    }

    fn requester_grant(
        &self,
        guild: Option<&str>,
        operator_roles: BTreeSet<String>,
    ) -> Option<RequesterCommandGrant> {
        (self.enabled && self.has_explicit_grants()).then(|| RequesterCommandGrant {
            users: self.users.clone().unwrap_or_default(),
            roles: guild
                .and_then(|id| {
                    self.guild_roles
                        .get(id)
                        .map(|roles| map_roles(Some(id), roles))
                })
                .unwrap_or_default(),
            operator_roles,
        })
    }
}

impl Default for DiscordCommandSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            sync: true,
            test_guild: None,
            users: None,
            guild_roles: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordDeliverySettings {
    pub reply_mode: ReplyMode,
    pub uploads: bool,
    pub max_inline_parts: usize,
    pub forum_posts: bool,
    pub typing: bool,
    pub reactions: bool,
    pub rename_auto_threads: bool,
    pub pin_first_thread_message: bool,
}
impl Default for DiscordDeliverySettings {
    fn default() -> Self {
        Self {
            reply_mode: ReplyMode::default(),
            uploads: true,
            max_inline_parts: 8,
            forum_posts: false,
            typing: false,
            reactions: false,
            rename_auto_threads: false,
            pin_first_thread_message: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscordMediaSettings {
    pub attachments: bool,
    pub max_inbound_bytes: u64,
    pub spoken_replies: SpokenReplyMode,
}
impl Default for DiscordMediaSettings {
    fn default() -> Self {
        Self {
            attachments: true,
            max_inbound_bytes: 32 * 1024 * 1024,
            spoken_replies: SpokenReplyMode::VoiceNotes,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscordDestination {
    pub guild: String,
    pub channel: String,
    pub thread: Option<String>,
}

pub(super) fn snowflake(value: &str) -> bool {
    !value.starts_with('0')
        && value.bytes().all(|c| c.is_ascii_digit())
        && value.parse::<u64>().is_ok()
}

/// Use only for a role verified in the given Discord guild.
pub(super) fn scoped_role(guild: &str, role: &str) -> String {
    format!("discord-role:{guild}:{role}")
}

pub(super) fn map_roles(guild: Option<&str>, roles: &BTreeSet<String>) -> BTreeSet<String> {
    guild
        .into_iter()
        .flat_map(|id| roles.iter().map(move |role| scoped_role(id, role)))
        .collect()
}

fn validate_ids<'a>(ids: impl IntoIterator<Item = &'a String>) -> Result<()> {
    ensure!(
        ids.into_iter().all(|id| snowflake(id)),
        "Expected a canonical Discord snowflake"
    );
    Ok(())
}

impl DiscordSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(proxy) = &self.proxy {
            ensure!(proxy.len() <= 512, "Invalid Discord proxy URL");
            parse_proxy(proxy)?;
        }
        validate_ids(
            self.operators
                .iter()
                .chain(&self.trusted_bots)
                .chain(&self.ignored_channels),
        )?;
        self.grants.validate(false)?;
        self.voice_fx.validate()?;
        self.direct.rules.validate(self.dm_role_guild.is_some())?;
        validate_ids(
            self.dm_role_guild
                .iter()
                .chain(self.commands.test_guild.iter()),
        )?;
        if let Some(guild) = &self.dm_role_guild {
            ensure!(
                self.guilds.contains_key(guild),
                "Choose a configured guild for Discord DM roles"
            );
        }
        if let Some(users) = &self.commands.users {
            validate_ids(users)?;
        }
        validate_ids(self.commands.guild_roles.keys())?;
        for roles in self.commands.guild_roles.values() {
            validate_ids(roles)?;
        }
        ensure!(
            (15..=3600).contains(&self.dispatch_silence_secs)
                && self.recovery.history_window_secs <= 31 * 86400
                && (1..=100).contains(&self.recovery.max_surfaces)
                && (1..=1000).contains(&self.recovery.max_messages)
                && self.recovery.context_messages <= 20,
            "Invalid Discord recovery limits"
        );
        ensure!(
            (1..=32).contains(&self.delivery.max_inline_parts)
                && (1..=crate::messaging::media::MAX_ADAPTER_FILE as u64).contains(&self.media.max_inbound_bytes),
            "Invalid Discord attachment or delivery limit"
        );
        if let Some(name) = &self.deployment_secret {
            ensure!(
                valid_secret_name(name),
                "Invalid Discord deployment secret name"
            );
        }
        validate_ids(self.guilds.keys())?;
        let mut seen = BTreeSet::new();
        for guild in self.guilds.values() {
            guild.rules.validate(true)?;
            validate_ids(&guild.operator_roles)?;
            validate_ids(guild.channels.keys())?;
            for (id, channel) in &guild.channels {
                ensure!(
                    seen.insert(id),
                    "Discord channel appears in more than one configured surface"
                );
                channel.rules.validate(true)?;
                validate_ids(channel.threads.keys())?;
                for (id, thread) in &channel.threads {
                    ensure!(
                        seen.insert(id),
                        "Discord thread appears in more than one configured surface"
                    );
                    thread.validate(true)?;
                }
            }
        }
        if let Some(target) = &self.default_delivery_target {
            validate_ids(
                [&target.guild, &target.channel]
                    .into_iter()
                    .chain(target.thread.iter()),
            )?;
            ensure!(
                self.allows_surface(
                    Some(&target.guild),
                    &target.channel,
                    target.thread.as_deref()
                ),
                "Choose an enabled Discord default delivery destination"
            );
        }
        Ok(())
    }

    pub(super) fn allows_surface(
        &self,
        guild: Option<&str>,
        channel: &str,
        thread: Option<&str>,
    ) -> bool {
        if !snowflake(channel)
            || thread.is_some_and(|id| !snowflake(id))
            || self.ignored_channels.contains(channel)
            || thread.is_some_and(|id| self.ignored_channels.contains(id))
        {
            return false;
        }
        let Some(guild) = guild else {
            return thread.is_none() && self.direct.rules.enabled;
        };
        self.guilds.get(guild).is_some_and(|guild| {
            guild.rules.enabled
                && guild.channels.get(channel).is_some_and(|channel| {
                    channel.rules.enabled
                        && thread
                            .and_then(|id| channel.threads.get(id))
                            .is_none_or(|rules| rules.enabled)
                })
        })
    }

    pub(super) fn mention_required(
        &self,
        guild: Option<&str>,
        channel: &str,
        thread: Option<&str>,
    ) -> bool {
        let Some(guild) = guild.and_then(|id| self.guilds.get(id)) else {
            return false;
        };
        let Some(channel) = guild.channels.get(channel) else {
            return true;
        };
        thread
            .and_then(|id| channel.threads.get(id))
            .and_then(|rules| rules.mention_required)
            .or(channel.rules.mention_required)
            .or(guild.rules.mention_required)
            .unwrap_or(true)
    }

    pub(super) fn has_thread_mention_override(
        &self,
        guild: &str,
        channel: &str,
        thread: &str,
    ) -> bool {
        self.guilds
            .get(guild)
            .and_then(|guild| guild.channels.get(channel))
            .and_then(|channel| channel.threads.get(thread))
            .is_some_and(|rules| rules.mention_required.is_some())
    }

    #[cfg(test)]
    pub(super) fn dm_open(&self) -> bool {
        self.direct.rules.enabled
            && self
                .direct
                .rules
                .grants
                .as_ref()
                .unwrap_or(&self.grants)
                .open
    }

    pub(super) fn allows_command(
        &self,
        user: &str,
        guild: Option<&str>,
        roles: &BTreeSet<String>,
    ) -> bool {
        self.commands.enabled
            && (!self.commands.has_explicit_grants()
                || self
                    .commands
                    .users
                    .as_ref()
                    .is_some_and(|users| users.contains(user))
                || guild
                    .and_then(|id| self.commands.guild_roles.get(id))
                    .is_some_and(|allowed| !allowed.is_disjoint(roles)))
    }

    pub(super) fn gateway_intents(&self) -> u64 {
        let guild_text = self.guilds.values().any(|guild| {
            guild.rules.enabled && guild.channels.values().any(|channel| channel.rules.enabled)
        });
        let memberships = self.dm_role_guild.is_some()
            || self.guilds.values().any(|guild| {
                !guild.operator_roles.is_empty()
                    || guild.rules.uses_roles()
                    || guild.channels.values().any(|channel| {
                        channel.rules.uses_roles()
                            || channel
                                .threads
                                .values()
                                .any(DiscordRoomSettings::uses_roles)
                    })
            })
            || !self.commands.guild_roles.is_empty();
        let mut intents = 0;
        if self.direct.rules.enabled {
            intents |= DIRECT_MESSAGES;
        }
        if guild_text || memberships || self.voice_enabled {
            intents |= GUILDS;
        }
        if guild_text {
            intents |= GUILD_MESSAGES | MESSAGE_CONTENT;
        }
        if memberships {
            intents |= GUILD_MEMBERS;
        }
        if self.voice_enabled {
            intents |= GUILD_VOICE_STATES;
        }
        intents
    }

    pub(crate) fn host_policy(&self) -> AccessPolicy {
        let mut policy = AccessPolicy {
            operators: self.operators.clone(),
            ..AccessPolicy::default()
        };
        policy.scope.require_actor_evidence = true;
        policy.scope.scoped_commands_disabled = !self.commands.enabled;
        policy.direct_spoken_replies = self.sends_voice_notes();
        policy.scope.spoken_reply_default = self.sends_voice_notes().then_some(self.media.spoken_replies);
        policy.scope.pair_direct = self.direct.pairing;
        policy
            .scope
            .denied_destinations
            .clone_from(&self.ignored_channels);
        let direct_grants = self.direct.rules.grants.as_ref().unwrap_or(&self.grants);
        policy.scope.direct = compile_room(
            &[&self.direct.rules],
            self.dm_role_guild.as_deref(),
            direct_grants,
            self.direct.pairing,
            false,
        );
        // Direct command grants use explicit users. The optional DM guild can
        // still prove a configured operator role for that requester.
        let dm_operator_roles = self
            .dm_role_guild
            .as_deref()
            .and_then(|id| {
                self.guilds
                    .get(id)
                    .map(|guild| map_roles(Some(id), &guild.operator_roles))
            })
            .unwrap_or_default();
        policy.scope.direct.requester_commands =
            self.commands.requester_grant(None, dm_operator_roles);
        for (guild_id, guild) in &self.guilds {
            let guild_grants = guild.rules.grants.as_ref().unwrap_or(&self.grants);
            let operator_roles = map_roles(Some(guild_id), &guild.operator_roles);
            policy.operator_roles.extend(operator_roles.iter().cloned());
            let requester_commands = self
                .commands
                .requester_grant(Some(guild_id), operator_roles);
            for (channel_id, channel) in &guild.channels {
                let mut room = compile_room(
                    &[&guild.rules, &channel.rules],
                    Some(guild_id),
                    guild_grants,
                    false,
                    true,
                );
                room.requester_commands = requester_commands.clone();
                policy.scope.rooms.insert(channel_id.clone(), room);
                if self.voice_enabled || self.sends_voice_notes() {
                    policy
                        .rooms
                        .entry(channel_id.clone())
                        .or_default()
                        .spoken_replies = true;
                }
                let mut threads = BTreeMap::new();
                for (thread_id, thread) in &channel.threads {
                    let mut rules =
                        compile_room(&[thread], Some(guild_id), guild_grants, false, true);
                    // Inherit the parent grant and presentation when the thread omitted them.
                    if thread.grants.is_none() {
                        rules.grant = None;
                    }
                    rules.mention_required = thread.mention_required;
                    rules.per_participant = thread.shared_session.map(|shared| !shared);
                    threads.insert(thread_id.clone(), rules);
                }
                policy.scope.threads.insert(channel_id.clone(), threads);
            }
        }
        policy
    }

    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: PLATFORM.into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: self.media.attachments,
                threads: true,
                interactive_prompts: self.commands.enabled,
                editing: true,
                progress: true,
                effects: EffectCapabilities {
                    prepare_resources: self.commands.enabled || self.auto_thread || self.delivery.forum_posts,
                    durable_presentation: true,
                    ..Capabilities::TEXT.effects
                },
                ..Capabilities::TEXT
            },
        }
    }

    pub(super) fn sends_voice_notes(&self) -> bool {
        self.media.attachments && self.delivery.uploads
            && self.media.spoken_replies != SpokenReplyMode::Off
    }

    fn invite_permissions(&self) -> u64 {
        let mut permissions = 0;
        if self.gateway_intents() & GUILD_MESSAGES != 0 {
            permissions |= SEE_CHANNEL | SEND_MESSAGES | READ_MESSAGE_HISTORY | SEND_IN_THREADS;
            if self.delivery.uploads {
                permissions |= ATTACH_FILES;
            }
            if self.delivery.reactions {
                permissions |= ADD_REACTIONS;
            }
            if self.delivery.pin_first_thread_message {
                permissions |= PIN_MESSAGES;
            }
            if self.sends_voice_notes() {
                permissions |= SEND_VOICE_MESSAGES;
            }
            if self.commands.enabled || self.auto_thread || self.delivery.forum_posts {
                permissions |= CREATE_PUBLIC_THREADS;
            }
        }
        if self.voice_enabled {
            permissions |= CONNECT_AND_SPEAK;
        }
        permissions
    }

    fn invite(&self, application: &str) -> Option<Url> {
        let mut invite = Url::parse("https://discord.com/oauth2/authorize").ok()?;
        let scopes = if self.commands.enabled {
            "bot applications.commands"
        } else {
            "bot"
        };
        invite
            .query_pairs_mut()
            .append_pair("client_id", application)
            .append_pair("scope", scopes)
            .append_pair("permissions", &self.invite_permissions().to_string())
            .append_pair("integration_type", "0");
        Some(invite)
    }
}

fn compile_room(
    layers: &[&DiscordRoomSettings],
    guild: Option<&str>,
    fallback: &DiscordGrants,
    paired: bool,
    group: bool,
) -> RoomRules {
    let mut result = RoomRules {
        enabled: Some(true),
        mention_required: Some(group),
        per_participant: Some(group),
        grant: Some(fallback.for_guild(guild, paired)),
        ..RoomRules::default()
    };
    for layer in layers {
        if !layer.enabled {
            result.enabled = Some(false);
        }
        result
            .denied_users
            .extend(layer.denied_users.iter().cloned());
        result
            .denied_roles
            .extend(map_roles(guild, &layer.denied_roles));
        if let Some(grants) = &layer.grants {
            result.grant = Some(grants.for_guild(guild, paired));
        }
        if let Some(mention) = layer.mention_required {
            result.mention_required = Some(mention);
        }
        if let Some(shared) = layer.shared_session {
            result.per_participant = Some(!shared);
        }
        result.turn.overlay(&layer.turn);
        if let Some(delivery) = &layer.delivery {
            result.delivery = Some(delivery.clone());
        }
    }
    result
}

fn valid_secret_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

fn deployment_credential(root: &Path, name: &str) -> Result<EnrollmentCredential> {
    ensure!(
        valid_secret_name(name),
        "Invalid Discord deployment secret name"
    );
    let root = root
        .canonicalize()
        .context("Discord profile is unavailable")?;
    let secret_dir = root.join("messaging-secrets");
    let resolved = secret_dir
        .join(name)
        .with_extension("secret")
        .canonicalize()
        .map_err(|_| anyhow::anyhow!("Discord deployment secret is unavailable"))?;
    ensure!(
        resolved.parent() == Some(secret_dir.as_path()),
        "Discord deployment secret is outside the profile"
    );
    ensure!(
        resolved.metadata()?.is_file(),
        "Discord deployment secret must be a file"
    );
    let file = File::open(resolved)
        .map_err(|_| anyhow::anyhow!("Discord deployment secret is unavailable"))?;
    let mut value = String::new();
    file.take(MAX_DEPLOYMENT_SECRET_BYTES + 1)
        .read_to_string(&mut value)
        .map_err(|_| anyhow::anyhow!("Discord deployment secret cannot be read"))?;
    ensure!(
        (value.len() as u64) <= MAX_DEPLOYMENT_SECRET_BYTES,
        "Discord deployment secret is too large"
    );
    let value = hermes_engine::secret::unseal(value.trim_end_matches(['\n', '\r']))?;
    Ok(EnrollmentCredential::new(value)?)
}

#[derive(Clone, Deserialize)]
pub(super) struct SessionStartLimit {
    pub total: u32,
    pub remaining: u32,
    pub reset_after: u64,
    pub max_concurrency: u32,
}

#[derive(Clone)]
pub(super) struct VerifiedBot {
    pub bot_id: String,
    pub application_id: String,
    pub gateway: Url,
    pub session_start_limit: SessionStartLimit,
    pub shards: u32,
}

impl VerifiedBot {
    pub(super) fn installation(&self) -> VerifiedInstallation {
        VerifiedInstallation {
            installation: self.bot_id.clone(),
            application: Some(self.application_id.clone()),
            tenant: None,
            receiver: None,
            origin: Some("https://discord.com".into()),
            device: None,
            local_client: None,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct RemoteUser {
    pub id: String,
    #[serde(default)]
    pub bot: bool,
    #[serde(default)]
    pub system: bool,
}

#[derive(Deserialize)]
struct RemoteApplication {
    id: String,
    #[serde(default)]
    bot: Option<ApplicationBot>,
    #[serde(default)]
    flags: u64,
}

#[derive(Deserialize)]
struct ApplicationBot {
    id: String,
    bot: Option<bool>,
}

#[derive(Deserialize)]
struct RemoteGateway {
    url: String,
    shards: u32,
    session_start_limit: SessionStartLimit,
}

#[derive(Clone, Default, Serialize)]
struct VerificationStatus {
    bot_id: Option<String>,
    application_id: Option<String>,
    rest_verified: bool,
    required_intents: u64,
    missing_intents: Vec<&'static str>,
    invite: Option<String>,
    required_permissions: u64,
    missing_permissions: Vec<&'static str>,
    recommended_shards: Option<u32>,
    session_starts_remaining: Option<u32>,
    session_start_reset_ms: Option<u64>,
    home_verified: Option<bool>,
    issue: Option<&'static str>,
}

impl VerificationStatus {
    fn configured(settings: &DiscordSettings) -> Self {
        Self {
            required_intents: settings.gateway_intents(),
            required_permissions: settings.invite_permissions(),
            ..Self::default()
        }
    }
}

pub(crate) struct DiscordFactory {
    settings: DiscordSettings,
    root: PathBuf,
    status: Mutex<VerificationStatus>,
    #[cfg(test)]
    fixture_api: Option<Url>,
}

impl DiscordFactory {
    pub(crate) fn new(root: PathBuf, settings: DiscordSettings) -> Result<Self> {
        settings.validate()?;
        let status = VerificationStatus::configured(&settings);
        Ok(Self {
            settings,
            root,
            status: Mutex::new(status),
            #[cfg(test)]
            fixture_api: None,
        })
    }

    #[cfg(test)]
    pub(super) fn for_fixture(root: PathBuf, settings: DiscordSettings, api: Url) -> Result<Self> {
        let mut factory = Self::new(root, settings)?;
        factory.fixture_api = Some(api);
        Ok(factory)
    }

    fn client(&self, credential: EnrollmentCredential) -> Result<DiscordClient> {
        ensure!(
            credential.secret().bytes().all(|c| c.is_ascii_graphic()),
            "Invalid Discord bot credential"
        );
        #[cfg(test)]
        if let Some(api) = &self.fixture_api {
            return DiscordClient::for_fixture(credential, api.clone());
        }
        DiscordClient::official(credential, self.settings.proxy.as_deref())
    }

    async fn verified(
        &self,
        credential: EnrollmentCredential,
        cancel: CancellationToken,
    ) -> Result<VerifiedAdapter, ConnectionFailure> {
        let mut status = VerificationStatus::configured(&self.settings);
        let client = self.client(credential).map_err(|_| {
            status.issue = Some("bot_credential_invalid");
            *lock(&self.status) = status.clone();
            ConnectionFailure::Credentials
        })?;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(ConnectionFailure::Stopped),
            result = self.prove(&client, &mut status) => result,
        };
        *lock(&self.status) = status;
        let bot = result?;
        Ok(VerifiedAdapter {
            account: bot.bot_id.clone(),
            adapter: Arc::new(DiscordAdapter::new(self.settings.clone(), client, bot)),
        })
    }

    async fn prove(
        &self,
        client: &DiscordClient,
        status: &mut VerificationStatus,
    ) -> Result<VerifiedBot, ConnectionFailure> {
        let bot: RemoteUser = client
            .get(&["users", "@me"])
            .await
            .map_err(|e| rest_failure(status, e))?;
        if !bot.bot || !snowflake(&bot.id) {
            status.issue = Some("bot_identity_invalid");
            return Err(ConnectionFailure::Credentials);
        }
        let application: RemoteApplication = client
            .get(&["applications", "@me"])
            .await
            .map_err(|e| rest_failure(status, e))?;
        if !snowflake(&application.id)
            || application
                .bot
                .as_ref()
                .map_or(application.id != bot.id, |user| {
                    user.bot == Some(false) || user.id != bot.id
                })
        {
            status.issue = Some("application_binding_invalid");
            return Err(ConnectionFailure::Credentials);
        }
        status.bot_id = Some(bot.id.clone());
        status.application_id = Some(application.id.clone());
        status.invite = self
            .settings
            .invite(&application.id)
            .map(|url| url.to_string());
        if status.required_intents & GUILD_MEMBERS != 0 && application.flags & MEMBERS_FLAGS == 0 {
            status.missing_intents.push("guild_members");
        }
        if status.required_intents & MESSAGE_CONTENT != 0 && application.flags & CONTENT_FLAGS == 0
        {
            status.missing_intents.push("message_content");
        }
        if !status.missing_intents.is_empty() {
            status.rest_verified = true;
            status.issue = Some("privileged_intents_unavailable");
            return Err(ConnectionFailure::Configuration);
        }
        let gateway: RemoteGateway = client
            .get(&["gateway", "bot"])
            .await
            .map_err(|e| rest_failure(status, e))?;
        let socket = client.gateway_url(&gateway.url).map_err(|_| {
            status.issue = Some("gateway_endpoint_invalid");
            ConnectionFailure::Configuration
        })?;
        let limit = &gateway.session_start_limit;
        if gateway.shards == 0
            || limit.total == 0
            || limit.remaining > limit.total
            || limit.max_concurrency == 0
        {
            status.issue = Some("gateway_session_limit_invalid");
            return Err(ConnectionFailure::Configuration);
        }
        status.rest_verified = true;
        status.recommended_shards = Some(gateway.shards);
        status.session_starts_remaining = Some(limit.remaining);
        status.session_start_reset_ms = Some(limit.reset_after);
        if let Some(target) = &self.settings.default_delivery_target {
            self.verify_home(client, target, &bot.id, status).await?;
        }
        Ok(VerifiedBot {
            bot_id: bot.id,
            application_id: application.id,
            gateway: socket,
            session_start_limit: gateway.session_start_limit,
            shards: gateway.shards,
        })
    }

    async fn verify_home(
        &self,
        client: &DiscordClient,
        target: &DiscordDestination,
        bot: &str,
        status: &mut VerificationStatus,
    ) -> Result<(), ConnectionFailure> {
        status.home_verified = Some(false);
        let parent: RemoteChannel = client
            .get(&["channels", &target.channel])
            .await
            .map_err(|error| home_failure(status, error))?;
        if parent.id != target.channel
            || parent.guild_id != target.guild
            || !matches!(parent.kind, 0 | 5 | 15 | 16)
        {
            status.issue = Some("default_destination_binding_mismatch");
            return Err(ConnectionFailure::Configuration);
        }
        if let Some(thread) = &target.thread {
            let found: RemoteChannel = client
                .get(&["channels", thread])
                .await
                .map_err(|error| home_failure(status, error))?;
            if found.id != *thread
                || found.guild_id != target.guild
                || found.parent_id.as_deref() != Some(target.channel.as_str())
                || !matches!(found.kind, 10 | 11 | 12)
            {
                status.issue = Some("default_thread_binding_mismatch");
                return Err(ConnectionFailure::Configuration);
            }
        } else if matches!(parent.kind, 15 | 16) && !self.settings.delivery.forum_posts {
            status.issue = Some("forum_delivery_disabled");
            return Err(ConnectionFailure::Configuration);
        }

        let guild: RemoteGuild = client
            .get(&["guilds", &target.guild])
            .await
            .map_err(|error| home_failure(status, error))?;
        let member: RemoteMember = client
            .get(&["guilds", &target.guild, "members", bot])
            .await
            .map_err(|error| home_failure(status, error))?;
        let roles: Vec<RemoteRole> = client
            .get(&["guilds", &target.guild, "roles"])
            .await
            .map_err(|error| home_failure(status, error))?;
        if !member.user.bot {
            status.issue = Some("default_destination_permissions_unverified");
            return Err(ConnectionFailure::Configuration);
        }
        let permissions = effective_permissions(
            &target.guild,
            bot,
            true,
            &guild,
            &member,
            &roles,
            &parent.overwrites,
        )
        .ok_or_else(|| {
            status.issue = Some("default_destination_permissions_unverified");
            ConnectionFailure::Configuration
        })?;
        let send = if target.thread.is_some() {
            (SEND_IN_THREADS, "send_messages_in_threads")
        } else {
            (SEND_MESSAGES, "send_messages")
        };
        let required = [(SEE_CHANNEL, "view_channel"), send];
        status.missing_permissions = required
            .into_iter()
            .filter_map(|(flag, name)| (permissions & flag == 0).then_some(name))
            .collect();
        let can_deliver = status.missing_permissions.is_empty();
        let wants_history = self.settings.delivery.reply_mode != ReplyMode::Off
            || self.settings.recovery.history_window_secs > 0
            || self.settings.recovery.context_messages > 0;
        if wants_history && permissions & READ_MESSAGE_HISTORY == 0 {
            status.missing_permissions.push("read_message_history");
        }
        if self.settings.delivery.uploads && permissions & ATTACH_FILES == 0 {
            status.missing_permissions.push("attach_files");
        }
        if self.settings.delivery.reactions && permissions & ADD_REACTIONS == 0 {
            status.missing_permissions.push("add_reactions");
        }
        if self.settings.sends_voice_notes() && permissions & SEND_VOICE_MESSAGES == 0 {
            status.missing_permissions.push("send_voice_messages");
        }
        if !can_deliver {
            status.issue = Some("default_destination_permissions_missing");
            return Err(ConnectionFailure::Configuration);
        }
        status.home_verified = Some(true);
        Ok(())
    }
}

#[derive(Deserialize)]
struct RemoteChannel {
    id: String,
    guild_id: String,
    parent_id: Option<String>,
    #[serde(rename = "type")]
    kind: u8,
    #[serde(default, rename = "permission_overwrites")]
    overwrites: Vec<PermissionOverwrite>,
}

#[derive(Deserialize)]
pub(super) struct RemoteGuild {
    id: String,
    owner_id: String,
}

#[derive(Deserialize)]
pub(super) struct RemoteMember {
    pub user: RemoteUser,
    pub roles: BTreeSet<String>,
}

#[derive(Deserialize)]
pub(super) struct RemoteRole {
    id: String,
    permissions: String,
}

#[derive(Deserialize)]
pub(super) struct PermissionOverwrite {
    id: String,
    #[serde(rename = "type")]
    kind: u8,
    allow: String,
    deny: String,
}

pub(super) async fn thread_permissions(
    client: &DiscordClient,
    guild: &str,
    parent: &str,
    bot: &str,
    actor: &str,
    bot_permissions: u64,
    actor_permissions: u64,
) -> Result<Option<BTreeSet<String>>, RestFailure> {
    let channel_path = ["channels", parent];
    let guild_path = ["guilds", guild];
    let bot_path = ["guilds", guild, "members", bot];
    let requester_path = ["guilds", guild, "members", actor];
    let roles_path = ["guilds", guild, "roles"];
    let requests = tokio::try_join!(
        client.get::<RemoteChannel>(&channel_path),
        client.get::<RemoteGuild>(&guild_path),
        client.get::<RemoteMember>(&bot_path),
        client.get::<RemoteMember>(&requester_path),
        client.get::<Vec<RemoteRole>>(&roles_path),
    );
    let (channel, installed, bot_member, requester, roles) = match requests {
        Ok(result) => result,
        Err(RestFailure::Forbidden | RestFailure::Missing | RestFailure::Invalid) => return Ok(None),
        Err(error) => return Err(error),
    };
    if channel.id != parent || channel.guild_id != guild || !matches!(channel.kind, 0 | 5)
        || requester.roles.len() > 128 || requester.roles.iter().any(|role| !snowflake(role))
    {
        return Ok(None);
    }
    let bot_access = effective_permissions(guild, bot, true, &installed, &bot_member, &roles, &channel.overwrites);
    let actor_access = effective_permissions(guild, actor, false, &installed, &requester, &roles, &channel.overwrites);
    Ok((bot_access.is_some_and(|bits| bits & bot_permissions == bot_permissions)
        && actor_access.is_some_and(|bits| bits & actor_permissions == actor_permissions))
        .then_some(requester.roles))
}

pub(super) fn effective_permissions(
    guild: &str,
    actor: &str,
    bot: bool,
    installed: &RemoteGuild,
    member: &RemoteMember,
    roles: &[RemoteRole],
    overwrites: &[PermissionOverwrite],
) -> Option<u64> {
    if installed.id != guild || member.user.id != actor || member.user.bot != bot {
        return None;
    }
    let everyone = roles.iter().find(|role| role.id == guild)?;
    let mut base: u64 = everyone.permissions.parse().ok()?;
    for id in &member.roles {
        let permissions: u64 = roles
            .iter()
            .find(|role| &role.id == id)?
            .permissions
            .parse()
            .ok()?;
        base |= permissions;
    }
    const ADMINISTRATOR: u64 = 1 << 3;
    if installed.owner_id == actor || base & ADMINISTRATOR != 0 {
        return Some(u64::MAX);
    }

    let mut everyone_mask = (0, 0);
    let mut role_mask = (0, 0);
    let mut member_mask = (0, 0);
    for overwrite in overwrites {
        let mask = match overwrite.kind {
            0 if overwrite.id == guild => &mut everyone_mask,
            0 if member.roles.contains(&overwrite.id) => &mut role_mask,
            1 if overwrite.id == actor => &mut member_mask,
            0 | 1 => continue,
            _ => return None,
        };
        mask.0 |= overwrite.deny.parse::<u64>().ok()?;
        mask.1 |= overwrite.allow.parse::<u64>().ok()?;
    }
    for (deny, allow) in [everyone_mask, role_mask, member_mask] {
        base = (base & !deny) | allow;
    }
    Some(base)
}

fn home_failure(status: &mut VerificationStatus, error: RestFailure) -> ConnectionFailure {
    match error {
        RestFailure::Forbidden | RestFailure::Missing => {
            status.issue = Some("default_destination_unavailable");
            ConnectionFailure::Configuration
        }
        error => rest_failure(status, error),
    }
}

impl AdapterFactory for DiscordFactory {
    fn descriptor(&self) -> Descriptor {
        self.settings.descriptor()
    }

    fn verification_status(&self) -> Option<serde_json::Value> {
        serde_json::to_value(&*lock(&self.status)).ok()
    }

    fn connect(
        &self,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async move {
            let Some(name) = self.settings.deployment_secret.clone() else {
                return Err(ConnectionFailure::Credentials);
            };
            let root = self.root.clone();
            let credential =
                tokio::task::spawn_blocking(move || deployment_credential(&root, &name))
                    .await
                    .map_err(|_| ConnectionFailure::Retryable)?
                    .map_err(|_| ConnectionFailure::Credentials)?;
            self.verified(credential, cancel).await
        })
    }

    fn connect_enrolled(
        &self,
        credential: EnrollmentCredential,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(self.verified(credential, cancel))
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;

fn rest_failure(status: &mut VerificationStatus, error: RestFailure) -> ConnectionFailure {
    match error {
        RestFailure::Credentials | RestFailure::Forbidden => {
            status.issue = Some("bot_credentials_rejected");
            ConnectionFailure::Credentials
        }
        RestFailure::Missing | RestFailure::Invalid => {
            status.issue = Some("unexpected_discord_rest_reply");
            ConnectionFailure::Configuration
        }
        RestFailure::RateLimited | RestFailure::Unavailable => {
            status.issue = Some("discord_rest_unavailable");
            ConnectionFailure::Retryable
        }
    }
}
