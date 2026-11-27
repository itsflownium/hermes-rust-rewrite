//! Bundled Discord transport for the profile-owned messaging gateway.

mod channels;
mod client;
mod delivery;
mod dispatch;
mod interactions;
mod manual_thread;
mod media;
mod membership;
mod messages;
mod recovery;
pub mod settings;
mod socket;
mod voice;
mod voice_note;

#[cfg(test)]
mod fixtures;

pub use settings::DiscordSettings;

use hermes_gateway_core::messaging;

#[cfg(test)]
use hermes_gateway_core::{GatewayConfig, RunningGateway};

#[cfg(test)]
async fn start_test_gateway(mut config: GatewayConfig) -> anyhow::Result<RunningGateway> {
    config.stock_adapters.push(STOCK_ADAPTER);
    hermes_gateway_core::Gateway::start(config).await
}

use messaging::{
    Adapter, AdapterFuture, ConnectionFailure, DeliveryBody, DeliveryIntent, DeliveryPart,
    DeliveryResult, Effect, FirstToolFeedback, InboundEvent, Ingress, Message, PreparationFailure,
    Room, RoomKind, SendAttempt, Sender, SpokenReplyMode, VerifiedInstallation,
};
use channels::Channel;
use client::DiscordClient;
use delivery::{DeliverySurface, PlanOptions};
use settings::VerifiedBot;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use voice::Voice;

pub const STOCK_ADAPTER: messaging::StockAdapter = messaging::StockAdapter {
    platform: settings::PLATFORM,
    schema: &[
        messages::MESSAGE_TABLES,
        recovery::RECOVERY_TABLES,
        socket::SOCKET_TABLES,
        delivery::DELIVERY_RATE_TABLES,
        delivery::FEEDBACK_TABLES,
        manual_thread::TABLES,
        delivery::thread_updates::TABLES,
        voice_note::TABLES,
    ],
    configure: settings::configure,
    replace_credential: settings::replace_credential,
};

fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct DiscordAdapter {
    client: DiscordClient,
    settings: DiscordSettings,
    bot: VerifiedBot,
    channels: Mutex<BTreeMap<String, Channel>>,
    callbacks: Arc<Semaphore>,
    preparations: Arc<Mutex<BTreeSet<String>>>,
    manual_recovery: Arc<Semaphore>,
    voice: Voice,
}

impl DiscordAdapter {
    fn new(settings: DiscordSettings, client: DiscordClient, bot: VerifiedBot) -> Self {
        let voice = Voice::new(bot.bot_id.parse().unwrap_or_default(), settings.voice_fx.clone());
        Self {
            client,
            settings,
            bot,
            channels: Mutex::default(),
            callbacks: Arc::new(Semaphore::new(32)),
            preparations: Arc::default(),
            manual_recovery: Arc::new(Semaphore::new(1)),
            voice,
        }
    }

    async fn transport_ready(
        &self,
        ingress: &Ingress,
        fresh: bool,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        if let Some(destination) = &self.settings.default_delivery_target {
            let id = destination
                .thread
                .as_deref()
                .unwrap_or(&destination.channel);
            let route = self.channel_route(id, Some(&destination.guild)).await?;
            if let Some(route) = route {
                ingress
                    .verify_destination(route.room, messaging::runtime::now() + 86_400_000)
                    .await
                    .map_err(socket::intake_failure)?;
            }
        }
        if fresh {
            self.remember_recovery(ingress)
                .map_err(socket::intake_failure)?;
        }
        self.recover_history(ingress, cancel).await?;
        self.resume_preparations(ingress, cancel).await?;
        if self.settings.commands.enabled && self.settings.commands.sync {
            let client = self.client.clone();
            let application = self.bot.application_id.clone();
            let guild = self.settings.commands.test_guild.clone();
            let voice = self.settings.voice_enabled;
            let voice_notes = self.settings.sends_voice_notes();
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cancel.cancelled() => {},
                    _ = interactions::synchronize(&client, &application, guild.as_deref(), voice, voice_notes) => {}
                }
            });
        }
        Ok(())
    }

    fn observe_voice(&self, kind: &str, payload: &serde_json::Value, ingress: &Ingress) {
        if self.settings.voice_enabled {
            self.voice.observe(kind, payload, ingress);
        }
    }

    async fn join_voice(
        &self,
        ingress: &Ingress,
        actor: &Sender,
        room: &Room,
        guild: &str,
    ) -> Result<serde_json::Value, messaging::GatewayError> {
        self.voice.join(ingress, actor, room, guild)
    }

    fn voice_housekeeping(&self, ingress: &Ingress) -> Vec<serde_json::Value> {
        if self.settings.voice_enabled {
            self.voice.housekeeping(ingress)
        } else {
            Vec::new()
        }
    }

    fn leave_voice(
        &self,
        ingress: &Ingress,
        actor: &Sender,
        room: &Room,
        guild: &str,
    ) -> Option<serde_json::Value> {
        self.voice
            .leave_bound(ingress, actor, room, guild)
            .ok()
            .flatten()
    }
}

impl DiscordAdapter {
    fn should_send_voice_note(
        &self,
        ingress: &Ingress,
        delivery: &str,
        room: &Room,
    ) -> bool {
        match ingress.spoken_reply_mode(room).unwrap_or(SpokenReplyMode::Off) {
            SpokenReplyMode::Off => false,
            SpokenReplyMode::VoiceNotes => voice_note::spoken_source(ingress, delivery, room).unwrap_or(false),
            SpokenReplyMode::Always => voice_note::channel_reply(ingress, delivery, room).unwrap_or(false),
        }
    }
}

struct ResetVoice<'a>(&'a Voice);

impl Drop for ResetVoice<'_> {
    fn drop(&mut self) {
        self.0.reset();
    }
}

impl Adapter for DiscordAdapter {
    fn initial_transport_ready(&self) -> bool {
        false
    }

    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(self.bot.installation())
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            let _voice = ResetVoice(&self.voice);
            tokio::select! {
                biased;
                failure = socket::listen(self, ingress.clone(), cancel.clone()) => failure,
                () = self.refresh_pending_memberships(&ingress, &cancel) => ConnectionFailure::Stopped,
            }
        })
    }

    fn first_tool_feedback(&self, ingress: Ingress, event: &InboundEvent) -> Option<FirstToolFeedback> {
        self.settings.voice_enabled.then(|| self.voice.first_tool_feedback(ingress, event)).flatten()
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        let forum = intent.room.kind == RoomKind::Group
            && intent.room.thread.is_none()
            && crate::lock(&self.channels)
                .get(&intent.room.chat)
                .is_some_and(|channel| matches!(channel.kind, 15 | 16));
        let surface = if forum {
            if !self.settings.delivery.forum_posts {
                return Err(PreparationFailure::Unsupported);
            }
            DeliverySurface::ForumPost
        } else {
            DeliverySurface::Existing
        };
        let mut parts = delivery::prepare_with(
            intent,
            PlanOptions {
                reply_mode: self.settings.delivery.reply_mode,
                uploads: self.settings.delivery.uploads,
                max_inline_parts: self.settings.delivery.max_inline_parts,
                surface,
            },
        )?;
        if self.settings.voice_enabled
            && let Some(operation_key) = self.voice.playback_key(intent)
            && let DeliveryBody::Reply(message) = &intent.body
            && !message.text.trim().is_empty()
        {
            parts.push(DeliveryPart {
                room: intent.room.clone(),
                body: DeliveryBody::Reply(Message::text(&message.text)),
                edit_target: None,
                effect: Effect {
                    operation_key: Some(operation_key),
                    ..Effect::default()
                },
            });
        }
        Ok(parts)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        let mut plan = self.prepare(intent)?;
        if matches!(intent.body, DeliveryBody::Reply(_))
            && self.voice.playback_key(intent).is_none()
            && self.should_send_voice_note(ingress, &intent.id, &intent.room)
        {
            delivery::add_voice_note(intent, &mut plan);
        }
        if let Err(error) = self.reserve_auto_thread_updates(ingress, intent) {
            eprintln!("Could not reserve optional Discord thread updates: {error}");
        }
        Ok(plan)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async move {
            if Voice::playback(&attempt) {
                return self.voice.play(&ingress, &attempt).await;
            }
            if delivery::sends_voice_note(&attempt)
                && !self.should_send_voice_note(&ingress, &attempt.delivery_id, &attempt.payload.room)
            {
                return DeliveryResult::AttachmentNotPublished;
            }
            delivery::send_as(self, attempt, ingress, cancel).await
        })
    }
}
