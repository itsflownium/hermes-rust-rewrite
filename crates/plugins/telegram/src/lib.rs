//! Telegram Bot API stock adapter for the native profile gateway.
mod address;
mod delivery;
mod format;
mod intake;
mod media;
mod settings;
mod updates;

pub use settings::{IntakeMode, Settings};

use hermes_gateway_core::messaging::{
    Adapter, AdapterFactory, AdapterFuture, CONTRACT_VERSION, Capabilities, ConnectionFailure,
    DeliveryIntent, DeliveryPart, DeliveryResult, Descriptor, EffectCapabilities,
    EnrollmentCredential, Ingress, PreparationFailure, SendAttempt, StockAdapter, VerifiedAdapter,
    VerifiedInstallation,
};
use hermes_telegram::{Client, Error, ErrorKind, Token, telegram::Bot};
use regex::RegexSet;
use serde_json::{Value, json};
use std::{
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

const PLATFORM: &str = "telegram";

pub const STOCK_ADAPTER: StockAdapter = StockAdapter {
    platform: PLATFORM,
    schema: &[intake::TABLES, delivery::TABLES, media::TABLES],
    configure: settings::configure,
    replace_credential: settings::replace_credential,
};

struct Factory {
    root: PathBuf,
    settings: Settings,
    patterns: RegexSet,
    verification: Arc<Mutex<Value>>,
}

#[derive(Clone)]
struct Telegram(Arc<Connection>);

struct Connection {
    root: PathBuf,
    client: Client,
    bot: Bot,
    settings: Settings,
    patterns: RegexSet,
    verification: Arc<Mutex<Value>>,
    intake: AsyncMutex<()>,
    predecessor_lease: OnceLock<Arc<File>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl AdapterFactory for Factory {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: PLATFORM.into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: true,
                threads: true,
                interactive_prompts: true,
                progress: true,
                editing: true,
                effects: EffectCapabilities {
                    durable_presentation: true,
                    ..Capabilities::TEXT.effects
                },
                ..Capabilities::TEXT
            },
        }
    }

    fn verification_status(&self) -> Option<Value> {
        let status = lock(&self.verification).clone();
        Some(status)
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async { Err(ConnectionFailure::Credentials) })
    }

    fn connect_enrolled(
        &self,
        credential: EnrollmentCredential,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async move {
            let token =
                Token::parse(credential.secret()).map_err(|_| ConnectionFailure::Credentials)?;
            let endpoints = self
                .settings
                .endpoints()
                .map_err(|_| ConnectionFailure::Configuration)?;
            let client = Client::with_endpoints(token, endpoints)
                .map_err(|_| ConnectionFailure::Configuration)?;
            let bot = client.identify(&cancel).await.map_err(connection_failure)?;
            if self.settings.bot_id != Some(bot.identity.id) {
                return Err(ConnectionFailure::Credentials);
            }
            let bot_id = bot.identity.id;
            let status = json!({
                "bot": bot.identity,
                "can_join_groups": bot.can_join_groups,
                "receives_all_group_messages": bot.receives_all_group_messages,
                "private_topics": bot.topic_mode,
                "user_topics": bot.user_topics,
                "intake": match self.settings.intake { IntakeMode::Polling => "polling", _ => "webhook" },
            });
            *lock(&self.verification) = status;
            let adapter = Telegram(Arc::new(Connection {
                root: self.root.clone(),
                client,
                bot,
                settings: self.settings.clone(),
                patterns: self.patterns.clone(),
                verification: self.verification.clone(),
                intake: AsyncMutex::new(()),
                predecessor_lease: OnceLock::new(),
            }));
            Ok(VerifiedAdapter {
                account: bot_id.to_string(),
                adapter: Arc::new(adapter),
            })
        })
    }
}

impl Adapter for Telegram {
    fn initial_transport_ready(&self) -> bool {
        false
    }

    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(VerifiedInstallation {
            installation: self.0.bot.identity.id.to_string(),
            tenant: None,
            application: None,
            receiver: None,
            origin: None,
            device: None,
            local_client: None,
        })
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            self.run(ingress, cancel)
                .await
                .unwrap_or_else(|failure| failure)
        })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        delivery::prepare(intent)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        delivery::prepare_with_ingress(intent, ingress)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async move { delivery::send(&self.0, attempt, &ingress, &cancel).await })
    }
}

fn connection_failure(error: Error) -> ConnectionFailure {
    match error.kind {
        ErrorKind::Cancelled => ConnectionFailure::Stopped,
        ErrorKind::Credentials => ConnectionFailure::Credentials,
        ErrorKind::Invalid | ErrorKind::Rejected => ConnectionFailure::Configuration,
        ErrorKind::Conflict | ErrorKind::Retryable | ErrorKind::RateLimited => {
            ConnectionFailure::Retryable
        }
    }
}
