//! Native Slack Socket Mode and Web API adapter for the profile gateway.
mod api;
mod callbacks;
mod context;
mod delivery;
mod hub;
mod identity;
mod intake;
mod reactions;
mod settings;
mod socket;

pub use settings::{PeerBotMode, ReactionTrigger, Settings};

use api::{Api, Credential, Failure};
use hermes_gateway_core::messaging::{
    Adapter, AdapterFactory, AdapterFuture, CONTRACT_VERSION, Capabilities, ConnectionFailure,
    DeliveryIntent, DeliveryPart, DeliveryResult, Descriptor, EffectCapabilities,
    EnrollmentCredential, Ingress, PreparationFailure, SendAttempt, StockAdapter, VerifiedAdapter,
    VerifiedInstallation,
};
use identity::Identity;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
};
use tokio_util::sync::CancellationToken;

pub const STOCK_ADAPTER: StockAdapter = StockAdapter {
    platform: "slack",
    schema: &[
        api::TABLES,
        callbacks::TABLES,
        intake::TABLES,
        reactions::TABLES,
        delivery::CONTROLS,
        delivery::STREAMS,
        delivery::FILES,
    ],
    configure: settings::configure,
    replace_credential: settings::replace_credential,
};

struct Factory {
    root: PathBuf,
    settings: Settings,
    verification: Arc<Mutex<Value>>,
}

struct Connection {
    root: PathBuf,
    settings: Settings,
    api: Api,
    id: Identity,
    verification: Arc<Mutex<Value>>,
}

struct Slack(Arc<Connection>);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl AdapterFactory for Factory {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "slack".into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: self.settings.files,
                threads: true,
                interactive_prompts: true,
                progress: self.settings.agent,
                editing: true,
                effects: EffectCapabilities {
                    prepare_resources: self.settings.files,
                    durable_presentation: true,
                    ..EffectCapabilities::default()
                },
                ..Capabilities::TEXT
            },
        }
    }

    fn verification_status(&self) -> Option<Value> {
        Some(lock(&self.verification).clone())
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
            let credential = Credential::parse(credential.secret(), self.settings.receiver_owner)
                .map_err(Failure::connection)?;
            let api = Api::new(&self.settings, credential).map_err(Failure::connection)?;
            let id = api
                .identify(&self.settings, &cancel)
                .await
                .map_err(|failure| {
                    lock(&self.verification)["verification"] = json!(match failure {
                        Failure::Permission => "missing_scope_or_app_access",
                        Failure::Credentials => "wrong_installation",
                        _ => "not_verified",
                    });
                    failure.connection()
                })?;
            let account = id.team.clone();
            *lock(&self.verification) = json!({
                "manifest":self.settings.manifest(),
                "verification":"verified",
                "app":id.app,"team":id.team,"enterprise":id.enterprise,
                "bot":id.bot,"bot_user":id.user,
                "receiver_owner":self.settings.receiver_owner,
                "socket":"waiting",
            });
            let adapter = Slack(Arc::new(Connection {
                root: self.root.clone(),
                settings: self.settings.clone(),
                api,
                id,
                verification: self.verification.clone(),
            }));
            Ok(VerifiedAdapter {
                account,
                adapter: Arc::new(adapter),
            })
        })
    }
}

impl Adapter for Slack {
    fn initial_transport_ready(&self) -> bool {
        false
    }

    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(self.0.id.proof())
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            let registration = match hub::register(self.0.clone(), ingress.clone()) {
                Ok(registration) => registration,
                Err(failure) => return failure,
            };
            let worker = intake::resume(&self.0, &ingress, &registration.hub, &cancel);
            if self.0.settings.receiver_owner {
                tokio::select! {
                    failure = socket::run(&self.0, &ingress, &registration.hub, &cancel) => failure,
                    _ = worker => ConnectionFailure::Stopped,
                }
            } else {
                worker.await;
                ConnectionFailure::Stopped
            }
        })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        delivery::prepare(&self.0, intent)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        delivery::prepare_scoped(&self.0, intent, ingress)
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
