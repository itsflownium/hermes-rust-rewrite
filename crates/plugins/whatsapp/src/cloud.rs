mod api;
mod delivery;

use crate::{
    common::{self, Media, MediaKind, bsuid, digits, is_loopback, lock, wamid},
    intake::{self, Source},
    outbound,
};
use anyhow::{Context, Result, ensure};
use api::{Api, Credential, Failure};
use hermes_gateway_core::messaging::{
    AccessPolicy, Adapter, AdapterFactory, AdapterFuture, CONTRACT_VERSION, Capabilities,
    ConnectionFailure, DeliveryIntent, DeliveryPart, DeliveryResult, Descriptor,
    EffectCapabilities, EnrollmentCredential, GatewayError, Ingress, PreparationFailure,
    ProviderDeliveryState, SendAttempt, StockAdapter, StockConfiguration, VerifiedAdapter,
    VerifiedInstallation,
    runtime::{db, now},
    storage,
};
use reqwest::{Url, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_whatsapp_cloud_windows (
    connection TEXT NOT NULL, recipient TEXT NOT NULL, newest INTEGER NOT NULL,
    provider_closed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(connection,recipient)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_cloud_window_events (
    connection TEXT NOT NULL, event TEXT NOT NULL, proof TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_cloud_cooldowns (
    app TEXT NOT NULL, phone TEXT NOT NULL, recipient TEXT NOT NULL, until_ms INTEGER NOT NULL,
    PRIMARY KEY(app,phone,recipient)
);
CREATE TABLE IF NOT EXISTS messaging_whatsapp_cloud_speech (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, part INTEGER NOT NULL, media TEXT NOT NULL,
    PRIMARY KEY(connection,delivery,part)
);
";

pub const CLOUD_ADAPTER: StockAdapter = StockAdapter {
    platform: "whatsapp_cloud",
    schema: &[common::TABLES, intake::TABLES, outbound::TABLES, TABLES],
    configure,
    replace_credential,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    app_id: String,
    waba_id: String,
    phone_id: String,
    #[serde(default)]
    receiver_owner: bool,
    #[serde(default)]
    access: AccessPolicy,
    #[serde(default)]
    graph_origin: Option<String>,
    #[serde(default)]
    read_signals: bool,
    #[serde(default)]
    typing: bool,
    #[serde(default)]
    audio_attachment: bool,
    #[serde(default)]
    home: Option<String>,
    #[cfg(test)]
    #[serde(default)]
    fixture_clock: Option<PathBuf>,
}

impl Settings {
    fn endpoint(&self) -> Result<Url> {
        if let Some(origin) = &self.graph_origin {
            let url = Url::parse(origin).context("Invalid Graph fixture URL")?;
            ensure!(
                url.scheme() == "http"
                    && is_loopback(&url)
                    && url.port().is_some()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.username().is_empty()
                    && url.password().is_none(),
                "Graph fixture URL must name a loopback HTTP listener"
            );
            return Ok(url);
        }
        Ok(Url::parse("https://graph.facebook.com/").expect("Graph API origin"))
    }

    fn clock(&self) -> i64 {
        #[cfg(test)]
        if let Some(path) = &self.fixture_clock
            && let Ok(value) = std::fs::read_to_string(path)
            && let Ok(value) = value.trim().parse()
        {
            return value;
        }
        now()
    }
}

fn configure(root: &Path, settings: Value) -> Result<StockConfiguration> {
    let settings: Settings = serde_json::from_value(settings)?;
    settings.endpoint()?;
    ensure!(
        [&settings.app_id, &settings.waba_id, &settings.phone_id]
            .into_iter()
            .all(|id| digits(id)),
        "Meta app, business account and phone number IDs are required"
    );
    ensure!(
        settings
            .home
            .as_deref()
            .is_none_or(|id| digits(id) || bsuid(id)),
        "Invalid Cloud home recipient"
    );
    let policy = common::strict_policy(settings.access.clone());
    Ok(StockConfiguration {
        settings: serde_json::to_value(&settings)?,
        policy,
        factory: Arc::new(Factory {
            root: root.to_owned(),
            settings,
            status: Arc::new(Mutex::new(json!({"registration":"setup_required"}))),
        }),
    })
}

fn replace_credential(old: &Value, new: &Value) -> bool {
    ["app_id", "waba_id", "phone_id", "graph_origin"]
        .into_iter()
        .any(|key| old[key] != new[key])
}

struct Factory {
    root: PathBuf,
    settings: Settings,
    status: Arc<Mutex<Value>>,
}

struct Connection {
    root: PathBuf,
    api: Api,
    settings: Settings,
    subscribed: AtomicBool,
    status: Arc<Mutex<Value>>,
}

struct Cloud(Arc<Connection>);

impl AdapterFactory for Factory {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "whatsapp_cloud".into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: true,
                interactive_prompts: true,
                effects: EffectCapabilities {
                    prepare_resources: true,
                    ..EffectCapabilities::default()
                },
                ..Capabilities::TEXT
            },
        }
    }

    fn verification_status(&self) -> Option<Value> {
        Some(lock(&self.status).clone())
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
            let settings = self.settings.clone();
            let api = Api::new(
                settings
                    .endpoint()
                    .map_err(|_| ConnectionFailure::Configuration)?,
                Credential::parse(credential.secret()).map_err(Failure::connection)?,
            )
            .map_err(Failure::connection)?;
            let subscribed = api
                .authenticate(
                    &settings.app_id,
                    &settings.waba_id,
                    &settings.phone_id,
                    &cancel,
                )
                .await
                .map_err(|failure| {
                    lock(&self.status)["registration"] = json!(match failure {
                        Failure::Credential => "invalid_token",
                        Failure::Permission | Failure::Invalid => "misrouted_registration",
                        _ => "verification_unavailable",
                    });
                    failure.connection()
                })?;
            *lock(&self.status) = json!({
                "registration":if subscribed { "verified" } else { "not_subscribed" },
                "app_id":settings.app_id, "waba_id":settings.waba_id,"phone_id":settings.phone_id,
                "graph_version":api::VERSION,
            });
            Ok(VerifiedAdapter {
                account: settings.phone_id.clone(),
                adapter: Arc::new(Cloud(Arc::new(Connection {
                    root: self.root.clone(),
                    api,
                    settings,
                    subscribed: AtomicBool::new(subscribed),
                    status: self.status.clone(),
                }))),
            })
        })
    }
}

impl Adapter for Cloud {
    fn initial_transport_ready(&self) -> bool {
        false
    }

    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        let settings = &self.0.settings;
        Some(VerifiedInstallation {
            installation: settings.phone_id.clone(),
            tenant: Some(settings.waba_id.clone()),
            application: Some(settings.app_id.clone()),
            receiver: Some(format!("whatsapp-cloud:{}", settings.app_id)),
            origin: Some("https://graph.facebook.com".into()),
            device: None,
            local_client: None,
        })
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move { self.0.clone().listen(ingress, cancel).await })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        outbound::plan(intent, None, true, false)
    }

    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        outbound::plan(intent, Some(ingress), true, true)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async move { delivery::send(&self.0, &ingress, &attempt, &cancel).await })
    }
}

#[derive(Clone)]
struct Binding {
    connection: Arc<Connection>,
    ingress: Ingress,
}

#[derive(Default)]
struct Hub {
    bindings: Mutex<BTreeMap<String, Binding>>,
    online: AtomicBool,
}

type Registry = BTreeMap<(PathBuf, String), Weak<Hub>>;
static APPS: OnceLock<Mutex<Registry>> = OnceLock::new();

struct Registered {
    hub: Arc<Hub>,
    phone: String,
    owns_receiver: bool,
}

impl Drop for Registered {
    fn drop(&mut self) {
        lock(&self.hub.bindings).remove(&self.phone);
        if self.owns_receiver {
            self.hub.online.store(false, Ordering::Release);
            self.hub.propagate();
        }
    }
}

impl Hub {
    fn register(
        connection: Arc<Connection>,
        ingress: Ingress,
    ) -> Result<Registered, ConnectionFailure> {
        let app = (connection.root.clone(), connection.settings.app_id.clone());
        let hub = {
            let mut all = lock(APPS.get_or_init(Mutex::default));
            all.retain(|_, hub| hub.strong_count() != 0);
            if let Some(hub) = all.get(&app).and_then(Weak::upgrade) {
                hub
            } else {
                let hub = Arc::new(Self::default());
                all.insert(app, Arc::downgrade(&hub));
                hub
            }
        };
        let phone = connection.settings.phone_id.clone();
        let owns_receiver = connection.settings.receiver_owner;
        let mut bindings = lock(&hub.bindings);
        if bindings.contains_key(&phone)
            || bindings.values().any(|peer| {
                peer.connection.api.credential.app_secret != connection.api.credential.app_secret
                    || owns_receiver && peer.connection.settings.receiver_owner
            })
        {
            return Err(ConnectionFailure::Configuration);
        }
        bindings.insert(
            phone.clone(),
            Binding {
                connection,
                ingress,
            },
        );
        drop(bindings);
        Ok(Registered {
            hub,
            phone,
            owns_receiver,
        })
    }

    fn propagate(&self) {
        let online = self.online.load(Ordering::Acquire);
        for member in lock(&self.bindings).values() {
            let subscribed = member.connection.subscribed.load(Ordering::Acquire);
            let _ = member.ingress.report_transport_ready(online && subscribed);
            lock(&member.connection.status)["registration"] = json!(if !subscribed {
                "not_subscribed"
            } else if online {
                "ready"
            } else {
                "receiver_unavailable"
            });
        }
    }

    async fn webhook(&self, ingress: Ingress, body: Vec<u8>) -> Result<(), GatewayError> {
        ingress.checked(true, |_| Ok(()))?;
        let envelope: Envelope =
            serde_json::from_slice(&body).map_err(|_| GatewayError::Invalid)?;
        if envelope.object != "whatsapp_business_account"
            || envelope.entry.is_empty()
            || envelope.entry.len() > 64
            || !self.online.load(Ordering::Acquire)
        {
            return Err(GatewayError::Invalid);
        }
        let mut rows = 0_usize;
        for entry in envelope.entry {
            if !digits(&entry.id) || entry.changes.len() > 128 {
                return Err(GatewayError::Invalid);
            }
            for offer in entry.changes {
                let change = &offer.value;
                for field in ["messages", "statuses"] {
                    if let Some(value) = change.get(field) {
                        rows = rows
                            .saturating_add(value.as_array().ok_or(GatewayError::Invalid)?.len());
                    }
                }
                if rows > 512 {
                    return Err(GatewayError::Invalid);
                }
                let member = change["metadata"]["phone_number_id"]
                    .as_str()
                    .and_then(|phone| lock(&self.bindings).get(phone).cloned())
                    .filter(|member| member.connection.settings.waba_id == entry.id);
                let Some(member) = member.filter(|_| {
                    offer.field == "messages" && change["messaging_product"] == "whatsapp"
                }) else {
                    let key = db(storage::key(
                        "cloud-refusal",
                        &(&entry.id, &offer.field, change),
                    ))?;
                    intake::refuse(&ingress, &key, "unmatched_webhook_binding")?;
                    continue;
                };
                if !member.connection.subscribed.load(Ordering::Acquire) {
                    return Err(GatewayError::TemporarilyUnavailable);
                }
                for message in change["messages"].as_array().into_iter().flatten() {
                    member.connection.message(&member.ingress, message).await?;
                }
                for status in change["statuses"].as_array().into_iter().flatten() {
                    member.connection.status(&member.ingress, status).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Envelope {
    object: String,
    entry: Vec<Entry>,
}
#[derive(Deserialize)]
struct Entry {
    id: String,
    changes: Vec<Change>,
}
#[derive(Deserialize)]
struct Change {
    field: String,
    value: Value,
}

impl Connection {
    async fn listen(
        self: Arc<Self>,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> ConnectionFailure {
        let registered = match Hub::register(self.clone(), ingress.clone()) {
            Ok(registration) => registration,
            Err(error) => return error,
        };
        let _lease = if self.settings.receiver_owner {
            let lease = match ingress.claim_receiver() {
                Ok(lease) => lease,
                Err(_) => return ConnectionFailure::Configuration,
            };
            let hub = registered.hub.clone();
            let receiver = Arc::new(move |scope, body| {
                let hub = hub.clone();
                Box::pin(async move { hub.webhook(scope, body).await })
                    as AdapterFuture<'static, Result<(), GatewayError>>
            });
            let endpoint = match lease.register_signed_webhook(
                header::HeaderName::from_static("x-hub-signature-256"),
                self.api.credential.app_secret.as_bytes(),
                self.api.credential.verify_token.clone(),
                receiver,
            ) {
                Ok(endpoint) => endpoint,
                Err(_) => return ConnectionFailure::Configuration,
            };
            lock(&self.status)["webhook_path"] = json!(endpoint);
            registered.hub.online.store(true, Ordering::Release);
            Some(lease)
        } else {
            None
        };
        registered.hub.propagate();
        let refresh = async {
            loop {
                tokio::time::sleep(Duration::from_secs(20)).await;
                match self
                    .api
                    .subscribed(&self.settings.waba_id, &self.settings.app_id, &cancel)
                    .await
                {
                    Ok(subscribed) => {
                        self.subscribed.store(subscribed, Ordering::Release);
                        registered.hub.propagate();
                    }
                    Err(Failure::Credential | Failure::Permission) => {
                        self.subscribed.store(false, Ordering::Release);
                        registered.hub.propagate();
                        lock(&self.status)["registration"] = json!("revoked");
                        return ConnectionFailure::Credentials;
                    }
                    _ => {}
                }
            }
        };
        tokio::select! {
            _ = cancel.cancelled() => ConnectionFailure::Stopped,
            _ = intake::resume(&ingress,self.as_ref(),&cancel) => ConnectionFailure::Stopped,
            state = refresh => state,
        }
    }

    async fn message(&self, ingress: &Ingress, message: &Value) -> Result<(), GatewayError> {
        let identity = db(storage::key("whatsapp-cloud-inbound", &message["id"]))?;
        let Some(anchor) = message["id"].as_str().filter(|id| wamid(id)) else {
            let key = db(storage::key("cloud-invalid-message", message))?;
            return intake::refuse(ingress, &key, "invalid_message_id");
        };
        let Some(time) = common::provider_millis(&message["timestamp"])
            .filter(|time| *time <= self.settings.clock().saturating_add(60_000))
        else {
            return intake::refuse(ingress, &identity, "invalid_timestamp");
        };
        if message["group_id"].is_string()
            || message["recipient_type"] == "group"
            || message["from_me"] == true
        {
            return intake::refuse(ingress, &identity, "unsupported_audience");
        }
        let phone = message["from"].as_str().filter(|id| digits(id));
        let user = message["from_user_id"].as_str().filter(|id| bsuid(id));
        let Some(recipient) = phone.or(user) else {
            return intake::refuse(ingress, &identity, "unknown_sender");
        };
        let Some(body) = payload(message) else {
            return intake::refuse(ingress, &identity, "unsupported_message");
        };
        let actor = match common::bind_aliases(ingress, recipient, user.filter(|_| phone.is_some()))
        {
            Ok(actor) => actor,
            Err(GatewayError::Conflict) => {
                return intake::refuse(ingress, &identity, "identity_conflict");
            }
            Err(error) => return Err(error),
        };
        let room = common::room(&actor, false);
        common::observe(ingress, &room, &actor).await?;
        if body.text != "/pair" {
            if !common::permits(ingress, &room, &actor)? {
                return intake::refuse(ingress, &identity, "actor_forbidden");
            }
        }
        let source = Source {
            event: identity,
            room: room.clone(),
            actor: actor.clone(),
            anchor: anchor.into(),
            provider_chat: actor.clone(),
            provider_actor: None,
            text: body.text,
            media: body.media.into_iter().collect(),
            quote: message["context"]["id"]
                .as_str()
                .filter(|id| wamid(id))
                .map(str::to_owned),
            choice: body.choice,
            mentioned: true,
            owner: false,
            speech_mentions: Vec::new(),
        };
        if source.text != "/pair"
            && (source.choice.is_none() || outbound::answer(ingress, &source)?.is_some())
            && !delivery::open_window(ingress, &source, time)?
        {
            intake::refuse(ingress, &source.event, "conflicting_window_event")?;
            return Ok(());
        }
        let admitted = intake::stage(ingress, source).await?;
        if admitted
            && self.settings.home.as_deref().is_some_and(|target| {
                target == actor || Some(target) == phone || Some(target) == user
            })
        {
            let route = ingress
                .verify_destination(room, now().saturating_add(common::EVIDENCE_MS))
                .await?;
            lock(&self.status)["home_route"] = json!(route);
        }
        Ok(())
    }

    async fn status(&self, ingress: &Ingress, receipt: &Value) -> Result<(), GatewayError> {
        let key = db(storage::key("cloud-status-refusal", receipt))?;
        let Some(id) = receipt["id"].as_str().filter(|id| wamid(id)) else {
            return intake::refuse(ingress, &key, "unknown_effect");
        };
        let kind = match receipt["status"].as_str() {
            Some("sent") => ProviderDeliveryState::Accepted,
            Some("delivered") => ProviderDeliveryState::Delivered,
            Some("read") => ProviderDeliveryState::Read,
            Some("failed") => ProviderDeliveryState::Failed,
            _ => return intake::refuse(ingress, &key, "unsupported_status"),
        };
        let phone = receipt["recipient_id"].as_str().filter(|id| digits(id));
        let user = receipt["recipient_user_id"].as_str().filter(|id| bsuid(id));
        let (left, right) = (
            phone
                .map(|id| common::lookup_alias(ingress, id))
                .transpose()?
                .flatten(),
            user.map(|id| common::lookup_alias(ingress, id))
                .transpose()?
                .flatten(),
        );
        if left.is_some() && right.is_some() && left != right {
            return intake::refuse(ingress, &key, "receipt_identity_conflict");
        }
        let Some(actor) = left.or(right) else {
            return intake::refuse(ingress, &key, "unknown_recipient");
        };
        let Some(time) = common::provider_millis(&receipt["timestamp"])
            .filter(|time| *time <= now().saturating_add(60_000))
        else {
            return intake::refuse(ingress, &key, "invalid_status_timestamp");
        };
        let operation = receipt["biz_opaque_callback_data"]
            .as_str()
            .filter(|key| {
                ["whatsapp-effect-", "whatsapp-voice-"]
                    .iter()
                    .any(|prefix| {
                        key.strip_prefix(prefix).is_some_and(|digest| {
                            digest.len() == 64
                                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                    })
            })
            .map(str::to_owned);
        outbound::observe_status(
            ingress,
            common::room(&actor, false),
            id.into(),
            operation,
            None,
            time,
            kind,
        )
        .await
    }
}

struct Payload {
    text: String,
    media: Option<Media>,
    choice: Option<String>,
}

fn payload(msg: &Value) -> Option<Payload> {
    let kind = msg["type"].as_str()?;
    let fields = &msg[kind];
    let mut out = Payload {
        text: String::new(),
        media: None,
        choice: None,
    };
    let media = match kind {
        "text" => {
            out.text = fields["body"].as_str()?.to_owned();
            return (!out.text.trim().is_empty()
                && out.text.len() <= 256 * 1024
                && !out.text.contains('\0'))
            .then_some(out);
        }
        "interactive" => {
            let selected = match fields["type"].as_str()? {
                "button_reply" => &fields["button_reply"],
                "list_reply" => &fields["list_reply"],
                _ => return None,
            };
            out.choice = Some(
                selected["id"]
                    .as_str()
                    .filter(|id| common::text_id(id))?
                    .into(),
            );
            return Some(out);
        }
        "location" => {
            let (lat, lon) = (fields["latitude"].as_f64()?, fields["longitude"].as_f64()?);
            if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                return None;
            }
            out.text = format!("Shared location {lat:.6}, {lon:.6}");
            return Some(out);
        }
        "image" => MediaKind::Image,
        "audio" => MediaKind::Audio,
        "video" => MediaKind::Video,
        "document" => MediaKind::Document,
        _ => return None,
    };
    out.text = fields["caption"]
        .as_str()
        .filter(|text| text.len() <= 8_192 && !text.contains('\0'))
        .unwrap_or_default()
        .into();
    out.media = Some(Media {
        id: fields["id"].as_str().filter(|id| digits(id))?.into(),
        kind: media,
        voice: kind == "audio" && fields["voice"] == true,
        name: fields["filename"]
            .as_str()
            .filter(|name| hermes_gateway_core::messaging::media::valid_name(name))
            .map(str::to_owned),
        locator: None,
    });
    Some(out)
}
