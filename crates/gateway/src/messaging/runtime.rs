use super::{
    EffectTransportRef, EffectVisibility, acknowledgements::Acknowledgements,
    effects::valid_effect_plan,
    profile::{
        AccountBinding, ConnectionSettings, EnrollmentCredential, EnrollmentLease,
        VerifiedInstallation,
    },
    sessions,
    stock::{StockAdapter, StockConfiguration, StockConnection},
    storage::AdapterStorage,
    store::{self, OutboundPolicy, PendingDelivery, Store},
    transport::{InstallationKey, ReceiverLease, SharedReceiver},
    types::*,
    webhook_receiver::Webhook,
};
use crate::{Shared, lock};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

pub const MAX_TEXT: usize = 256 * 1024;
pub(super) const MAX_BATCH: usize = 100;
const MAX_PARTS: usize = 128;
const POLL: Duration = Duration::from_millis(100);

pub fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
}

fn transport_scope(installation: Option<&VerifiedInstallation>) -> Result<String> {
    installation.map_or_else(|| Ok(String::new()), |proof| store::key("transport-scope", proof))
}

pub fn valid_input_id(value: &str) -> bool {
    valid_id(value) && !super::profile::resource_event_id(value) && !super::observations::is_observation(value)
}

pub fn valid_advance(advance: Option<&Advance>) -> bool {
    let token = |value: &str| !value.is_empty() && value.len() <= 64 * 1024 && !value.contains('\0');
    advance.is_none_or(|advance| {
        valid_id(&advance.stream)
            && token(&advance.checkpoint)
            && advance.previous.as_deref().is_none_or(token)
    })
}

pub(crate) struct Host {
    store: Arc<Store>,
    slots: AsyncMutex<BTreeMap<String, Managed>>,
    factories: Mutex<BTreeMap<String, Arc<dyn AdapterFactory>>>,
    stock: BTreeMap<&'static str, StockAdapter>,
}

struct Managed {
    slot: Arc<Slot>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

struct Slot {
    name: String,
    descriptor: Descriptor,
    owner: Weak<Shared>,
    state: Mutex<State>,
    acknowledgements: Arc<Mutex<Acknowledgements>>,
    changed: Notify,
}

pub struct State {
    pub policy: AccessPolicy,
    pub(super) revision: i64,
    pub(super) evidence_epoch: u64,
    generation: Option<String>,
    identity: Option<String>,
    installation: Option<VerifiedInstallation>,
    accepting: bool,
    pub(super) health: Health,
    gap: bool,
    webhook: Option<Webhook>,
}

impl State {
    pub fn revision(&self) -> i64 { self.revision }
}

/// The only host handle a connected adapter receives. Its generation is
/// fenced on disconnect and it carries no profile administration interface.
#[derive(Clone)]
pub struct Ingress {
    slot: Arc<Slot>,
    pub(super) store: Arc<Store>,
    pub(super) connection: String,
    pub(super) generation: String,
    installation: Option<VerifiedInstallation>,
    root: PathBuf,
}

impl Ingress {
    /// Claim the receiver verified for this installation. The gateway lease
    /// spans profiles in the installation, just like a shared receiver lease.
    pub fn claim_receiver(&self) -> Result<ReceiverLease, GatewayError> {
        self.checked(true, |_| {
            let receiver = self
                .installation()
                .and_then(|proof| proof.receiver.as_ref())
                .ok_or(GatewayError::Unsupported)?;
            let identity = db(store::key("receiver", receiver))?;
            let lease = db(account_lease(&self.root, &identity))?.ok_or(GatewayError::Conflict)?;
            Ok(ReceiverLease {
                scope: self.clone(),
                _lease: lease,
            })
        })
    }

    pub(super) fn name(&self) -> &str {
        &self.slot.name
    }

    pub(super) fn attach_webhook(&self, webhook: Webhook) -> Result<(), GatewayError> {
        let mut state = lock(&self.slot.state);
        if !state.accepting || state.generation.as_deref() != Some(&self.generation) {
            return Err(GatewayError::StaleConnection);
        }
        state.webhook = Some(webhook);
        Ok(())
    }

    pub fn connection_name(&self) -> &str {
        &self.connection
    }

    /// Storage for bundled adapters that update their protocol state in the
    /// same transaction as gateway receipts and checkpoints.
    pub fn storage(&self) -> AdapterStorage<'_> {
        AdapterStorage::new(&self.store, &self.connection)
    }

    pub fn installation(&self) -> Option<&VerifiedInstallation> {
        self.installation.as_ref()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn checked<T>(
        &self,
        accepting: bool,
        action: impl FnOnce(&State) -> Result<T, GatewayError>,
    ) -> Result<T, GatewayError> {
        let state = lock(&self.slot.state);
        if state.generation.as_deref() != Some(&self.generation) || (accepting && !state.accepting)
        {
            return Err(GatewayError::StaleConnection);
        }
        action(&state)
    }

    pub(super) fn apply_evidence_invalidation(
        &self,
        invalidate: impl FnOnce() -> Result<(), GatewayError>,
    ) -> Result<(), GatewayError> {
        let mut state = lock(&self.slot.state);
        if state.generation.as_deref() != Some(&self.generation) {
            return Err(GatewayError::StaleConnection);
        }
        invalidate()?;
        state.evidence_epoch = state.evidence_epoch.wrapping_add(1);
        Ok(())
    }

    pub(super) fn acknowledgements(&self) -> Arc<Mutex<Acknowledgements>> {
        self.slot.acknowledgements.clone()
    }

    pub(super) fn platform(&self) -> &str {
        &self.slot.descriptor.platform
    }

    pub fn capabilities(&self) -> Capabilities {
        self.slot.descriptor.capabilities
    }

    pub fn configuration_revision(&self) -> Result<u64, GatewayError> {
        self.checked(true, |state| Ok(state.revision as u64))
    }

    /// Store a provider-issued renewal proven for this exact live installation.
    /// Replacing the account or device always requires owner enrollment.
    pub async fn renew_enrolled_credential(
        &self,
        previous_revision: u64,
        installation: &VerifiedInstallation,
        credential: EnrollmentCredential,
    ) -> Result<u64, GatewayError> {
        let revision = i64::try_from(previous_revision).map_err(|_| GatewayError::Invalid)?;
        if self.installation() != Some(installation) {
            return Err(GatewayError::Forbidden);
        }
        let scope = self.clone();
        blocking(move || {
            let value = db(hermes_engine::secret::seal(
                scope.root(),
                credential.secret(),
            ))?;
            let mut state = lock(&scope.slot.state);
            if state.generation.as_deref() != Some(&scope.generation)
                || !state.accepting
                || state.revision != revision
            {
                return Err(GatewayError::StaleConnection);
            }
            let next = db(scope
                .store
                .renew_credential(&scope.slot.name, revision, &value))?
            .ok_or(GatewayError::Forbidden)?;
            state.revision = next;
            Ok(next as u64)
        })
        .await
    }

    pub fn notify(&self) {
        self.slot.changed.notify_one();
    }

    /// Resolve a native stop or correction against the currently owned turn.
    pub async fn active_execution(&self, room: &Room, sender: &Sender) -> Result<Option<u64>, GatewayError> {
        let session = self.checked(true, |state| {
            if !self.authorize_operator(&state.policy, room, sender)? {
                return Err(GatewayError::Forbidden);
            }
            let event = InboundEvent { id: "active-execution".into(), room: room.clone(), sender: sender.clone(),
                reply_anchor: None, mentioned: true, body: InboundBody::Command(super::Command::Status) };
            let Ok(route) = self.route_event(&state.policy, &event)? else { return Err(GatewayError::Forbidden) };
            let Some(route) = db(self.store.route(&route.id))?.filter(|route| !route.blocked) else {
                return Ok(None);
            };
            let route = db(self.store.conversation(&route))?;
            Ok(route.session.filter(|_| !route.blocked))
        })?;
        let Some(id) = session else { return Ok(None) };
        let owner = self.slot.owner.upgrade().ok_or(GatewayError::StaleConnection)?;
        let session = owner.sessions.lock().await.get(&id).cloned();
        self.checked(true, |_| Ok(session.and_then(|session| {
            let view = lock(&session.view);
            view.running.then_some(view.generation)
        })))
    }

    pub fn report_transport_ready(&self, ready: bool) -> Result<(), GatewayError> {
        let mut state = lock(&self.slot.state);
        if !state.accepting || state.generation.as_deref() != Some(self.generation.as_str()) {
            return Err(GatewayError::StaleConnection);
        }
        state.health = if ready { Health::Ready } else { Health::Connecting };
        drop(state);
        self.notify();
        Ok(())
    }

    pub(super) fn model_provider_scope(&self) -> Result<String, GatewayError> {
        let owner = self.slot.owner.upgrade().ok_or(GatewayError::StaleConnection)?;
        db(store::key("channel-model-provider", &(
            owner.provider.base_url.trim().trim_end_matches('/'),
            owner.provider.api_key.trim(),
            owner.provider.model.as_str(),
        )))
    }

    pub fn transport_scope(&self) -> Result<String, GatewayError> {
        db(transport_scope(self.installation()))
    }

    pub(super) fn scoped_transport_key(&self, kind: &str, value: &str) -> Result<String, GatewayError> {
        let scope = self.transport_scope()?;
        if scope.is_empty() {
            return Ok(value.to_owned());
        }
        db(store::key(kind, &(scope, value)))
    }

    pub fn scoped_advance(&self, advance: Option<Advance>) -> Result<Option<Advance>, GatewayError> {
        advance.map(|mut advance| {
            advance.stream = self.scoped_transport_key("checkpoint-stream", &advance.stream)?;
            Ok(advance)
        }).transpose()
    }

    pub async fn checkpoint(&self, stream: String) -> Result<Option<String>, GatewayError> {
        if !valid_id(&stream) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                let stream = scope.scoped_transport_key("checkpoint-stream", &stream)?;
                db(scope.store.checkpoint(&scope.connection, &stream))
            })
        })
        .await
    }

    pub async fn submit(
        &self,
        events: Vec<InboundEvent>,
        advance: Option<Advance>,
    ) -> Result<IntakeResult, GatewayError> {
        if events.len() > MAX_BATCH
            || events.iter().any(|event| !valid_input_id(&event.id) || matches!(event.body, InboundBody::Wake))
            || !valid_advance(advance.as_ref())
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let result = blocking(move || {
            scope.checked(true, |state| {
                let mut candidates = Vec::with_capacity(events.len());
                for event in events {
                    let decision = scope.route_event(&state.policy, &event)?;
                    candidates.push(scope.intake_candidate(&state.policy, event, decision, None)?);
                }
                let advance = scope.scoped_advance(advance)?;
                db(scope.store.intake(
                    &scope.connection,
                    state.revision,
                    candidates,
                    advance.as_ref(),
                ))?
                .ok_or(GatewayError::CheckpointChanged)
            })
        })
        .await?;
        self.notify();
        Ok(result)
    }

    pub fn route_event(
        &self,
        policy: &AccessPolicy,
        event: &InboundEvent,
    ) -> Result<Result<store::Route, Disposition>, GatewayError> {
        self.route_candidate(policy, event, false)
    }

    pub fn route_candidate(
        &self,
        policy: &AccessPolicy,
        event: &InboundEvent,
        preparing: bool,
    ) -> Result<Result<store::Route, Disposition>, GatewayError> {
        if !valid_id(&event.room.chat)
            || !valid_id(&event.sender.id)
            || event.room.thread.as_ref().is_some_and(|v| !valid_id(v))
            || event.reply_anchor.as_ref().is_some_and(|v| !valid_id(v))
            || event.sender.roles.len() > 128
            || event.sender.roles.iter().any(|id| !valid_id(id))
        {
            return Ok(Err(Disposition::Invalid));
        }
        let resource = match self.recheck_resource(policy, event) {
            Ok(resource) => resource,
            Err(GatewayError::Forbidden) => return Ok(Err(Disposition::Forbidden)),
            Err(error) => return Err(error),
        };
        let caps = self.capabilities();
        if event.room.thread.is_some() && !caps.threads {
            return Ok(Err(Disposition::Unsupported));
        }
        let Some(room) = policy.room_policy(&event.room) else {
            return Ok(Err(Disposition::Forbidden));
        };
        let Some((sender, kind)) = self.authorized_identity(policy, &event.room, &event.sender)?
        else {
            return Ok(Err(Disposition::Forbidden));
        };
        if kind != super::ActorKind::Person
            && !matches!(event.body, InboundBody::Message(_) | InboundBody::Command(super::Command::Status))
        {
            return Ok(Err(Disposition::Forbidden));
        }
        match &event.body {
            InboundBody::Message(message) => {
                if !caps.text_intake {
                    return Ok(Err(Disposition::Unsupported));
                }
                if !(valid_message(message)
                    || preparing && message.text.is_empty() && message.attachments.is_empty())
                {
                    return Ok(Err(Disposition::Invalid));
                }
                if !message.attachments.is_empty() && !caps.attachments {
                    return Ok(Err(Disposition::Unsupported));
                }
                if room.mention_required && !event.mentioned {
                    return Ok(Err(Disposition::Forbidden));
                }
            }
            InboundBody::Control(Control::Respond { .. }) if !caps.interactive_prompts => {
                return Ok(Err(Disposition::Unsupported));
            }
            InboundBody::Control(control) if !valid_control(control) => {
                return Ok(Err(Disposition::Invalid));
            }
            InboundBody::Control(_) => {}
            InboundBody::Command(command) => {
                if !caps.text_intake || matches!(command, super::Command::FetchResult { .. }) && !caps.interactive_prompts {
                    return Ok(Err(Disposition::Unsupported));
                }
                if !command.valid() {
                    return Ok(Err(Disposition::Invalid));
                }
                if command.uses_scoped_grant()
                    && !policy.allows_scoped_command(&event.room, &sender)
                {
                    return Ok(Err(Disposition::Forbidden));
                }
                if let super::Command::UseSkill { skill, .. } = command
                    && (!room.tools || !policy.turn_policy(&event.room).skills.is_some_and(|skills| skills.contains(skill)))
                {
                    return Ok(Err(Disposition::Forbidden));
                }
            }
            InboundBody::Wake => return Ok(Err(Disposition::Unsupported)),
        }
        if !preparing {
            match self.check_source_protection(policy, &event.room, &sender.id, &event.id) {
                Ok(_) => {}
                Err(GatewayError::ProtectionUnavailable) => return Ok(Err(Disposition::ProtectionUnavailable)),
                Err(error) => return Err(error),
            }
        }
        if matches!(event.body, InboundBody::Command(super::Command::Background { .. })) {
            if !caps.text_delivery || preparing {
                return Ok(Err(Disposition::Unsupported));
            }
            if event.reply_anchor.is_some() {
                return Ok(Err(Disposition::Invalid));
            }
            return self.background_route(&event.room, &sender.id, &event.id).map(Ok);
        }
        let participant = room.per_participant.then_some(sender.id.as_str());
        let route = db(store::Route::new(
            &self.connection,
            event.room.clone(),
            participant,
        ))?;
        match resource {
            Some(source) => db(source.scope_route(route)).map(Ok),
            None => Ok(Ok(route)),
        }
    }

    /// Repeat the host's exact attempt checks after a provider preflight or
    /// file read and immediately before dispatch.
    pub async fn recheck_send(&self, attempt: &SendAttempt) -> Result<(), GatewayError> {
        self.recheck_send_destination(attempt, &attempt.payload.room).await
    }

    pub async fn recheck_send_destination(
        &self,
        attempt: &SendAttempt,
        destination: &Room,
    ) -> Result<(), GatewayError> {
        let (scope, attempt, destination) = (self.clone(), attempt.clone(), destination.clone());
        blocking(move || scope.checked(true, |state| {
            let pending = db(scope.store.delivery_for_recheck(&scope.connection, &scope.generation, &attempt))?
                .ok_or(GatewayError::Forbidden)?;
            if !delivery_authorized(&scope, state, &pending)? {
                return Err(GatewayError::Forbidden);
            }
            let policy = outbound_policy(&scope, state, &pending, attempt.payload.effect.transport.as_ref())?;
            if !db(scope.store.recheck(&scope.connection, &scope.generation, &pending, &attempt, &policy))? {
                return Err(GatewayError::Forbidden);
            }
            let original = &pending.intent.room;
            if destination == *original {
                return Ok(());
            }
            if store::Route::is_background_id(&pending.route) {
                return Err(GatewayError::Forbidden);
            }
            let confirmed_thread = original.thread.is_none()
                && destination.chat == original.chat
                && destination.kind == original.kind
                && attempt.effect.dependencies.iter().any(|(_, receipt)| {
                    receipt.kind == super::ReceiptKind::Destination
                        && destination.thread.as_deref() == Some(&receipt.id)
                });
            if !confirmed_thread || state.policy.room_policy(&destination).is_none() {
                return Err(GatewayError::Forbidden);
            }
            if let Some(sender) = &pending.sender
                && !scope.authorize_sender(&state.policy, &destination, sender)?
            {
                return Err(GatewayError::Forbidden);
            }
            scope.check_delivery_protection(
                &state.policy, &destination, &pending.intent.id, attempt.payload.effect.protection.as_deref(),
            )?;
            if !db(scope.store.read(|db| store::eligible_consent(
                db, &scope.connection, &destination, attempt.payload.effect.consent_required,
            )))? {
                return Err(GatewayError::Forbidden);
            }
            Ok(())
        })).await
    }

    /// List attempts from this authenticated account that need platform evidence.
    pub async fn uncertain_deliveries(&self) -> Result<Vec<UncertainDelivery>, GatewayError> {
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.uncertain_deliveries(&scope.connection))
            })
        })
        .await
    }

    /// Resolve an exact uncertain attempt using positive platform evidence.
    /// A retryable result must prove that the platform did not accept the attempt.
    pub async fn reconcile_delivery(
        &self,
        delivery: UncertainDelivery,
        evidence: DeliveryResult,
    ) -> Result<bool, GatewayError> {
        match &evidence {
            DeliveryResult::Confirmed { receipt, .. } if valid_id(receipt) => {}
            DeliveryResult::EffectConfirmed(receipt) if valid_id(&receipt.id) => {}
            DeliveryResult::Retryable { .. } => {}
            _ => return Err(GatewayError::Invalid),
        }
        let scope = self.clone();
        let changed = blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.reconcile(
                    &scope.connection,
                    scope.installation(),
                    &delivery,
                    evidence,
                    scope.capabilities().effects.durable_presentation,
                ))
            })
        })
        .await?;
        if changed {
            self.notify();
        }
        Ok(changed)
    }

    pub fn report_gap(&self) -> Result<(), GatewayError> {
        let mut state = lock(&self.slot.state);
        if state.generation.as_deref() != Some(&self.generation) || !state.accepting {
            return Err(GatewayError::StaleConnection);
        }
        db(self.store.report_gap(&self.connection))?;
        state.gap = true;
        Ok(())
    }

    fn drain(&self) {
        let mut state = lock(&self.slot.state);
        if state.generation.as_deref() == Some(&self.generation) {
            state.accepting = false;
            state.webhook = None;
        }
    }

    fn revoke(&self) {
        let mut state = lock(&self.slot.state);
        if state.generation.as_deref() == Some(&self.generation) {
            state.generation = None;
            state.webhook = None;
            state.accepting = false;
        }
    }
}

pub(super) fn valid_message(message: &Message) -> bool {
    message.text.len() <= MAX_TEXT
        && (!message.text.trim().is_empty() || !message.attachments.is_empty())
        && message.attachments.len() <= 8
        && message
            .attachments
            .iter()
            .all(|file| valid_id(&file.id) && valid_id(&file.name))
}

fn valid_control(control: &Control) -> bool {
    let valid_text = |text: &str| !text.trim().is_empty() && text.len() <= MAX_TEXT;
    match control {
        Control::Respond {
            action_id,
            response,
        } => {
            valid_id(action_id)
                && match response {
                    Response::Clarify(answer) => valid_text(answer),
                    Response::Approve | Response::ApproveForSession | Response::ApprovePersistently | Response::Deny => true,
                }
        }
        Control::ReplaceQueued { event_id, text } => valid_id(event_id) && valid_text(text),
        Control::Steer { text, .. } | Control::Redirect { text, .. } => valid_text(text),
        Control::Interrupt { .. } => true,
    }
}

pub fn db<T>(result: Result<T>) -> Result<T, GatewayError> {
    result.map_err(|_| GatewayError::TemporarilyUnavailable)
}

pub async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, GatewayError> + Send + 'static,
) -> Result<T, GatewayError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| GatewayError::TemporarilyUnavailable)?
}

impl Host {
    pub fn open(
        root: &Path, audio: Option<Arc<dyn super::AudioService>>, adapters: Vec<StockAdapter>,
    ) -> Result<Self> {
        let mut stock = BTreeMap::new();
        for adapter in adapters {
            adapter.validate()?;
            ensure!(stock.insert(adapter.platform, adapter).is_none(), "Duplicate stock messaging adapter");
        }
        let store = Arc::new(Store::open_with_audio(root, audio)?);
        for adapter in stock.values().copied() {
            store.install_stock(adapter)?;
        }
        Ok(Self { store, slots: AsyncMutex::default(), factories: Mutex::default(), stock })
    }

    pub(crate) fn stock_method<'a>(&self, method: &'a str) -> Option<(StockAdapter, &'a str)> {
        let (platform, operation) = method.strip_prefix("messaging.profile.")?.rsplit_once('.')?;
        if !matches!(operation, "list" | "create" | "update") {
            return None;
        }
        self.stock.get(platform).copied().map(|adapter| (adapter, operation))
    }

    pub(crate) fn stock_methods(&self) -> impl Iterator<Item = String> + '_ {
        self.stock.keys().flat_map(|platform| {
            ["list", "create", "update"].map(|operation| format!("messaging.profile.{platform}.{operation}"))
        })
    }

    fn has_stock_settings(&self, name: &str) -> Result<bool> {
        let Some(connection) = self.store.connection_setting(name)? else { return Ok(false) };
        let Some(adapter) = self.stock.get(connection.platform.as_str()) else { return Ok(false) };
        Ok(self.store.stock_settings(*adapter, name)?.is_some())
    }

    pub fn validate(config: &AdapterConfig) -> Result<Descriptor> {
        let descriptor = config.factory.descriptor();
        Self::validate_descriptor(&config.name, &descriptor)?;
        Ok(descriptor)
    }

    fn validate_descriptor(name: &str, descriptor: &Descriptor) -> Result<()> {
        ensure!(
            valid_id(name) && valid_id(&descriptor.platform),
            "Invalid messaging adapter identity"
        );
        ensure!(
            descriptor.version == CONTRACT_VERSION,
            "Unsupported gateway adapter contract version"
        );
        ensure!(
            if descriptor.capabilities.wake {
                !descriptor.capabilities.text_intake && !descriptor.capabilities.text_delivery
            } else {
                descriptor.capabilities.text_intake && descriptor.capabilities.text_delivery
            },
            "Gateway adapters require text intake and delivery or wake-only capability"
        );
        Ok(())
    }

    pub async fn configure(&self, owner: &Arc<Shared>, config: AdapterConfig) -> Result<()> {
        self.configure_inner(owner, config, false).await
    }

    pub async fn restore(&self, owner: &Arc<Shared>, config: AdapterConfig) -> Result<()> {
        self.configure_inner(owner, config, true).await
    }

    async fn configure_inner(
        &self,
        owner: &Arc<Shared>,
        config: AdapterConfig,
        restore: bool,
    ) -> Result<()> {
        let descriptor = Self::validate(&config)?;
        let mut slots = self.slots.lock().await;
        ensure!(!owner.shutdown.is_cancelled(), "Gateway is shutting down");
        ensure!(!self.has_stock_settings(&config.name)?,
            "Use the owner-managed settings for this messaging connection");
        let mut saved =
            self.store
                .register_connection(&config.name, &descriptor.platform, &config.policy)?;
        if !restore && (!saved.enabled || saved.policy != config.policy) {
            saved = self
                .store
                .enable_connection_with_policy(&config.name, &config.policy)?;
        } else if restore
            && saved.credential_ref.is_none()
            && saved.enabled
            && saved.policy != config.policy
        {
            // Programmatic adapters bring their policy with their factory.
            // Owner-enrolled adapters restore their persisted settings.
            saved.revision =
                self.store
                    .set_connection_policy(&config.name, &config.policy)? as u64;
            saved.policy = config.policy;
        }
        lock(&self.factories).insert(config.name, config.factory.clone());
        self.activate(owner, &mut slots, config.factory, descriptor, saved)
            .await;
        Ok(())
    }

    pub(crate) async fn save_stock(
        &self,
        owner: &Arc<Shared>,
        adapter: StockAdapter,
        name: &str,
        revision: Option<u64>,
        enabled: bool,
        configuration: StockConfiguration,
    ) -> Result<StockConnection> {
        let descriptor = configuration.factory.descriptor();
        Self::validate_descriptor(name, &descriptor)?;
        ensure!(descriptor.platform == adapter.platform, "Invalid stock messaging adapter identity");
        let mut slots = self.slots.lock().await;
        ensure!(!owner.shutdown.is_cancelled(), "Gateway is shutting down");
        {
            let mut state = revision.and_then(|_| slots.get(name)).map(|managed| lock(&managed.slot.state));
            self.store.save_stock(adapter, name, revision, enabled, &configuration)?;
            if let Some(state) = &mut state {
                state.generation = None;
                state.webhook = None;
                state.accepting = false;
            }
        }
        let connection = self.store.connection_setting(name)?.context("Messaging connection is unavailable")?;
        lock(&self.factories).insert(name.to_owned(), configuration.factory.clone());
        self.activate(owner, &mut slots, configuration.factory, descriptor, connection.clone()).await;
        Ok(StockConnection { connection, settings: configuration.settings })
    }

    pub(crate) async fn restore_stock(&self, owner: &Arc<Shared>) -> Result<()> {
        let mut slots = self.slots.lock().await;
        for adapter in self.stock.values().copied() {
            for (name, settings) in self.store.stock_connections(adapter)? {
                let mut connection = self.store.connection_setting(&name)?.context("Messaging connection is unavailable")?;
                let configuration = (adapter.configure)(owner.library.root(), settings)?;
                let descriptor = configuration.factory.descriptor();
                Self::validate_descriptor(&name, &descriptor)?;
                ensure!(connection.platform == descriptor.platform && descriptor.platform == adapter.platform,
                    "Invalid saved messaging connection platform");
                if connection.policy != configuration.policy {
                    connection.revision = self.store.set_connection_policy(&name, &configuration.policy)? as u64;
                    connection.policy = configuration.policy;
                }
                lock(&self.factories).insert(name, configuration.factory.clone());
                self.activate(owner, &mut slots, configuration.factory, descriptor, connection).await;
            }
        }
        Ok(())
    }

    pub(crate) async fn stock_connections(&self, adapter: StockAdapter) -> Result<Vec<StockConnection>> {
        let _slots = self.slots.lock().await;
        self.store.stock_connections(adapter)?.into_iter().map(|(name, settings)| {
            let connection = self.store.connection_setting(&name)?.context("Messaging connection is unavailable")?;
            Ok(StockConnection { connection, settings })
        }).collect()
    }

    async fn activate(
        &self,
        owner: &Arc<Shared>,
        slots: &mut BTreeMap<String, Managed>,
        factory: Arc<dyn AdapterFactory>,
        descriptor: Descriptor,
        saved: ConnectionSettings,
    ) {
        if let Some(previous) = slots.remove(&saved.name) {
            {
                let mut state = lock(&previous.slot.state);
                state.accepting = false;
                state.generation = None;
                state.webhook = None;
            }
            previous.stop.cancel();
            let _ = previous.task.await;
        }
        if !saved.enabled || owner.shutdown.is_cancelled() {
            return;
        }
        let stop = owner.shutdown.child_token();
        let slot = Arc::new(Slot {
            name: saved.name.clone(),
            descriptor,
            owner: Arc::downgrade(owner),
            state: Mutex::new(State {
                policy: saved.policy,
                revision: saved.revision as i64,
                evidence_epoch: 0,
                generation: None,
                accepting: false,
                identity: None,
                installation: None,
                health: Health::Connecting,
                gap: false,
                webhook: None,
            }),
            acknowledgements: Arc::default(),
            changed: Notify::new(),
        });
        let task = tokio::spawn(supervise(
            Arc::downgrade(owner),
            self.store.clone(),
            owner.library.root().to_owned(),
            slot.clone(),
            factory,
            saved.binding,
            saved.credential_ref,
            stop.clone(),
        ));
        slots.insert(saved.name, Managed { slot, stop, task });
    }

    pub async fn settings(&self) -> Result<Vec<ConnectionSettings>> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || store.connection_settings()).await?
    }

    pub async fn begin_enrollment(&self, name: &str, ttl_ms: i64) -> Result<EnrollmentLease> {
        let (factory, mut lease) = {
            let _slots = self.slots.lock().await;
            let factory = lock(&self.factories)
                .get(name)
                .cloned()
                .context("No adapter factory for this connection")?;
            (factory, self.store.create_enrollment(name, ttl_ms)?)
        };
        let cancel = CancellationToken::new();
        let remaining = lease
            .expires_at
            .saturating_sub(super::profile::time_ms())
            .max(0);
        let challenge = tokio::time::timeout(
            Duration::from_millis(remaining as u64),
            factory.enrollment_prompt(&lease, cancel.clone()),
        )
        .await;
        cancel.cancel();
        let prompt = challenge
            .context("Enrollment expired")?
            .map_err(|_| anyhow::anyhow!("Adapter could not create an enrollment challenge"))?;
        ensure!(
            prompt
                .as_ref()
                .is_none_or(super::EnrollmentPrompt::validate),
            "Invalid enrollment challenge"
        );
        let _slots = self.slots.lock().await;
        ensure!(
            self.store.enrollment_deadline(name, &lease.id)?.is_some()
                && lock(&self.factories)
                    .get(name)
                    .is_some_and(|current| Arc::ptr_eq(current, &factory)),
            "Enrollment expired or was replaced"
        );
        lease.prompt = prompt;
        Ok(lease)
    }

    pub async fn complete_enrollment(
        &self,
        owner: &Arc<Shared>,
        name: &str,
        lease: &str,
        credential: EnrollmentCredential,
    ) -> Result<()> {
        let deadline = self
            .store
            .enrollment_deadline(name, lease)?
            .context("Enrollment expired or was replaced")?;
        let factory = lock(&self.factories)
            .get(name)
            .cloned()
            .context("No adapter factory for this connection")?;
        let cancel = owner.shutdown.child_token();
        let remaining = deadline.saturating_sub(super::profile::time_ms()).max(0);
        let verified = tokio::select! {
            _ = cancel.cancelled() => anyhow::bail!("Gateway is shutting down"),
            answer = tokio::time::timeout(Duration::from_millis(remaining as u64),
                factory.connect_enrolled(credential.clone(), cancel.clone())) => answer,
        };
        cancel.cancel();
        let verified = verified
            .context("Enrollment expired")?
            .map_err(|_| anyhow::anyhow!("Adapter credential verification failed"))?;
        let proof = verified.adapter.verified_installation();
        ensure!(
            valid_id(&verified.account)
                && proof.as_ref().is_none_or(VerifiedInstallation::validate),
            "Adapter did not verify a valid account"
        );
        let binding = AccountBinding::new(verified.account, proof.as_ref());
        drop(verified.adapter);
        let descriptor = factory.descriptor();
        let value = hermes_engine::secret::seal(owner.library.root(), credential.secret())?;
        let mut slots = self.slots.lock().await;
        ensure!(!owner.shutdown.is_cancelled(), "Gateway is shutting down");
        let saved = self
            .store
            .connection_setting(name)?
            .context("No such messaging connection")?;
        ensure!(
            saved.platform == descriptor.platform
                && descriptor.version == CONTRACT_VERSION
                && lock(&self.factories)
                    .get(name)
                    .is_some_and(|current| Arc::ptr_eq(current, &factory)),
            "Adapter factory changed during enrollment"
        );
        if let Some(managed) = slots.get(name) {
            let mut state = lock(&managed.slot.state);
            self.store
                .complete_enrollment(name, lease, &binding, &value)?;
            state.generation = None;
            state.webhook = None;
            state.accepting = false;
        } else {
            self.store
                .complete_enrollment(name, lease, &binding, &value)?;
        }
        let saved = self
            .store
            .connection_setting(name)?
            .context("No such messaging connection")?;
        self.activate(owner, &mut slots, factory, descriptor, saved)
            .await;
        Ok(())
    }

    pub async fn enable(&self, owner: &Arc<Shared>, name: &str, enabled: bool) -> Result<()> {
        let mut slots = self.slots.lock().await;
        if enabled {
            let factory = lock(&self.factories)
                .get(name)
                .cloned()
                .context("No adapter factory for this connection")?;
            self.store.set_connection_enabled(name, true)?;
            let saved = self
                .store
                .connection_setting(name)?
                .context("No such messaging connection")?;
            self.activate(
                owner,
                &mut slots,
                factory.clone(),
                factory.descriptor(),
                saved,
            )
            .await;
        } else {
            if let Some(managed) = slots.get(name) {
                let mut state = lock(&managed.slot.state);
                self.store.set_connection_enabled(name, false)?;
                state.generation = None;
                state.webhook = None;
                state.accepting = false;
            } else {
                self.store.set_connection_enabled(name, false)?;
            }
            if let Some(managed) = slots.remove(name) {
                managed.stop.cancel();
                let _ = managed.task.await;
            }
        }
        Ok(())
    }

    pub async fn pairing(&self, name: &str, code: &str, revoke: Option<&str>) -> Result<bool> {
        ensure!(valid_id(name) && valid_id(code), "Invalid pairing scope");
        let slots = self.slots.lock().await;
        let slot = &slots
            .get(name)
            .context("No such messaging connection")?
            .slot;
        let state = lock(&slot.state);
        let connection = state
            .identity
            .as_deref()
            .filter(|_| state.accepting)
            .context("Connection is not verified")?;
        let changed = match revoke {
            Some(chat) => {
                ensure!(valid_id(chat), "Invalid pairing scope");
                self.store.revoke_pairing(connection, code, chat)?
            }
            None => {
                ensure!(state.policy.scope.pair_direct, "Pairing is disabled");
                self.store.approve_pairing(connection, code)?
            }
        };
        slot.changed.notify_one();
        Ok(changed)
    }

    pub(crate) async fn pending_pairings(&self, name: &str) -> Result<Vec<super::profile::PendingPairing>> {
        let slots = self.slots.lock().await;
        let slot = &slots.get(name).context("No such messaging connection")?.slot;
        let state = lock(&slot.state);
        let connection = state.identity.as_deref().filter(|_| state.accepting)
            .context("Connection is not verified")?;
        ensure!(state.policy.scope.pair_direct, "Pairing is disabled");
        self.store.pending_pairings(connection)
    }

    pub(super) async fn find_webhook(&self, id: &str) -> Option<(Ingress, Webhook)> {
        let slots = self.slots.lock().await;
        slots.values().find_map(|managed| {
            let slot = &managed.slot;
            let state = lock(&slot.state);
            let webhook = state.webhook.as_ref().filter(|webhook| webhook.id == id)?;
            if !state.accepting {
                return None;
            }
            let owner = slot.owner.upgrade()?;
            let ingress = Ingress {
                slot: slot.clone(),
                store: self.store.clone(),
                connection: state.identity.clone()?,
                generation: state.generation.clone()?,
                installation: state.installation.clone(),
                root: owner.library.root().to_owned(),
            };
            Some((ingress, webhook.clone()))
        })
    }

    pub async fn bind_receiver(
        &self,
        root: &Path,
        receiver: &str,
        names: &[String],
    ) -> Result<SharedReceiver> {
        ensure!(
            valid_id(receiver) && !names.is_empty() && names.len() <= 100,
            "Invalid shared receiver"
        );
        let slots = self.slots.lock().await;
        let mut routes = Vec::with_capacity(names.len());
        for name in names {
            let slot = &slots
                .get(name)
                .context("No such messaging connection")?
                .slot;
            let state = lock(&slot.state);
            let generation = state
                .generation
                .clone()
                .filter(|_| state.accepting)
                .context("Receiver connection is not verified")?;
            let installation = state
                .installation
                .clone()
                .filter(|installation| installation.receiver.as_deref() == Some(receiver))
                .context("Connection verification did not bind this receiver")?;
            let key = InstallationKey::from(&installation);
            ensure!(
                !routes.iter().any(|(existing, _)| existing == &key),
                "Ambiguous receiver installation"
            );
            routes.push((
                key,
                Ingress {
                    slot: slot.clone(),
                    store: self.store.clone(),
                    connection: state
                        .identity
                        .clone()
                        .context("Receiver connection is not verified")?,
                    generation,
                    installation: Some(installation),
                    root: root.to_owned(),
                },
            ));
        }
        let identity = store::key("receiver", &receiver)?;
        let lease = account_lease(root, &identity)?.context("Receiver already has an owner")?;
        Ok(SharedReceiver {
            identity,
            routes,
            _lease: lease,
        })
    }

    pub async fn policy(&self, name: &str, policy: AccessPolicy) -> Result<()> {
        let slots = self.slots.lock().await;
        ensure!(!self.has_stock_settings(name)?,
            "Use the owner-managed settings for this messaging connection");
        if let Some(managed) = slots.get(name) {
            let mut state = lock(&managed.slot.state);
            state.revision = self.store.set_connection_policy(name, &policy)?;
            state.policy = policy;
            managed.slot.changed.notify_one();
        } else {
            self.store.set_connection_policy(name, &policy)?;
        }
        Ok(())
    }

    pub async fn stop(&self, name: &str) -> bool {
        let mut slots = self.slots.lock().await;
        let Some(managed) = slots.remove(name) else {
            return false;
        };
        managed.stop.cancel();
        let _ = managed.task.await;
        true
    }

    pub async fn shutdown(&self) {
        let slots = std::mem::take(&mut *self.slots.lock().await);
        for managed in slots.values() {
            managed.stop.cancel();
        }
        for managed in slots.into_values() {
            let _ = managed.task.await;
        }
    }

    pub async fn status(&self, profile: &str) -> Result<serde_json::Value> {
        let connections: Vec<_> = self
            .slots
            .lock()
            .await
            .values()
            .map(|managed| {
                let slot = &managed.slot;
                let state = lock(&slot.state);
                ConnectionStatus {
                    name: slot.name.clone(),
                    platform: slot.descriptor.platform.clone(),
                    identity: state.identity.clone(),
                    health: state.health,
                    gap: state.gap,
                    enabled: true,
                    revision: state.revision as u64,
                }
            })
            .collect();
        let store = self.store.clone();
        let (input, deliveries, settings) = tokio::task::spawn_blocking(move || -> Result<_> {
            Ok((
                store.input_status()?,
                store.delivery_status()?,
                store.connection_settings()?,
            ))
        })
        .await??;
        let mut connections = connections;
        for saved in settings {
            if !connections
                .iter()
                .any(|connection| connection.name == saved.name)
            {
                connections.push(ConnectionStatus {
                    name: saved.name,
                    platform: saved.platform,
                    identity: None,
                    health: Health::Stopped,
                    gap: false,
                    enabled: saved.enabled,
                    revision: saved.revision,
                });
            }
        }
        let verification: BTreeMap<_, _> = lock(&self.factories).iter()
            .filter_map(|(name, factory)| factory.verification_status().map(|status| (name.clone(), status)))
            .collect();
        Ok(serde_json::json!({
            "profile_id":profile,"connections":connections,"input":input,"deliveries":deliveries,
            "verification":verification,
        }))
    }

    pub async fn resolve(
        &self,
        connection: String,
        delivery: String,
        part: u32,
        attempt: u32,
        outcome: DeliveryResolution,
    ) -> Result<bool> {
        let store = self.store.clone();
        Ok(tokio::task::spawn_blocking(move || {
            store.resolve(&connection, &delivery, part, attempt, outcome)
        })
        .await??)
    }

    pub async fn bind_conversation(&self, destination: &str, binding: &str) -> Result<()> {
        let destination = self.store.route(destination)?.context("Unknown messaging destination")?;
        let binding = self.store.route(binding)?.context("Unknown messaging conversation")?;
        let slots = self.slots.lock().await;
        let state = slots.values().find_map(|managed| {
            let state = lock(&managed.slot.state);
            (state.accepting && state.identity.as_deref() == Some(&destination.connection)).then_some(state)
        }).context("Messaging connection is unavailable")?;
        ensure!(
            state.policy.room_policy(&destination.room).is_some()
                && state.policy.room_policy(&binding.room).is_some(),
            "Messaging conversation policy changed"
        );
        let transport = transport_scope(state.installation.as_ref())?;
        self.store.read(|db| {
            ensure!(super::routes::destination_permission(db, &destination.id, now(), &transport)? == Some(true)
                && super::routes::destination_permission(db, &binding.id, now(), &transport)?.unwrap_or(true),
                "Verified messaging destination permission expired or changed");
            Ok(())
        })?;
        self.store.share_conversation(&destination, &binding)
    }

    pub async fn bind_provisioned_destination(
        &self,
        root: &Path,
        connection: &str,
        delivery: &str,
        part: u32,
        attempt: u32,
    ) -> Result<String> {
        ensure!(valid_id(connection) && valid_id(delivery) && attempt != 0, "Invalid provisioned effect identity");
        let slots = self.slots.lock().await;
        let (slot, state) = slots.values().find_map(|managed| {
            let state = lock(&managed.slot.state);
            (state.accepting && state.identity.as_deref() == Some(connection))
                .then_some((&managed.slot, state))
        }).context("Messaging connection is unavailable")?;
        let caps = slot.descriptor.capabilities;
        ensure!(caps.threads && caps.effects.prepare_resources, "Provisioned destinations are unavailable");
        let generation = state.generation.as_deref().context("Unverified messaging connection")?;
        let provisioned = self.store.provisioned_destination(connection, delivery, (part, attempt))?
            .context("No current confirmed destination for this effect attempt")?;
        let scope = Ingress {
            slot: slot.clone(),
            store: self.store.clone(),
            connection: connection.to_owned(),
            generation: generation.to_owned(),
            installation: state.installation.clone(),
            root: root.to_path_buf(),
        };
        ensure!(
            provisioned.scope.allows_installation(scope.installation()),
            "Provisioned destination installation changed"
        );
        let child = &provisioned.room;
        let policy = &state.policy;
        let sender = provisioned.sender.as_ref();
        let child_policy = policy.room_policy(child).context("Provisioned destination is forbidden")?;
        let participant = if child_policy.per_participant {
            Some(sender.context("Provisioned destination requires an authorized participant")?.id.as_str())
        } else {
            None
        };
        for (room, route) in [
            (&provisioned.parent.room, Some(&provisioned.parent)),
            (&provisioned.binding.room, Some(&provisioned.binding)),
            (child, None),
        ] {
            let rules = policy.room_policy(room).context("Provisioned conversation scope is forbidden")?;
            ensure!(
                rules.per_participant == child_policy.per_participant,
                "Provisioned conversation participant scope changed"
            );
            if let Some(sender) = sender {
                ensure!(scope.authorize_sender(policy, room, sender)?, "Provisioning actor is no longer authorized");
            }
            if let Some(route) = route {
                // A resource-scoped session cannot be transferred to an ordinary thread.
                ensure!(
                    store::Route::new(connection, room.clone(), participant)?.id == route.id,
                    "Provisioned conversation route scope changed"
                );
            }
        }
        if let Some(required) = &provisioned.effect.transport {
            ensure!(
                scope.authorize_effect_transport(policy, required, &provisioned.parent.room, sender)?.is_some(),
                "Provisioned destination transport address is no longer authorized"
            );
        }
        let destination = store::Route::new(connection, child.clone(), participant)?;
        self.store.bind_provisioned_destination(
            delivery, (part, attempt), &provisioned, &destination, generation, &scope.transport_scope()?, policy,
        )?;
        slot.changed.notify_waiters();
        Ok(destination.id)
    }

    pub async fn set_wake_target(&self, name: &str, target: super::WakeTarget) -> Result<()> {
        let slots = self.slots.lock().await;
        let slot = &slots.get(name).context("Unknown messaging connection")?.slot;
        let state = lock(&slot.state);
        ensure!(slot.descriptor.capabilities.wake && state.accepting, "Wake connection is unavailable");
        let identity = state.identity.as_deref().context("Unverified wake connection")?;
        self.store.set_wake_target(identity, &target)
    }

    pub fn manual_notification(&self, route: &str, request: &str, text: String) -> Result<String> {
        ensure!(valid_id(route) && valid_id(request), "Invalid messaging destination or request identity");
        let route = self.store.route(route)?.context("Unknown messaging destination")?;
        ensure!(!route.blocked && !route.is_background(), "Messaging destination is unavailable");
        let producer = store::key("manual", &(&route.id, request))?;
        self.store.notification(&route, &producer, text)
    }

    pub fn attributed_result(&self, source: &str, route: &str, request: &str) -> Result<String> {
        ensure!(valid_id(source) && valid_id(route) && valid_id(request), "Invalid messaging result identity");
        self.store.enqueue_attributed_result(source, route, request)
    }

    pub async fn restore_route(&self, route: String) -> Result<bool> {
        let store = self.store.clone();
        Ok(tokio::task::spawn_blocking(move || store.restore_route(&route)).await??)
    }

    pub(crate) fn authorized_route(&self, id: &str) -> Result<bool> {
        Ok(self.store.route(id)?.is_some_and(|route| !route.blocked && !route.is_background()))
    }

    pub(crate) fn notification_text(
        &self,
        route: &str,
        producer: &impl serde::Serialize,
    ) -> Result<Option<String>> {
        self.store
            .notification_text(route, &store::key("notification", producer)?)
    }

    /// Called by the authoritative durable producer before it records completion.
    pub(crate) fn notify_route(
        &self,
        id: &str,
        producer: &impl serde::Serialize,
        text: String,
    ) -> Result<()> {
        if text.trim().is_empty() {
            return Ok(());
        }
        // A route might be deleted or blocked after the producer was configured.
        // Preserve the intent for any known route, with delivery held by routing policy.
        let Some(route) = self.store.route(id)? else {
            return Ok(());
        };
        let id = store::key("notification", producer)?;
        self.store.enqueue(
            &route,
            &DeliveryIntent {
                id,
                room: route.room.clone(),
                reply_anchor: None,
                edit_target: None,
                body: DeliveryBody::Reply(Message::text(text)),
            },
        )
    }
}

fn account_lease(root: &Path, account: &str) -> Result<Option<File>> {
    let install = root
        .parent()
        .filter(|dir| dir.file_name().is_some_and(|name| name == "profiles"))
        .and_then(Path::parent)
        .unwrap_or(root);
    let dir = install.join("gateway-adapter-leases");
    fs::create_dir_all(&dir)?;
    ensure!(
        fs::symlink_metadata(&dir)?.is_dir() && dir.canonicalize()? == dir,
        "Invalid adapter lease directory"
    );
    let lease = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(account))?;
    match lease.try_lock() {
        Ok(()) => Ok(Some(lease)),
        Err(fs::TryLockError::WouldBlock) => Ok(None),
        Err(fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

async fn supervise(
    owner: Weak<Shared>,
    store: Arc<Store>,
    root: std::path::PathBuf,
    slot: Arc<Slot>,
    factory: Arc<dyn AdapterFactory>,
    expected_binding: Option<AccountBinding>,
    credential: Option<String>,
    stop: CancellationToken,
) {
    let mut wait = Duration::from_millis(250);
    while !stop.is_cancelled() && owner.strong_count() != 0 {
        lock(&slot.state).health = Health::Connecting;
        let child = stop.child_token();
        let connect = async {
            match &credential {
                Some(reference) => {
                    let store = store.clone();
                    let reference = reference.clone();
                    let credential =
                        tokio::task::spawn_blocking(move || store.credential(&reference))
                            .await
                            .map_err(|_| ConnectionFailure::Retryable)?
                            .map_err(|_| ConnectionFailure::Credentials)?
                            .ok_or(ConnectionFailure::Credentials)?;
                    factory.connect_enrolled(credential, child.clone()).await
                }
                None => factory.connect(child.clone()).await,
            }
        };
        let verified = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            result = connect => result,
        };
        let checked = verified.and_then(|verified| {
            let proof = verified.adapter.verified_installation();
            if !valid_id(&verified.account)
                || !proof.as_ref().is_none_or(VerifiedInstallation::validate)
            {
                return Err(ConnectionFailure::Credentials);
            }
            let binding = AccountBinding::new(verified.account, proof.as_ref());
            if expected_binding
                .as_ref()
                .is_some_and(|expected| expected != &binding)
            {
                return Err(ConnectionFailure::Credentials);
            }
            let identity = binding
                .key(&slot.descriptor.platform)
                .map_err(|_| ConnectionFailure::Configuration)?;
            Ok((verified.adapter, identity, proof))
        });
        let health = match checked {
            Ok((adapter, identity, installation)) => {
                let lease = account_lease(&root, &identity);
                match lease {
                    Ok(Some(_lease)) => {
                        let Ok(gap) = store
                            .recover_sends(&identity)
                            .and_then(|()| transport_scope(installation.as_ref()))
                            .and_then(|scope| store.retire_foreign_repairs(&identity, &scope))
                            .and_then(|()| store.gap(&identity))
                        else {
                            lock(&slot.state).health = Health::Backoff;
                            tokio::select! {
                                _ = stop.cancelled() => break,
                                _ = tokio::time::sleep(wait) => continue,
                            }
                        };
                        wait = Duration::from_millis(250);
                        let ingress = Ingress {
                            slot: slot.clone(),
                            store: store.clone(),
                            connection: identity.clone(),
                            generation: crate::new_id(),
                            installation: installation.clone(),
                            root: root.clone(),
                        };
                        {
                            let mut state = lock(&slot.state);
                            state.identity = Some(identity);
                            state.installation = installation;
                            state.generation = Some(ingress.generation.clone());
                            state.accepting = true;
                            state.health = if adapter.initial_transport_ready() { Health::Ready } else { Health::Connecting };
                            state.gap = gap;
                        }
                        let result =
                            drive(owner.clone(), ingress.clone(), adapter, child.clone()).await;
                        ingress.revoke();
                        let _ = store.recover_sends(&ingress.connection);
                        failure_health(result)
                    }
                    Ok(None) => Health::AccountInUse,
                    Err(_) => Health::Configuration,
                }
            }
            Err(failure) => failure_health(failure),
        };
        child.cancel();
        lock(&slot.state).health = health;
        if matches!(
            health,
            Health::Configuration | Health::Credentials | Health::Stopped
        ) {
            return;
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(wait) => wait = (wait * 2).min(Duration::from_secs(5)),
        }
    }
    lock(&slot.state).health = Health::Stopped;
}

fn failure_health(failure: ConnectionFailure) -> Health {
    match failure {
        ConnectionFailure::Credentials => Health::Credentials,
        ConnectionFailure::Configuration => Health::Configuration,
        ConnectionFailure::Retryable => Health::Backoff,
        ConnectionFailure::Stopped => Health::Stopped,
    }
}

async fn drive(
    owner: Weak<Shared>,
    scope: Ingress,
    adapter: Arc<dyn Adapter>,
    stop: CancellationToken,
) -> ConnectionFailure {
    let (transport, work) = tokio::join!(
        async {
            let outcome = tokio::select! {
                biased;
                _ = stop.cancelled() => ConnectionFailure::Stopped,
                outcome = adapter.listen(scope.clone(), stop.clone()) => outcome,
            };
            scope.drain();
            stop.cancel();
            outcome
        },
        async {
            let outcome = work_loop(owner, &scope, &adapter, &stop).await;
            scope.drain();
            stop.cancel();
            outcome
        }
    );
    if work.is_err() {
        ConnectionFailure::Retryable
    } else {
        transport
    }
}

async fn work_loop(
    owner: Weak<Shared>,
    scope: &Ingress,
    adapter: &Arc<dyn Adapter>,
    stop: &CancellationToken,
) -> Result<(), GatewayError> {
    let mut tick = tokio::time::interval(POLL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sends = JoinSet::new();
    let mut result = loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break Ok(()),
            Some(sent) = sends.join_next(), if !sends.is_empty() => {
                if let Err(error) = sent.unwrap_or(Err(GatewayError::TemporarilyUnavailable)) { break Err(error); }
            }
            _ = scope.slot.changed.notified() => {}
            _ = tick.tick() => {}
        }
        if lock(&scope.slot.state).health != Health::Ready {
            continue;
        }
        let Some(shared) = owner.upgrade() else {
            break Ok(());
        };
        if let Err(error) = sessions::advance(&shared, scope, adapter.as_ref(), stop).await {
            break if error == GatewayError::StaleConnection {
                Ok(())
            } else {
                Err(error)
            };
        }
        if let Err(error) = deliver(scope, adapter, stop, &mut sends).await {
            break if error == GatewayError::StaleConnection {
                Ok(())
            } else {
                Err(error)
            };
        }
    };
    scope.drain();
    stop.cancel();
    while let Some(sent) = sends.join_next().await {
        result = result.and(sent.unwrap_or(Err(GatewayError::TemporarilyUnavailable)));
    }
    result
}

async fn deliver(
    scope: &Ingress,
    adapter: &Arc<dyn Adapter>,
    stop: &CancellationToken,
    sends: &mut JoinSet<Result<(), GatewayError>>,
) -> Result<(), GatewayError> {
    let stored = scope.clone();
    let ready =
        blocking(move || db(stored.store.ready_deliveries(&stored.connection, now()))).await?;
    for mut pending in ready {
        if stop.is_cancelled() {
            return Ok(());
        }
        if !scope.checked(true, |state| delivery_authorized(scope, state, &pending))? {
            continue;
        }
        if !pending.planned {
            if scope.capabilities().editing && matches!(pending.intent.body, DeliveryBody::Reply(_))
            {
                pending.intent.edit_target = db(scope
                    .store
                    .editable_progress(&pending.route, &pending.intent.id))?;
            }
            let plan = adapter.prepare_with_ingress(&pending.intent, scope).and_then(|mut parts| {
                let protection = scope.checked(true, |state| {
                    scope.delivery_protection(&state.policy, &pending.intent.room, &pending.intent.id)
                }).map_err(|_| PreparationFailure::Rejected)?;
                for part in &mut parts {
                    if store::Route::is_background_id(&pending.route)
                        && matches!(part.effect.operation, super::EffectOperation::Provision { .. })
                    {
                        return Err(PreparationFailure::Unsupported);
                    }
                    if let Some(required) = &protection {
                        if part.effect.protection.as_ref().is_some_and(|value| value != required) {
                            return Err(PreparationFailure::Rejected);
                        }
                        part.effect.protection = Some(required.clone());
                    }
                }
                Ok(parts)
            });
            let refusal = match plan {
                Ok(parts) if valid_plan(scope.capabilities(), &pending.intent, &parts) => {
                    let stored = scope.checked(true, |state| {
                        if !delivery_authorized(scope, state, &pending)? {
                            return Ok(None);
                        }
                        db(scope.store.plan(&scope.connection, &pending.intent.id, &parts)).map(Some)
                    })?;
                    match stored {
                        None => continue,
                        Some(false) => Some("conflicting_operation_identity"),
                        Some(true) => None,
                    }
                }
                Err(PreparationFailure::Unsupported) => Some("unsupported_delivery"),
                Err(PreparationFailure::Rejected) => Some("delivery_rejected"),
                Ok(_) => Some("invalid_delivery_plan"),
            };
            if let Some(reason) = refusal {
                scope.checked(true, |state| {
                    if delivery_authorized(scope, state, &pending)? {
                        db(scope.store.reject(&scope.connection, &pending.intent.id, reason))?;
                    }
                    Ok(())
                })?;
                continue;
            }
        }
        let attempt = scope.checked(true, |state| {
            if !delivery_authorized(scope, state, &pending)? {
                return Ok(None);
            }
            let transport = db(scope.store.next_transport(&scope.connection,&pending.intent.id))?;
            let policy = outbound_policy(scope, state, &pending, transport.as_ref())?;
            db(scope.store.claim(&scope.connection, &scope.generation, &pending, &policy))
        })?;
        let Some(attempt) = attempt else { continue };
        let scope = scope.clone();
        let adapter = adapter.clone();
        let cancel = stop.child_token();
        sends.spawn(async move {
            let authorized = scope.checked(false, |state| {
                if !state.accepting || !delivery_authorized(&scope, state, &pending)? {
                    return Ok(false);
                }
                let policy = outbound_policy(&scope, state, &pending, attempt.payload.effect.transport.as_ref())?;
                db(scope.store.recheck(&scope.connection, &scope.generation, &pending, &attempt, &policy))
            })?;
            if cancel.is_cancelled() || !authorized {
                return scope.checked(false, |_| {
                    db(scope
                        .store
                        .release(&scope.connection, &scope.generation, &attempt))
                });
            }
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => DeliveryResult::Uncertain,
                result = adapter.send(attempt.clone(), scope.clone(), cancel.clone()) => result,
            };
            let stored = scope.clone();
            blocking(move || {
                stored.checked(false, |_| {
                    db(stored.store.record(
                        &stored.connection,
                        Some(&stored.generation),
                        &attempt.delivery_id,
                        (attempt.part, attempt.attempt),
                        result,
                        stored.capabilities().effects.durable_presentation,
                    ))
                })
            })
            .await?;
            scope.notify();
            Ok(())
        });
    }
    Ok(())
}

fn delivery_authorized(
    scope: &Ingress,
    state: &State,
    pending: &PendingDelivery,
) -> Result<bool, GatewayError> {
    if state.health != Health::Ready {
        return Ok(false);
    }
    let Some(policy) = state.policy.room_policy(&pending.intent.room) else {
        return Ok(false);
    };
    let Some(route) = db(scope.store.route(&pending.route))? else {
        return Ok(false);
    };
    if route.blocked || route.connection != scope.connection || route.room != pending.intent.room
        || !db(scope.store.result_released(&scope.connection, &pending.intent.id))?
        || !scope.recheck_quote_delivery(state, &route, &pending.intent.id)?
        || !scope.thread_provision_allowed(state, &pending.intent, pending.sender.as_ref())?
    {
        return Ok(false);
    }
    if route.is_background() {
        return scope.background_delivery_allowed(state, &route, pending);
    }
    if let Some(sender) = &pending.sender {
        let participant = policy.per_participant.then_some(sender.id.as_str());
        let current = db(store::Route::new(
            &scope.connection,
            route.room,
            participant,
        ))?;
        let current = match scope.recheck_resource_delivery(
            &state.policy,
            &pending.intent.id,
            &pending.intent.room,
            sender,
        ) {
            Ok(Some(source)) => db(source.scope_route(current))?,
            Ok(None) => current,
            Err(GatewayError::Forbidden) => return Ok(false),
            Err(error) => return Err(error),
        };
        return Ok(
            scope.authorize_sender(&state.policy, &pending.intent.room, sender)?
                && current.id == route.id
                && scope.thread_reply_allowed(state, &pending.intent, sender)?,
        );
    }
    db(scope.store.allows_notification(&route.id, now(), &scope.transport_scope()?))
}

fn outbound_policy(
    scope: &Ingress,
    state: &State,
    delivery: &PendingDelivery,
    requirement: Option<&EffectTransportRef>,
) -> Result<OutboundPolicy, GatewayError> {
    let room = &delivery.intent.room;
    let policy = state.policy.delivery_policy(room);
    let caps = scope.capabilities().effects;
    let transport = match requirement {
        Some(address) => scope.authorize_effect_transport(&state.policy,address,room,delivery.sender.as_ref())?,
        None => None,
    };
    Ok(OutboundPolicy {
        now: now(),
        installation: scope.installation().cloned(),
        visibility: EffectVisibility {
            notify: policy.notifications,
            mention: policy.mentions,
        },
        protection: policy.protection,
        submitted: caps.submitted && policy.submitted,
        transport,
        subsets: caps.recipient_subsets,
        retransmission: caps.retransmission,
    })
}

fn valid_plan(caps: Capabilities, intent: &DeliveryIntent, plan: &[DeliveryPart]) -> bool {
    let has_attachment_alternative = plan.iter().any(|part| part.effect.attachment_fallback_for.is_some());
    let limit = if has_attachment_alternative {
        super::effects::MAX_ATTACHMENT_PLAN_PARTS
    } else {
        MAX_PARTS
    };
    if plan.is_empty() || plan.len() > limit || intent.room.thread.is_some() && !caps.threads {
        return false;
    }
    plan.iter().enumerate().all(|(index, part)| {
        let first_alternative = index == 1
            && part.effect.attachment_fallback_for == Some(0)
            && part.edit_target == plan[0].edit_target;
        let may_edit = caps.editing && (index == 0 || first_alternative)
            && part.edit_target == intent.edit_target;
        if part.room != intent.room || part.edit_target.is_some() && !may_edit
        {
            return false;
        }
        match (&part.body, &intent.body) {
            (DeliveryBody::Reply(part), DeliveryBody::Reply(original)) => {
                valid_message(part)
                    && (part.attachments.is_empty() || caps.attachments)
                    && part
                        .attachments
                        .iter()
                        .all(|file| original.attachments.contains(file))
            }
            (DeliveryBody::Prompt { .. }, DeliveryBody::Prompt { .. }) => {
                caps.interactive_prompts && plan.len() == 1 && part.body == intent.body
            }
            (DeliveryBody::Progress { .. }, DeliveryBody::Progress { .. }) => {
                caps.progress && plan.len() == 1 && part.body == intent.body
            }
            (DeliveryBody::Retire { .. }, DeliveryBody::Retire { .. }) => {
                caps.effects.durable_presentation && plan.len() == 1 && part.body == intent.body
            }
            _ => false,
        }
    }) && valid_effect_plan(caps, intent, plan)
}

pub fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
