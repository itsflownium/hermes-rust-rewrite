//! Provider-neutral operations and evidence for the durable messaging outbox.
use super::{AttachmentRef, Room};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

// Leave room for the complete 256 KiB reply and repeated code fences.
pub const MAX_ATTACHMENT_PLAN_PARTS: usize = 160;

/// Additional operations an adapter explicitly supports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EffectCapabilities {
    pub prepare_resources: bool,
    pub durable_presentation: bool,
    pub submitted: bool,
    pub recipient_subsets: bool,
    pub retransmission: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Effect {
    pub operation: EffectOperation,
    pub visibility: EffectVisibility,
    /// Provider-issued eligibility registered against the source input.
    pub reply_grant: Option<String>,
    /// An already verified provider address required by this delivery operation.
    pub transport: Option<EffectTransportRef>,
    pub consent_required: bool,
    /// The operation can finish with a local write and has no remote acceptance receipt.
    pub write_only: bool,
    /// Required transport protection. The host also pins the owner policy at preparation.
    pub protection: Option<String>,
    pub recipients: BTreeSet<String>,
    /// Operations sharing this provider acknowledgement scope cannot overlap.
    pub correlation_scope: Option<String>,
    /// Exact provider-visible operation key, when the transport returns it as evidence.
    pub operation_key: Option<String>,
    pub replay: Option<ReplayGuarantee>,
    /// Permit a quiet send in the same room after proof that an edit did not occur.
    pub publish_after_definite_edit_refusal: bool,
    /// An ordered text alternative released only if this earlier part's attachment
    /// was definitively refused before the message was published.
    pub attachment_fallback_for: Option<u32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectOperation {
    #[default]
    Message,
    Provision {
        resource: ProvisionResource,
    },
    Upload {
        artifact: AttachmentRef,
        step: UploadStep,
    },
    Publish {
        using: Vec<EffectDependency>,
    },
    Present {
        marker: String,
        revision: u32,
        phase: PresentationPhase,
    },
    Retire {
        marker: MarkerRef,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionResource {
    Thread,
    /// Create a thread from the exact previously published starter message.
    ThreadFromMessage { starter: u32 },
    Topic,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadStep {
    Single,
    Begin,
    Chunk { begin: u32, index: u32 },
    Finish { begin: u32, chunks: Vec<u32> },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectDependency {
    pub part: u32,
    pub kind: ReceiptKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptKind {
    Message,
    Destination,
    UploadSession,
    UploadChunk,
    File,
    Presentation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresentationPhase {
    Create,
    Publish,
    Revise,
    Finalize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MarkerRef {
    pub delivery: String,
    pub part: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectVisibility {
    pub notify: bool,
    pub mention: bool,
}

/// A current, authenticated provider scope. Domain changes when its deduplication
/// cache is replaced, even if the device and endpoint still have the same names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayScope {
    pub device: String,
    pub endpoint: String,
    pub domain: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplayGuarantee {
    pub key: String,
    pub scope: ReplayScope,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectReceipt {
    pub kind: ReceiptKind,
    pub id: String,
}

/// An immutable, credential-free selector for a verified address binding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectTransportRef {
    pub reference: String,
    pub resource: String,
    pub operation: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectTransportPin {
    pub(super) requirement: EffectTransportRef,
    pub(super) fingerprint: String,
    pub(super) revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedEffect {
    /// This flag is only set when the adapter affirmed an unchanged provider replay scope.
    pub retransmission: bool,
    /// The address proof used by this attempt. It carries no endpoint or secret.
    pub transport: Option<EffectTransportPin>,
    /// Earlier typed effects, all durably confirmed before this claim.
    pub dependencies: Vec<(u32, EffectReceipt)>,
    /// Exact confirmed gateway-owned marker to update or retire.
    pub marker: Option<EffectReceipt>,
    /// Explicit group subset, including the initial audience if one was supplied.
    pub recipients: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecipientReceipt {
    pub recipient: String,
    pub outcome: RecipientOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecipientOutcome {
    Accepted { receipt: String },
    Rejected,
    NotAccepted,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecipientResults {
    /// Provider identity of the common effect, if available.
    pub receipt: Option<String>,
    pub recipients: Vec<RecipientReceipt>,
    /// Present only if the provider guarantees the reported non-acceptances are retryable.
    pub retry_after: Option<Duration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplyGrant {
    pub id: String,
    pub source_event: String,
    pub room: Room,
    pub operation: ReplyOperation,
    pub observed_at_ms: i64,
    pub expires_at_ms: i64,
    pub absolute_expires_at_ms: i64,
    pub slots: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyOperation {
    Publish,
    Edit,
    Either,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConsentUpdate {
    pub event: String,
    pub room: Room,
    pub observed_at_ms: i64,
    pub eligible: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectIdentity {
    /// An exact, authenticated association with the outbound provider key.
    OperationKey(String),
    ProviderId(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectObservation {
    pub event: String,
    pub room: Room,
    pub identity: EffectIdentity,
    pub recipient: Option<String>,
    pub observed_at_ms: i64,
    pub state: ProviderDeliveryState,
    /// Some status callbacks also return the provider's final effect ID.
    pub receipt: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderDeliveryState {
    Accepted,
    Delivered,
    Read,
    Played,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationResult {
    Applied,
    Pending,
    Repeated,
    Conflict,
}

impl EffectOperation {
    pub(super) fn receipt_kind(&self) -> Option<ReceiptKind> {
        match self {
            Self::Message | Self::Publish { .. } => Some(ReceiptKind::Message),
            Self::Provision { .. } => Some(ReceiptKind::Destination),
            Self::Upload { step, .. } => Some(match step {
                UploadStep::Single => ReceiptKind::File,
                UploadStep::Begin => ReceiptKind::UploadSession,
                UploadStep::Chunk { .. } => ReceiptKind::UploadChunk,
                UploadStep::Finish { .. } => ReceiptKind::File,
            }),
            Self::Present { .. } => Some(ReceiptKind::Presentation),
            Self::Retire { .. } => None,
        }
    }

    pub(super) fn dependencies(&self) -> Vec<EffectDependency> {
        let dependency = |part, kind| EffectDependency { part, kind };
        match self {
            Self::Provision { resource: ProvisionResource::ThreadFromMessage { starter } } => {
                vec![dependency(*starter, ReceiptKind::Message)]
            }
            Self::Upload {
                step: UploadStep::Chunk { begin, .. },
                ..
            } => {
                vec![dependency(*begin, ReceiptKind::UploadSession)]
            }
            Self::Upload {
                step: UploadStep::Finish { begin, chunks },
                ..
            } => {
                let mut uses = vec![dependency(*begin, ReceiptKind::UploadSession)];
                uses.extend(
                    chunks
                        .iter()
                        .map(|chunk| dependency(*chunk, ReceiptKind::UploadChunk)),
                );
                uses
            }
            Self::Publish { using } => using.clone(),
            _ => Vec::new(),
        }
    }
}

impl Effect {
    pub(super) fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl EffectIdentity {
    pub(super) fn key(&self) -> (&'static str, &str) {
        match self {
            Self::OperationKey(id) => ("operation", id),
            Self::ProviderId(id) => ("provider", id),
        }
    }
}

use super::{
    Capabilities, DeliveryBody, DeliveryIntent, DeliveryPart, GatewayError, Ingress,
    runtime::{blocking, db, valid_id},
};

fn valid_room(room: &Room) -> bool {
    valid_id(&room.chat) && room.thread.as_deref().is_none_or(valid_id)
}

impl Effect {
    pub(super) fn provider_operation(&self) -> Option<&str> {
        self.operation_key
            .as_deref()
            .or_else(|| self.replay.as_ref().map(|r| r.key.as_str()))
    }
}

impl ReplayScope {
    fn valid(&self) -> bool {
        valid_id(&self.device)
            && valid_id(&self.domain)
            && matches!(
                reqwest::Url::parse(&self.endpoint),
                Ok(url) if url.scheme() == "https" && url.origin().ascii_serialization() == self.endpoint
            )
    }
}

impl Ingress {
    /// The listener calls this only with an authenticated provider grant. The
    /// source event must already have a durable, admitted host receipt.
    pub async fn register_reply_grant(&self, grant: ReplyGrant) -> Result<bool, GatewayError> {
        self.save_reply_grant(grant, false).await
    }

    /// Record later provider evidence that explicitly renews the exact unused grant.
    pub async fn refresh_reply_grant(&self, grant: ReplyGrant) -> Result<bool, GatewayError> {
        self.save_reply_grant(grant, true).await
    }

    async fn save_reply_grant(
        &self,
        grant: ReplyGrant,
        refresh: bool,
    ) -> Result<bool, GatewayError> {
        if !valid_id(&grant.id)
            || !valid_id(&grant.source_event)
            || !valid_room(&grant.room)
            || !(1..=1024).contains(&grant.slots)
            || !valid_observation_time(grant.observed_at_ms)
            || grant.expires_at_ms <= grant.observed_at_ms
            || grant.absolute_expires_at_ms < grant.expires_at_ms
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let saved = blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.reply_grant(&scope.connection, &grant, refresh))
            })
        })
        .await?;
        if saved {
            self.notify();
        }
        Ok(saved)
    }

    /// Project a verified provider opt-in or opt-out separately from Mercury grants.
    pub async fn update_delivery_consent(
        &self,
        update: ConsentUpdate,
    ) -> Result<bool, GatewayError> {
        if !valid_id(&update.event)
            || !valid_room(&update.room)
            || !valid_observation_time(update.observed_at_ms)
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let changed = blocking(move || {
            scope.checked(true, |_| {
                db(scope
                    .store
                    .update_delivery_consent(&scope.connection, &update))
            })
        })
        .await?;
        if changed {
            self.notify();
        }
        Ok(changed)
    }

    /// Publish continuity evidence from the authenticated provider connection.
    /// Clearing the scope fences replay while preserving all unsettled receipts.
    pub async fn set_replay_scope(
        &self,
        evidence: Option<ReplayScope>,
    ) -> Result<(), GatewayError> {
        if let Some(scope) = &evidence {
            if !self.capabilities().effects.retransmission {
                return Err(GatewayError::Unsupported);
            }
            if !scope.valid() {
                return Err(GatewayError::Invalid);
            }
            if self.installation().is_none_or(|verified| {
                verified.device.as_deref() != Some(&scope.device)
                    || verified.origin.as_deref() != Some(&scope.endpoint)
            }) {
                return Err(GatewayError::Forbidden);
            }
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.set_delivery_scope(
                    &scope.connection,
                    &scope.generation,
                    evidence.as_ref(),
                ))
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    /// Record negotiated transport protection for the exact destination.
    pub async fn update_delivery_protection(
        &self,
        room: Room,
        protection: Option<String>,
    ) -> Result<(), GatewayError> {
        if !valid_room(&room) || protection.as_deref().is_some_and(|class| !valid_id(class)) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.set_delivery_protection(
                    &scope.connection,
                    &scope.generation,
                    &room,
                    protection.as_deref(),
                ))
            })
        })
        .await?;
        self.notify();
        Ok(())
    }

    /// The originating listener must verify the callback's author, association,
    /// account and audience before reporting the normalized observation.
    pub async fn observe_delivery(
        &self,
        update: EffectObservation,
    ) -> Result<ObservationResult, GatewayError> {
        if !valid_id(&update.event)
            || !valid_room(&update.room)
            || !valid_id(update.identity.key().1)
            || !valid_observation_time(update.observed_at_ms)
            || update.recipient.as_deref().is_some_and(|id| !valid_id(id))
            || update.receipt.as_deref().is_some_and(|id| !valid_id(id))
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let result = blocking(move || {
            scope.checked(true, |_| {
                db(scope
                    .store
                    .observe_delivery(&scope.connection, &update, super::runtime::now()))
            })
        })
        .await?;
        if result == ObservationResult::Applied {
            self.notify();
        }
        Ok(result)
    }

    /// Retire a confirmed gateway-owned marker without publishing new content.
    pub async fn retire_marker(&self, marker: MarkerRef) -> Result<bool, GatewayError> {
        if !self.capabilities().effects.durable_presentation {
            return Err(GatewayError::Unsupported);
        }
        if !valid_id(&marker.delivery) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        let placed = blocking(move || {
            scope.checked(true, |_| {
                db(scope.store.retire_marker(&scope.connection, &marker))
            })
        })
        .await?;
        if placed {
            self.notify();
        }
        Ok(placed)
    }
}

fn valid_observation_time(ms: i64) -> bool {
    ms > 0 && ms <= super::runtime::now().saturating_add(60_000)
}

/// The transport can render the authorized body, but cannot expand its scope,
/// reference later parts or change the kind of a persisted provider receipt.
pub(super) fn valid_effect_plan(
    caps: Capabilities,
    intent: &DeliveryIntent,
    parts: &[DeliveryPart],
) -> bool {
    let mut operations = BTreeSet::new();
    for (index, part) in parts.iter().enumerate() {
        let effect = &part.effect;
        if effect
            .provider_operation()
            .is_some_and(|id| !operations.insert(id))
            || [
                effect.operation_key.as_deref(),
                effect.reply_grant.as_deref(),
                effect.protection.as_deref(),
                effect.correlation_scope.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|id| !valid_id(id))
            || effect.transport.as_ref().is_some_and(|transport| {
                [
                    &transport.reference,
                    &transport.resource,
                    &transport.operation,
                ]
                .into_iter()
                .any(|id| !valid_id(id))
            })
            || effect.recipients.len() > 256
            || effect.recipients.iter().any(|id| !valid_id(id))
            || effect.replay.as_ref().is_some_and(|replay| {
                !caps.effects.retransmission
                    || !valid_id(&replay.key)
                    || !replay.scope.valid()
                    || effect
                        .operation_key
                        .as_ref()
                        .is_some_and(|key| key != &replay.key)
                    || !effect.recipients.is_empty()
            })
            || effect.publish_after_definite_edit_refusal
                && (part.edit_target.is_none()
                    || effect.transport.is_some()
                    || effect.provider_operation().is_some()
                    || !effect.recipients.is_empty()
                    || !matches!(effect.operation, EffectOperation::Message)
                    || effect.visibility != EffectVisibility::default())
        {
            return false;
        }
        let previous = &parts[..index];
        if effect.operation.dependencies().iter().any(|dependency| {
            previous
                .get(dependency.part as usize)
                .and_then(|p| p.effect.operation.receipt_kind())
                != Some(dependency.kind)
        }) || !valid_operation(caps, intent, part, previous)
            || !valid_attachment_fallback(caps, part, previous)
        {
            return false;
        }
    }
    !matches!(intent.body, DeliveryBody::Reply(_))
        || parts.iter().any(|p| {
            matches!(
                p.effect.operation,
                EffectOperation::Message
                    | EffectOperation::Publish { .. }
                    | EffectOperation::Present {
                        phase: PresentationPhase::Finalize,
                        ..
                    }
            )
        })
}

fn valid_attachment_fallback(
    caps: Capabilities,
    part: &DeliveryPart,
    previous: &[DeliveryPart],
) -> bool {
    let effect = &part.effect;
    let Some(source) = effect.attachment_fallback_for else {
        return true;
    };
    let Some(primary) = previous.get(source as usize) else {
        return false;
    };
    let first = previous.len() - 1 == source as usize;
    let original = &primary.effect;
    caps.attachments
        && matches!(&primary.body, DeliveryBody::Reply(_))
        && matches!(&part.body, DeliveryBody::Reply(message)
            if !message.text.trim().is_empty() && message.attachments.is_empty())
        && primary.room == part.room
        && original.attachment_fallback_for.is_none()
        && matches!(
            &original.operation,
            EffectOperation::Message | EffectOperation::Publish { .. }
        )
        && original.operation == effect.operation
        && effect
            .operation
            .dependencies()
            .iter()
            .all(|dependency| dependency.kind == ReceiptKind::Destination)
        && original.visibility == EffectVisibility::default()
        && effect.visibility == original.visibility
        && effect.protection == original.protection
        && effect.transport == original.transport
        && effect.consent_required == original.consent_required
        && effect.reply_grant == original.reply_grant
        && effect.correlation_scope == original.correlation_scope
        && original.recipients.is_empty()
        && effect.recipients.is_empty()
        && !original.write_only
        && !effect.write_only
        && (part.edit_target.is_none() || first && part.edit_target == primary.edit_target)
        && (first
            || previous
                .last()
                .is_some_and(|part| part.effect.attachment_fallback_for == Some(source)))
}

fn valid_operation(
    caps: Capabilities,
    intent: &DeliveryIntent,
    part: &DeliveryPart,
    previous: &[DeliveryPart],
) -> bool {
    if part.effect.write_only
        && (!caps.effects.submitted
            || !matches!(part.effect.operation, EffectOperation::Message)
            || part.edit_target.is_some()
            || !part.effect.recipients.is_empty()
            || part.effect.replay.is_some()
            || !matches!(part.body, DeliveryBody::Reply(_)))
    {
        return false;
    }
    let publish = matches!(
        part.effect.operation,
        EffectOperation::Message
            | EffectOperation::Publish { .. }
            | EffectOperation::Present {
                phase: PresentationPhase::Publish | PresentationPhase::Finalize,
                ..
            }
    );
    let message_effect = matches!(
        part.effect.operation,
        EffectOperation::Message | EffectOperation::Publish { .. }
    );
    if !message_effect
        && (!part.effect.recipients.is_empty()
            || part.effect.reply_grant.is_some()
            || part.edit_target.is_some())
        || !publish && part.effect.visibility != EffectVisibility::default()
    {
        return false;
    }
    match &part.effect.operation {
        EffectOperation::Message => !matches!(intent.body, DeliveryBody::Retire { .. }),
        EffectOperation::Provision { .. } => {
            caps.threads
                && caps.effects.prepare_resources
                && matches!(intent.body, DeliveryBody::Reply(_))
        }
        EffectOperation::Upload { artifact, step } => {
            caps.effects.prepare_resources
                && caps.attachments
                && matches!(&part.body, DeliveryBody::Reply(message) if message.attachments.contains(artifact))
                && valid_upload(previous, artifact, step)
        }
        EffectOperation::Publish { using } => {
            caps.effects.prepare_resources
                && matches!(part.body, DeliveryBody::Reply(_))
                && !using.is_empty()
                && using.len() <= 128
                && using
                    .iter()
                    .all(|d| matches!(d.kind, ReceiptKind::Destination | ReceiptKind::File))
        }
        EffectOperation::Present {
            marker,
            revision,
            phase,
        } => {
            caps.effects.durable_presentation
                && valid_id(marker)
                && valid_presentation(previous, marker, *revision, *phase)
        }
        EffectOperation::Retire { marker } => {
            caps.effects.durable_presentation
                && previous.is_empty()
                && matches!(&intent.body, DeliveryBody::Retire { marker: target } if target==marker)
        }
    }
}

fn same_upload(parts: &[DeliveryPart], begin: u32, resource: &AttachmentRef) -> bool {
    parts.get(begin as usize).is_some_and(|p| {
        matches!(&p.effect.operation, EffectOperation::Upload { artifact,step:UploadStep::Begin } if artifact==resource)
    })
}

fn valid_upload(previous: &[DeliveryPart], resource: &AttachmentRef, step: &UploadStep) -> bool {
    match step {
        UploadStep::Single | UploadStep::Begin => true,
        UploadStep::Chunk { begin, index } => {
            same_upload(previous, *begin, resource)
                && !previous.iter().any(|p| match &p.effect.operation {
                    EffectOperation::Upload {
                        step:
                            UploadStep::Chunk {
                                begin: old,
                                index: chunk,
                            },
                        ..
                    } => old == begin && chunk == index,
                    EffectOperation::Upload {
                        step: UploadStep::Finish { begin: old, .. },
                        ..
                    } => old == begin,
                    _ => false,
                })
        }
        UploadStep::Finish { begin, chunks } => {
            let uploaded: Vec<u32> = previous.iter().enumerate().filter_map(|(ordinal,p)| {
                matches!(&p.effect.operation, EffectOperation::Upload { step:UploadStep::Chunk { begin:b,.. },.. } if b==begin)
                    .then_some(ordinal as u32)
            }).collect();
            same_upload(previous,*begin,resource) && !chunks.is_empty() && uploaded==*chunks
                && !previous.iter().any(|p| matches!(&p.effect.operation,
                    EffectOperation::Upload { step:UploadStep::Finish { begin:old,.. },.. } if begin==old))
        }
    }
}

fn valid_presentation(
    previous: &[DeliveryPart],
    key: &str,
    revision: u32,
    phase: PresentationPhase,
) -> bool {
    let prior = previous
        .iter()
        .rev()
        .find_map(|p| match &p.effect.operation {
            EffectOperation::Present {
                marker,
                revision,
                phase,
            } if marker == key => Some((*revision, *phase)),
            _ => None,
        });
    use PresentationPhase::*;
    match (prior, phase) {
        (None, Create) => revision == 0,
        (Some((last, Create)), Publish) | (Some((last, Publish | Revise)), Revise | Finalize) => {
            last.checked_add(1) == Some(revision)
        }
        _ => false,
    }
}
