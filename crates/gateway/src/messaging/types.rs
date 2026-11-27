use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

use super::{
    Effect, EffectCapabilities, EffectReceipt, EnrollmentCredential, EnrollmentLease, EnrollmentPrompt,
    Ingress, MarkerRef, RecipientResults, ResolvedEffect, ScopePolicy, VerifiedInstallation,
};

/// Major version of the native gateway adapter interface.
pub const CONTRACT_VERSION: u32 = 1;

pub type AdapterFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Live feedback attached to an execution. The turn token is cancelled on
/// settlement. No feedback is stored or replayed as a delivery effect.
pub type FirstToolFeedback = Arc<dyn Fn(CancellationToken) + Send + Sync>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Capabilities {
    pub text_intake: bool,
    pub text_delivery: bool,
    pub attachments: bool,
    pub threads: bool,
    pub interactive_prompts: bool,
    pub progress: bool,
    pub editing: bool,
    pub wake: bool,
    pub effects: EffectCapabilities,
}

impl Capabilities {
    pub const TEXT: Self = Self {
        text_intake: true,
        text_delivery: true,
        attachments: false,
        threads: false,
        interactive_prompts: false,
        progress: false,
        editing: false,
        wake: false,
        effects: EffectCapabilities {
            prepare_resources: false,
            durable_presentation: false,
            submitted: false,
            recipient_subsets: false,
            retransmission: false,
        },
    };
}

pub struct Descriptor {
    pub platform: String,
    pub version: u32,
    pub capabilities: Capabilities,
}

pub struct AdapterConfig {
    pub name: String,
    pub factory: Arc<dyn AdapterFactory>,
    pub policy: AccessPolicy,
}

pub trait AdapterFactory: Send + Sync {
    fn descriptor(&self) -> Descriptor;

    /// Owner-facing verification diagnostics. Only include public account
    /// identifiers and fixed status values.
    fn verification_status(&self) -> Option<serde_json::Value> { None }

    /// Authenticate the configured account and return its stable platform ID.
    fn connect(
        &self,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>>;

    /// Request the provider challenge for this private setup lease, if needed.
    /// Factories tag any provider-side state with the supplied lease identity.
    fn enrollment_prompt<'a>(
        &'a self,
        _lease: &'a EnrollmentLease,
        _cancel: CancellationToken,
    ) -> AdapterFuture<'a, Result<Option<EnrollmentPrompt>, ConnectionFailure>> {
        Box::pin(std::future::ready(Ok(None)))
    }

    /// Authenticate with a credential approved by the private profile owner.
    /// Factories offering interactive enrollment implement this operation.
    fn connect_enrolled(
        &self,
        _credential: EnrollmentCredential,
        _cancel: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(std::future::ready(Err(ConnectionFailure::Configuration)))
    }
}

pub struct VerifiedAdapter {
    pub account: String,
    pub adapter: Arc<dyn Adapter>,
}

pub trait Adapter: Send + Sync {
    /// Some transports must verify an additional authenticated session before work can start.
    fn initial_transport_ready(&self) -> bool {
        true
    }

    /// Installation scope verified as part of this authenticated connection.
    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        None
    }

    /// Run intake until cancellation or disconnection. Ingress only accepts
    /// events from this verified connection generation.
    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure>;

    /// Optionally bind live, best-effort feedback to this input before execution.
    /// The returned callback runs once on the first tool call and must return promptly.
    fn first_tool_feedback(
        &self,
        _ingress: Ingress,
        _event: &InboundEvent,
    ) -> Option<FirstToolFeedback> {
        None
    }

    /// Render the committed intent into bounded platform operations without network effects.
    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure>;

    /// Render with read access to this connection's committed delivery provenance.
    fn prepare_with_ingress(
        &self,
        intent: &DeliveryIntent,
        _ingress: &Ingress,
    ) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        self.prepare(intent)
    }

    /// Execute a single platform operation. The gateway records its attempt before calling.
    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionFailure {
    Retryable,
    Credentials,
    Configuration,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparationFailure {
    Unsupported,
    Rejected,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessPolicy {
    pub users: BTreeSet<String>,
    pub roles: BTreeSet<String>,
    pub operators: BTreeSet<String>,
    pub operator_roles: BTreeSet<String>,
    pub rooms: BTreeMap<String, RoomPolicy>,
    pub direct_tools: bool,
    #[serde(default)]
    pub direct_spoken_replies: bool,
    #[serde(default)]
    pub scope: ScopePolicy,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoomPolicy {
    pub mention_required: bool,
    pub per_participant: bool,
    pub tools: bool,
    #[serde(default)]
    pub spoken_addressing: Vec<String>,
    #[serde(default)]
    pub spoken_replies: bool,
}

impl AccessPolicy {
    pub(super) fn allows(&self, sender: &Sender) -> bool {
        self.users.contains(&sender.id) || !self.roles.is_disjoint(&sender.roles)
    }

    pub(super) fn operator(&self, sender: &Sender) -> bool {
        self.operators.contains(&sender.id) || !self.operator_roles.is_disjoint(&sender.roles)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Sender {
    pub id: String,
    #[serde(default)]
    pub roles: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoomKind {
    Direct,
    Group,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Room {
    pub kind: RoomKind,
    pub chat: String,
    pub thread: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub(super) id: String,
    pub(super) name: String,
}

impl AttachmentRef {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

impl Message {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            attachments: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InboundEvent {
    pub id: String,
    pub room: Room,
    pub sender: Sender,
    pub reply_anchor: Option<String>,
    pub mentioned: bool,
    pub body: InboundBody,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InboundBody {
    Message(Message),
    Control(Control),
    Command(super::Command),
    Wake,
}

impl InboundBody {
    pub(super) fn is_control(&self) -> bool {
        match self {
            Self::Control(_) => true,
            Self::Command(super::Command::UseSkill { .. } | super::Command::Background { .. }) => false,
            Self::Command(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    Respond {
        action_id: String,
        response: Response,
    },
    ReplaceQueued {
        event_id: String,
        text: String,
    },
    Interrupt {
        execution_generation: u64,
    },
    Steer {
        execution_generation: u64,
        text: String,
    },
    Redirect {
        execution_generation: u64,
        text: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    Approve,
    ApproveForSession,
    ApprovePersistently,
    Deny,
    Clarify(String),
}

/// Checkpoints are opaque to the gateway. Each advance compares the exact last commit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Advance {
    pub stream: String,
    pub previous: Option<String>,
    pub checkpoint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IntakeReceipt {
    pub id: String,
    pub route: Option<String>,
    pub disposition: Disposition,
    pub repeated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Accepted,
    Deferred,
    ProtectionUnavailable,
    Expired,
    Coalesced,
    Historical,
    Forbidden,
    Unsupported,
    Invalid,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntakeResult {
    pub receipts: Vec<IntakeReceipt>,
    pub checkpoint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryIntent {
    pub id: String,
    pub room: Room,
    pub reply_anchor: Option<String>,
    /// A platform message from this reply's confirmed progress operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit_target: Option<String>,
    pub body: DeliveryBody,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryBody {
    Reply(Message),
    Prompt { action_id: String, prompt: Prompt },
    Progress { text: String, reply_id: String },
    Retire { marker: MarkerRef },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Prompt {
    Approval {
        text: String,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        allow_session: bool,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        allow_persistent: bool,
    },
    Clarification {
        question: String,
        choices: Vec<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeliveryPart {
    pub room: Room,
    pub body: DeliveryBody,
    #[serde(default, skip_serializing_if = "Effect::is_default")]
    pub effect: Effect,
    /// The first reply part may replace the editable message provided by the intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edit_target: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SendAttempt {
    pub delivery_id: String,
    pub part: u32,
    pub attempt: u32,
    pub reply_anchor: Option<String>,
    pub payload: DeliveryPart,
    pub effect: ResolvedEffect,
}

/// A previously attempted operation whose platform outcome is unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UncertainDelivery {
    pub delivery_id: String,
    pub part: u32,
    pub attempt: u32,
    pub room: Room,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryScope {
    /// Defer this exact outbox part. The adapter can enforce its own
    /// provider bucket across requests before dispatch.
    Part,
    Destination,
    Account,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryBlock {
    ReplyWindowClosed,
}

/// Only use Retryable when the platform definitively did not accept the request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryResult {
    Confirmed {
        receipt: String,
        /// An editable platform message ID, when the operation created one.
        editable_message: Option<String>,
    },
    Retryable {
        after: Duration,
        scope: RetryScope,
    },
    EffectConfirmed(EffectReceipt),
    Recipients(RecipientResults),
    /// A complete write on a negotiated write-only transport.
    Submitted,
    /// Evidence that the requested edit was definitively not published.
    EditNotPublished,
    /// The attachment was definitively refused without publishing the message.
    /// Only a text alternative in the stored plan can follow this result.
    AttachmentNotPublished,
    /// A definite policy or provider eligibility refusal before publication.
    Blocked(DeliveryBlock),
    Rejected,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryResolution {
    Delivered,
    Discard,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Health {
    Connecting,
    Ready,
    Backoff,
    AccountInUse,
    Credentials,
    Configuration,
    Stopped,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConnectionStatus {
    pub name: String,
    pub platform: String,
    pub identity: Option<String>,
    pub health: Health,
    pub gap: bool,
    pub enabled: bool,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayError {
    Invalid,
    Unsupported,
    Forbidden,
    StaleConnection,
    CheckpointChanged,
    Conflict,
    ProtectionUnavailable,
    TemporarilyUnavailable,
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Invalid => "Invalid adapter input",
            Self::Unsupported => "Adapter capability unavailable",
            Self::Forbidden => "Adapter action forbidden",
            Self::StaleConnection => "Adapter connection was replaced",
            Self::CheckpointChanged => "Adapter checkpoint changed",
            Self::Conflict => "Adapter input identity was reused with different data",
            Self::ProtectionUnavailable => "Required transport protection or audience trust is unavailable",
            Self::TemporarilyUnavailable => "Gateway messaging store unavailable",
        })
    }
}

impl std::error::Error for GatewayError {}
