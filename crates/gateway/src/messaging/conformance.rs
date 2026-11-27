use super::{store, test_support::Model, *};
use crate::{RunningGateway, Settings, lock};
use hermes_engine::CancellationToken;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    sync::oneshot,
    time::{Instant, sleep, timeout},
};

#[path = "conformance_stock.rs"]
mod stock;
#[path = "conformance_quotes.rs"]
mod quote_conformance;
#[path = "media_conformance.rs"]
mod media_conformance;
#[path = "protection_conformance.rs"]
mod protection_conformance;
#[path = "effects_conformance.rs"]
mod effects_conformance;

const LIMIT: Duration = Duration::from_secs(12);

#[derive(Clone, Copy)]
enum Transport {
    Poll,
    Push,
}

struct Frame {
    events: Vec<InboundEvent>,
    checkpoint: Option<Advance>,
    answer: oneshot::Sender<Result<IntakeResult, GatewayError>>,
}

struct Sent {
    attempt: SendAttempt,
    evidence: Option<oneshot::Sender<DeliveryResult>>,
}

impl Sent {
    fn finish(self, result: DeliveryResult) {
        self.evidence.expect("manual send").send(result).unwrap();
    }
}

#[derive(Clone)]
struct Fixture {
    shape: Transport,
    account: String,
    frames: async_channel::Receiver<Frame>,
    ingress: async_channel::Sender<Ingress>,
    sent: async_channel::Sender<Sent>,
    capabilities: Capabilities,
    manual: bool,
}

struct Driver {
    fixture: Fixture,
    frames: async_channel::Sender<Frame>,
    ingress: async_channel::Receiver<Ingress>,
    sent: async_channel::Receiver<Sent>,
}

impl Driver {
    fn new(shape: Transport, account: &str, manual: bool) -> Self {
        let (push, frames) = async_channel::bounded(16);
        let (ingress, ready) = async_channel::bounded(16);
        let (sent, attempts) = async_channel::bounded(64);
        let fixture = Fixture {
            shape,
            account: account.into(),
            frames,
            ingress,
            sent,
            manual,
            capabilities: Capabilities {
                threads: true,
                attachments: true,
                interactive_prompts: true,
                ..Capabilities::TEXT
            },
        };
        Self {
            fixture,
            frames: push,
            ingress: ready,
            sent: attempts,
        }
    }

    fn config(&self, policy: AccessPolicy) -> AdapterConfig {
        AdapterConfig {
            name: "fixture".into(),
            factory: Arc::new(self.fixture.clone()),
            policy,
        }
    }

    async fn ready(&self) -> Ingress {
        timeout(LIMIT, self.ingress.recv())
            .await
            .expect("adapter did not connect")
            .unwrap()
    }

    async fn push(&self, event: InboundEvent) -> IntakeReceipt {
        self.frame(vec![event], None)
            .await
            .unwrap()
            .receipts
            .remove(0)
    }

    async fn frame(
        &self,
        events: Vec<InboundEvent>,
        checkpoint: Option<Advance>,
    ) -> Result<IntakeResult, GatewayError> {
        let (answer, response) = oneshot::channel();
        self.frames
            .send(Frame {
                events,
                checkpoint,
                answer,
            })
            .await
            .unwrap();
        timeout(LIMIT, response)
            .await
            .expect("adapter did not acknowledge")
            .unwrap()
    }

    async fn sent(&self) -> Sent {
        timeout(LIMIT, self.sent.recv())
            .await
            .expect("reply did not reach adapter")
            .unwrap()
    }
}

impl AdapterFactory for Fixture {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "fixture".into(),
            version: CONTRACT_VERSION,
            capabilities: self.capabilities,
        }
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            Ok(VerifiedAdapter {
                account: self.account.clone(),
                adapter: Arc::new(self.clone()),
            })
        })
    }
}

impl Adapter for Fixture {
    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            if self.ingress.send(ingress.clone()).await.is_err() {
                return ConnectionFailure::Stopped;
            }
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return ConnectionFailure::Stopped,
                    received = self.frames.recv() => {
                        let Ok(frame) = received else { return ConnectionFailure::Stopped };
                        let result = match self.shape {
                            Transport::Poll => ingress.submit(frame.events, frame.checkpoint).await,
                            Transport::Push if frame.events.len() == 1 && frame.checkpoint.is_none() => {
                                ingress.submit(frame.events, None).await
                            }
                            Transport::Push => Err(GatewayError::Invalid),
                        };
                        let _ = frame.answer.send(result);
                    }
                }
            }
        })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        let bodies = match &intent.body {
            DeliveryBody::Reply(message) if message.text.contains('|') => message
                .text
                .split('|')
                .map(|part| DeliveryBody::Reply(Message::text(part)))
                .collect::<Vec<_>>(),
            body => vec![body.clone()],
        };
        Ok(bodies
            .into_iter()
            .enumerate()
            .map(|(index, body)| DeliveryPart {
                room: intent.room.clone(),
                body,
                effect: Effect::default(),
                edit_target: if index == 0 {
                    intent.edit_target.clone()
                } else {
                    None
                },
            })
            .collect())
    }

    fn send(
        &self,
        attempt: SendAttempt,
        _: Ingress,
        _: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(async move {
            let receipt = format!("{}:{}", attempt.part, attempt.attempt);
            if self.manual {
                let (evidence, decision) = oneshot::channel();
                let _ = self
                    .sent
                    .send(Sent {
                        attempt,
                        evidence: Some(evidence),
                    })
                    .await;
                decision.await.unwrap_or(DeliveryResult::Uncertain)
            } else {
                let _ = self
                    .sent
                    .send(Sent {
                        attempt,
                        evidence: None,
                    })
                    .await;
                DeliveryResult::Confirmed {
                    receipt,
                    editable_message: None,
                }
            }
        })
    }
}

fn policy() -> AccessPolicy {
    AccessPolicy {
        users: ["alice"].into_iter().map(str::to_owned).collect(),
        operators: ["alice"].into_iter().map(str::to_owned).collect(),
        rooms: [(
            "group".into(),
            RoomPolicy {
                mention_required: true,
                per_participant: true,
                tools: false,
                ..RoomPolicy::default()
            },
        )]
        .into_iter()
        .collect(),
        ..Default::default()
    }
}

fn dm(id: &str, chat: &str, text: &str) -> InboundEvent {
    InboundEvent {
        id: id.into(),
        room: Room {
            kind: RoomKind::Direct,
            chat: chat.into(),
            thread: None,
        },
        sender: Sender {
            id: "alice".into(),
            roles: Default::default(),
        },
        reply_anchor: Some(id.into()),
        mentioned: true,
        body: InboundBody::Message(Message::text(text)),
    }
}

fn control(id: &str, chat: &str, action: Control) -> InboundEvent {
    InboundEvent {
        body: InboundBody::Control(action),
        ..dm(id, chat, "")
    }
}

fn advance(previous: Option<&str>, next: &str) -> Advance {
    Advance {
        stream: "inbox".into(),
        previous: previous.map(str::to_owned),
        checkpoint: next.into(),
    }
}

fn confirmed(id: &str) -> DeliveryResult {
    DeliveryResult::Confirmed {
        receipt: id.into(),
        editable_message: None,
    }
}

async fn status(gateway: &RunningGateway, predicate: impl Fn(&Value) -> bool) -> Value {
    let until = Instant::now() + LIMIT;
    loop {
        let value = gateway.messaging_status().await.unwrap();
        if predicate(&value) {
            return value;
        }
        assert!(Instant::now() < until, "missing gateway state: {value}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn input<'a>(state: &'a Value, id: &str) -> Option<&'a Value> {
    state["input"]
        .as_array()?
        .iter()
        .find(|row| row["id"] == id)
}

fn delivery<'a>(state: &'a Value, id: &str, part: u32) -> Option<&'a Value> {
    state["deliveries"]
        .as_array()?
        .iter()
        .find(|row| row["id"] == id && row["part"] == part)
}

async fn applied(gateway: &RunningGateway, id: &str, reason: &str) {
    status(gateway, |state| {
        input(state, id).is_some_and(|row| row["reason"] == reason)
    })
    .await;
}

fn reply(attempt: &SendAttempt) -> &str {
    let DeliveryBody::Reply(message) = &attempt.payload.body else {
        panic!("not a final reply")
    };
    &message.text
}

fn prompt_id(attempt: &SendAttempt) -> &str {
    let DeliveryBody::Prompt { action_id, .. } = &attempt.payload.body else {
        panic!("not an operator prompt")
    };
    action_id
}

#[tokio::test]
async fn polling_checkpoint_deduplicates_and_routes_across_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let poll = Driver::new(Transport::Poll, "poll-account", false);
    let gateway = model
        .gateway(root.path(), vec![poll.config(policy())])
        .await;
    let ingress = poll.ready().await;
    let first = dm("first", "dm", "hello");
    let mut thread = dm("topic", "group", "in the topic");
    thread.room = Room {
        kind: RoomKind::Group,
        chat: "group".into(),
        thread: Some("thread".into()),
    };
    let mut denied = dm("denied", "dm", "private contents");
    denied.sender.id = "intruder".into();

    let got = poll
        .frame(
            vec![first.clone(), first.clone(), thread, denied],
            Some(advance(None, "4")),
        )
        .await
        .unwrap();
    assert_eq!(got.checkpoint.as_deref(), Some("4"));
    assert_eq!(
        ingress.checkpoint("inbox".into()).await.unwrap(),
        Some("4".into())
    );
    assert!(got.receipts[1].repeated);
    assert_eq!(got.receipts[0].route, got.receipts[1].route);
    assert_ne!(got.receipts[0].route, got.receipts[2].route);
    assert_eq!(got.receipts[3].disposition, Disposition::Forbidden);
    assert!(got.receipts[3].route.is_none());
    let first_two = [poll.sent().await, poll.sent().await];
    assert!(
        first_two
            .iter()
            .any(|p| p.attempt.payload.room.thread.as_deref() == Some("thread"))
    );
    assert_eq!(model.requests().len(), 2);
    let before = status(&gateway, |s| {
        input(s, "first").is_some_and(|row| row["state"] == "done")
    })
    .await;
    let first_session = input(&before, "first").unwrap()["session"].clone();
    assert_ne!(first_session, input(&before, "topic").unwrap()["session"]);
    assert!(!before.to_string().contains("private contents"));

    let stale = poll
        .frame(
            vec![dm("stale", "dm", "must not run")],
            Some(advance(None, "99")),
        )
        .await;
    assert_eq!(stale.unwrap_err(), GatewayError::CheckpointChanged);
    assert_eq!(
        ingress.checkpoint("inbox".into()).await.unwrap().as_deref(),
        Some("4")
    );
    gateway.shutdown().await.unwrap();
    assert_eq!(
        ingress.submit(vec![first.clone()], None).await.unwrap_err(),
        GatewayError::StaleConnection
    );

    let restarted = model
        .gateway(root.path(), vec![poll.config(policy())])
        .await;
    let resumed = poll.ready().await;
    assert_eq!(
        resumed.checkpoint("inbox".into()).await.unwrap(),
        Some("4".into())
    );
    let repeat = poll
        .frame(
            vec![
                first,
                dm("first", "dm", "conflict"),
                dm("next", "dm", "follow up"),
            ],
            Some(advance(Some("4"), "6")),
        )
        .await
        .unwrap();
    assert!(repeat.receipts[0].repeated);
    assert_eq!(repeat.receipts[1].disposition, Disposition::Conflict);
    assert_eq!(reply(&poll.sent().await.attempt), "answer follow up");
    let state = status(&restarted, |s| {
        input(s, "next").is_some_and(|row| row["state"] == "done")
    })
    .await;
    assert_eq!(input(&state, "next").unwrap()["session"], first_session);
    assert_eq!(model.requests().len(), 3);
    assert!(input(&state, "stale").is_none());
    assert_eq!(input(&state, "first").unwrap()["conflict"], true);
    assert_eq!(input(&state, "denied").unwrap()["state"], "refused");
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn push_delivery_reconciles_rate_limits_and_uncertainty_in_order() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "push-account", true);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    assert_eq!(
        push.push(dm("split", "one", "split")).await.disposition,
        Disposition::Accepted
    );
    let first = push.sent().await;
    let original = first.attempt.delivery_id.clone();
    assert_eq!(first.attempt.part, 0);
    assert_eq!(reply(&first.attempt), "first");
    first.finish(confirmed("platform-first"));
    let second = push.sent().await;
    assert_eq!(second.attempt.part, 1);
    assert_eq!(second.attempt.delivery_id, original);
    second.finish(DeliveryResult::Retryable {
        after: Duration::from_millis(450),
        scope: RetryScope::Destination,
    });
    status(&gateway, |s| {
        delivery(s, &original, 1).is_some_and(|p| p["error"] == "rate_limited")
    })
    .await;
    let rate_observed = Instant::now();
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    let retry = push.sent().await;
    assert_eq!(retry.attempt.delivery_id, original);
    assert_eq!(retry.attempt.part, 1);
    assert_eq!(retry.attempt.attempt, 2);
    assert!(rate_observed.elapsed() >= Duration::from_millis(370));
    retry.finish(DeliveryResult::Uncertain);
    let state = status(&gateway, |s| {
        delivery(s, &original, 1).is_some_and(|p| p["part_state"] == "uncertain")
    })
    .await;
    let connection = state["connections"][0]["identity"]
        .as_str()
        .unwrap()
        .to_owned();
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    push.push(dm("one-later", "one", "waiting behind receipt"))
        .await;
    push.push(dm("different-room", "two", "independent")).await;
    let other = push.sent().await;
    assert_eq!(other.attempt.payload.room.chat, "two");
    other.finish(DeliveryResult::Rejected);
    assert!(
        !gateway
            .resolve_delivery(
                connection.clone(),
                original.clone(),
                1,
                1,
                DeliveryResolution::Discard
            )
            .await
            .unwrap()
    );
    assert!(
        gateway
            .resolve_delivery(
                connection,
                original.clone(),
                1,
                2,
                DeliveryResolution::Delivered
            )
            .await
            .unwrap()
    );
    let next = push.sent().await;
    assert_eq!(next.attempt.payload.room.chat, "one");
    assert_eq!(reply(&next.attempt), "answer waiting behind receipt");
    next.finish(confirmed("later"));
    status(&gateway, |s| {
        delivery(s, &original, 1).is_some_and(|p| p["part_state"] == "sent")
    })
    .await;
    assert_eq!(model.requests().len(), 3);
    let lost = push
        .push(dm("lost-evidence", "three", "confirmed on platform"))
        .await;
    let sent = push.sent().await;
    let id = sent.attempt.delivery_id.clone();
    sent.finish(DeliveryResult::Uncertain);
    status(&gateway, |s| {
        delivery(s, &id, 0).is_some_and(|p| p["part_state"] == "uncertain")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let restarted = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    let resumed = push.ready().await;
    let [unconfirmed]: [UncertainDelivery; 1] = resumed
        .uncertain_deliveries()
        .await
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(unconfirmed.delivery_id, id);
    assert_eq!(unconfirmed.room.chat, "three");
    let mut forged = unconfirmed.clone();
    forged.attempt += 1;
    assert!(
        !resumed
            .reconcile_delivery(forged, confirmed("forged"))
            .await
            .unwrap()
    );
    assert!(
        resumed
            .reconcile_delivery(unconfirmed.clone(), confirmed("platform-evidence"))
            .await
            .unwrap()
    );
    assert!(
        !resumed
            .reconcile_delivery(unconfirmed, confirmed("repeat"))
            .await
            .unwrap()
    );
    let after = status(&restarted, |s| {
        delivery(s, &id, 0).is_some_and(|p| p["part_state"] == "sent")
    })
    .await;
    assert_eq!(
        delivery(&after, &id, 0).unwrap()["route"],
        lost.route.unwrap()
    );
    assert!(
        timeout(Duration::from_millis(200), push.sent.recv())
            .await
            .is_err()
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn prompt_actions_are_bound_to_actor_route_and_execution() {
    let root = TempDir::new().unwrap();
    hermes_engine::settings::ProfileSettings::update(
        root.path(),
        json!({"approval_mode":"manual"}).as_object().unwrap(),
    )
    .unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "controls", false);
    let mut access = policy();
    access.direct_tools = true;
    access.users.insert("bob".into());
    access.operators.insert("bob".into());
    let gateway = model.gateway(root.path(), vec![push.config(access)]).await;
    push.ready().await;
    push.push(dm("approval", "a", "approval")).await;

    let ask = push.sent().await;
    assert!(matches!(
        ask.attempt.payload.body,
        DeliveryBody::Prompt {
            prompt: Prompt::Approval { .. },
            ..
        }
    ));
    let approval = prompt_id(&ask.attempt).to_owned();
    let forged = control(
        "forged",
        "another-chat",
        Control::Respond {
            action_id: approval.clone(),
            response: Response::Approve,
        },
    );
    push.push(forged).await;
    applied(&gateway, "forged", "stale").await;
    let mut wrong_actor = control(
        "wrong-actor",
        "a",
        Control::Respond {
            action_id: approval.clone(),
            response: Response::Approve,
        },
    );
    wrong_actor.sender.id = "bob".into();
    push.push(wrong_actor).await;
    applied(&gateway, "wrong-actor", "forbidden").await;
    assert_eq!(model.requests().len(), 1);
    let answer = control(
        "approve",
        "a",
        Control::Respond {
            action_id: approval.clone(),
            response: Response::Approve,
        },
    );
    push.push(answer.clone()).await;
    push.push(answer).await;
    let done = push.sent().await;
    assert!(reply(&done.attempt).starts_with("answer"));
    applied(&gateway, "approve", "applied").await;

    push.push(dm("clarify", "a", "clarify")).await;
    let ask = push.sent().await;
    assert!(matches!(&ask.attempt.payload.body,
        DeliveryBody::Prompt { prompt: Prompt::Clarification {choices, ..}, .. } if choices == &["stable", "preview"]));
    push.push(control(
        "old-decision",
        "a",
        Control::Respond {
            action_id: approval,
            response: Response::Deny,
        },
    ))
    .await;
    applied(&gateway, "old-decision", "stale").await;
    push.push(control(
        "answer",
        "a",
        Control::Respond {
            action_id: prompt_id(&ask.attempt).to_owned(),
            response: Response::Clarify("preview".into()),
        },
    ))
    .await;
    assert!(reply(&push.sent().await.attempt).contains("preview"));
    applied(&gateway, "answer", "applied").await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn scoped_account_lease_rejects_second_profile_and_replaced_ingress() {
    let root = TempDir::new().unwrap();
    let profile = root.path().join("profiles").join("other");
    let model = Model::new().await;
    let a = Driver::new(Transport::Push, "same-account", false);
    let first = model.gateway(root.path(), vec![a.config(policy())]).await;
    let ingress = a.ready().await;
    let b = Driver::new(Transport::Poll, "same-account", false);
    let second = model.gateway(&profile, vec![b.config(policy())]).await;
    status(&second, |s| {
        s["connections"][0]["health"] == "account_in_use"
    })
    .await;
    assert!(
        second
            .configure_adapter(AdapterConfig {
                name: "another".into(),
                factory: Arc::new(InvalidVersion),
                policy: policy(),
            })
            .await
            .is_err()
    );
    a.push(dm("shared-id", "chat", "profile one")).await;
    let _ = a.sent().await;
    let before = status(&first, |s| {
        input(s, "shared-id").is_some_and(|row| row["state"] == "done")
    })
    .await;
    a.push(dm("unresolved", "chat", "clarify")).await;
    let source_prompt = a.sent().await;
    let source_action = prompt_id(&source_prompt.attempt).to_owned();
    assert!(first.stop_adapter("fixture").await);
    assert_eq!(
        ingress
            .submit(vec![dm("stale", "chat", "no")], None)
            .await
            .unwrap_err(),
        GatewayError::StaleConnection
    );
    let next = b.ready().await;
    next.report_gap().unwrap();
    status(&second, |s| s["connections"][0]["gap"] == true).await;
    let batch = b
        .frame(
            vec![dm("shared-id", "chat", "profile two")],
            Some(advance(None, "1")),
        )
        .await
        .unwrap();
    assert!(!batch.receipts[0].repeated);
    assert_eq!(reply(&b.sent().await.attempt), "answer profile two");
    b.frame(
        vec![control(
            "cross-profile",
            "chat",
            Control::Respond {
                action_id: source_action,
                response: Response::Clarify("stable".into()),
            },
        )],
        Some(advance(Some("1"), "2")),
    )
    .await
    .unwrap();
    applied(&second, "cross-profile", "stale").await;
    let state = status(&second, |s| {
        input(s, "shared-id").is_some_and(|row| row["state"] == "done")
    })
    .await;
    let first_id = input(&before, "shared-id").unwrap()["session"]
        .as_str()
        .unwrap();
    let second_id = input(&state, "shared-id").unwrap()["session"]
        .as_str()
        .unwrap();
    // The same deterministic session key in two profile databases remains isolated.
    let first_summary = first.shared.library.session(first_id).unwrap();
    let second_summary = second.shared.library.session(second_id).unwrap();
    assert_ne!(first_summary.directory, second_summary.directory);
    let (first_history, _) = hermes_engine::read_conversation(&first_summary.directory).unwrap();
    let (second_history, _) = hermes_engine::read_conversation(&second_summary.directory).unwrap();
    assert_eq!(first_history.turns[0].prompt, "profile one");
    assert_eq!(second_history.turns[0].prompt, "profile two");
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();

    let reconnected = model.gateway(&profile, vec![b.config(policy())]).await;
    b.ready().await;
    status(&reconnected, |s| s["connections"][0]["gap"] == true).await;
    reconnected.shutdown().await.unwrap();
}

struct InvalidVersion;

impl AdapterFactory for InvalidVersion {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "invalid".into(),
            version: CONTRACT_VERSION + 1,
            capabilities: Capabilities::TEXT,
        }
    }
    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        panic!("invalid descriptor must be refused before connect")
    }
}

#[tokio::test]
async fn controls_edit_interrupt_and_redirect_through_native_authority() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "queued-controls", false);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    push.push(dm("hold", "a", "hold")).await;
    let active = active_generation(&gateway, "hold").await;
    push.push(dm("later", "a", "original")).await;
    push.push(control(
        "edit",
        "a",
        Control::ReplaceQueued {
            event_id: "later".into(),
            text: "edited later".into(),
        },
    ))
    .await;
    applied(&gateway, "edit", "applied").await;
    push.push(control(
        "wrong-generation",
        "a",
        Control::Interrupt {
            execution_generation: active + 100,
        },
    ))
    .await;
    applied(&gateway, "wrong-generation", "stale").await;
    push.push(control(
        "redirect",
        "a",
        Control::Redirect {
            execution_generation: active,
            text: "redirected".into(),
        },
    ))
    .await;
    applied(&gateway, "redirect", "applied").await;
    assert_eq!(reply(&push.sent().await.attempt), "answer redirected");
    assert_eq!(reply(&push.sent().await.attempt), "answer edited later");

    push.push(dm("steer-wait", "a", "steer wait")).await;
    let active = active_generation(&gateway, "steer-wait").await;
    wait_for_model(&model, "steer wait").await;
    push.push(control(
        "steer",
        "a",
        Control::Steer {
            execution_generation: active,
            text: "steered".into(),
        },
    ))
    .await;
    applied(&gateway, "steer", "applied").await;
    model.release_waiting_turn();
    assert_eq!(reply(&push.sent().await.attempt), "answer steered");

    push.push(dm("hold-stop", "a", "hold")).await;
    let active = active_generation(&gateway, "hold-stop").await;
    push.push(control(
        "interrupt",
        "a",
        Control::Interrupt {
            execution_generation: active,
        },
    ))
    .await;
    applied(&gateway, "interrupt", "applied").await;
    status(&gateway, |s| {
        input(s, "hold-stop").is_some_and(|row| row["state"] == "done")
    })
    .await;
    gateway.shutdown().await.unwrap();
}

async fn active_generation(gateway: &RunningGateway, event: &str) -> u64 {
    let state = status(gateway, |s| {
        input(s, event).is_some_and(|row| row["session"].is_string())
    })
    .await;
    let id = input(&state, event).unwrap()["session"].as_str().unwrap();
    let session = gateway.shared.session(id).await.unwrap();
    let until = Instant::now() + LIMIT;
    loop {
        {
            let view = lock(&session.view);
            if view.running
                && view.authority.receipts.iter().any(|receipt| {
                    receipt.input_id.as_ref().is_some_and(|id| {
                        let connection = state["connections"][0]["identity"].as_str().unwrap();
                        *id == store::key("channel-input", &(connection, event)).unwrap()
                    }) && view.current_admission.as_ref() == Some(&receipt.id)
                })
            {
                return view.generation;
            }
        }
        assert!(Instant::now() < until, "turn {event} did not start");
        sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_model(model: &Model, text: &str) {
    let start = Instant::now();
    while !model
        .requests()
        .iter()
        .any(|request| request["messages"].as_array().unwrap().last().unwrap()["content"] == text)
    {
        assert!(start.elapsed() < LIMIT, "model did not see {text}");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn revoked_actor_cannot_claim_queued_work_or_receive_a_reply() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "revocation", false);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;

    let route = push
        .push(dm("first", "a", "steer wait"))
        .await
        .route
        .unwrap();
    active_generation(&gateway, "first").await;
    wait_for_model(&model, "steer wait").await;
    push.push(dm("queued", "a", "forbidden after revocation"))
        .await;
    let admitted = status(&gateway, |s| {
        input(s, "queued").is_some_and(|row| row["state"] == "admitted")
    })
    .await;
    let queued = input(&admitted, "queued").unwrap();
    let connection = queued["connection"].as_str().unwrap();
    let session = gateway
        .shared
        .session(queued["session"].as_str().unwrap())
        .await
        .unwrap();
    let key = store::key("channel-input", &(connection, "queued")).unwrap();
    assert_eq!(
        gateway.shared.submission_receipt(&session, &key).unwrap()["status"],
        "queued"
    );
    drop(session);
    gateway
        .set_adapter_policy("fixture", AccessPolicy::default())
        .await
        .unwrap();
    model.release_waiting_turn();

    status(&gateway, |s| {
        input(s, "first").is_some_and(|row| row["state"] == "done")
            && input(s, "queued").is_some_and(|row| row["state"] == "admitted")
            && s["deliveries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|row| row["route"] == route && row["state"] == "pending")
    })
    .await;
    assert!(
        timeout(Duration::from_millis(300), push.sent.recv())
            .await
            .is_err()
    );
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();

    let replacement = Driver::new(Transport::Push, "different-account", false);
    let restarted = model
        .gateway(root.path(), vec![replacement.config(policy())])
        .await;
    replacement.ready().await;
    replacement.push(dm("new", "a", "new identity")).await;
    assert_eq!(
        reply(&replacement.sent().await.attempt),
        "answer new identity"
    );
    assert_eq!(model.requests().len(), 2);
    assert!(
        timeout(Duration::from_millis(300), replacement.sent.recv())
            .await
            .is_err()
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn reopens_durable_intake_queue_claim_and_committed_result() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "restart-stages", false);
    let connection = store::key("connection", &("fixture", "restart-stages")).unwrap();
    let inputs = ["intake", "queued", "claimed", "committed", "sending"].map(|id| dm(id, id, id));
    let routes: Vec<_> = inputs
        .iter()
        .map(|event| store::Route::new(&connection, event.room.clone(), None).unwrap())
        .collect();

    // Record the exact durable boundaries the next gateway owner must reopen.
    // In particular the claimed receipt has no running task to finish it.
    let before = model.gateway(root.path(), vec![]).await;
    let db = store::Store::open(root.path()).unwrap();
    db.intake(
        &connection,
        0,
        inputs
            .iter()
            .cloned()
            .zip(routes.iter().cloned())
            .map(|(event, route)| store::Candidate {
                event,
                decision: Ok(route),
                protection: None,
            })
            .collect(),
        Some(&advance(None, "5")),
    )
    .unwrap()
    .unwrap();
    for index in [1, 2, 3] {
        let event = &inputs[index];
        let route = &routes[index];
        let key = store::key("channel-session", &(&route.id, route.revision)).unwrap();
        let summary = before
            .shared
            .library
            .create_background_session("recovery fixture", &key, |summary| {
                Settings {
                    source: "channel".into(),
                    channel_route: Some(route.id.clone()),
                    creation_key: Some(key.clone()),
                    ..Default::default()
                }
                .save(&summary.directory)
            })
            .unwrap();
        db.bind(route, &summary.id).unwrap();
        let session = before.shared.session(&summary.id).await.unwrap();
        let id = store::key("channel-input", &(&connection, &event.id)).unwrap();
        before
            .shared
            .submit_channel(session.clone(), event.id.clone(), vec![], id.clone())
            .await
            .unwrap();
        db.mark(route, &event.id, "admitted", None).unwrap();
        if index >= 2 {
            let claimed = before
                .shared
                .claim_channel(
                    &session,
                    &id,
                    crate::authority::ChannelExecution::default(),
                )
                .unwrap()
                .unwrap();
            if index == 3 {
                before.shared.spawn_worker(session.clone(), claimed).await;
                timeout(LIMIT, async {
                    while before.shared.submission_receipt(&session, &id).unwrap()["status"]
                        != "terminal"
                    {
                        sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
            }
        }
    }
    let route = &routes[4];
    let intent = DeliveryIntent {
        id: "prior-attempt".into(),
        room: route.room.clone(),
        reply_anchor: None,
        edit_target: None,
        body: DeliveryBody::Reply(Message::text("might have reached the platform")),
    };
    db.enqueue(route, &intent).unwrap();
    db.plan(
        &connection,
        &intent.id,
        &[DeliveryPart {
            room: intent.room.clone(),
            body: intent.body.clone(),
            effect: Effect::default(),
            edit_target: None,
        }],
    )
    .unwrap();
    let pending = db.ready_deliveries(&connection, 1).unwrap().remove(0);
    assert_eq!(
        db.claim(&connection, "crashed-generation", &pending, &store::OutboundPolicy {
            now: 1,
            installation: None,
            visibility: EffectVisibility::default(),
            protection: None,
            submitted: false,
            transport: None,
            subsets: false,
            retransmission: false,
        })
            .unwrap()
            .unwrap()
            .attempt,
        1
    );
    db.mark(route, "sending", "done", None).unwrap();
    before.shutdown().await.unwrap();

    let after = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let now = driver.ready().await;
    assert_eq!(
        now.checkpoint("inbox".into()).await.unwrap().as_deref(),
        Some("5")
    );
    let mut replies = Vec::new();
    for _ in 0..3 {
        replies.push(reply(&driver.sent().await.attempt).to_owned());
    }
    replies.sort();
    assert_eq!(
        replies,
        ["answer committed", "answer intake", "answer queued"]
    );
    assert_eq!(model.requests().len(), 3);
    let result = status(&after, |s| {
        input(s, "claimed").is_some_and(|row| row["state"] == "uncertain")
            && delivery(s, &intent.id, 0).is_some_and(|row| row["part_state"] == "uncertain")
    })
    .await;
    let claimed_session = input(&result, "claimed").unwrap()["session"]
        .as_str()
        .unwrap();
    let native = after.shared.session(claimed_session).await.unwrap();
    let receipt = after
        .shared
        .submission_receipt(
            &native,
            &store::key("channel-input", &(&connection, "claimed")).unwrap(),
        )
        .unwrap();

    driver
        .frame(
            vec![dm("behind", "claimed", "after uncertain turn")],
            Some(advance(Some("5"), "6")),
        )
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(200), driver.sent.recv())
            .await
            .is_err()
    );
    let ticket = after.shared.tickets.issue_native().unwrap();
    let client = crate::client::GatewayClient::default();
    client
        .connect_local_grant(&after.http_url(), &ticket)
        .await
        .unwrap();
    client
        .request(
            "session.resume",
            json!({"session_id":claimed_session,"omit_messages":true}),
        )
        .await
        .unwrap();
    client.request("prompt.resolve_unknown", json!({
        "session_id":claimed_session, "admission_id":receipt["admission_id"], "execution_generation":receipt["execution_generation"],
    })).await.unwrap();
    assert_eq!(
        reply(&driver.sent().await.attempt),
        "answer after uncertain turn"
    );
    assert_eq!(model.requests().len(), 4);
    assert!(
        after
            .resolve_delivery(connection, intent.id, 0, 1, DeliveryResolution::Discard)
            .await
            .unwrap()
    );
    after.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_and_signed_webhook_use_the_same_delivery_path() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "producer", false);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    let route = push
        .push(dm("route", "destination", "establish route"))
        .await
        .route
        .unwrap();
    assert_eq!(reply(&push.sent().await.attempt), "answer establish route");
    let destination = format!("channel:{route}");
    let job = gateway
        .shared
        .scheduler
        .create(
            hermes_engine::cron::Draft {
                name: "channel routine".into(),
                prompt: "scheduled result".into(),
                schedule: "every 1h".into(),
                deliver: destination.clone(),
                ..Default::default()
            },
            chrono::Utc::now(),
        )
        .unwrap();
    gateway
        .shared
        .scheduler
        .claim_gateway_now(&job.id, "adapter-contract-job", chrono::Utc::now())
        .unwrap();
    gateway.shared.scheduler_notify.notify_one();
    let scheduled = push.sent().await;
    assert_eq!(scheduled.attempt.payload.room.chat, "destination");
    assert_eq!(reply(&scheduled.attempt), "answer scheduled result");

    let hook = gateway
        .shared
        .library
        .create_webhook(hermes_library::webhooks::Webhook {
            name: "adapter-webhook".into(),
            prompt: "webhook notification".into(),
            deliver: destination,
            ..Default::default()
        })
        .unwrap();
    let body = r#"{"event":"fixture"}"#;
    let digest = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, hook.secret.as_bytes()),
        body.as_bytes(),
    );
    let signature: String = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let client = reqwest::Client::new();
    let send = || {
        client
            .post(format!("{}/webhooks/{}", gateway.http_url(), hook.name))
            .header("x-hermes-signature", &signature)
            .header("x-hermes-delivery-id", "webhook-once")
            .body(body)
            .send()
    };
    assert_eq!(send().await.unwrap().status(), 202);
    let webhook = push.sent().await;
    assert_eq!(webhook.attempt.payload.room.chat, "destination");
    assert!(reply(&webhook.attempt).starts_with("answer webhook notification"));
    assert_ne!(webhook.attempt.delivery_id, scheduled.attempt.delivery_id);
    assert_eq!(send().await.unwrap().status(), 202);
    assert!(
        timeout(Duration::from_millis(300), push.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_recovers_a_committed_script_notification_without_rerunning_the_script() {
    use hermes_engine::cron::{Draft, RunStatus};
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "script-recovery", false);
    let mut gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    let route = push
        .push(dm("route", "destination", "create route"))
        .await
        .route
        .unwrap();
    push.sent().await;

    // Reopen after the outbox commit and before the scheduler recorded completion.
    let scheduler = gateway.scheduler_task.take().unwrap();
    scheduler.abort();
    let _ = scheduler.await;
    assert!(gateway.stop_adapter("fixture").await);
    let store = &gateway.shared.scheduler;
    let job = store
        .create(
            Draft {
                script: "absent.py".into(),
                no_agent: true,
                paused: true,
                deliver: format!("channel:{route}"),
                ..Draft::default()
            },
            chrono::Utc::now(),
        )
        .unwrap();
    let claim = store
        .claim_gateway_now(&job.id, "one-script-run", chrono::Utc::now())
        .unwrap();
    store.begin(&claim, chrono::Utc::now()).unwrap();
    gateway
        .shared
        .messaging
        .notify_route(
            &route,
            &("cron", &claim.run.id),
            "saved script output".into(),
        )
        .unwrap();
    gateway.shutdown().await.unwrap();

    let restarted = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    assert_eq!(reply(&push.sent().await.attempt), "saved script output");
    timeout(LIMIT, async {
        loop {
            let run = restarted.shared.scheduler.runs(&job.id).unwrap().remove(0);
            if !run.status.live() {
                assert_eq!(run.status, RunStatus::Completed);
                assert_eq!(run.output, "saved script output");
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(model.requests().len(), 1);
    assert!(
        timeout(Duration::from_millis(200), push.sent.recv())
            .await
            .is_err()
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn progress_edits_only_its_confirmed_platform_message() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let mut push = Driver::new(Transport::Push, "progress", true);
    push.fixture.capabilities.progress = true;
    push.fixture.capabilities.editing = true;
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    push.push(dm("input", "destination", "steer wait")).await;
    let progress = push.sent().await;
    let DeliveryBody::Progress { reply_id, .. } = &progress.attempt.payload.body else {
        panic!("expected progress")
    };
    let reply_id = reply_id.clone();
    let progress_id = progress.attempt.delivery_id.clone();
    assert!(progress.attempt.payload.edit_target.is_none());
    progress.finish(DeliveryResult::Confirmed {
        receipt: "progress-receipt".into(),
        editable_message: Some("platform-message".into()),
    });
    status(&gateway, |s| {
        delivery(s, &progress_id, 0).is_some_and(|p| p["part_state"] == "sent")
    })
    .await;
    model.release_waiting_turn();
    let response = push.sent().await;
    assert_eq!(response.attempt.delivery_id, reply_id);
    assert_eq!(
        response.attempt.payload.edit_target.as_deref(),
        Some("platform-message")
    );
    assert_eq!(reply(&response.attempt), "Working");
    response.finish(confirmed("edit-receipt"));
    status(&gateway, |s| {
        delivery(s, &reply_id, 0).is_some_and(|p| p["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_mapped_session_waits_for_owner_to_restore_its_route() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "restore", false);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    push.ready().await;
    let route = push
        .push(dm("original", "destination", "original session"))
        .await
        .route
        .unwrap();
    assert_eq!(reply(&push.sent().await.attempt), "answer original session");
    let before = status(&gateway, |s| {
        input(s, "original").is_some_and(|row| row["state"] == "done")
    })
    .await;
    let previous = input(&before, "original").unwrap()["session"]
        .as_str()
        .unwrap();
    let ticket = gateway.shared.tickets.issue_native().unwrap();
    let owner = crate::client::GatewayClient::default();
    owner
        .connect_local_grant(&gateway.http_url(), &ticket)
        .await
        .unwrap();
    owner
        .request(
            "catalog.session.set",
            json!({ "session_id":previous, "archived":true }),
        )
        .await
        .unwrap();
    owner
        .request("catalog.session.delete", json!({ "session_id":previous }))
        .await
        .unwrap();

    push.push(dm("blocked", "destination", "pending until restoration"))
        .await;
    let blocked = status(&gateway, |s| {
        input(s, "blocked").is_some_and(|row| row["state"] == "blocked")
    })
    .await;
    assert_eq!(input(&blocked, "blocked").unwrap()["session"], previous);
    assert_eq!(
        input(&blocked, "blocked").unwrap()["reason"],
        "route_unavailable"
    );
    assert_eq!(model.requests().len(), 1);
    assert_eq!(
        owner
            .request("messaging.restore_route", json!({"route":route}))
            .await
            .unwrap()["restored"],
        true
    );
    assert_eq!(
        reply(&push.sent().await.attempt),
        "answer pending until restoration"
    );
    let restored = status(&gateway, |s| {
        input(s, "blocked").is_some_and(|row| row["state"] == "done")
    })
    .await;
    assert_ne!(input(&restored, "blocked").unwrap()["session"], previous);
    assert_eq!(input(&restored, "blocked").unwrap()["route"], route);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn attachment_and_reply_artifact_stay_with_the_verified_account() {
    let root = TempDir::new().unwrap();
    hermes_engine::settings::ProfileSettings::update(
        root.path(),
        json!({"approval_mode":"off"}).as_object().unwrap(),
    )
    .unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "media", true);
    let mut access = policy();
    access.direct_tools = true;
    let gateway = model.gateway(root.path(), vec![push.config(access)]).await;
    let ingress = push.ready().await;
    let metadata = dm("attachment", "destination", "");
    let grant = ingress.authorize_media(
        MediaSource::from(&metadata),
        AttachmentManifest { name: "note.txt".into(), size: Some(11), sha256: None },
        MediaPurpose::Attachment,
        Duration::from_secs(10),
    ).await.unwrap();
    let attachment = ingress
        .stage_attachment(grant, || async { Ok(&b"staged text"[..]) })
        .await
        .unwrap();
    let input = InboundEvent {
        body: InboundBody::Message(Message {
            text: "read this".into(),
            attachments: vec![attachment],
        }),
        ..dm("attachment", "destination", "")
    };
    assert_eq!(push.push(input).await.disposition, Disposition::Accepted);
    let received = push.sent().await;
    assert_eq!(reply(&received.attempt), "answer attachment");
    received.finish(confirmed("attachment-reply"));
    assert!(model.requests()[0].to_string().contains("staged text"));
    push.push(dm("artifact", "destination", "write artifact"))
        .await;
    let artifact = push.sent().await;
    let DeliveryBody::Reply(message) = &artifact.attempt.payload.body else {
        panic!("expected an artifact reply")
    };
    assert_eq!(message.text, "saved");
    let file = &message.attachments[0];
    assert_eq!(file.name(), "result.txt");
    assert_eq!(
        ingress
            .reply_artifact(&artifact.attempt, file)
            .await
            .unwrap(),
        b"scoped artifact"
    );
    let mut forged = artifact.attempt.clone();
    forged.attempt += 1;
    assert_eq!(
        ingress.reply_artifact(&forged, file).await.unwrap_err(),
        GatewayError::Forbidden
    );
    artifact.finish(confirmed("artifact-reply"));
    gateway.shutdown().await.unwrap();
}
