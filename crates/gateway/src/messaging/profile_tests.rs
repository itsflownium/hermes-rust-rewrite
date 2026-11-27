use super::*;
use crate::{Gateway, GatewayConfig, RunningGateway};
use std::{path::Path, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Fixture {
    name: String,
    proof: VerifiedInstallation,
    account: String,
    credential_required: bool,
    ready: async_channel::Sender<Ingress>,
}

impl AdapterFactory for Fixture {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "proof-fixture".into(),
            version: CONTRACT_VERSION,
            capabilities: Capabilities {
                attachments: true,
                ..Capabilities::TEXT
            },
        }
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            if self.credential_required {
                Err(ConnectionFailure::Credentials)
            } else {
                Ok(VerifiedAdapter {
                    account: self.account.clone(),
                    adapter: Arc::new(self.clone()),
                })
            }
        })
    }

    fn enrollment_prompt<'a>(
        &'a self,
        lease: &'a EnrollmentLease,
        _: CancellationToken,
    ) -> AdapterFuture<'a, Result<Option<EnrollmentPrompt>, ConnectionFailure>> {
        Box::pin(async {
            Ok(self.credential_required.then(|| EnrollmentPrompt::Qr {
                payload: format!("fixture:{}", lease.id),
            }))
        })
    }

    fn connect_enrolled(
        &self,
        credential: EnrollmentCredential,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async move {
            let account = match credential.secret() {
                "owner-secret" | "renewed-secret" => self.account.clone(),
                "other-secret" => "different-account".into(),
                _ => return Err(ConnectionFailure::Credentials),
            };
            Ok(VerifiedAdapter {
                account,
                adapter: Arc::new(self.clone()),
            })
        })
    }
}

impl Adapter for Fixture {
    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(self.proof.clone())
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            if self.ready.send(ingress).await.is_err() {
                return ConnectionFailure::Stopped;
            }
            cancel.cancelled().await;
            ConnectionFailure::Stopped
        })
    }

    fn prepare(&self, _: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        Err(PreparationFailure::Unsupported)
    }

    fn send(
        &self,
        _: SendAttempt,
        _: Ingress,
        _: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(std::future::ready(DeliveryResult::Rejected))
    }
}

fn fixture(name: &str, tenant: &str) -> (Fixture, async_channel::Receiver<Ingress>) {
    let (ready, receive) = async_channel::unbounded();
    (
        Fixture {
            name: name.into(),
            account: "provider-account".into(),
            credential_required: false,
            proof: VerifiedInstallation {
                application: Some(name.into()),
                tenant: Some(tenant.into()),
                installation: "provider-installation".into(),
                receiver: Some("public-receiver".into()),
                origin: Some("https://provider.example".into()),
                device: Some("device-one".into()),
                local_client: None,
            },
            ready,
        },
        receive,
    )
}

async fn open(root: &Path, factories: &[Fixture], policy: &AccessPolicy) -> RunningGateway {
    let mut config = GatewayConfig::new(
        root,
        hermes_engine::Config {
            base_url: "http://127.0.0.1:1/v1".into(),
            model: "fixture".into(),
            api_key: String::new(),
        },
    );
    config.adapters = factories
        .iter()
        .map(|factory| AdapterConfig {
            name: factory.name.clone(),
            factory: Arc::new(factory.clone()),
            policy: policy.clone(),
        })
        .collect();
    Gateway::start(config).await.unwrap()
}

async fn ready(receiver: &async_channel::Receiver<Ingress>) -> Ingress {
    timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

fn principal(id: &str) -> Sender {
    Sender {
        id: id.into(),
        roles: Default::default(),
    }
}

fn room(chat: &str) -> Room {
    Room {
        kind: RoomKind::Direct,
        chat: chat.into(),
        thread: None,
    }
}

fn message(id: &str, sender: Sender, room: Room) -> InboundEvent {
    InboundEvent {
        id: id.into(),
        room,
        sender,
        reply_anchor: None,
        mentioned: true,
        body: InboundBody::Message(Message::text("fixture")),
    }
}

fn person(actor: &str, room: Room) -> ActorEvidence {
    ActorEvidence {
        actor: actor.into(),
        kind: ActorKind::Person,
        revision: 1,
        expires_at: super::profile::time_ms() + 60_000,
        aliases: Vec::new(),
        memberships: vec![MemberEvidence {
            room,
            roles: Default::default(),
        }],
    }
}

fn owner_policy() -> AccessPolicy {
    let mut result = AccessPolicy {
        users: ["canonical".into()].into(),
        ..Default::default()
    };
    result.scope.require_actor_evidence = true;
    result
}

#[tokio::test]
async fn owner_enrollment_expires_rotates_and_survives_restart() {
    let temp = TempDir::new().unwrap();
    let (mut factory, queue) = fixture("enrolled", "tenant");
    factory.credential_required = true;
    let mut policy = owner_policy();
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let expired = gateway
        .shared
        .messaging
        .begin_enrollment(&factory.name, 1)
        .await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let secret = EnrollmentCredential::new("owner-secret".into()).unwrap();
    match expired {
        Ok(expired) => assert!(
            gateway
                .complete_adapter_enrollment(&factory.name, &expired.id, secret.clone())
                .await
                .is_err()
        ),
        Err(expired) => assert!(expired.to_string().contains("expired")),
    }

    let lease = gateway
        .begin_adapter_enrollment(&factory.name, Duration::from_secs(30))
        .await
        .unwrap();
    assert!(
        matches!(&lease.prompt, Some(EnrollmentPrompt::Qr { payload }) if payload == &format!("fixture:{}", lease.id))
    );
    assert!(
        gateway
            .complete_adapter_enrollment(
                &factory.name,
                &lease.id,
                EnrollmentCredential::new("wrong".into()).unwrap(),
            )
            .await
            .is_err()
    );
    gateway
        .complete_adapter_enrollment(&factory.name, &lease.id, secret.clone())
        .await
        .unwrap();
    let first = ready(&queue).await;
    let before = gateway.adapter_settings().await.unwrap().remove(0);
    assert!(before.credential_ref.is_some());
    assert!(
        !serde_json::to_string(&before)
            .unwrap()
            .contains(secret.secret())
    );
    let stale_owner_lease = gateway
        .begin_adapter_enrollment(&factory.name, Duration::from_secs(30))
        .await
        .unwrap();
    let revision = first.configuration_revision().unwrap();
    let automatic = first
        .renew_enrolled_credential(
            revision,
            &factory.proof,
            EnrollmentCredential::new("renewed-secret".into()).unwrap(),
        )
        .await
        .unwrap();
    assert!(automatic > revision);
    assert_eq!(
        gateway.adapter_settings().await.unwrap()[0].credential_ref,
        before.credential_ref
    );
    assert_eq!(
        first
            .renew_enrolled_credential(revision, &factory.proof, secret.clone(),)
            .await
            .unwrap_err(),
        GatewayError::StaleConnection
    );
    assert!(
        gateway
            .complete_adapter_enrollment(&factory.name, &stale_owner_lease.id, secret.clone(),)
            .await
            .is_err()
    );

    let renewed = gateway
        .begin_adapter_enrollment(&factory.name, Duration::from_secs(30))
        .await
        .unwrap();
    gateway
        .complete_adapter_enrollment(
            &factory.name,
            &renewed.id,
            EnrollmentCredential::new("renewed-secret".into()).unwrap(),
        )
        .await
        .unwrap();
    let second = ready(&queue).await;
    assert_eq!(first.connection, second.connection);
    assert_ne!(
        gateway.adapter_settings().await.unwrap()[0].credential_ref,
        before.credential_ref
    );
    assert_eq!(
        first.checkpoint("poll".into()).await.unwrap_err(),
        GatewayError::StaleConnection
    );
    assert!(
        gateway
            .complete_adapter_enrollment(&factory.name, &lease.id, secret)
            .await
            .is_err()
    );

    policy.scope.denied_users.insert("canonical".into());
    gateway
        .set_adapter_policy(&factory.name, policy.clone())
        .await
        .unwrap();
    gateway.enable_adapter(&factory.name, false).await.unwrap();
    gateway.shutdown().await.unwrap();
    let gateway = open(temp.path(), &[factory.clone()], &owner_policy()).await;
    let saved = gateway.adapter_settings().await.unwrap().remove(0);
    assert!(!saved.enabled);
    assert_eq!(saved.policy, policy);
    assert!(
        timeout(Duration::from_millis(50), queue.recv())
            .await
            .is_err()
    );
    gateway.enable_adapter(&factory.name, true).await.unwrap();
    let resumed = ready(&queue).await;
    assert_eq!(resumed.connection, first.connection);

    let replacement = gateway
        .begin_adapter_enrollment(&factory.name, Duration::from_secs(30))
        .await
        .unwrap();
    gateway
        .complete_adapter_enrollment(
            &factory.name,
            &replacement.id,
            EnrollmentCredential::new("other-secret".into()).unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(ready(&queue).await.connection, first.connection);
    assert_eq!(
        resumed.checkpoint("poll".into()).await.unwrap_err(),
        GatewayError::StaleConnection
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn receiver_routes_exactly_one_verified_installation_and_fences_old_routes() {
    let temp = TempDir::new().unwrap();
    let (a, qa) = fixture("app-a", "tenant-a");
    let (b, qb) = fixture("app-b", "tenant-a");
    let (c, qc) = fixture("app-c", "tenant-b");
    let gateway = open(
        temp.path(),
        &[a.clone(), b.clone(), c.clone()],
        &owner_policy(),
    )
    .await;
    let first = ready(&qa).await;
    let second = ready(&qb).await;
    let third = ready(&qc).await;
    let names = [a.name.clone(), b.name.clone(), c.name.clone()];
    let receiver = gateway
        .bind_adapter_receiver("public-receiver", &names)
        .await
        .unwrap();
    let ids = [
        InstallationKey::from(&a.proof),
        InstallationKey::from(&b.proof),
        InstallationKey::from(&c.proof),
    ];
    assert_eq!(
        receiver
            .route_verified(&[ids[0].clone()])
            .unwrap()
            .connection,
        first.connection
    );
    assert_eq!(
        receiver
            .route_verified(&[ids[1].clone()])
            .unwrap()
            .connection,
        second.connection
    );
    assert_eq!(
        receiver
            .route_verified(&[ids[2].clone()])
            .unwrap()
            .connection,
        third.connection
    );
    assert!(matches!(
        receiver.route_verified(&ids[..2]),
        Err(GatewayError::Forbidden)
    ));
    assert!(
        gateway
            .bind_adapter_receiver("public-receiver", &names)
            .await
            .is_err()
    );
    assert!(
        gateway
            .bind_adapter_receiver("different-receiver", &names)
            .await
            .is_err()
    );
    gateway.enable_adapter(&a.name, false).await.unwrap();
    assert!(matches!(
        receiver.route_verified(&[ids[0].clone()]),
        Err(GatewayError::StaleConnection)
    ));
    assert!(receiver.route_verified(&[ids[1].clone()]).is_ok());
    drop(receiver);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn receiver_replays_a_partially_committed_envelope_across_restart() {
    let temp = TempDir::new().unwrap();
    let (a, qa) = fixture("batch-a", "tenant-a");
    let (b, qb) = fixture("batch-b", "tenant-a");
    let policy = owner_policy();
    let factories = [a.clone(), b.clone()];
    let names = [a.name.clone(), b.name.clone()];
    let ids = [InstallationKey::from(&a.proof), InstallationKey::from(&b.proof)];
    let chat = room("same-chat");
    let envelope = |text: &str| {
        let entry = |matches, text: &str| {
            let mut event = message("same-provider-id", principal("canonical"), chat.clone());
            event.body = InboundBody::Message(Message::text(text));
            ReceiverItem { matches, input: HandoffItem { event, preparation: None } }
        };
        vec![
            entry(vec![ids[0].clone()], text),
            entry(ids.to_vec(), "ambiguous item"),
            entry(vec![ids[1].clone()], "second account"),
        ]
    };
    let gateway = open(temp.path(), &factories, &policy).await;
    let first = ready(&qa).await;
    let second = ready(&qb).await;
    for ingress in [&first, &second] {
        ingress.record_actor(person("canonical", chat.clone())).await.unwrap();
    }
    let receiver = gateway.bind_adapter_receiver("public-receiver", &names).await.unwrap();
    gateway.enable_adapter(&b.name, false).await.unwrap();
    assert_eq!(
        receiver.stage_envelope("frame".into(), envelope("first account")).await,
        Err(GatewayError::StaleConnection)
    );
    assert_eq!(
        receiver.stage_envelope("frame".into(), envelope("changed body")).await,
        Err(GatewayError::Conflict)
    );
    let state = gateway.messaging_status().await.unwrap();
    let inputs = state["input"].as_array().unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0]["connection"], first.connection);

    drop(receiver);
    gateway.shutdown().await.unwrap();
    let gateway = open(temp.path(), &factories, &policy).await;
    let first = ready(&qa).await;
    gateway.enable_adapter(&b.name, true).await.unwrap();
    let second = ready(&qb).await;
    let receiver = gateway.bind_adapter_receiver("public-receiver", &names).await.unwrap();
    let receipts = receiver
        .stage_envelope("frame".into(), envelope("first account"))
        .await
        .unwrap();
    assert_eq!(receipts[0].disposition, Disposition::Accepted);
    assert!(receipts[0].repeated);
    assert_eq!(receipts[1].disposition, Disposition::Forbidden);
    assert!(receipts[1].route.is_none());
    assert_eq!(receipts[2].disposition, Disposition::Accepted);
    assert!(!receipts[2].repeated);
    assert_ne!(receipts[0].route, receipts[2].route);
    assert!(
        receiver.stage_envelope("frame".into(), envelope("first account")).await
            .unwrap().iter().all(|receipt| receipt.repeated)
    );
    let state = gateway.messaging_status().await.unwrap();
    let inputs = state["input"].as_array().unwrap();
    assert_eq!(inputs.len(), 2);
    assert!(inputs.iter().any(|row| row["connection"] == first.connection));
    assert!(inputs.iter().any(|row| row["connection"] == second.connection));
    drop(receiver);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_aliases_route_together_and_membership_changes_fence_access() {
    let temp = TempDir::new().unwrap();
    let (factory, queue) = fixture("alias", "tenant");
    let mut policy = owner_policy();
    policy.operators.insert("canonical".into());
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    let room = room("private-chat");
    let first = ActorAlias {
        namespace: "user".into(),
        id: "ID".into(),
    };
    let second = ActorAlias {
        namespace: "directory".into(),
        id: "ID".into(),
    };
    let one = principal(&first.sender_id().unwrap());
    let two = principal(&second.sender_id().unwrap());
    let mut actor = person("canonical", room.clone());
    let alternate = Room { chat: "another-private-room".into(), ..room.clone() };
    actor.memberships.push(MemberEvidence { room: alternate.clone(), roles: Default::default() });
    actor.aliases = vec![first, second];
    ingress.record_actor(actor.clone()).await.unwrap();
    let a = ingress
        .submit(
            vec![message("provider-event", one.clone(), room.clone())],
            None,
        )
        .await
        .unwrap();
    let b = ingress
        .submit(
            vec![message("provider-event", two.clone(), room.clone())],
            None,
        )
        .await
        .unwrap();
    assert_eq!(a.receipts[0].disposition, Disposition::Accepted);
    assert_eq!(a.receipts[0].route, b.receipts[0].route);
    assert!(b.receipts[0].repeated);
    let other = ingress
        .submit(
            vec![message("other-event", one.clone(), room.clone())],
            None,
        )
        .await
        .unwrap();
    assert!(!other.receipts[0].repeated);

    let claimed = ActorEvidence {
        actor: "impostor".into(),
        ..actor.clone()
    };
    assert_eq!(
        ingress.record_actor(claimed).await.unwrap_err(),
        GatewayError::Forbidden
    );
    let outsider = ingress
        .submit(
            vec![message("unknown", principal("unknown"), room.clone())],
            None,
        )
        .await
        .unwrap();
    assert_eq!(outsider.receipts[0].disposition, Disposition::Forbidden);
    assert!(ingress.authorize_operator(&policy, &room, &one).unwrap());

    ingress.invalidate_actor_evidence(Some(room.clone())).await.unwrap();
    assert!(!ingress.authorize_operator(&policy, &room, &one).unwrap());
    assert!(ingress.authorize_operator(&policy, &alternate, &one).unwrap());
    ingress.invalidate_actor_evidence(None).await.unwrap();
    assert!(!ingress.authorize_operator(&policy, &alternate, &one).unwrap());

    actor.revision += 1;
    actor.aliases.pop();
    ingress.record_actor(actor.clone()).await.unwrap();
    let retired = ingress
        .submit(vec![message("retired", two, room.clone())], None)
        .await
        .unwrap();
    assert_eq!(retired.receipts[0].disposition, Disposition::Forbidden);
    actor.revision += 1;
    actor.memberships.clear();
    ingress.record_actor(actor).await.unwrap();
    assert!(!ingress.authorize_operator(&policy, &room, &one).unwrap());
    gateway.shutdown().await.unwrap();
    let gateway = open(temp.path(), &[factory], &policy).await;
    let ingress = ready(&queue).await;
    let revoked = ingress
        .submit(vec![message("after-reopen", one, room)], None)
        .await
        .unwrap();
    assert_eq!(revoked.receipts[0].disposition, Disposition::Forbidden);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn requester_command_rechecks_exact_room_roles_and_revocation() {
    use super::commands::{RequesterCommand, RequesterInvocation};

    let temp = TempDir::new().unwrap();
    let (factory, receiver) = fixture("requester-commands", "tenant");
    let mut policy: AccessPolicy = serde_json::from_value(serde_json::json!({
        "operator_roles":["discord-role:11:31"],
        "scope":{
            "require_actor_evidence":true,
            "rooms":{
                "21":{
                    "enabled":true,"grant":{"users":["7"]},
                    "requester_commands":{"roles":["discord-role:11:31"],"operator_roles":["discord-role:11:31"]}
                },
                "23":{"enabled":true,"grant":{"users":["7"]}}
            }
        }
    })).unwrap();
    let gateway = open(temp.path(), &[factory], &policy).await;
    let ingress = ready(&receiver).await;
    let exact = Room { kind: RoomKind::Group, chat: "21".into(), thread: None };
    let foreign = Room { kind: RoomKind::Group, chat: "23".into(), thread: None };
    let sender = Sender { id: "8".into(), roles: ["discord-role:11:31".into()].into() };
    let mut evidence = person("8", exact.clone());
    evidence.memberships[0].roles = sender.roles.clone();
    evidence.memberships.push(MemberEvidence {
        room: foreign.clone(), roles: sender.roles.clone(),
    });
    ingress.record_actor(evidence.clone()).await.unwrap();
    let reader = RequesterInvocation {
        id: "private-status".into(), room: exact.clone(), sender: sender.clone(),
        command: RequesterCommand::Status,
    };
    let advance = |before: Option<&str>, next: &str| Advance {
        stream: "interactions".into(), previous: before.map(str::to_owned), checkpoint: next.into(),
    };
    assert_eq!(
        ingress.commit_requester_command(reader.clone(), advance(None, "1")).await.unwrap(),
        Disposition::Accepted,
    );
    assert!(ingress.requester_command_response(&reader).unwrap().operator);
    let wrong_room = RequesterInvocation { id: "forged-role".into(), room: foreign, ..reader.clone() };
    assert_eq!(
        ingress.commit_requester_command(wrong_room.clone(), advance(Some("1"), "2")).await.unwrap(),
        Disposition::Forbidden,
    );
    assert!(matches!(ingress.requester_command_response(&wrong_room), Err(GatewayError::Forbidden)));
    let misplaced = RequesterInvocation { id: reader.id.clone(), ..wrong_room };
    assert!(matches!(ingress.requester_command_response(&misplaced), Err(GatewayError::Forbidden)));
    assert_eq!(
        ingress.submit(vec![message("chat", sender.clone(), exact.clone())], None).await.unwrap().receipts[0].disposition,
        Disposition::Forbidden,
    );
    assert!(!ingress.authorize_operator(&policy, &exact, &sender).unwrap());
    assert_eq!(ingress.checkpoint("interactions".into()).await.unwrap().as_deref(), Some("2"));

    // Membership or even just command policy revocation fences an accepted
    // receipt before the adapter can read its private result.
    ingress.invalidate_actor_evidence(Some(exact)).await.unwrap();
    assert_eq!(
        ingress.commit_requester_command(reader.clone(), advance(Some("2"), "3")).await.unwrap(),
        Disposition::Accepted,
    );
    assert!(matches!(ingress.requester_command_response(&reader), Err(GatewayError::Forbidden)));
    evidence.revision += 1;
    ingress.record_actor(evidence).await.unwrap();
    assert!(ingress.requester_command_response(&reader).is_ok());
    policy.scope.rooms.get_mut("21").unwrap().requester_commands = None;
    gateway.set_adapter_policy("requester-commands", policy).await.unwrap();
    assert!(matches!(ingress.requester_command_response(&reader), Err(GatewayError::Forbidden)));

    let status = gateway.messaging_status().await.unwrap();
    let entry = status["input"].as_array().unwrap().iter()
        .find(|item| item["id"] == "private-status").unwrap();
    assert!(entry["route"].is_null() && entry["session"].is_null());
    assert_eq!(entry["state"], "done");
    assert_eq!(status["deliveries"], serde_json::json!([]));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn connection_observations_use_weak_provenance_without_merging_identical_text() {
    let temp = TempDir::new().unwrap();
    let (factory, queue) = fixture("weak-provenance", "tenant");
    let mut policy = owner_policy();
    policy.users.insert("socket-principal".into());
    policy.operators.insert("socket-principal".into());
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    let topic = room("topic");
    ingress.record_actor(ActorEvidence {
        kind: ActorKind::Connection,
        ..person("socket-principal", topic.clone())
    }).await.unwrap();
    ingress.record_actor(person("canonical", topic.clone())).await.unwrap();
    let observe = |ordinal, actor: &str| ConnectionObservation {
        ordinal, room: topic.clone(), principal: principal(actor),
        mentioned: true, message: Message::text("same text"),
    };
    assert_eq!(
        ingress.submit_observation(observe(0, "canonical")).await,
        Err(GatewayError::Forbidden)
    );
    let first = ingress.submit_observation(observe(1, "socket-principal")).await.unwrap();
    let second = ingress.submit_observation(observe(2, "socket-principal")).await.unwrap();
    assert_eq!(first.disposition, Disposition::Accepted);
    assert_eq!(second.disposition, Disposition::Accepted);
    assert_ne!(first.id, second.id);
    assert_eq!(first.route, second.route);
    assert!(ingress.submit_observation(observe(1, "socket-principal")).await.unwrap().repeated);
    assert_eq!(
        ingress.submit(vec![message(&first.id, principal("canonical"), topic.clone())], None).await,
        Err(GatewayError::Invalid)
    );
    assert!(!ingress.authorize_operator(&policy, &topic, &principal("socket-principal")).unwrap());
    gateway.shutdown().await.unwrap();

    let gateway = open(temp.path(), &[factory], &policy).await;
    let ingress = ready(&queue).await;
    let next = ingress.submit_observation(observe(1, "socket-principal")).await.unwrap();
    assert!(!next.repeated);
    assert_ne!(next.id, first.id);
    let state = gateway.messaging_status().await.unwrap();
    let inputs = state["input"].as_array().unwrap();
    assert_eq!(inputs.len(), 3);
    assert!(inputs.iter().all(|row| row["provenance"] == "connection_observation"));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn scoped_pairing_and_shared_principals_do_not_grant_control() {
    let temp = TempDir::new().unwrap();
    let (factory, queue) = fixture("pairing", "tenant");
    let mut policy = owner_policy();
    policy.scope.pair_direct = true;
    policy.users.insert("topic".into());
    policy.operators.insert("topic".into());
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    let private = room("private");
    let topic = room("topic");
    ingress
        .record_actor(person("asker", private.clone()))
        .await
        .unwrap();
    ingress
        .record_actor(ActorEvidence {
            kind: ActorKind::Shared,
            ..person("topic", topic.clone())
        })
        .await
        .unwrap();
    let challenge = ingress
        .begin_pairing(private.clone(), principal("asker"))
        .await
        .unwrap();
    assert!(
        gateway
            .shared
            .messaging
            .pairing(&factory.name, &challenge.code, None)
            .await
            .unwrap()
    );
    assert!(
        !gateway
            .shared
            .messaging
            .pairing(&factory.name, &challenge.code, None)
            .await
            .unwrap()
    );
    let paired = ingress
        .submit(
            vec![message("paired", principal("asker"), private.clone())],
            None,
        )
        .await
        .unwrap();
    assert_eq!(paired.receipts[0].disposition, Disposition::Accepted);
    assert!(
        gateway
            .shared
            .messaging
            .pairing(&factory.name, "asker", Some(&private.chat))
            .await
            .unwrap()
    );
    assert!(
        !ingress
            .authorize_sender(&policy, &private, &principal("asker"))
            .unwrap()
    );

    let mut input = message("forged-control", principal("topic"), topic);
    input.body = InboundBody::Control(Control::Interrupt {
        execution_generation: 1,
    });
    let denied = ingress.submit(vec![input], None).await.unwrap();
    assert_eq!(denied.receipts[0].disposition, Disposition::Forbidden);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn delegated_authority_pins_actor_resource_origin_and_device_across_restart() {
    let temp = TempDir::new().unwrap();
    let (mut factory, queue) = fixture("transport", "tenant");
    let policy = owner_policy();
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    let room = room("room");
    ingress
        .record_actor(person("canonical", room.clone()))
        .await
        .unwrap();
    let mut address = TransportAddress {
        reference: "grant".into(),
        endpoint: "https://provider.example/files/one".into(),
        resource: "file:1".into(),
        operation: "upload".into(),
        actor: Some("canonical".into()),
        room: Some(room),
        expires_at: super::profile::time_ms() + 60_000,
        revision: 1,
    };
    let mut use_ = TransportUse {
        reference: address.reference.clone(),
        endpoint: address.endpoint.clone(),
        resource: address.resource.clone(),
        operation: address.operation.clone(),
        actor: address.actor.clone(),
        room: address.room.clone(),
    };
    let secret = EnrollmentCredential::new("actor-credential".into()).unwrap();
    ingress
        .record_transport_address(address.clone(), Some(secret.clone()))
        .await
        .unwrap();
    assert_eq!(
        ingress
            .transport_authority(&use_)
            .await
            .unwrap()
            .credential(),
        Some("actor-credential")
    );
    let redirect = TransportUse {
        endpoint: "https://foreign.example/files/one".into(),
        ..use_.clone()
    };
    assert!(matches!(
        ingress.transport_authority(&redirect).await,
        Err(GatewayError::Forbidden)
    ));
    // A valid media grant does not extend a provider address grant or allow
    // that address's credential to follow a foreign origin.
    let read_endpoint = TransportAddress {
        reference: "read-grant".into(),
        operation: "download_media".into(),
        ..address.clone()
    };
    ingress
        .record_transport_address(read_endpoint.clone(), Some(secret.clone()))
        .await
        .unwrap();
    let read_use = TransportUse {
        reference: read_endpoint.reference,
        operation: read_endpoint.operation,
        endpoint: "https://foreign.example/forged".into(),
        ..use_.clone()
    };
    let source = MediaSource::from(&message(
        "download",
        principal("canonical"),
        address.room.clone().unwrap(),
    ));
    let manifest = AttachmentManifest {
        name: "note.txt".into(),
        size: Some(8),
        sha256: None,
    };
    assert_eq!(
        ingress
            .download_attachment(source, manifest, read_use, Duration::from_secs(30))
            .await
            .unwrap_err(),
        GatewayError::Forbidden,
    );
    use_.actor = Some("other".into());
    assert!(matches!(
        ingress.transport_authority(&use_).await,
        Err(GatewayError::Forbidden)
    ));
    use_.actor = address.actor.clone();
    let expires = super::profile::time_ms() + 60_000;
    let destination_room = self::room("device-destination");
    let home_room = self::room("device-home");
    let destination = ingress.verify_destination(destination_room.clone(), expires).await.unwrap();
    let home = ingress.verify_destination(home_room.clone(), expires).await.unwrap();
    let source = message("device-message", principal("canonical"), address.room.clone().unwrap());
    let cursor = Advance { stream: "history".into(), previous: None, checkpoint: "token".into() };
    assert_eq!(ingress.submit(vec![source.clone()], Some(cursor.clone())).await.unwrap().receipts[0].disposition,
        Disposition::Accepted);
    ingress.begin_repair(RepairWindow {
        id: "device-window".into(), stream: cursor.stream.clone(), room: source.room.clone(),
        last_committed: cursor.checkpoint.clone(), expires_ms: super::profile::time_ms() + 60_000, limit: 10,
    }).await.unwrap();
    gateway.shutdown().await.unwrap();

    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    assert_eq!(
        ingress
            .transport_authority(&use_)
            .await
            .unwrap()
            .credential(),
        Some("actor-credential")
    );
    assert_eq!(ingress.checkpoint(cursor.stream.clone()).await.unwrap().as_deref(), Some("token"));
    assert_eq!(ingress.pending_repairs().await.unwrap().len(), 1);
    assert!(ingress.store.allows_notification(&destination, super::profile::time_ms(), &ingress.transport_scope().unwrap()).unwrap());
    gateway.shutdown().await.unwrap();

    factory.proof.device = Some("device-two".into());
    let gateway = open(temp.path(), &[factory], &policy).await;
    let ingress = ready(&queue).await;
    assert!(gateway.bind_messaging_conversation(&destination, &home).await.is_err());
    assert!(!ingress.store.allows_notification(&destination, super::profile::time_ms(), &ingress.transport_scope().unwrap()).unwrap());
    assert_eq!(ingress.verify_destination(destination_room, expires).await.unwrap(), destination);
    assert_eq!(ingress.verify_destination(home_room, expires).await.unwrap(), home);
    gateway.bind_messaging_conversation(&destination, &home).await.unwrap();
    assert!(matches!(
        ingress.transport_authority(&use_).await,
        Err(GatewayError::Forbidden)
    ));
    let room = address.room.as_ref().unwrap();
    assert!(ingress.checkpoint(cursor.stream.clone()).await.unwrap().is_none());
    assert!(ingress.pending_repairs().await.unwrap().is_empty());
    assert_eq!(gateway.messaging_status().await.unwrap()["connections"][0]["gap"], true);
    assert!(
        !ingress
            .authorize_sender(&policy, room, &principal("canonical"))
            .unwrap()
    );
    ingress
        .record_actor(person("canonical", room.clone()))
        .await
        .unwrap();
    assert!(
        ingress
            .authorize_sender(&policy, room, &principal("canonical"))
            .unwrap()
    );
    assert!(ingress.submit(vec![source], Some(cursor)).await.unwrap().receipts[0].repeated);
    address.revision += 1;
    ingress
        .record_transport_address(address.clone(), Some(secret))
        .await
        .unwrap();
    assert!(ingress.transport_authority(&use_).await.is_ok());
    assert!(
        ingress
            .revoke_transport_address("grant", address.revision + 1)
            .await
            .unwrap()
    );
    assert!(matches!(
        ingress.transport_authority(&use_).await,
        Err(GatewayError::Forbidden)
    ));
    assert_eq!(
        ingress
            .record_transport_address(address, None)
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn replacing_a_local_store_fences_reused_actor_and_item_ids() {
    let temp = TempDir::new().unwrap();
    let (mut factory, queue) = fixture("local-client", "tenant");
    factory.proof.local_client = Some(LocalClientIdentity {
        service: "daemon".into(),
        store_incarnation: "store-one".into(),
    });
    let policy = owner_policy();
    let room = room("local-chat");
    let sender = principal("canonical");
    let event = message("local-item", sender.clone(), room.clone());

    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    ingress
        .record_actor(person("canonical", room.clone()))
        .await
        .unwrap();
    let accepted = ingress.submit(vec![event.clone()], None).await.unwrap();
    let original_route = accepted.receipts[0].route.clone().unwrap();
    gateway.shutdown().await.unwrap();

    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let same_store = ready(&queue).await;
    assert_eq!(same_store.connection, ingress.connection);
    let repeated = same_store.submit(vec![event.clone()], None).await.unwrap();
    assert!(repeated.receipts[0].repeated);
    gateway.shutdown().await.unwrap();

    factory
        .proof
        .local_client
        .as_mut()
        .unwrap()
        .store_incarnation = "store-two".into();
    let gateway = open(temp.path(), &[factory], &policy).await;
    let other_store = ready(&queue).await;
    assert_ne!(other_store.connection, ingress.connection);
    assert!(
        !other_store
            .authorize_sender(&policy, &room, &sender)
            .unwrap()
    );
    other_store
        .record_actor(person("canonical", room))
        .await
        .unwrap();
    let fresh = other_store.submit(vec![event], None).await.unwrap();
    assert_eq!(fresh.receipts[0].disposition, Disposition::Accepted);
    assert!(!fresh.receipts[0].repeated);
    assert_ne!(
        fresh.receipts[0].route.as_deref(),
        Some(original_route.as_str())
    );
    let status = gateway.messaging_status().await.unwrap();
    assert_eq!(
        status["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["id"] == "local-item")
            .count(),
        2
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_events_reopen_in_their_own_binding_and_recheck_revoked_grants() {
    let temp = TempDir::new().unwrap();
    let (factory, queue) = fixture("resource-client", "tenant");
    let route = room("resource-notifications");
    let sender = principal("canonical");
    let source = ResourceSource {
        operation: "document.comment".into(),
        resource: "doc-a".into(),
    };
    let mut policy = owner_policy();
    policy.scope.events.insert(
        source.operation.clone(),
        ResourcePolicy {
            actors: [sender.id.clone()].into(),
            resources: [source.resource.clone()].into(),
            destinations: vec![route.clone()],
        },
    );
    let gateway = open(temp.path(), &[factory.clone()], &policy).await;
    let ingress = ready(&queue).await;
    ingress
        .record_actor(person(&sender.id, route.clone()))
        .await
        .unwrap();

    let input = message("external-42", sender.clone(), route.clone());
    let first = ingress
        .submit_resource_event(
            source.clone(),
            input.clone(),
            Some(Advance {
                stream: "documents".into(),
                previous: None,
                checkpoint: "seq1".into(),
            }),
        )
        .await
        .unwrap();
    assert_eq!(first.disposition, Disposition::Accepted);
    assert_eq!(
        ingress
            .checkpoint("documents".into())
            .await
            .unwrap()
            .as_deref(),
        Some("seq1")
    );
    let repeat = ingress
        .submit_resource_event(source.clone(), input.clone(), None)
        .await
        .unwrap();
    assert_eq!(repeat.id, first.id);
    assert!(repeat.repeated);
    let other = ResourceSource {
        resource: "another-document".into(),
        ..source.clone()
    };
    let denied = ingress
        .submit_resource_event(other, input.clone(), None)
        .await
        .unwrap();
    assert_eq!(denied.disposition, Disposition::Forbidden);
    let plain = ingress.submit(vec![input.clone()], None).await.unwrap();
    assert_ne!(plain.receipts[0].route, first.route);
    let forged = message("resource-event-fabricated", sender.clone(), route.clone());
    assert!(matches!(
        ingress.route_event(&policy, &forged).unwrap(),
        Err(Disposition::Forbidden)
    ));
    assert_eq!(
        ingress.submit(vec![forged], None).await.unwrap_err(),
        GatewayError::Invalid
    );

    gateway.stop_adapter(&factory.name).await;
    let saved = ingress
        .store
        .route(first.route.as_ref().unwrap())
        .unwrap()
        .unwrap();
    let event = InboundEvent {
        id: first.id.clone(),
        ..input
    };
    let intent = DeliveryIntent {
        id: "resource-reply".into(),
        room: route.clone(),
        reply_anchor: None,
        edit_target: None,
        body: DeliveryBody::Reply(Message::text("resource result")),
    };
    ingress
        .store
        .publish_reply(&saved, &event, &intent)
        .unwrap();
    gateway.shutdown().await.unwrap();

    let gateway = open(temp.path(), &[factory], &policy).await;
    let ingress = ready(&queue).await;
    let restored = ingress.route_event(&policy, &event).unwrap().unwrap();
    assert_eq!(restored.id, saved.id);
    assert_eq!(
        ingress
            .recheck_resource_delivery(&policy, &intent.id, &route, &sender)
            .unwrap(),
        Some(source),
    );
    policy.scope.events.clear();
    assert!(matches!(
        ingress.route_event(&policy, &event).unwrap(),
        Err(Disposition::Forbidden)
    ));
    assert!(matches!(
        ingress.recheck_resource_delivery(&policy, &intent.id, &route, &sender),
        Err(GatewayError::Forbidden)
    ));
    gateway.shutdown().await.unwrap();
}
