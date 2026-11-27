use super::*;
use crate::messaging::{
    DeliveryPolicy, EnrollmentCredential, TransportAddress, VerifiedInstallation,
};

type Render = dyn Fn(&DeliveryIntent) -> Vec<DeliveryPart> + Send + Sync;

#[derive(Clone)]
struct EffectFixture {
    wire: Fixture,
    render: Arc<Render>,
    device: String,
}

fn stock(
    driver: &Driver,
    policy: AccessPolicy,
    capabilities: EffectCapabilities,
    render: impl Fn(&DeliveryIntent) -> Vec<DeliveryPart> + Send + Sync + 'static,
) -> AdapterConfig {
    stock_on_device(driver, policy, capabilities, "device", render)
}

fn stock_on_device(
    driver: &Driver,
    policy: AccessPolicy,
    capabilities: EffectCapabilities,
    device: &str,
    render: impl Fn(&DeliveryIntent) -> Vec<DeliveryPart> + Send + Sync + 'static,
) -> AdapterConfig {
    let mut wire = driver.fixture.clone();
    wire.capabilities.effects = capabilities;
    let mut config = driver.config(policy);
    config.factory = Arc::new(EffectFixture {
        wire,
        render: Arc::new(render),
        device: device.into(),
    });
    config
}

impl AdapterFactory for EffectFixture {
    fn descriptor(&self) -> Descriptor {
        self.wire.descriptor()
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            Ok(VerifiedAdapter {
                account: self.wire.account.clone(),
                adapter: Arc::new(self.clone()),
            })
        })
    }
}

impl Adapter for EffectFixture {
    fn verified_installation(&self) -> Option<VerifiedInstallation> {
        Some(VerifiedInstallation {
            installation: "workspace".into(),
            tenant: None,
            application: None,
            receiver: None,
            origin: Some("https://fixture.example".into()),
            device: Some(self.device.clone()),
            local_client: None,
        })
    }

    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        self.wire.listen(ingress, cancel)
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        Ok((self.render)(intent))
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        self.wire.send(attempt, ingress, cancel)
    }
}

fn part(intent: &DeliveryIntent, effect: Effect) -> DeliveryPart {
    DeliveryPart {
        room: intent.room.clone(),
        body: intent.body.clone(),
        effect,
        edit_target: None,
    }
}

fn typed(kind: ReceiptKind, id: &str) -> DeliveryResult {
    DeliveryResult::EffectConfirmed(EffectReceipt {
        kind,
        id: id.into(),
    })
}

fn now() -> i64 {
    crate::messaging::runtime::now()
}

async fn phase(gateway: &RunningGateway, id: &str, ordinal: u32, part: &str) -> Value {
    status(gateway, |value| {
        delivery(value, id, ordinal).is_some_and(|row| row["part_state"] == part)
    })
    .await
}

fn receipt_for(
    event: &str,
    room: &Room,
    identity: EffectIdentity,
    user: Option<&str>,
    provider: &str,
    state: ProviderDeliveryState,
) -> EffectObservation {
    EffectObservation {
        event: event.into(),
        room: room.clone(),
        identity,
        recipient: user.map(str::to_owned),
        observed_at_ms: now(),
        state,
        receipt: Some(provider.into()),
    }
}

fn group_plan(intent: &DeliveryIntent) -> Vec<DeliveryPart> {
    vec![part(
        intent,
        Effect {
            recipients: ["alice", "bob", "chris"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            operation_key: Some("group-attempt-key".into()),
            ..Effect::default()
        },
    )]
}

#[tokio::test]
async fn group_partial_result_and_late_status_are_scoped_across_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "group-outbox", true);
    let config = || {
        stock(
            &driver,
            policy(),
            EffectCapabilities {
                recipient_subsets: true,
                ..EffectCapabilities::default()
            },
            group_plan,
        )
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    driver
        .push(InboundEvent {
            room: Room {
                kind: RoomKind::Group,
                chat: "group".into(),
                thread: None,
            },
            ..dm("group-input", "ignored", "hello")
        })
        .await;
    let first = driver.sent().await;
    let room = first.attempt.payload.room.clone();
    let id = first.attempt.delivery_id.clone();
    assert_eq!(first.attempt.effect.recipients.len(), 3);
    let early = receipt_for(
        "early",
        &room,
        EffectIdentity::ProviderId("group-provider".into()),
        Some("alice"),
        "a",
        ProviderDeliveryState::Read,
    );
    assert_eq!(
        ingress.observe_delivery(early.clone()).await.unwrap(),
        ObservationResult::Pending
    );
    first.finish(DeliveryResult::Recipients(RecipientResults {
        receipt: Some("group-provider".into()),
        retry_after: Some(Duration::from_millis(1)),
        recipients: vec![
            RecipientReceipt {
                recipient: "alice".into(),
                outcome: RecipientOutcome::Accepted {
                    receipt: "a".into(),
                },
            },
            RecipientReceipt {
                recipient: "bob".into(),
                outcome: RecipientOutcome::Uncertain,
            },
            RecipientReceipt {
                recipient: "chris".into(),
                outcome: RecipientOutcome::NotAccepted,
            },
        ],
    }));
    let state = phase(&gateway, &id, 0, "uncertain").await;
    let row = delivery(&state, &id, 0).unwrap();
    assert_eq!(row["provider_status"][0]["state"], "read");
    assert_eq!(row["recipients"]["alice"]["accepted"]["receipt"], "a");
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let reopened_ingress = driver.ready().await;
    assert_eq!(
        ingress.observe_delivery(early.clone()).await,
        Err(GatewayError::StaleConnection)
    );
    assert_eq!(
        reopened_ingress.observe_delivery(early).await.unwrap(),
        ObservationResult::Repeated
    );
    let wrong = receipt_for(
        "wrong",
        &Room {
            chat: "other".into(),
            ..room.clone()
        },
        EffectIdentity::ProviderId("group-provider".into()),
        Some("bob"),
        "b",
        ProviderDeliveryState::Delivered,
    );
    assert_eq!(
        reopened_ingress.observe_delivery(wrong).await.unwrap(),
        ObservationResult::Pending
    );
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    let late = receipt_for(
        "late",
        &room,
        EffectIdentity::ProviderId("group-provider".into()),
        Some("bob"),
        "b",
        ProviderDeliveryState::Delivered,
    );
    assert_eq!(
        reopened_ingress.observe_delivery(late).await.unwrap(),
        ObservationResult::Applied
    );
    let second = driver.sent().await;
    assert_eq!(
        second.attempt.effect.recipients,
        ["chris".to_owned()].into_iter().collect()
    );
    assert!(!second.attempt.effect.retransmission);
    second.finish(DeliveryResult::Uncertain);
    phase(&reopened, &id, 0, "uncertain").await;
    let unrelated = receipt_for(
        "later-a",
        &room,
        EffectIdentity::ProviderId("a".into()),
        Some("alice"),
        "a",
        ProviderDeliveryState::Played,
    );
    assert_eq!(
        reopened_ingress.observe_delivery(unrelated).await.unwrap(),
        ObservationResult::Applied
    );
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    let last_recipient = receipt_for(
        "late-c",
        &room,
        EffectIdentity::OperationKey("group-attempt-key".into()),
        Some("chris"),
        "c",
        ProviderDeliveryState::Accepted,
    );
    assert_eq!(
        reopened_ingress
            .observe_delivery(last_recipient)
            .await
            .unwrap(),
        ObservationResult::Applied
    );
    phase(&reopened, &id, 0, "sent").await;
    let failure = receipt_for(
        "failed",
        &room,
        EffectIdentity::ProviderId("c".into()),
        Some("chris"),
        "c",
        ProviderDeliveryState::Failed,
    );
    assert_eq!(
        reopened_ingress.observe_delivery(failure).await.unwrap(),
        ObservationResult::Applied
    );
    let value = reopened.messaging_status().await.unwrap();
    let row = delivery(&value, &id, 0).unwrap();
    assert_eq!(row["state"], "sent");
    assert!(
        row["provider_status"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["recipient"] == "chris" && item["state"] == "failed")
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn provider_consent_revocation_survives_restart_and_old_opt_in() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "consent", true);
    let config = || {
        stock(&driver, policy(), EffectCapabilities::default(), |intent| {
            [0, 1]
                .into_iter()
                .map(|_| {
                    part(
                        intent,
                        Effect {
                            consent_required: true,
                            ..Effect::default()
                        },
                    )
                })
                .collect()
        })
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let event = dm("consent-input", "consent-chat", "hello");
    let room = event.room.clone();
    let before = ConsentUpdate {
        event: "opt-in".into(),
        room: room.clone(),
        observed_at_ms: now(),
        eligible: true,
    };
    assert!(
        ingress
            .update_delivery_consent(before.clone())
            .await
            .unwrap()
    );
    driver.push(event).await;
    let first = driver.sent().await;
    let id = first.attempt.delivery_id.clone();
    assert!(
        ingress
            .update_delivery_consent(ConsentUpdate {
                event: "opt-out".into(),
                room,
                observed_at_ms: now(),
                eligible: false,
            })
            .await
            .unwrap()
    );
    first.finish(confirmed("first"));
    phase(&gateway, &id, 1, "blocked").await;
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let repeated = ConsentUpdate {
        observed_at_ms: now() + 1,
        ..before
    };
    assert!(!ingress.update_delivery_consent(repeated).await.unwrap());
    let state = phase(&reopened, &id, 1, "blocked").await;
    assert_eq!(
        delivery(&state, &id, 1).unwrap()["error"],
        "eligibility_consent"
    );
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn last_reply_grant_slot_stays_bound_after_a_lost_response() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "reply-grant", true);
    let config = || {
        stock(&driver, policy(), EffectCapabilities::default(), |intent| {
            ["first", "second"]
                .into_iter()
                .map(|key| {
                    part(
                        intent,
                        Effect {
                            operation_key: Some(key.into()),
                            reply_grant: Some("grant".into()),
                            ..Effect::default()
                        },
                    )
                })
                .collect()
        })
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let event = dm("grant-anchor", "grant-chat", "hello");
    let grant = ReplyGrant {
        id: "grant".into(),
        source_event: event.id.clone(),
        room: event.room.clone(),
        operation: ReplyOperation::Publish,
        observed_at_ms: now(),
        expires_at_ms: now() + 20_000,
        absolute_expires_at_ms: now() + 40_000,
        slots: 1,
    };
    assert!(!ingress.register_reply_grant(grant.clone()).await.unwrap());
    driver.push(event).await;
    assert!(ingress.register_reply_grant(grant.clone()).await.unwrap());
    let first = driver.sent().await;
    let id = first.attempt.delivery_id.clone();
    first.finish(DeliveryResult::Uncertain);
    phase(&gateway, &id, 0, "uncertain").await;
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let fresh = driver.ready().await;
    let updated = ReplyGrant {
        observed_at_ms: now(),
        expires_at_ms: grant.expires_at_ms + 1,
        ..grant.clone()
    };
    assert!(!fresh.refresh_reply_grant(updated).await.unwrap());
    let observed = receipt_for(
        "first-remote",
        &grant.room,
        EffectIdentity::OperationKey("first".into()),
        None,
        "provider-effect",
        ProviderDeliveryState::Accepted,
    );
    assert_eq!(
        fresh.observe_delivery(observed).await.unwrap(),
        ObservationResult::Applied
    );
    let state = phase(&reopened, &id, 1, "blocked").await;
    assert_eq!(
        delivery(&state, &id, 1).unwrap()["error"],
        "eligibility_reply_slots_exhausted"
    );
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn qualified_replay_keeps_attempt_and_effect_identity() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "replay-effect", true);
    let replay_scope = ReplayScope {
        device: "device".into(),
        endpoint: "https://fixture.example".into(),
        domain: "known-continuity".into(),
    };
    let guarantee = ReplayGuarantee {
        key: "op-key".into(),
        scope: replay_scope.clone(),
        expires_at_ms: now() + 30_000,
    };
    let config = || {
        let guarantee = guarantee.clone();
        stock(
            &driver,
            policy(),
            EffectCapabilities {
                retransmission: true,
                ..EffectCapabilities::default()
            },
            move |intent| {
                vec![part(
                    intent,
                    Effect {
                        replay: Some(guarantee.clone()),
                        ..Effect::default()
                    },
                )]
            },
        )
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    ingress
        .set_replay_scope(Some(replay_scope.clone()))
        .await
        .unwrap();
    driver
        .push(dm("replay-anchor", "replay-chat", "hello"))
        .await;
    let first = driver.sent().await;
    let original = first.attempt.clone();
    first.finish(DeliveryResult::Uncertain);
    phase(&gateway, &original.delivery_id, 0, "uncertain").await;
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    ingress
        .set_replay_scope(Some(ReplayScope {
            domain: "different-cache".into(),
            ..replay_scope.clone()
        }))
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    ingress.set_replay_scope(Some(replay_scope)).await.unwrap();
    let retry = driver.sent().await;
    assert!(retry.attempt.effect.retransmission);
    assert_eq!(retry.attempt.delivery_id, original.delivery_id);
    assert_eq!(retry.attempt.attempt, original.attempt);
    assert_eq!(retry.attempt.payload, original.payload);
    retry.finish(confirmed("committed"));
    phase(&reopened, &original.delivery_id, 0, "sent").await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_only_completion_does_not_claim_remote_acceptance() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "limited-link", true);
    let config = || {
        let mut owner = policy();
        owner.scope.direct.delivery = Some(DeliveryPolicy {
            submitted: true,
            ..DeliveryPolicy::default()
        });
        stock(
            &driver,
            owner,
            EffectCapabilities {
                submitted: true,
                ..EffectCapabilities::default()
            },
            |intent| {
                vec![part(
                    intent,
                    Effect {
                        write_only: true,
                        ..Effect::default()
                    },
                )]
            },
        )
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    driver.ready().await;
    driver.push(dm("write-only", "limited", "hello")).await;
    let first = driver.sent().await;
    let id = first.attempt.delivery_id.clone();
    first.finish(DeliveryResult::Submitted);
    let state = phase(&gateway, &id, 0, "submitted").await;
    assert_eq!(delivery(&state, &id, 0).unwrap()["state"], "submitted");
    assert_eq!(
        delivery(&state, &id, 0).unwrap()["provider_status"],
        json!([])
    );
    driver
        .push(dm("second-write", "limited", "send next"))
        .await;
    let second = driver.sent().await;
    let uncertain = second.attempt.delivery_id.clone();
    second.finish(DeliveryResult::Uncertain);
    phase(&gateway, &uncertain, 0, "uncertain").await;
    gateway.shutdown().await.unwrap();
    let after = model.gateway(root.path(), vec![config()]).await;
    driver.ready().await;
    phase(&after, &uncertain, 0, "uncertain").await;
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    after.shutdown().await.unwrap();
}

fn staged_plan(intent: &DeliveryIntent) -> Vec<DeliveryPart> {
    let DeliveryBody::Reply(message) = &intent.body else {
        return vec![part(intent, Effect::default())];
    };
    let Some(artifact) = message.attachments.first() else {
        return vec![part(intent, Effect::default())];
    };
    let operations = [
        EffectOperation::Provision {
            resource: ProvisionResource::Thread,
        },
        EffectOperation::Upload {
            artifact: artifact.clone(),
            step: UploadStep::Begin,
        },
        EffectOperation::Upload {
            artifact: artifact.clone(),
            step: UploadStep::Chunk { begin: 1, index: 0 },
        },
        EffectOperation::Upload {
            artifact: artifact.clone(),
            step: UploadStep::Finish {
                begin: 1,
                chunks: vec![2],
            },
        },
        EffectOperation::Publish {
            using: vec![
                EffectDependency {
                    part: 0,
                    kind: ReceiptKind::Destination,
                },
                EffectDependency {
                    part: 3,
                    kind: ReceiptKind::File,
                },
            ],
        },
    ];
    operations
        .into_iter()
        .map(|operation| {
            part(
                intent,
                Effect {
                    operation,
                    ..Effect::default()
                },
            )
        })
        .collect()
}

#[tokio::test]
async fn resource_receipts_resume_upload_and_publish_in_order() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "segmented-upload", true);
    let config = || {
        stock(
            &driver,
            policy(),
            EffectCapabilities {
                prepare_resources: true,
                ..EffectCapabilities::default()
            },
            staged_plan,
        )
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let route_id = driver
        .push(dm("staged-route", "upload-chat", "hello"))
        .await
        .route
        .unwrap();
    let warmup = driver.sent().await;
    warmup.finish(confirmed("warmup"));
    let route = ingress.store.route(&route_id).unwrap().unwrap();
    let file = ingress
        .store
        .put_attachment(&ingress.connection, "evidence.txt".into(), b"attachment")
        .unwrap();
    let id = "segmented-test";
    ingress
        .store
        .enqueue(
            &route,
            &DeliveryIntent {
                id: id.into(),
                room: route.room.clone(),
                reply_anchor: None,
                edit_target: None,
                body: DeliveryBody::Reply(Message {
                    text: "ship this".into(),
                    attachments: vec![file],
                }),
            },
        )
        .unwrap();
    let reserve = driver.sent().await;
    reserve.finish(typed(ReceiptKind::Destination, "provisioned"));
    let begin = driver.sent().await;
    begin.finish(typed(ReceiptKind::UploadSession, "upload-session"));
    let chunk = driver.sent().await;
    assert_eq!(chunk.attempt.effect.dependencies[0].1.id, "upload-session");
    chunk.finish(DeliveryResult::Uncertain);
    phase(&gateway, id, 2, "uncertain").await;
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let old = ingress
        .uncertain_deliveries()
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.delivery_id == id)
        .unwrap();
    assert!(
        ingress
            .reconcile_delivery(old, typed(ReceiptKind::UploadChunk, "chunk-confirmed"))
            .await
            .unwrap()
    );
    let finish = driver.sent().await;
    assert_eq!(
        finish
            .attempt
            .effect
            .dependencies
            .iter()
            .map(|effect| effect.1.id.as_str())
            .collect::<Vec<_>>(),
        ["upload-session", "chunk-confirmed"]
    );
    finish.finish(typed(ReceiptKind::File, "provider-file"));
    let publish = driver.sent().await;
    assert_eq!(
        publish
            .attempt
            .effect
            .dependencies
            .iter()
            .map(|effect| effect.1.id.as_str())
            .collect::<Vec<_>>(),
        ["provisioned", "provider-file"]
    );
    publish.finish(confirmed("submitted-message"));
    phase(&reopened, id, 4, "sent").await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn provisioned_destination_requires_exact_owner_binding_and_survives_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "provision-account", true);
    let another = Driver::new(Transport::Push, "other-account", true);
    let config = |driver: &Driver, device, name: &str| {
        let mut config = stock_on_device(
            driver,
            policy(),
            EffectCapabilities {
                prepare_resources: true,
                ..EffectCapabilities::default()
            },
            device,
            |intent| {
                if intent.room.kind != RoomKind::Group || intent.room.thread.is_some() {
                    return vec![part(intent, Effect::default())];
                }
                vec![
                    part(
                        intent,
                        Effect {
                            operation: EffectOperation::Provision {
                                resource: ProvisionResource::Topic,
                            },
                            ..Effect::default()
                        },
                    ),
                    part(
                        intent,
                        Effect {
                            operation: EffectOperation::Publish {
                                using: vec![EffectDependency {
                                    part: 0,
                                    kind: ReceiptKind::Destination,
                                }],
                            },
                            ..Effect::default()
                        },
                    ),
                ]
            },
        );
        config.name = name.to_owned();
        config
    };
    let before = model
        .gateway(root.path(), vec![config(&driver, "device", "fixture")])
        .await;
    let first = driver.ready().await;
    let mut event = dm("provision-request", "group", "open a topic");
    event.room.kind = RoomKind::Group;
    let parent = driver.push(event.clone()).await.route.unwrap();
    let provision = driver.sent().await;
    let delivery_id = provision.attempt.delivery_id.clone();
    let part = provision.attempt.part;
    let attempt = provision.attempt.attempt;
    let connection = first.connection.clone();
    assert!(matches!(
        provision.attempt.payload.effect.operation,
        EffectOperation::Provision { .. }
    ));
    provision.finish(typed(ReceiptKind::File, "wrong-receipt-kind"));
    let recorded = phase(&before, &delivery_id, part, "uncertain").await;
    let session = input(&recorded, &event.id).unwrap()["session"].clone();
    assert!(session.is_string());
    assert!(
        before
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
            .await
            .is_err()
    );
    let uncertain = first.uncertain_deliveries().await.unwrap().pop().unwrap();
    before.shutdown().await.unwrap();

    let changed = model
        .gateway(
            root.path(),
            vec![config(&driver, "replacement-device", "fixture")],
        )
        .await;
    let replacement = driver.ready().await;
    assert_eq!(replacement.connection, connection);
    assert!(
        !replacement
            .reconcile_delivery(
                uncertain.clone(),
                typed(ReceiptKind::Destination, "unsafe-topic")
            )
            .await
            .unwrap()
    );
    assert!(
        changed
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
            .await
            .is_err()
    );
    changed.shutdown().await.unwrap();

    let owner = model
        .gateway(
            root.path(),
            vec![
                config(&driver, "device", "fixture"),
                config(&another, "device", "second"),
            ],
        )
        .await;
    let ingress = driver.ready().await;
    let wrong_account = another.ready().await;
    assert!(
        !wrong_account
            .reconcile_delivery(
                uncertain.clone(),
                typed(ReceiptKind::Destination, "wrong-scope")
            )
            .await
            .unwrap()
    );
    assert!(
        ingress
            .reconcile_delivery(uncertain, typed(ReceiptKind::Destination, "exact-topic"))
            .await
            .unwrap()
    );
    phase(&owner, &delivery_id, part, "sent").await;
    let publication = driver.sent().await;
    assert_eq!(
        publication.attempt.effect.dependencies[0].1.id,
        "exact-topic"
    );
    publication.finish(confirmed("provisioned-message"));
    phase(&owner, &delivery_id, part + 1, "sent").await;
    assert!(
        owner
            .bind_messaging_provisioned_destination(
                &wrong_account.connection,
                &delivery_id,
                part,
                attempt
            )
            .await
            .is_err()
    );
    assert!(
        owner
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt + 1)
            .await
            .is_err()
    );

    let mut child = dm("provisioned-input", "group", "reply in the topic");
    child.room.kind = RoomKind::Group;
    child.room.thread = Some("exact-topic".into());
    let expected = store::Route::new(&connection, child.room.clone(), Some("alice")).unwrap();
    assert!(ingress.store.route(&expected.id).unwrap().is_none());
    let mut revoked = policy();
    revoked.scope.denied_users.insert("alice".into());
    owner.set_adapter_policy("fixture", revoked).await.unwrap();
    assert!(
        owner
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
            .await
            .is_err()
    );
    assert!(ingress.store.route(&expected.id).unwrap().is_none());
    owner.set_adapter_policy("fixture", policy()).await.unwrap();
    let bound = owner
        .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
        .await
        .unwrap();
    assert_eq!(bound, expected.id);
    owner.shutdown().await.unwrap();

    let restored = model
        .gateway(root.path(), vec![config(&driver, "device", "fixture")])
        .await;
    let ingress = driver.ready().await;
    assert_eq!(
        restored
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
            .await
            .unwrap(),
        bound,
    );
    assert_eq!(
        driver.push(child.clone()).await.route.as_deref(),
        Some(bound.as_str())
    );
    let reply = driver.sent().await;
    assert_eq!(reply.attempt.payload.room, child.room);
    let child_delivery = reply.attempt.delivery_id.clone();
    reply.finish(confirmed("topic-message"));
    let after = phase(&restored, &child_delivery, 0, "sent").await;
    assert_eq!(input(&after, &child.id).unwrap()["session"], session);

    ingress.store.block_route(&parent).unwrap();
    assert!(restored.restore_messaging_route(parent).await.unwrap());
    assert!(
        restored
            .bind_messaging_provisioned_destination(&connection, &delivery_id, part, attempt)
            .await
            .is_err()
    );
    restored.shutdown().await.unwrap();
}

fn presentation_plan(intent: &DeliveryIntent) -> Vec<DeliveryPart> {
    if intent.room.chat != "presentation" {
        return vec![part(intent, Effect::default())];
    }
    [
        PresentationPhase::Create,
        PresentationPhase::Publish,
        PresentationPhase::Revise,
        PresentationPhase::Finalize,
    ]
    .into_iter()
    .enumerate()
    .map(|(revision, phase)| {
        part(
            intent,
            Effect {
                operation: EffectOperation::Present {
                    marker: "draft".into(),
                    revision: revision as u32,
                    phase,
                },
                correlation_scope: Some("same-provider-ack".into()),
                ..Effect::default()
            },
        )
    })
    .collect()
}

#[tokio::test]
async fn verified_publication_precedes_stream_revisions_and_finalization() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "presentations", true);
    let config = || {
        stock(
            &driver,
            policy(),
            EffectCapabilities {
                durable_presentation: true,
                ..EffectCapabilities::default()
            },
            presentation_plan,
        )
    };
    let gateway = model.gateway(root.path(), vec![config()]).await;
    let old_ingress = driver.ready().await;
    driver.push(dm("present", "presentation", "hello")).await;
    let first = driver.sent().await;
    let id = first.attempt.delivery_id.clone();
    first.finish(typed(ReceiptKind::Presentation, "created-draft"));
    let publish = driver.sent().await;
    assert_eq!(
        publish.attempt.effect.marker.as_ref().unwrap().id,
        "created-draft"
    );
    publish.finish(DeliveryResult::Uncertain);
    phase(&gateway, &id, 1, "uncertain").await;
    driver.push(dm("independent", "other", "hello")).await;
    let independent = driver.sent().await;
    assert_eq!(independent.attempt.payload.room.chat, "other");
    independent.finish(confirmed("unrelated-send"));
    gateway.shutdown().await.unwrap();

    let reopened = model.gateway(root.path(), vec![config()]).await;
    let ingress = driver.ready().await;
    let old = ingress
        .uncertain_deliveries()
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.delivery_id == id)
        .unwrap();
    assert_eq!(
        old_ingress
            .reconcile_delivery(
                old.clone(),
                typed(ReceiptKind::Presentation, "wrong-generation")
            )
            .await,
        Err(GatewayError::StaleConnection)
    );
    assert!(
        ingress
            .reconcile_delivery(old, typed(ReceiptKind::Presentation, "published-draft"))
            .await
            .unwrap()
    );
    let revision = driver.sent().await;
    assert_eq!(revision.attempt.part, 2);
    assert_eq!(
        revision.attempt.effect.marker.as_ref().unwrap().id,
        "published-draft"
    );
    assert_eq!(
        revision.attempt.effect.dependencies[0].1.id,
        "published-draft"
    );
    revision.finish(typed(ReceiptKind::Presentation, "confirmed-revision"));
    let finalization = driver.sent().await;
    assert_eq!(
        finalization.attempt.effect.dependencies[0].1.id,
        "confirmed-revision"
    );
    finalization.finish(typed(ReceiptKind::Presentation, "confirmed-final"));
    phase(&reopened, &id, 3, "sent").await;
    assert!(
        !ingress
            .retire_marker(MarkerRef {
                delivery: id,
                part: 0
            })
            .await
            .unwrap()
    );
    reopened.shutdown().await.unwrap();
}

fn marker_retirement_plan(intent: &DeliveryIntent) -> Vec<DeliveryPart> {
    let effect = match &intent.body {
        DeliveryBody::Retire { marker } => Effect {
            operation: EffectOperation::Retire {
                marker: marker.clone(),
            },
            ..Effect::default()
        },
        _ => Effect::default(),
    };
    vec![part(intent, effect)]
}

#[tokio::test]
async fn confirmed_prompt_retires_only_after_its_native_decision() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "confirmed-prompt-cleanup", true);
    let mut access = policy();
    access.direct_tools = true;
    access.scope.direct.turn.approval = Some(crate::messaging::ApprovalPolicy::Manual);
    let config = stock(
        &driver,
        access,
        EffectCapabilities {
            durable_presentation: true,
            ..EffectCapabilities::default()
        },
        marker_retirement_plan,
    );
    let gateway = model.gateway(root.path(), vec![config]).await;
    let ingress = driver.ready().await;
    driver.push(dm("prompt", "scope", "approval")).await;
    let prompt = driver.sent().await;
    let marker = MarkerRef {
        delivery: prompt_id(&prompt.attempt).to_owned(),
        part: 0,
    };
    prompt.finish(DeliveryResult::Confirmed {
        receipt: "prompt-message".into(),
        editable_message: Some("exact-prompt".into()),
    });
    phase(&gateway, &marker.delivery, 0, "sent").await;
    assert!(!ingress.retire_marker(marker.clone()).await.unwrap());
    driver
        .push(control(
            "decision",
            "scope",
            Control::Respond {
                action_id: marker.delivery,
                response: Response::Deny,
            },
        ))
        .await;
    applied(&gateway, "decision", "applied").await;
    let cleanup = driver.sent().await;
    assert_eq!(
        cleanup.attempt.effect.marker.as_ref().unwrap().id,
        "exact-prompt"
    );
    let cleanup_id = cleanup.attempt.delivery_id.clone();
    cleanup.finish(confirmed("cleanup"));
    phase(&gateway, &cleanup_id, 0, "sent").await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn committed_prompt_retires_once_after_late_confirmation_and_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "prompt-cleanup", true);
    let config = |access| {
        stock(
            &driver,
            access,
            EffectCapabilities {
                durable_presentation: true,
                ..EffectCapabilities::default()
            },
            marker_retirement_plan,
        )
    };
    let mut access = policy();
    access.direct_tools = true;
    access.scope.direct.turn.approval = Some(crate::messaging::ApprovalPolicy::Manual);
    let gateway = model.gateway(root.path(), vec![config(access)]).await;
    let ingress = driver.ready().await;
    driver
        .push(dm("prompt-input", "approval-room", "approval"))
        .await;
    let prompt = driver.sent().await;
    let marker = MarkerRef {
        delivery: prompt_id(&prompt.attempt).to_owned(),
        part: 0,
    };
    assert_eq!(prompt.attempt.delivery_id, marker.delivery);
    let original_room = prompt.attempt.payload.room.clone();
    prompt.finish(DeliveryResult::Uncertain);
    phase(&gateway, &marker.delivery, 0, "uncertain").await;
    let platform_prompt = ingress.uncertain_deliveries().await.unwrap().pop().unwrap();
    let decision = control(
        "approval-answer",
        "approval-room",
        Control::Respond {
            action_id: marker.delivery.clone(),
            response: Response::Approve,
        },
    );
    driver.push(decision.clone()).await;
    applied(&gateway, "approval-answer", "applied").await;
    assert!(driver.push(decision.clone()).await.repeated);
    status(&gateway, |state| {
        input(state, "prompt-input").is_some_and(|row| row["state"] == "done")
    })
    .await;
    let native_calls = model.requests().len();
    gateway.shutdown().await.unwrap();

    let reopened = model
        .gateway(root.path(), vec![config(AccessPolicy::default())])
        .await;
    let ingress = driver.ready().await;
    assert!(driver.push(decision.clone()).await.repeated);
    assert!(
        ingress
            .reconcile_delivery(
                platform_prompt,
                DeliveryResult::Confirmed {
                    receipt: "approval-card".into(),
                    editable_message: Some("exact-approval-card".into())
                },
            )
            .await
            .unwrap()
    );
    let cleanup = driver.sent().await;
    assert_eq!(cleanup.attempt.payload.room, original_room);
    assert_eq!(
        cleanup.attempt.payload.body,
        DeliveryBody::Retire {
            marker: marker.clone()
        }
    );
    assert_eq!(
        cleanup.attempt.payload.effect.visibility,
        EffectVisibility::default()
    );
    assert_eq!(
        cleanup.attempt.effect.marker.as_ref().unwrap().id,
        "exact-approval-card"
    );
    let cleanup_id = cleanup.attempt.delivery_id.clone();
    cleanup.finish(DeliveryResult::Uncertain);
    phase(&reopened, &cleanup_id, 0, "uncertain").await;
    let platform_cleanup = ingress.uncertain_deliveries().await.unwrap().pop().unwrap();
    assert_eq!(platform_cleanup.delivery_id, cleanup_id);
    reopened.shutdown().await.unwrap();

    let restored = model
        .gateway(root.path(), vec![config(AccessPolicy::default())])
        .await;
    let ingress = driver.ready().await;
    assert!(driver.push(decision).await.repeated);
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    assert!(
        ingress
            .reconcile_delivery(platform_cleanup, confirmed("removed-approval-card"))
            .await
            .unwrap()
    );
    phase(&restored, &cleanup_id, 0, "sent").await;
    assert!(!ingress.retire_marker(marker).await.unwrap());
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    assert_eq!(model.requests().len(), native_calls);
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoked_actor_can_only_leave_a_scoped_marker_retirement() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let mut driver = Driver::new(Transport::Push, "cleanup", true);
    driver.fixture.capabilities.progress = true;
    driver.fixture.capabilities.editing = true;
    let config = stock(
        &driver,
        policy(),
        EffectCapabilities {
            durable_presentation: true,
            ..EffectCapabilities::default()
        },
        marker_retirement_plan,
    );
    let gateway = model.gateway(root.path(), vec![config]).await;
    let ingress = driver.ready().await;
    driver
        .push(dm("cleanup-input", "cleanup-room", "steer wait"))
        .await;
    let working = driver.sent().await;
    assert!(matches!(
        working.attempt.payload.body,
        DeliveryBody::Progress { .. }
    ));
    let marker = MarkerRef {
        delivery: working.attempt.delivery_id.clone(),
        part: 0,
    };
    working.finish(DeliveryResult::Confirmed {
        receipt: "progress".into(),
        editable_message: Some("exact-marker".into()),
    });
    phase(&gateway, &marker.delivery, 0, "sent").await;
    gateway
        .set_adapter_policy("fixture", AccessPolicy::default())
        .await
        .unwrap();
    model.release_waiting_turn();
    assert!(ingress.retire_marker(marker.clone()).await.unwrap());
    let cleanup = driver.sent().await;
    assert!(matches!(
        cleanup.attempt.payload.body,
        DeliveryBody::Retire { .. }
    ));
    assert_eq!(
        cleanup.attempt.payload.effect.visibility,
        EffectVisibility::default()
    );
    assert_eq!(
        cleanup.attempt.effect.marker.as_ref().unwrap().id,
        "exact-marker"
    );
    let cleanup_id = cleanup.attempt.delivery_id.clone();
    cleanup.finish(confirmed("retired-marker"));
    phase(&gateway, &cleanup_id, 0, "sent").await;
    assert!(!ingress.retire_marker(marker).await.unwrap());
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn delivery_address_is_verified_before_use_and_late_receipts_survive_revocation() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "address-bound", true);
    let config = stock(&driver, policy(), EffectCapabilities::default(), |intent| {
        vec![part(
            intent,
            Effect {
                operation_key: Some("address-op".into()),
                transport: Some(EffectTransportRef {
                    reference: "provider-address".into(),
                    resource: "provider-room".into(),
                    operation: "send".into(),
                }),
                ..Effect::default()
            },
        )]
    });
    let gateway = model.gateway(root.path(), vec![config]).await;
    let ingress = driver.ready().await;
    let event = dm("address-input", "address-room", "hello");
    let room = event.room.clone();
    driver.push(event).await;
    status(&gateway, |value| {
        value["deliveries"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["part_state"] == "blocked" && row["error"] == "eligibility_transport_address"
            })
        })
    })
    .await;
    let address = TransportAddress {
        reference: "provider-address".into(),
        endpoint: "https://fixture.example/send?query=private-url".into(),
        resource: "provider-room".into(),
        operation: "send".into(),
        actor: None,
        room: Some(room.clone()),
        revision: 1,
        expires_at: now() + 15_000,
    };
    ingress
        .record_transport_address(
            address,
            Some(EnrollmentCredential::new("private-credential".into()).unwrap()),
        )
        .await
        .unwrap();
    let sent = driver.sent().await;
    let id = sent.attempt.delivery_id.clone();
    let authority = ingress.delivery_authority(&sent.attempt).await.unwrap();
    assert_eq!(authority.credential(), Some("private-credential"));
    assert_eq!(authority.endpoint.host_str(), Some("fixture.example"));
    let debug = format!("{:?}", sent.attempt);
    assert!(!debug.contains("private-credential") && !debug.contains("private-url"));
    assert!(
        ingress
            .revoke_transport_address("provider-address", 2)
            .await
            .unwrap()
    );
    assert!(matches!(
        ingress.delivery_authority(&sent.attempt).await,
        Err(GatewayError::Forbidden)
    ));
    sent.finish(DeliveryResult::Uncertain);
    phase(&gateway, &id, 0, "uncertain").await;
    let late = receipt_for(
        "address-reply",
        &room,
        EffectIdentity::OperationKey("address-op".into()),
        None,
        "recorded-message",
        ProviderDeliveryState::Accepted,
    );
    assert_eq!(
        ingress.observe_delivery(late).await.unwrap(),
        ObservationResult::Applied
    );
    phase(&gateway, &id, 0, "sent").await;
    gateway.shutdown().await.unwrap();
}
