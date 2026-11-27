use super::*;

fn expires() -> i64 {
    chrono::Utc::now().timestamp_millis() + 60_000
}

fn handoff(id: &str, items: Vec<InboundEvent>, advance: Option<Advance>) -> Handoff {
    Handoff {
        id: id.into(),
        items: items
            .into_iter()
            .map(|event| HandoffItem {
                event,
                preparation: None,
            })
            .collect(),
        advance,
    }
}

#[tokio::test]
async fn durable_preparation_orders_routes_and_reopens_before_hydration() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "durable-handoff", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let mut batch = handoff(
        "batch",
        vec![dm("deferred", "one", "")],
        Some(advance(None, "next")),
    );
    batch.items[0].preparation = Some(Preparation {
        token: "source-transfer".into(),
        purpose: PreparationPurpose::Body,
        expires_ms: expires(),
        protection: None,
    });
    let staged = ingress.stage_handoff(batch.clone()).await.unwrap();
    assert_eq!(staged.checkpoint.as_deref(), Some("next"));
    assert_eq!(staged.receipts[0].disposition, Disposition::Deferred);
    driver.push(dm("later", "one", "later")).await;
    driver.push(dm("other", "two", "other")).await;
    assert_eq!(reply(&driver.sent().await.attempt), "answer other");
    assert!(driver.sent.try_recv().is_err());

    gateway.shutdown().await.unwrap();
    let resumed = Driver::new(Transport::Poll, "durable-handoff", false);
    let gateway = model
        .gateway(root.path(), vec![resumed.config(policy())])
        .await;
    let ingress = resumed.ready().await;
    let repeat = ingress.stage_handoff(batch.clone()).await.unwrap();
    assert!(repeat.receipts[0].repeated);
    assert_eq!(repeat.receipts[0].disposition, Disposition::Deferred);
    let mut conflict = batch;
    conflict.items[0].preparation.as_mut().unwrap().token = "different".into();
    assert_eq!(
        ingress.stage_handoff(conflict).await.unwrap_err(),
        GatewayError::Conflict
    );
    assert_eq!(
        ingress.pending_preparations().await.unwrap()[0].event.id,
        "deferred"
    );
    assert_eq!(
        ingress
            .complete_preparation("deferred".into(), "forged".into(), Message::text("invalid"))
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    let done = ingress
        .complete_preparation(
            "deferred".into(),
            "source-transfer".into(),
            Message::text("hydrated"),
        )
        .await
        .unwrap();
    assert_eq!(done.disposition, Disposition::Accepted);
    assert!(
        ingress
            .complete_preparation(
                "deferred".into(),
                "source-transfer".into(),
                Message::text("hydrated")
            )
            .await
            .unwrap()
            .repeated
    );
    assert_eq!(
        ingress
            .complete_preparation(
                "deferred".into(),
                "source-transfer".into(),
                Message::text("different")
            )
            .await
            .unwrap_err(),
        GatewayError::Conflict
    );
    assert_eq!(reply(&resumed.sent().await.attempt), "answer hydrated");
    assert_eq!(reply(&resumed.sent().await.attempt), "answer later");
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn leased_acknowledgement_waits_for_preparation_and_uses_the_latest_token() {
    use AcknowledgementOperation::{Acknowledge, Extend};
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "leased-intake", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let item = HandoffItem {
        event: dm("lease-event", "one", ""),
        preparation: Some(Preparation {
            token: "stable-source".into(),
            purpose: PreparationPurpose::Body,
            expires_ms: expires(),
            protection: None,
        }),
    };
    let lease = |token: &str, bytes| AcknowledgementLease {
        token: token.into(),
        expires_ms: expires(),
        payload_bytes: bytes,
    };
    ingress
        .submit(Vec::new(), Some(advance(None, "baseline")))
        .await
        .unwrap();
    ingress
        .begin_repair(RepairWindow {
            id: "open-gap".into(),
            stream: "inbox".into(),
            room: item.event.room.clone(),
            last_committed: "baseline".into(),
            expires_ms: expires(),
            limit: 8,
        })
        .await
        .unwrap();
    assert!(matches!(
        ingress
            .stage_leased_input(item.clone(), lease("precommit-token", 10))
            .await,
        Err(GatewayError::TemporarilyUnavailable)
    ));
    assert!(input(&gateway.messaging_status().await.unwrap(), "lease-event").is_none());
    ingress
        .finish_repair("open-gap".into(), None)
        .await
        .unwrap();

    let first = ingress
        .stage_leased_input(item.clone(), lease("first-token", 16 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(first.receipt.disposition, Disposition::Deferred);
    assert_eq!(first.token_for(Acknowledge).await.unwrap(), None);
    assert_eq!(first.token_for(Extend).await.unwrap(), Some("first-token"));
    assert!(first.record_extension(expires() + 60_000).await.unwrap());
    let other = HandoffItem {
        event: dm("bounded", "two", "later"),
        preparation: None,
    };
    assert!(matches!(
        ingress
            .stage_leased_input(other, lease("over-budget", 1))
            .await,
        Err(GatewayError::TemporarilyUnavailable)
    ));
    let latest = ingress
        .stage_leased_input(item.clone(), lease("latest-token", 10))
        .await
        .unwrap();
    assert!(latest.receipt.repeated);
    assert_eq!(first.token_for(Extend).await.unwrap(), None);
    assert_eq!(
        ingress
            .authorize_preparation("lease-event".into(), "forged".into())
            .await,
        Err(GatewayError::Forbidden)
    );
    ingress
        .authorize_preparation("lease-event".into(), "stable-source".into())
        .await
        .unwrap();
    ingress
        .complete_preparation(
            "lease-event".into(),
            "stable-source".into(),
            Message::text("ready"),
        )
        .await
        .unwrap();
    assert_eq!(latest.token_for(Extend).await.unwrap(), None);
    assert_eq!(
        latest.token_for(Acknowledge).await.unwrap(),
        Some("latest-token")
    );
    let send = driver.sent().await;
    status(&gateway, |state| {
        delivery(state, &send.attempt.delivery_id, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "leased-intake", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    assert_eq!(
        latest.token_for(Acknowledge).await,
        Err(GatewayError::StaleConnection)
    );
    let repeat = ingress
        .stage_leased_input(item, lease("after-restart", 10))
        .await
        .unwrap();
    assert!(repeat.receipt.repeated);
    assert_eq!(repeat.receipt.disposition, Disposition::Accepted);
    assert_eq!(
        repeat.token_for(Acknowledge).await.unwrap(),
        Some("after-restart")
    );
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn compatible_burst_keeps_original_identities_and_separates_same_text_events() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "bursts", false);
    let mut access = policy();
    access.users.insert("bob".into());
    let gateway = model
        .gateway(root.path(), vec![driver.config(access.clone())])
        .await;
    let ingress = driver.ready().await;
    let mut first = dm("one", "chat", "line");
    first.reply_anchor = None;
    let mut second = dm("two", "chat", "line");
    second.reply_anchor = None;
    let group = handoff(
        "two-items",
        vec![first.clone(), second.clone()],
        Some(advance(None, "observed")),
    );
    let mut different = second.clone();
    different.sender.id = "bob".into();
    assert_eq!(
        ingress
            .stage_text_burst(handoff("different-sender", vec![first, different], None))
            .await
            .unwrap_err(),
        GatewayError::Invalid
    );
    let result = ingress.stage_text_burst(group.clone()).await.unwrap();
    assert_eq!(result.receipts[0].disposition, Disposition::Accepted);
    assert_eq!(result.receipts[1].disposition, Disposition::Coalesced);
    let sent = driver.sent().await;
    assert_eq!(reply(&sent.attempt), "answer line\nline");
    status(&gateway, |state| {
        delivery(state, &sent.attempt.delivery_id, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "bursts", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(access)])
        .await;
    let ingress = driver.ready().await;
    let repeat = ingress.stage_text_burst(group).await.unwrap();
    assert!(repeat.receipts.iter().all(|receipt| receipt.repeated));
    assert_eq!(repeat.checkpoint.as_deref(), Some("observed"));
    let mut separate = second.clone();
    separate.id = "three".into();
    let result = ingress
        .submit(
            vec![second, separate],
            Some(advance(Some("observed"), "later")),
        )
        .await
        .unwrap();
    assert_eq!(result.receipts[0].disposition, Disposition::Coalesced);
    assert!(result.receipts[0].repeated);
    assert_eq!(result.receipts[1].disposition, Disposition::Accepted);
    assert_eq!(reply(&driver.sent().await.attempt), "answer line");
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn repair_merges_live_and_history_in_source_order_after_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "repair", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    driver
        .frame(
            vec![dm("baseline", "room", "baseline")],
            Some(advance(None, "known")),
        )
        .await
        .unwrap();
    driver.sent().await;
    let older = dm("earlier", "room", "earlier");
    let newer = dm("newer", "room", "newer");
    let window = RepairWindow {
        id: "gap".into(),
        stream: "inbox".into(),
        room: older.room.clone(),
        last_committed: "known".into(),
        expires_ms: expires(),
        limit: 8,
    };
    ingress.begin_repair(window).await.unwrap();
    let first = RepairPage {
        id: "first".into(),
        previous: None,
        items: vec![RepairItem {
            source_order: 20,
            event: newer.clone(),
        }],
    };
    ingress
        .stage_repair_page("gap".into(), first.clone())
        .await
        .unwrap();
    assert_eq!(
        ingress.submit(vec![newer.clone()], None).await.unwrap_err(),
        GatewayError::CheckpointChanged
    );
    assert!(driver.sent.try_recv().is_err());
    gateway.shutdown().await.unwrap();
    let driver = Driver::new(Transport::Poll, "repair", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    assert_eq!(ingress.pending_repairs().await.unwrap()[0].staged, 1);
    assert_eq!(
        ingress
            .stage_repair_page("gap".into(), first)
            .await
            .unwrap()
            .staged,
        1
    );
    let second = RepairPage {
        id: "second".into(),
        previous: Some("first".into()),
        items: vec![
            RepairItem {
                source_order: 10,
                event: older,
            },
            RepairItem {
                source_order: 20,
                event: newer,
            },
        ],
    };
    assert_eq!(
        ingress
            .stage_repair_page("gap".into(), second)
            .await
            .unwrap()
            .staged,
        2
    );
    let recovered = ingress
        .finish_repair("gap".into(), Some(advance(Some("known"), "repaired")))
        .await
        .unwrap();
    assert!(!recovered.gap);
    assert_eq!(recovered.checkpoint.as_deref(), Some("repaired"));
    assert_eq!(reply(&driver.sent().await.attempt), "answer earlier");
    assert_eq!(reply(&driver.sent().await.attempt), "answer newer");
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn attributed_producer_result_keeps_its_original_audience() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "attributed-results", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let input = driver
        .push(dm("original", "private", "scoped result"))
        .await;
    let source_route = input.route.unwrap();
    let source = driver.sent().await.attempt.delivery_id;
    status(&gateway, |state| {
        delivery(state, &source, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    let other = dm("other", "different", "unrelated");
    let other_route = ingress
        .verify_destination(other.room.clone(), expires())
        .await
        .unwrap();

    let withheld = gateway
        .publish_messaging_result(&source, &other_route, "external-audience")
        .unwrap();
    assert_eq!(
        gateway
            .publish_messaging_result(&source, &other_route, "external-audience")
            .unwrap(),
        withheld
    );
    let published = gateway
        .publish_messaging_result(&source, &source_route, "original-audience")
        .unwrap();
    let delivery = driver.sent().await;
    assert_eq!(delivery.attempt.delivery_id, published);
    assert_eq!(delivery.attempt.payload.room.chat, "private");
    assert_eq!(reply(&delivery.attempt), "answer scoped result");
    driver.push(other).await;
    assert_eq!(reply(&driver.sent().await.attempt), "answer unrelated");
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "attributed-results", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    driver.ready().await;
    assert_eq!(
        gateway
            .publish_messaging_result(&source, &other_route, "external-audience")
            .unwrap(),
        withheld
    );
    let state = gateway.messaging_status().await.unwrap();
    let held = state["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == withheld)
        .unwrap();
    assert_eq!(held["state"], "blocked");
    assert_eq!(held["error"], "source_audience_unauthorized");
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_input_aliases_share_one_receipt_and_cannot_merge_unrelated_inputs() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "logical-inputs", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let observe = |id| {
        let mut event = dm(id, "chat", "same text");
        event.reply_anchor = Some("verified-anchor".into());
        event
    };
    let logical = InputAlias {
        namespace: "message".into(),
        id: "message-one".into(),
    };
    let first = ingress
        .submit_aliased(observe("received"), vec![logical.clone()], None)
        .await
        .unwrap();
    let first = &first.receipts[0];
    assert_eq!(first.disposition, Disposition::Accepted);
    let first_send = driver.sent().await;
    status(&gateway, |state| {
        delivery(state, &first_send.attempt.delivery_id, 0)
            .is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    let repeat = ingress
        .submit_aliased(observe("published"), vec![logical.clone()], None)
        .await
        .unwrap();
    assert!(repeat.receipts[0].repeated);
    assert_eq!(repeat.receipts[0].id, first.id);
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "logical-inputs", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    assert!(driver.push(observe("published")).await.repeated);
    assert_eq!(
        driver.push(observe("separate")).await.disposition,
        Disposition::Accepted
    );
    let other = driver.sent().await;
    assert_eq!(reply(&other.attempt), "answer same text");
    let conflict = || ingress.submit_aliased(observe("separate"), vec![logical.clone()], None);
    let first_conflict = conflict().await.unwrap();
    assert_eq!(
        first_conflict.receipts[0].disposition,
        Disposition::Conflict
    );
    assert!(!first_conflict.receipts[0].repeated);
    assert!(conflict().await.unwrap().receipts[0].repeated);
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn timed_poll_reopens_an_inclusive_boundary_and_keeps_saturation_visible() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "time-poll", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let item = |timestamp_ms, id: &str| TimedEvent {
        timestamp_ms,
        event: dm(id, "room", id),
    };
    let checkpoint = ingress
        .open_time_checkpoint("timed-inbox".into(), 100, 15)
        .await
        .unwrap();
    let first = ingress
        .submit_timed(
            "timed-inbox".into(),
            checkpoint,
            vec![item(110, "first"), item(99, "historical")],
            TimeCoverage::Complete { through_ms: 110 },
        )
        .await
        .unwrap();
    assert_eq!(first.receipts[0].disposition, Disposition::Historical);
    assert_eq!(first.checkpoint.boundary_ids, ["first".to_owned()].into());
    assert_eq!(first.checkpoint.discovery_baseline_ms, 100);
    let send = driver.sent().await;
    assert_eq!(reply(&send.attempt), "answer first");
    status(&gateway, |state| {
        delivery(state, &send.attempt.delivery_id, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Poll, "time-poll", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let old = ingress
        .open_time_checkpoint("timed-inbox".into(), 100, 15)
        .await
        .unwrap();
    assert_eq!(old, first.checkpoint);
    let live = item(110, "live");
    let duplicate = item(110, "first");
    driver.push(live.event.clone()).await;
    assert_eq!(reply(&driver.sent().await.attempt), "answer live");
    let saturated = ingress
        .submit_timed(
            "timed-inbox".into(),
            old.clone(),
            vec![live.clone(), duplicate.clone()],
            TimeCoverage::Saturated,
        )
        .await
        .unwrap();
    assert!(saturated.gap);
    assert_eq!(saturated.checkpoint, old);
    assert!(saturated.receipts.iter().all(|receipt| receipt.repeated));
    let merged = ingress
        .submit_timed(
            "timed-inbox".into(),
            old,
            vec![live, duplicate, item(110, "another")],
            TimeCoverage::Complete { through_ms: 110 },
        )
        .await
        .unwrap();
    assert_eq!(
        merged.checkpoint.boundary_ids,
        ["another".to_owned(), "first".to_owned(), "live".to_owned()].into()
    );
    assert_eq!(merged.receipts.iter().filter(|r| !r.repeated).count(), 1);
    assert_eq!(reply(&driver.sent().await.attempt), "answer another");
    assert_eq!(model.requests().len(), 3);
    assert_eq!(
        gateway.messaging_status().await.unwrap()["connections"][0]["gap"],
        true
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_conversation_keeps_destinations_and_command_outcomes_scoped() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "shared-destinations", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let first = dm("first", "one", "first");
    let second = dm("second", "two", "second");
    let anchor = ingress
        .verify_destination(first.room.clone(), expires())
        .await
        .unwrap();
    let member = ingress
        .verify_destination(second.room.clone(), expires())
        .await
        .unwrap();
    gateway
        .bind_messaging_conversation(&member, &anchor)
        .await
        .unwrap();
    let set_model = InboundEvent {
        body: InboundBody::Command(Command::SelectModel {
            model: "second-model".into(),
        }),
        ..dm("change-model", "two", "")
    };
    driver.push(set_model.clone()).await;
    applied(&gateway, "change-model", "command_complete").await;
    assert_eq!(
        ingress
            .invocation_outcome(set_model.id, set_model.room, set_model.sender)
            .await
            .unwrap(),
        Some(InvocationOutcome::Applied)
    );
    driver.push(first).await;
    driver.push(second).await;
    assert_eq!(driver.sent().await.attempt.payload.room.chat, "one");
    assert_eq!(driver.sent().await.attempt.payload.room.chat, "two");
    let final_state = status(&gateway, |s| {
        input(s, "second").is_some_and(|row| row["state"] == "done")
    })
    .await;
    let first = input(&final_state, "first").unwrap();
    let second = input(&final_state, "second").unwrap();
    assert_eq!(first["session"], second["session"]);
    assert!(first["session"].is_string());
    assert_ne!(first["route"], second["route"]);
    assert_eq!(model.requests()[0]["model"], "fixture");
    assert_eq!(model.requests()[1]["model"], "second-model");
    let id = gateway
        .send_messaging_notification(&member, "manual-1", "notice".into())
        .unwrap();
    assert_eq!(
        id,
        gateway
            .send_messaging_notification(&member, "manual-1", "notice".into())
            .unwrap()
    );
    assert!(
        gateway
            .send_messaging_notification(&member, "manual-1", "changed".into())
            .is_err()
    );
    assert_eq!(driver.sent().await.attempt.delivery_id, id);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_native_result_survives_restart_and_is_claimed_once() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "result-actions", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let original = dm("question", "one", "steer wait");
    driver.push(original.clone()).await;
    wait_for_model(&model, "steer wait").await;
    let action = ingress
        .defer_result(
            original.id.clone(),
            original.room.clone(),
            original.sender.clone(),
            expires(),
        )
        .await
        .unwrap();
    model.release_waiting_turn();
    status(&gateway, |s| {
        input(s, "question").is_some_and(|row| row["state"] == "done")
    })
    .await;
    assert!(driver.sent.try_recv().is_err());
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "result-actions", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let fetch = |id, chat| InboundEvent {
        body: InboundBody::Command(Command::FetchResult {
            action: action.id.clone(),
        }),
        ..dm(id, chat, "")
    };
    let wrong = fetch("wrong", "two");
    driver.push(wrong.clone()).await;
    applied(&gateway, "wrong", "command_complete").await;
    assert_eq!(
        ingress
            .invocation_outcome(wrong.id, wrong.room, wrong.sender)
            .await
            .unwrap(),
        Some(InvocationOutcome::Forbidden)
    );
    let correct = fetch("retrieve", "one");
    driver.push(correct.clone()).await;
    applied(&gateway, "retrieve", "command_complete").await;
    let receipt = ingress
        .invocation_outcome(
            correct.id.clone(),
            correct.room.clone(),
            correct.sender.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        receipt,
        Some(InvocationOutcome::ResultReady { .. })
    ));
    assert!(driver.push(correct.clone()).await.repeated);
    assert!(
        ingress
            .release_result(
                action.id.clone(),
                correct.id.clone(),
                correct.room.clone(),
                correct.sender.clone()
            )
            .await
            .unwrap()
    );
    let sent = driver.sent().await;
    assert_eq!(
        receipt,
        Some(InvocationOutcome::ResultReady {
            delivery: sent.attempt.delivery_id
        })
    );
    assert!(
        !ingress
            .release_result(action.id, correct.id, correct.room, correct.sender)
            .await
            .unwrap()
    );
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn wake_only_recovers_new_work_without_a_message_send() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let make = || {
        let mut driver = Driver::new(Transport::Push, "wake-only", false);
        driver.fixture.capabilities = Capabilities {
            wake: true,
            ..Default::default()
        };
        driver
    };
    let driver = make();
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    assert_eq!(
        ingress.wake("first".into()).await.unwrap_err(),
        GatewayError::TemporarilyUnavailable
    );
    gateway
        .set_messaging_wake_target(
            "fixture",
            WakeTarget {
                target: "inbox".into(),
                instructions: "steer wait".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        ingress.wake("first".into()).await.unwrap().disposition,
        Disposition::Accepted
    );
    wait_for_model(&model, "steer wait").await;
    assert!(ingress.wake("first".into()).await.unwrap().repeated);
    assert_eq!(
        ingress.wake("second".into()).await.unwrap().disposition,
        Disposition::Accepted
    );
    model.release_waiting_turn();
    model.release_waiting_turn();
    status(&gateway, |s| {
        ["first", "second"]
            .iter()
            .all(|id| input(s, id).is_some_and(|r| r["state"] == "done"))
    })
    .await;
    assert!(driver.sent.try_recv().is_err());
    assert!(
        ingress
            .drain_activity(16)
            .await
            .unwrap()
            .events
            .iter()
            .any(|e| e.status == WakeStatus::Complete)
    );
    gateway.shutdown().await.unwrap();
    let driver = make();
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    assert!(ingress.wake("second".into()).await.unwrap().repeated);
    assert_eq!(model.requests().len(), 2);
    assert!(!ingress.capabilities().text_delivery);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn prompt_decision_is_durable_and_reuse_checks_the_current_route_policy() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let mut policy = policy();
    policy.direct_tools = true;
    policy.scope.direct.turn.approval = Some(ApprovalPolicy::Manual);
    policy.scope.direct.turn.approval_reuse = Some(ApprovalReuse::Persistent);
    let driver = Driver::new(Transport::Push, "approval-reuse", true);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy.clone())])
        .await;
    driver.ready().await;
    driver.push(dm("first", "one", "approval")).await;
    let prompt = driver.sent().await;
    let id = prompt_id(&prompt.attempt).to_owned();
    assert!(matches!(
        prompt.attempt.payload.body,
        DeliveryBody::Prompt {
            prompt: Prompt::Approval {
                allow_session: true,
                allow_persistent: true,
                ..
            },
            ..
        }
    ));
    prompt.finish(confirmed("approval-card"));
    let approve = control(
        "approve",
        "one",
        Control::Respond {
            action_id: id,
            response: Response::ApprovePersistently,
        },
    );
    driver.push(approve.clone()).await;
    applied(&gateway, "approve", "applied").await;
    let first = driver.sent().await;
    let delivery_id = first.attempt.delivery_id.clone();
    first.finish(confirmed("first-reply"));
    status(&gateway, |state| {
        delivery(state, &delivery_id, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Push, "approval-reuse", true);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy.clone())])
        .await;
    let ingress = driver.ready().await;
    assert!(driver.push(approve.clone()).await.repeated);
    assert_eq!(
        ingress
            .callback_outcome(
                approve.id.clone(),
                approve.room.clone(),
                approve.sender.clone(),
                expires()
            )
            .await
            .unwrap(),
        Some(InvocationOutcome::Applied)
    );
    assert_eq!(
        ingress
            .callback_outcome(approve.id, approve.room, approve.sender, 1)
            .await
            .unwrap(),
        Some(InvocationOutcome::Expired)
    );
    driver.push(dm("second", "one", "approval")).await;
    let second = driver.sent().await;
    assert!(matches!(
        second.attempt.payload.body,
        DeliveryBody::Reply(_)
    ));
    second.finish(confirmed("second-reply"));
    policy.scope.direct.turn.approval_reuse = Some(ApprovalReuse::Session);
    gateway.set_adapter_policy("fixture", policy).await.unwrap();
    driver.push(dm("third", "one", "approval")).await;
    let prompt = driver.sent().await;
    let id = prompt_id(&prompt.attempt).to_owned();
    assert!(matches!(
        prompt.attempt.payload.body,
        DeliveryBody::Prompt {
            prompt: Prompt::Approval {
                allow_persistent: false,
                ..
            },
            ..
        }
    ));
    prompt.finish(confirmed("next-prompt"));
    driver
        .push(control(
            "too-broad",
            "one",
            Control::Respond {
                action_id: id.clone(),
                response: Response::ApprovePersistently,
            },
        ))
        .await;
    applied(&gateway, "too-broad", "forbidden").await;
    driver
        .push(control(
            "deny",
            "one",
            Control::Respond {
                action_id: id,
                response: Response::Deny,
            },
        ))
        .await;
    applied(&gateway, "deny", "applied").await;
    driver.sent().await.finish(confirmed("third-reply"));
    gateway.shutdown().await.unwrap();
}
