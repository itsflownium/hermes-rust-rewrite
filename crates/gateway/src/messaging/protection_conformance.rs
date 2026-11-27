use super::*;
use std::io::Cursor;

fn deferred(event: InboundEvent) -> Handoff {
    Handoff {
        id: "protected-handoff".into(),
        items: vec![HandoffItem {
            event,
            preparation: Some(Preparation {
                token: "provider-preparation".into(),
                purpose: PreparationPurpose::ProtectedContent,
                expires_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                protection: Some("encrypted".into()),
            }),
        }],
        advance: Some(advance(None, "protected-offset")),
    }
}

#[tokio::test]
async fn protected_input_waits_across_restart_and_requires_current_room_proof() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "protected-preparation", false);
    let event = dm("protected-event", "secure", "");
    let mut restricted = policy();
    restricted
        .scope
        .rooms
        .entry(event.room.chat.clone())
        .or_default()
        .delivery = Some(DeliveryPolicy {
        protection: Some("encrypted".into()),
        ..Default::default()
    });
    let gateway = model
        .gateway(root.path(), vec![driver.config(restricted)])
        .await;
    let ingress = driver.ready().await;
    assert_eq!(
        driver
            .push(dm("unverified-content", "secure", "cannot stage"))
            .await
            .disposition,
        Disposition::ProtectionUnavailable,
    );
    let mut eager = deferred(dm("eager", "secure", "cannot queue clear content"));
    eager.id = "eager-handoff".into();
    eager.advance = None;
    assert_eq!(
        ingress.stage_handoff(eager).await.unwrap().receipts[0].disposition,
        Disposition::Invalid,
    );
    let batch = deferred(event.clone());
    let receipt = ingress.stage_handoff(batch.clone()).await.unwrap();
    assert_eq!(receipt.receipts[0].disposition, Disposition::Deferred);
    assert_eq!(receipt.checkpoint.as_deref(), Some("protected-offset"));
    ingress
        .update_delivery_protection(event.room.clone(), Some("encrypted".into()))
        .await
        .unwrap();
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();

    let resumed = Driver::new(Transport::Poll, "protected-preparation", false);
    let gateway = model
        .gateway(root.path(), vec![resumed.config(policy())])
        .await;
    let ingress = resumed.ready().await;
    let replay = ingress.stage_handoff(batch).await.unwrap();
    assert!(replay.receipts[0].repeated);
    assert_eq!(replay.receipts[0].disposition, Disposition::Deferred);
    let pending = ingress.pending_preparations().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event, event);
    assert_eq!(
        pending[0].preparation.protection.as_deref(),
        Some("encrypted")
    );

    let other_room = dm("independent", "clear", "independent");
    ingress
        .update_delivery_protection(other_room.room.clone(), Some("encrypted".into()))
        .await
        .unwrap();
    let prepare = || {
        ingress.complete_preparation(
            event.id.clone(),
            pending[0].preparation.token.clone(),
            Message::text("protected content"),
        )
    };
    assert_eq!(
        prepare().await.unwrap_err(),
        GatewayError::ProtectionUnavailable
    );
    assert_eq!(
        resumed.push(other_room).await.disposition,
        Disposition::Accepted
    );
    assert_eq!(reply(&resumed.sent().await.attempt), "answer independent");

    let source = MediaSource {
        event_id: event.id.clone(),
        room: event.room.clone(),
        sender: event.sender.clone(),
        mentioned: event.mentioned,
    };
    let authorize = || {
        ingress.authorize_media(
            source.clone(),
            AttachmentManifest {
                name: "secure.txt".into(),
                size: Some(6),
                sha256: None,
            },
            MediaPurpose::Attachment,
            Duration::from_secs(30),
        )
    };
    assert!(matches!(
        authorize().await,
        Err(GatewayError::ProtectionUnavailable)
    ));
    ingress
        .update_delivery_protection(event.room.clone(), Some("encrypted".into()))
        .await
        .unwrap();
    let grant = authorize().await.unwrap();
    ingress
        .update_delivery_protection(event.room.clone(), None)
        .await
        .unwrap();
    let opened = std::cell::Cell::new(false);
    assert_eq!(
        ingress
            .stage_attachment(grant, || {
                opened.set(true);
                async { Ok(Cursor::new(b"secret")) }
            })
            .await
            .unwrap_err(),
        GatewayError::ProtectionUnavailable,
    );
    assert!(!opened.get());
    assert_eq!(
        prepare().await.unwrap_err(),
        GatewayError::ProtectionUnavailable
    );
    assert_eq!(ingress.pending_preparations().await.unwrap().len(), 1);
    assert_eq!(model.requests().len(), 1);

    ingress
        .update_delivery_protection(event.room.clone(), Some("encrypted".into()))
        .await
        .unwrap();
    let grant = authorize().await.unwrap();
    let artifact = ingress
        .stage_attachment(grant, || async { Ok(Cursor::new(b"secret")) })
        .await
        .unwrap();
    assert_eq!(artifact.name(), "secure.txt");
    assert_eq!(prepare().await.unwrap().disposition, Disposition::Accepted);
    assert!(prepare().await.unwrap().repeated);
    let sent = resumed.sent().await;
    assert_eq!(reply(&sent.attempt), "answer protected content");
    assert_eq!(
        sent.attempt.payload.effect.protection.as_deref(),
        Some("encrypted")
    );
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn protected_reply_holds_after_trust_revocation_even_if_owner_relaxes_policy() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "protected-reply", false);
    let event = dm("protected-turn", "secure", "steer wait");
    let mut restricted = policy();
    restricted
        .scope
        .rooms
        .entry(event.room.chat.clone())
        .or_default()
        .delivery = Some(DeliveryPolicy {
        protection: Some("encrypted".into()),
        ..Default::default()
    });
    let gateway = model
        .gateway(root.path(), vec![driver.config(restricted)])
        .await;
    let ingress = driver.ready().await;
    ingress
        .update_delivery_protection(event.room.clone(), Some("encrypted".into()))
        .await
        .unwrap();
    let route = driver.push(event.clone()).await.route.unwrap();
    wait_for_model(&model, "steer wait").await;

    gateway
        .set_adapter_policy("fixture", policy())
        .await
        .unwrap();
    ingress
        .update_delivery_protection(event.room.clone(), None)
        .await
        .unwrap();
    model.release_waiting_turn();

    status(&gateway, |s| {
        s["deliveries"].as_array().unwrap().iter().any(|part| {
            part["route"] == route
                && part["part_state"] == "blocked"
                && part["error"] == "eligibility_protection"
        })
    })
    .await;
    let clear = dm("clear-turn", "clear", "clear room");
    assert_eq!(driver.push(clear).await.disposition, Disposition::Accepted);
    assert_eq!(reply(&driver.sent().await.attempt), "answer clear room");
    assert!(driver.sent.try_recv().is_err());

    ingress
        .update_delivery_protection(event.room.clone(), Some("different".into()))
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(250), driver.sent.recv())
            .await
            .is_err()
    );
    ingress
        .update_delivery_protection(event.room, Some("encrypted".into()))
        .await
        .unwrap();
    let sent = driver.sent().await;
    assert_eq!(
        sent.attempt.payload.effect.protection.as_deref(),
        Some("encrypted")
    );
    assert_eq!(reply(&sent.attempt), "Working");
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}
