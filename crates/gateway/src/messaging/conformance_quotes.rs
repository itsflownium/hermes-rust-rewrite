use super::*;

fn quote_handoff(id: &str, chat: &str, source_room: Option<Room>) -> Handoff {
    let event = dm(id, chat, "Explain the quoted message");
    let source = QuoteReference {
        room: source_room.unwrap_or_else(|| event.room.clone()),
        message_id: "prior-message-id".into(),
    };
    Handoff {
        id: format!("handoff-{id}"),
        items: vec![HandoffItem {
            event,
            preparation: Some(Preparation {
                token: "context-transfer".into(),
                purpose: PreparationPurpose::Quote(source),
                expires_ms: chrono::Utc::now().timestamp_millis() + 60_000,
                protection: None,
            }),
        }],
        advance: None,
    }
}

fn protected_quote(id: &str, source_room: Option<Room>) -> Handoff {
    let mut handoff = quote_handoff(id, "one", source_room);
    handoff.items[0].event.body = InboundBody::Message(Message::text(""));
    handoff.items[0].preparation.as_mut().unwrap().protection = Some("encrypted".into());
    handoff
}

const TOKEN: &str = "context-transfer";

#[tokio::test]
async fn quoted_context_recovers_with_exact_provenance_and_its_own_checkpoint() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Poll, "quote-account", false);
    let mut protected_policy = policy();
    protected_policy
        .scope
        .rooms
        .entry("one".into())
        .or_default()
        .delivery = Some(DeliveryPolicy {
        protection: Some("encrypted".into()),
        ..Default::default()
    });
    let gateway = model
        .gateway(root.path(), vec![driver.config(protected_policy.clone())])
        .await;
    let initial = driver.ready().await;
    let mut batch = protected_quote("question", None);
    batch.advance = Some(advance(None, "durable-input"));
    let receipt = initial.stage_handoff(batch.clone()).await.unwrap();
    assert_eq!(receipt.checkpoint.as_deref(), Some("durable-input"));
    assert_eq!(receipt.receipts[0].disposition, Disposition::Deferred);
    gateway.shutdown().await.unwrap();

    let driver = Driver::new(Transport::Poll, "quote-account", false);
    let other = Driver::new(Transport::Poll, "other-quote-account", false);
    let mut other_config = other.config(policy());
    other_config.name = "other".into();
    let gateway = model
        .gateway(
            root.path(),
            vec![driver.config(protected_policy), other_config],
        )
        .await;
    let ingress = driver.ready().await;
    let other = other.ready().await;
    assert_eq!(
        other
            .hydrate_quote("question", TOKEN, |_| async {
                panic!("A different account reached the protected quote source")
            })
            .await
            .unwrap_err(),
        GatewayError::Forbidden,
    );
    assert_eq!(ingress.pending_preparations().await.unwrap().len(), 1);
    let room = batch.items[0].event.room.clone();
    assert!(ingress.stage_handoff(batch).await.unwrap().receipts[0].repeated);
    assert_eq!(
        ingress
            .hydrate_quote("question", TOKEN, |_| async {
                panic!("An unprotected connection reached the protected quote source")
            })
            .await
            .unwrap_err(),
        GatewayError::ProtectionUnavailable,
    );
    ingress
        .update_delivery_protection(room, Some("encrypted".into()))
        .await
        .unwrap();
    assert_eq!(
        ingress
            .authorize_preparation("question".into(), TOKEN.into())
            .await
            .unwrap_err(),
        GatewayError::Forbidden,
    );
    assert_eq!(
        ingress
            .complete_preparation(
                "question".into(),
                TOKEN.into(),
                Message::text("unsupported")
            )
            .await
            .unwrap_err(),
        GatewayError::Unsupported,
    );
    assert_eq!(
        ingress
            .hydrate_quote("question", TOKEN, |mut source| async move {
                source.message_id = "different-upstream-item".into();
                Ok(QuoteHydration {
                    message: None,
                    quote: QuoteLookup::Found {
                        source,
                        text: "unverified text".into(),
                    },
                    advance: None,
                })
            })
            .await
            .unwrap_err(),
        GatewayError::Invalid,
    );
    assert_eq!(
        ingress
            .quote_checkpoint("question", TOKEN, "inbox")
            .await
            .unwrap(),
        None,
    );
    let accepted = ingress
        .hydrate_quote("question", TOKEN, |source| async move {
            assert_eq!(source.room.chat, "one");
            assert_eq!(source.message_id, "prior-message-id");
            Ok(QuoteHydration {
                message: Some(Message::text("Explain the quoted message")),
                quote: QuoteLookup::Found {
                    source,
                    text: "authorized quoted text".into(),
                },
                advance: Some(advance(None, "durable-context")),
            })
        })
        .await
        .unwrap();
    assert_eq!(accepted.disposition, Disposition::Accepted);
    assert!(
        ingress
            .hydrate_quote("question", TOKEN, |_| async {
                panic!("A committed quote was read again")
            })
            .await
            .unwrap()
            .repeated,
    );
    assert_eq!(
        ingress.checkpoint("inbox".into()).await.unwrap().as_deref(),
        Some("durable-input"),
    );
    assert!(reply(&driver.sent().await.attempt).contains("authorized quoted text"));
    let request = &model.requests()[0];
    let messages = request["messages"].as_array().unwrap();
    assert!(
        messages[0]["content"]
            .as_str()
            .is_some_and(|text| !text.contains("authorized quoted text"))
    );
    assert!(
        messages.last().unwrap()["content"]
            .as_str()
            .is_some_and(|text| text.contains("Explain the quoted message")
                && text.contains("authorized quoted text"))
    );

    let followup = protected_quote("unavailable", None);
    assert_eq!(
        ingress.stage_handoff(followup).await.unwrap().receipts[0].disposition,
        Disposition::Deferred,
    );
    assert_eq!(
        ingress
            .quote_checkpoint("unavailable", TOKEN, "inbox")
            .await
            .unwrap()
            .as_deref(),
        Some("durable-context"),
    );
    let unchanged = ingress
        .hydrate_quote("unavailable", TOKEN, |_| async {
            Ok(QuoteHydration {
                message: Some(Message::text("Quote may be missing")),
                quote: QuoteLookup::Unavailable,
                advance: Some(advance(None, "old-position")),
            })
        })
        .await
        .unwrap_err();
    assert_eq!(unchanged, GatewayError::CheckpointChanged);
    ingress
        .hydrate_quote("unavailable", TOKEN, |_| async {
            Ok(QuoteHydration {
                message: Some(Message::text("Quote may be missing")),
                quote: QuoteLookup::Unavailable,
                advance: None,
            })
        })
        .await
        .unwrap();
    assert!(reply(&driver.sent().await.attempt).contains(r#""text":null"#));
    assert_eq!(model.requests().len(), 2);

    let event = dm("one", "one", "");
    let mut wrong_thread = event.room.clone();
    wrong_thread.thread = Some("another-thread".into());
    assert_eq!(
        ingress
            .stage_handoff(protected_quote("across-thread", Some(wrong_thread)))
            .await
            .unwrap()
            .receipts[0]
            .disposition,
        Disposition::Forbidden,
    );

    let expires = chrono::Utc::now().timestamp_millis() + 60_000;
    let anchor = ingress
        .verify_destination(event.room, expires)
        .await
        .unwrap();
    let member = ingress
        .verify_destination(dm("two", "two", "").room, expires)
        .await
        .unwrap();
    assert!(
        gateway
            .bind_messaging_conversation(&member, &anchor)
            .await
            .is_err()
    );

    let anchor = ingress
        .verify_destination(dm("three", "three", "").room, expires)
        .await
        .unwrap();
    gateway
        .bind_messaging_conversation(&member, &anchor)
        .await
        .unwrap();
    assert_eq!(
        ingress
            .stage_handoff(quote_handoff("share", "three", None))
            .await
            .unwrap()
            .receipts[0]
            .disposition,
        Disposition::Forbidden,
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoked_quote_cannot_commit_or_claim_and_does_not_pin_the_route() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let driver = Driver::new(Transport::Push, "quote-revocation", false);
    let gateway = model
        .gateway(root.path(), vec![driver.config(policy())])
        .await;
    let ingress = driver.ready().await;
    let staged = ingress
        .stage_handoff(quote_handoff("revoked-during-read", "one", None))
        .await
        .unwrap();
    assert_eq!(staged.receipts[0].disposition, Disposition::Deferred);
    let refused = ingress
        .hydrate_quote("revoked-during-read", TOKEN, |source| async {
            gateway
                .set_adapter_policy("fixture", AccessPolicy::default())
                .await
                .unwrap();
            Ok(QuoteHydration {
                message: None,
                quote: QuoteLookup::Found {
                    source,
                    text: "forbidden after read".into(),
                },
                advance: Some(advance(None, "forbidden-advance")),
            })
        })
        .await
        .unwrap();
    assert_eq!(refused.disposition, Disposition::Forbidden);
    assert!(model.requests().is_empty());
    gateway
        .set_adapter_policy("fixture", policy())
        .await
        .unwrap();

    driver.push(dm("busy", "one", "steer wait")).await;
    wait_for_model(&model, "steer wait").await;
    let staged = ingress
        .stage_handoff(quote_handoff("revoked-before-claim", "one", None))
        .await
        .unwrap();
    assert_eq!(staged.receipts[0].disposition, Disposition::Deferred);
    assert_eq!(
        ingress
            .quote_checkpoint("revoked-before-claim", TOKEN, "inbox")
            .await
            .unwrap(),
        None,
    );
    assert_eq!(
        ingress
            .hydrate_quote("revoked-before-claim", TOKEN, |source| async move {
                Ok(QuoteHydration {
                    message: None,
                    quote: QuoteLookup::Found {
                        source,
                        text: "forbidden at claim".into(),
                    },
                    advance: None,
                })
            })
            .await
            .unwrap()
            .disposition,
        Disposition::Accepted,
    );
    status(&gateway, |s| {
        input(s, "revoked-before-claim").is_some_and(|r| r["state"] == "admitted")
    })
    .await;

    // Change only another actor's grant. This input still has ordinary room
    // access, but its committed context binding no longer matches the policy.
    let mut changed = policy();
    changed
        .scope
        .denied_users
        .insert("newly-revoked-user".into());
    gateway
        .set_adapter_policy("fixture", changed)
        .await
        .unwrap();
    model.release_waiting_turn();
    driver.sent().await;
    status(&gateway, |s| {
        input(s, "revoked-before-claim").is_some_and(|r| r["state"] == "refused")
    })
    .await;
    driver.push(dm("later", "one", "after revoked quote")).await;
    assert_eq!(
        reply(&driver.sent().await.attempt),
        "answer after revoked quote"
    );
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}
