use super::super::settings::SEE_CHANNEL;
use super::*;

pub(super) fn thread_request(offset: u64, name: &str, starter: Option<&str>) -> Value {
    let id = (((now() - 1_420_070_400_000) as u64) << 22) + offset;
    let mut options = vec![
        json!({"name":"name","value":name}),
        json!({"name":"auto_archive_duration","value":60}),
    ];
    if let Some(starter) = starter {
        options.push(json!({"name":"message","value":starter}));
    }
    json!({
        "id":id.to_string(),"application_id":"90","type":2,"token":format!("callback-{id}"),
        "channel_id":"21","guild_id":"11","app_permissions":"8",
        "member":{"roles":[],"permissions":"8","user":{"id":"7"}},
        "data":{"name":"thread","options":options}
    })
}

async fn acknowledge(fixture: &mut Fixtures) {
    let Write::Callback(payload) = fixture
        .next_write("a private thread acknowledgement", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(payload, json!({"type":5,"data":{"flags":64}}));
}

async fn outcome(fixture: &mut Fixtures, expected: &str) {
    let Write::Followup(payload) = fixture
        .next_write("the private thread outcome", |write| {
            matches!(write, Write::Followup(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(payload["content"], expected);
    assert_eq!(payload["allowed_mentions"]["parse"], json!([]));
}

async fn gateway(fixture: &mut Fixtures, model: &Model, root: &Path) -> (RunningGateway, Wire) {
    let gateway = model.gateway(root, vec![fixture.configuration(root)]).await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "manual-thread").await;
    (gateway, wire)
}

#[tokio::test]
async fn discord_manual_thread_orders_quiet_publication_and_replays_the_private_result() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let (gateway, wire) = gateway(&mut fixture, &model, root.path()).await;
    let request = thread_request(2, "Release review", Some("Review it with @everyone"));
    wire.dispatch(2, "INTERACTION_CREATE", request.clone())
        .await;

    let mut marker = None;
    let mut started = false;
    let mut published = false;
    let mut acknowledged = false;
    loop {
        match fixture.write().await {
            Write::Callback(payload) => {
                assert_eq!(payload, json!({"type":5,"data":{"flags":64}}));
                acknowledged = true;
            }
            Write::Message {
                channel,
                payload,
                id,
            } if channel == "21" => {
                assert!(marker.is_none());
                assert_eq!(payload["content"], "Discussion requested.");
                assert_eq!(payload["allowed_mentions"]["parse"], json!([]));
                assert_eq!(payload["enforce_nonce"], true);
                marker = Some(id);
            }
            Write::Thread {
                parent,
                starter,
                payload,
            } => {
                assert!(!started);
                assert_eq!(parent, "21");
                assert_eq!(Some(starter), marker);
                assert_eq!(
                    payload,
                    json!({"name":"Release review","auto_archive_duration":60})
                );
                started = true;
            }
            Write::Message {
                channel, payload, ..
            } => {
                assert!(started && !published);
                assert_eq!(Some(channel), marker);
                assert_eq!(payload["content"], "Review it with @everyone");
                assert_eq!(payload["allowed_mentions"]["parse"], json!([]));
                assert_eq!(payload["enforce_nonce"], true);
                published = true;
            }
            Write::VoiceNote { .. } => panic!("manual thread unexpectedly sent a voice note"),
            Write::Followup(payload) => {
                assert!(acknowledged && published);
                assert_eq!(
                    payload["content"],
                    format!("Created <#{}>.", marker.as_ref().unwrap())
                );
                break;
            }
        }
    }
    wire.dispatch(3, "INTERACTION_CREATE", request.clone())
        .await;
    acknowledge(&mut fixture).await;
    outcome(&mut fixture, &format!("Created <#{}>.", marker.unwrap())).await;
    let id = request["id"].as_str().unwrap();
    let paths = crate::lock(&fixture.transport.callback_paths).clone();
    assert_eq!(
        paths,
        [
            format!("interactions/{id}/callback-{id}/callback"),
            format!("webhooks/90/callback-{id}/messages/@original"),
            format!("interactions/{id}/callback-{id}/callback"),
            format!("webhooks/90/callback-{id}/messages/@original"),
        ]
    );
    assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
    assert_eq!(crate::lock(&fixture.transport.messages).len(), 2);
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_manual_thread_refuses_live_permission_loss_and_unrelated_child_evidence() {
    for revoke in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
        let mut fixture = Fixtures::start().await;
        let (gateway, wire) = gateway(&mut fixture, &model, root.path()).await;

        if revoke {
            *crate::lock(&fixture.transport.thread_requester_permissions) = Some(SEE_CHANNEL);
        } else {
            crate::lock(&fixture.transport.threads).insert(
                "500".into(),
                json!({"id":"500","parent_id":"23","guild_id":"11","owner_id":"9","type":11}),
            );
        }

        wire.dispatch(
            2,
            "INTERACTION_CREATE",
            thread_request(2, "Review", Some("child only")),
        )
        .await;
        acknowledge(&mut fixture).await;
        outcome(&mut fixture, "The thread was not created.").await;
        assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 0);
        {
            let messages = crate::lock(&fixture.transport.messages);
            assert_eq!(messages.len(), usize::from(!revoke));
            assert!(messages.keys().all(|(channel, _)| channel == "21"));
        }
        assert!(model.requests().is_empty());
        gateway.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn discord_manual_thread_reconciles_the_original_ambiguous_create_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    fixture.transport.counter.store(400, Ordering::SeqCst);
    let (gateway, wire) = gateway(&mut fixture, &model, root.path()).await;
    wire.dispatch(
        2,
        "INTERACTION_CREATE",
        thread_request(2, "Review", Some("child only")),
    )
    .await;
    wait_status(&gateway, |status| {
        status["deliveries"].as_array().is_some_and(|deliveries| {
            deliveries
                .iter()
                .any(|delivery| delivery["part_state"] == "uncertain")
        })
    })
    .await;
    assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
    assert_eq!(crate::lock(&fixture.transport.messages).len(), 1);
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 2);
    wire.dispatch(3, "RESUMED", json!({})).await;
    crate::lock(&fixture.transport.threads).insert(
        "400".into(),
        json!({"id":"400","parent_id":"21","guild_id":"11","owner_id":"9","type":11}),
    );
    let Write::Message {
        channel, payload, ..
    } = fixture
        .next_write(
            "starter in the exact recovered thread",
            |write| matches!(write, Write::Message { channel, .. } if channel == "400"),
        )
        .await
    else {
        unreachable!();
    };
    assert_eq!(channel, "400");
    assert_eq!(payload["content"], "child only");
    wait_status(&gateway, |status| {
        status["deliveries"].as_array().is_some_and(|deliveries| {
            deliveries
                .iter()
                .any(|delivery| delivery["state"] == "sent")
        })
    })
    .await;
    assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
    assert_eq!(crate::lock(&fixture.transport.messages).len(), 2);
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}
