use super::*;

async fn verify_model_outcome(fixture: &mut Fixtures, expected: &str) {
    let Write::Callback(acknowledged) = fixture
        .next_write("private command acknowledgement", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!(acknowledged["type"], 5);
    assert_eq!(acknowledged["data"]["flags"], 64);
    let Write::Followup(result) = fixture
        .next_write("native command outcome", |write| {
            matches!(write, Write::Followup(_))
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!(result["content"], expected);
}

fn model_interaction(offset: u64, actor: &str, channel: &str, model: Option<&str>) -> Value {
    let id = (((now() - 1_420_070_400_000) as u64) << 22) + offset;
    let mut interaction = json!({
        "id":id.to_string(),"application_id":"90","type":2,"token":format!("callback-{id}"),
        "channel_id":channel,"data":{"name":"model","options":model.map(|model| vec![json!({"name":"model","value":model})]).unwrap_or_default()},
    });
    if channel == "20" {
        interaction["user"] = json!({"id":actor});
    } else {
        interaction["guild_id"] = json!("11");
        interaction["member"] = json!({"roles":[],"user":{"id":actor}});
    }
    interaction
}

async fn open_model_picker(
    wire: &Wire,
    fixture: &mut Fixtures,
    root: &Path,
    sequence: u64,
) -> Value {
    let request = model_interaction(sequence, "7", "20", None);
    let event = format!("discord-interaction-{}", request["id"].as_str().unwrap());
    wire.dispatch(sequence, "INTERACTION_CREATE", request).await;
    let Write::Callback(acknowledged) = fixture.write().await else {
        panic!("expected private picker acknowledgement");
    };
    assert_eq!(acknowledged["type"], 5);
    assert_eq!(acknowledged["data"]["flags"], 64);
    let Write::Followup(followup) = fixture.write().await else {
        panic!("expected a model picker");
    };
    let picker = followup
        .pointer("/components/0/components/0")
        .unwrap()
        .clone();
    let db = hermes_engine::db::open(root).unwrap();
    timeout(LIMIT, async {
        loop {
            let message: Option<String> = db
                .query_row(
                    "SELECT json_extract(outcome, '$.model_picker.message')
                 FROM messaging_native_invocations WHERE event = ?1",
                    [&event],
                    |row| row.get(0),
                )
                .unwrap();
            if message.as_deref() == Some("600") {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("picker is bound to the Discord message");
    picker
}

#[tokio::test]
async fn discord_model_selection_uses_the_native_catalog_and_fences_private_pickers() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7","8"]}, "operators":["7"],
        "guilds":{"11":{"channels":{"21":{}}}},"commands":{"sync":false}
    });
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), settings.clone())],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "model-choice").await;

    wire.dispatch(
        2,
        "INTERACTION_CREATE",
        model_interaction(2, "7", "20", Some("second-model")),
    )
    .await;
    verify_model_outcome(&mut fixture, "Applied.").await;
    wire.dispatch(
        3,
        "INTERACTION_CREATE",
        model_interaction(3, "7", "20", Some("unlisted-model")),
    )
    .await;
    verify_model_outcome(&mut fixture, "You aren't allowed to apply this action.").await;
    wire.dispatch(
        4,
        "INTERACTION_CREATE",
        model_interaction(4, "8", "21", Some("fixture")),
    )
    .await;
    verify_model_outcome(&mut fixture, "You aren't allowed to apply this action.").await;
    assert!(model.requests().is_empty());

    wire.dispatch(
        5,
        "MESSAGE_CREATE",
        message("701", "20", "after direct selection", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(payload["content"], "answer after direct selection");
    assert_eq!(model.requests()[0]["model"], "second-model");
    wait_reply(&gateway, "discord-message-701").await;

    let picker = open_model_picker(&wire, &mut fixture, root.path(), 6).await;
    let initial_option = picker["options"]
        .as_array()
        .unwrap()
        .iter()
        .find(|choice| choice["label"] == "fixture")
        .unwrap()["value"]
        .clone();
    let choose = |offset, picker: &Value, message: &str| {
        let mut interaction = model_interaction(offset, "7", "20", None);
        interaction["type"] = json!(3);
        interaction["data"] =
            json!({"component_type":3,"custom_id":picker["custom_id"],"values":[initial_option]});
        interaction["message"] = json!({"id":message,"author":{"id":"9","bot":true}});
        interaction
    };
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), settings.clone())],
        )
        .await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 6);
    wire.dispatch(7, "RESUMED", json!({})).await;
    wire.dispatch(8, "INTERACTION_CREATE", choose(8, &picker, "999"))
        .await;
    verify_model_outcome(&mut fixture, "You aren't allowed to apply this action.").await;
    wire.dispatch(9, "INTERACTION_CREATE", choose(9, &picker, "600"))
        .await;
    verify_model_outcome(&mut fixture, "Applied.").await;
    wire.dispatch(10, "INTERACTION_CREATE", choose(10, &picker, "600"))
        .await;
    verify_model_outcome(&mut fixture, "The result was already claimed.").await;

    let stale = open_model_picker(&wire, &mut fixture, root.path(), 11).await;
    let changed: DiscordSettings = serde_json::from_value(settings).unwrap();
    gateway
        .set_adapter_policy("fixture-discord", changed.host_policy())
        .await
        .unwrap();
    wire.dispatch(12, "INTERACTION_CREATE", choose(12, &stale, "600"))
        .await;
    verify_model_outcome(&mut fixture, "The request is no longer current.").await;

    wire.dispatch(
        13,
        "MESSAGE_CREATE",
        message("702", "20", "after model picker", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(payload["content"], "answer after model picker");
    assert_eq!(model.requests()[1]["model"], "fixture");
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_reset_rotates_an_idle_native_session_once_and_preserves_the_old_transcript() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7","8"]},"operators":["7"],"commands":{"sync":false},
        "guilds":{"11":{"channels":{"21":{}}}}
    });
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), settings)],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "reset").await;
    let reset = |offset, actor, channel| {
        let mut request = model_interaction(offset, actor, channel, None);
        request["data"]["name"] = json!("reset");
        request
    };
    wire.dispatch(2, "INTERACTION_CREATE", reset(2, "8", "21"))
        .await;
    verify_model_outcome(&mut fixture, "You aren't allowed to apply this action.").await;

    wire.dispatch(
        3,
        "MESSAGE_CREATE",
        message("751", "20", "before the reset", false),
    )
    .await;
    let _ = fixture.reply().await;
    wait_reply(&gateway, "discord-message-751").await;
    let status = gateway.messaging_status().await.unwrap();
    let original = status["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == "discord-message-751")
        .unwrap();
    let old_route = original["route"].as_str().unwrap();
    let old_session = original["session"].as_str().unwrap();
    let db = hermes_engine::db::open(root.path()).unwrap();
    let revision = |id| {
        db.query_row(
            "SELECT revision FROM messaging_routes WHERE id = ?1",
            [id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
    };
    let old_revision = revision(old_route);

    wire.dispatch(
        4,
        "MESSAGE_CREATE",
        message("752", "20", "steer wait", false),
    )
    .await;
    timeout(LIMIT, async {
        while model.requests().len() < 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    wire.dispatch(5, "INTERACTION_CREATE", reset(5, "7", "20"))
        .await;
    verify_model_outcome(
        &mut fixture,
        "The conversation still has work. Retry after it settles.",
    )
    .await;
    assert_eq!(revision(old_route), old_revision);
    model.release_waiting_turn();
    wait_reply(&gateway, "discord-message-752").await;

    let confirmed = reset(6, "7", "20");
    wire.dispatch(6, "INTERACTION_CREATE", confirmed.clone())
        .await;
    verify_model_outcome(&mut fixture, "Applied.").await;
    wire.dispatch(7, "INTERACTION_CREATE", confirmed).await;
    verify_model_outcome(&mut fixture, "Applied.").await;
    assert_eq!(revision(old_route), old_revision + 1);

    wire.dispatch(
        8,
        "MESSAGE_CREATE",
        message("753", "20", "after the reset", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(payload["content"], "answer after the reset");
    wait_reply(&gateway, "discord-message-753").await;
    let status = gateway.messaging_status().await.unwrap();
    let current = status["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["id"] == "discord-message-753")
        .unwrap();
    assert_eq!(current["route"], old_route);
    assert_ne!(current["session"], old_session);
    let prior = hermes_library::Library::open(root.path()).unwrap().session(old_session).unwrap();
    let (history, _) = hermes_engine::read_conversation(&prior.directory).unwrap();
    assert!(
        history
            .turns
            .iter()
            .any(|turn| turn.prompt == "before the reset")
    );
    assert!(
        !model.requests()[2]["messages"]
            .to_string()
            .contains("before the reset")
    );
    gateway.shutdown().await.unwrap();
}
