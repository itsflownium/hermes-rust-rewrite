use super::*;

fn bot_message(id: &str, actor: &str, room: &str, text: &str, addressed: bool) -> Value {
    let mut input = message(id, room, text, addressed);
    input["author"] = json!({"id":actor,"bot":true});
    input
}

fn burst_settings(auto_thread: bool) -> Value {
    json!({
        "grants":{"users":["7","31","36"]},
        "trusted_bots":["31"],
        "auto_thread":auto_thread,
        "commands":{"sync":false},
        "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
    })
}

#[tokio::test]
async fn discord_coalesces_a_trusted_bot_burst_and_fences_replayed_authority() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let config =
        |fixture: &Fixtures| fixture.configuration_with(root.path(), burst_settings(false));
    let gateway = model.gateway(root.path(), vec![config(&fixture)]).await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "bot-handoff").await;
    let starter = bot_message("741", "31", "21", "<@9> first line", true);
    let continued = bot_message("742", "31", "21", "second line", false);
    wire.dispatch(2, "MESSAGE_CREATE", starter.clone()).await;
    wire.dispatch(3, "MESSAGE_CREATE", continued.clone()).await;
    wire.dispatch(
        4,
        "MESSAGE_CREATE",
        bot_message("743", "36", "21", "<@9> untrusted", true),
    )
    .await;
    wire.dispatch(
        5,
        "MESSAGE_CREATE",
        bot_message("744", "31", "22", "wrong thread", false),
    )
    .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!()
    };
    assert_eq!(channel, "21");
    assert_eq!(payload["content"], "answer first line\nsecond line");
    wait_reply(&gateway, "discord-message-741").await;
    wait_status(&gateway, |status| {
        status["input"]
            .as_array()
            .is_some_and(|inputs| inputs.len() == 4)
    })
    .await;
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();

    let gateway = model.gateway(root.path(), vec![config(&fixture)]).await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 5);
    wire.dispatch(6, "RESUMED", json!({})).await;
    wire.dispatch(7, "MESSAGE_CREATE", starter).await;
    wire.dispatch(8, "MESSAGE_CREATE", continued).await;
    wire.dispatch(
        9,
        "MESSAGE_CREATE",
        bot_message("745", "31", "21", "late continuation", false),
    )
    .await;
    wire.dispatch(
        10,
        "MESSAGE_CREATE",
        message("746", "21", "<@9> human input", true),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(payload["content"], "answer human input");
    wait_reply(&gateway, "discord-message-746").await;
    assert_eq!(model.requests().len(), 2);
    let db = hermes_engine::db::open(root.path()).unwrap();
    let disposition = |id: &str| -> String {
        db.query_row(
            "SELECT disposition FROM messaging_ingress WHERE event=?1",
            [format!("discord-message-{id}")],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(disposition("742"), r#""coalesced""#);
    for id in ["743", "744", "745"] {
        assert_eq!(disposition(id), r#""forbidden""#);
    }
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_auto_thread_provisions_once_and_carries_the_durable_coalesced_text() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), burst_settings(true))],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "bot-handoff-thread").await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        bot_message("747", "31", "21", "<@9> new discussion", true),
    )
    .await;
    wire.dispatch(
        3,
        "MESSAGE_CREATE",
        bot_message("748", "31", "21", "details for discussion", false),
    )
    .await;
    let Write::Thread { parent, starter, .. } = fixture
        .next_write("thread creation", |write| {
            matches!(write, Write::Thread { .. })
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!((parent.as_str(), starter.as_str()), ("21", "747"));
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!()
    };
    assert_eq!(channel, "747");
    assert_eq!(
        payload["content"],
        "answer new discussion\ndetails for discussion"
    );
    wait_reply(&gateway, "discord-message-747").await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
    gateway.shutdown().await.unwrap();
}
