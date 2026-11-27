use super::*;

#[tokio::test]
async fn discord_scrollback_is_attributed_and_fenced_to_older_messages_in_the_triggering_channel() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7"]},
        "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
        "commands":{"sync":false},
        "recovery":{"context_messages":6},
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
    fixture.ready(&wire, "attributed-scrollback").await;
    crate::lock(&fixture.transport.history).insert(
        "21".into(),
        vec![
            message("809", "21", "future message", false),
            message("806", "21", "the triggering message", false),
            message("805", "22", "foreign thread", false),
            message("804", "21", "second relevant note", false),
            message("802", "21", "first relevant note", false),
            message("802", "21", "repeated history row", false),
        ],
    );
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("806", "21", "<@9> actual question", true),
    )
    .await;
    let Write::Message { channel, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(channel, "21");
    wait_reply(&gateway, "discord-message-806").await;
    let requests = model.requests();
    assert_eq!(requests.len(), 1);
    let prompt = requests[0]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|entry| entry["role"] == "user")
        .unwrap()["content"]
        .as_str()
        .unwrap();
    assert!(prompt.starts_with("actual question\n\nEarlier messages from this channel."));
    assert!(prompt.contains("\"author_id\":\"7\""));
    assert!(
        prompt.find("first relevant note").unwrap() < prompt.find("second relevant note").unwrap()
    );
    for excluded in [
        "future message",
        "the triggering message",
        "foreign thread",
        "repeated history row",
    ] {
        assert!(!prompt.contains(excluded));
    }
    let state = gateway.messaging_status().await.unwrap();
    assert_eq!(state["input"].as_array().unwrap().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_history_exposes_only_delivered_turns_from_the_requesters_exact_route() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7","8"]},
        "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
        "commands":{"sync":false}
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
    fixture.ready(&wire, "scoped-history").await;
    for (sequence, channel, id, actor, text) in [
        (2, "21", "701", "7", "mine"),
        (3, "21", "702", "8", "other sender"),
        (4, "22", "703", "7", "other thread"),
    ] {
        let mut input = message(id, channel, &format!("<@9> {text}"), true);
        input["author"]["id"] = json!(actor);
        wire.dispatch(sequence, "MESSAGE_CREATE", input).await;
        let Write::Message { payload, .. } = fixture.reply().await else {
            unreachable!()
        };
        assert_eq!(payload["content"], format!("answer {text}"));
        wait_reply(&gateway, &format!("discord-message-{id}")).await;
    }
    let id = (((now() - 1_420_070_400_000) as u64) << 22).to_string();
    wire.dispatch(
        5,
        "INTERACTION_CREATE",
        json!({
            "id":id,"application_id":"90","type":2,"token":"history-callback",
            "channel_id":"21","guild_id":"11","member":{"roles":[],"user":{"id":"7"}},
            "data":{"name":"history"},
        }),
    )
    .await;
    let Write::Callback(acknowledged) = fixture.write().await else {
        panic!("the history invocation was not acknowledged");
    };
    assert_eq!(acknowledged["type"], 5);
    assert_eq!(acknowledged["data"]["flags"], 64);
    let Write::Followup(presentation) = fixture.write().await else {
        panic!("the history was not delivered privately");
    };
    assert_eq!(
        presentation["content"],
        "**You**\nmine\n**Mercury**\nanswer mine"
    );
    assert_eq!(presentation["allowed_mentions"]["parse"], json!([]));
    assert_eq!(model.requests().len(), 3);
    let db = hermes_engine::db::open(root.path()).unwrap();
    let outcome: String = db
        .query_row(
            "SELECT outcome FROM messaging_native_invocations WHERE event=?1",
            [format!("discord-interaction-{id}")],
            |row| row.get(0),
        )
        .unwrap();
    assert!(outcome.contains("history_cursor"));
    assert!(!outcome.contains("answer mine"));
    gateway.shutdown().await.unwrap();
}
