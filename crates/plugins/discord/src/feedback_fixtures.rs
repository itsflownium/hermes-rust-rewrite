use super::*;

#[tokio::test]
async fn discord_feedback_is_scoped_once_and_an_ambiguous_reaction_does_not_delay_the_reply() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7"]},
        "commands":{"sync":false},
        "delivery":{"typing":true,"reactions":true},
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
    fixture.ready(&wire, "scoped-feedback").await;
    let input = message("801", "20", "steer wait", false);
    crate::lock(&fixture.transport.messages).insert(("20".into(), "801".into()), input.clone());
    wire.dispatch(2, "MESSAGE_CREATE", input.clone()).await;
    fixture.next_write("committed progress", |write| {
        matches!(write, Write::Message { payload, .. } if payload["content"] == "Working…")
    }).await;
    assert_eq!(
        crate::lock(&fixture.transport.feedbacks).as_slice(),
        [
            "channels/20/typing",
            "channels/20/messages/801/reactions/%F0%9F%91%80/@me",
        ]
    );
    wire.dispatch(3, "MESSAGE_CREATE", input).await;
    model.release_waiting_turn();
    fixture.next_write("committed text after unknown reaction", |write| {
        matches!(write, Write::Message { payload, .. } if payload["content"] == "Working")
    }).await;
    wait_reply(&gateway, "discord-message-801").await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(crate::lock(&fixture.transport.feedbacks).len(), 2);

    let db = hermes_engine::db::open(root.path()).unwrap();
    let count: u32 = db.query_row(
        "SELECT count(*) FROM messaging_discord_feedback WHERE (kind='typing' AND state='confirmed')
           OR (kind='reaction' AND state='uncertain')",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(count, 2);
    gateway.shutdown().await.unwrap();
}
