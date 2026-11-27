use super::{manual_thread_fixtures::thread_request, *};
use crate::settings::{READ_MESSAGE_HISTORY, SEE_CHANNEL, SEND_IN_THREADS};

fn configuration(fixture: &Fixtures, root: &Path) -> AdapterConfig {
    fixture.configuration_with(
        root,
        json!({
            "grants":{"users":["7"]},
            "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
            "auto_thread":true,
            "commands":{"enabled":true,"sync":false},
            "delivery":{"rename_auto_threads":true,"pin_first_thread_message":true},
        }),
    )
}

async fn start(fixture: &mut Fixtures, model: &Model, root: &Path) -> (RunningGateway, Wire) {
    let gateway = model
        .gateway(root, vec![configuration(fixture, root)])
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "thread-updates").await;
    (gateway, wire)
}

async fn updates(root: &Path, state: &str, count: u32) {
    let db = hermes_engine::db::open(root).unwrap();
    timeout(LIMIT, async {
        loop {
            let found: u32 = db
                .query_row(
                    "SELECT COUNT(*) FROM messaging_discord_thread_updates WHERE state=?1",
                    [state],
                    |row| row.get(0),
                )
                .unwrap();
            if found == count {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("Expected {count} thread updates in {state}"));
}

fn writes(fixture: &Fixtures) -> Vec<(String, Option<String>)> {
    let mut writes = crate::lock(&fixture.transport.thread_update_writes).clone();
    writes.sort();
    writes
}

#[tokio::test]
async fn discord_thread_updates_rename_and_pin_only_the_original_confirmed_reply() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let (gateway, wire) = start(&mut fixture, &model, root.path()).await;
    let source = message("410", "21", "<@9> release notes", true);
    wire.dispatch(2, "MESSAGE_CREATE", source.clone()).await;
    let Write::Message { channel, id, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(channel, "410");
    wait_reply(&gateway, "discord-message-410").await;
    updates(root.path(), "applied", 2).await;
    let applied = [
        ("channels/410".into(), Some("answer release notes".into())),
        (format!("channels/410/messages/pins/{id}"), None),
    ];
    assert_eq!(writes(&fixture), applied);
    gateway.shutdown().await.unwrap();

    crate::lock(&fixture.transport.threads)
        .get_mut("410")
        .unwrap()["name"] = json!("Moderator title");
    crate::lock(&fixture.transport.messages)
        .get_mut(&("410".into(), id.clone()))
        .unwrap()["pinned"] = json!(false);
    let gateway = model
        .gateway(root.path(), vec![configuration(&fixture, root.path())])
        .await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 2);
    wire.dispatch(3, "RESUMED", json!({})).await;
    wire.dispatch(4, "MESSAGE_CREATE", source).await;
    wire.dispatch(
        5,
        "MESSAGE_CREATE",
        message("411", "410", "<@9> follow up", true),
    )
    .await;
    wire.dispatch(
        6,
        "MESSAGE_CREATE",
        message("412", "22", "<@9> existing discussion", true),
    )
    .await;
    wait_reply(&gateway, "discord-message-411").await;
    wait_reply(&gateway, "discord-message-412").await;
    sleep(Duration::from_millis(2200)).await;
    assert_eq!(model.requests().len(), 3);
    assert_eq!(writes(&fixture), applied);
    assert_eq!(
        crate::lock(&fixture.transport.threads)["410"]["name"],
        "Moderator title"
    );
    assert_eq!(
        crate::lock(&fixture.transport.messages)[&("410".into(), id)]["pinned"],
        false
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_thread_updates_reconcile_lost_responses_after_restart_without_repeating_writes() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    fixture
        .transport
        .lose_thread_update_responses
        .store(true, Ordering::SeqCst);
    let (gateway, wire) = start(&mut fixture, &model, root.path()).await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("413", "21", "<@9> recovery", true),
    )
    .await;
    let Write::Message { id, .. } = fixture.reply().await else {
        unreachable!()
    };
    updates(root.path(), "issued", 2).await;
    timeout(LIMIT, async {
        while writes(&fixture).len() != 2 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    gateway.shutdown().await.unwrap();

    fixture
        .transport
        .lose_thread_update_responses
        .store(false, Ordering::SeqCst);
    let gateway = model
        .gateway(root.path(), vec![configuration(&fixture, root.path())])
        .await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 2);
    wire.dispatch(3, "RESUMED", json!({})).await;
    updates(root.path(), "applied", 2).await;
    assert_eq!(
        writes(&fixture),
        [
            ("channels/413".into(), Some("answer recovery".into())),
            (format!("channels/413/messages/pins/{id}"), None),
        ]
    );
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_thread_updates_preserve_a_moderator_title_and_respect_pin_permissions() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    *crate::lock(&fixture.transport.thread_requester_permissions) =
        Some(SEE_CHANNEL | READ_MESSAGE_HISTORY);
    *crate::lock(&fixture.transport.thread_bot_permissions) =
        Some(SEE_CHANNEL | READ_MESSAGE_HISTORY | SEND_IN_THREADS);
    crate::lock(&fixture.transport.threads).insert("414".into(), json!({
        "id":"414","type":11,"owner_id":"9","parent_id":"21","guild_id":"11","name":"Moderator title",
        "thread_metadata":{"archived":false,"locked":false}
    }));
    let (gateway, wire) = start(&mut fixture, &model, root.path()).await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("414", "21", "<@9> permissions", true),
    )
    .await;
    let Write::Message { channel, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(channel, "414");
    wait_reply(&gateway, "discord-message-414").await;
    updates(root.path(), "skipped", 2).await;
    assert!(writes(&fixture).is_empty());
    assert_eq!(
        crate::lock(&fixture.transport.threads)["414"]["name"],
        "Moderator title"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_thread_updates_share_the_discord_account_rate_limit() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    fixture
        .transport
        .limit_next_pin
        .store(true, Ordering::SeqCst);
    let (gateway, wire) = start(&mut fixture, &model, root.path()).await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("415", "21", "<@9> account limit", true),
    )
    .await;
    let Write::Message { id, .. } = fixture.reply().await else {
        unreachable!()
    };
    wait_reply(&gateway, "discord-message-415").await;
    let db = hermes_engine::db::open(root.path()).unwrap();
    timeout(LIMIT, async {
        loop {
            let held: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_cooldowns WHERE scope='' AND until_ms>?1)",
                [now()], |row| row.get(0),
            ).unwrap();
            if held {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(writes(&fixture).is_empty());
    updates(root.path(), "applied", 2).await;
    assert_eq!(
        writes(&fixture),
        [
            ("channels/415".into(), Some("answer account limit".into())),
            (format!("channels/415/messages/pins/{id}"), None),
        ]
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_thread_updates_pin_a_manual_starter_inside_its_named_thread() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let (gateway, wire) = start(&mut fixture, &model, root.path()).await;
    let request = thread_request(1, "Review plan", Some("Starter for the review"));
    wire.dispatch(2, "INTERACTION_CREATE", request).await;
    let Write::Message { channel, id, .. } = fixture.next_write("a manual starter", |write| {
        matches!(write, Write::Message {payload, ..} if payload["content"] == "Starter for the review")
    }).await else { unreachable!() };
    assert_ne!(channel, "21");
    assert_ne!(channel, id);
    updates(root.path(), "applied", 1).await;
    assert_eq!(
        writes(&fixture),
        [(format!("channels/{channel}/messages/pins/{id}"), None)]
    );
    assert_eq!(
        crate::lock(&fixture.transport.threads)[&channel]["name"],
        "Review plan"
    );
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}
