use super::*;

fn guild_settings() -> Value {
    json!({
        "grants":{"users":["8"]},
        "guilds":{"11":{"rules":{"grants":{"roles":["41"]}},"channels":{"21":{"threads":{"22":{}}}}}},
        "commands":{"sync":false},
    })
}

async fn open_replaying(fixture: &mut Fixtures, gateway: &RunningGateway) -> Wire {
    fixture.activate(gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "membership-resume").await;
    wait_status(gateway, |status| {
        status["connections"][0]["health"] == "ready"
    })
    .await;
    wire.data(json!({"op":7})).await;
    let mut resumed = fixture.wire().await;
    assert_eq!(resumed.authentication(6).await["d"]["seq"], 1);
    resumed
}

fn guild_message(id: &str, text: &str) -> Value {
    let mut value = message(id, "22", &format!("<@9> {text}"), true);
    value["member"]["roles"] = json!(["41"]);
    value
}

fn set_member(fixture: &Fixtures, roles: &[&str], held: Option<(Arc<Notify>, Arc<Semaphore>)>) {
    crate::lock(&fixture.transport.member_responses).insert(
        ("11".to_owned(), "7".to_owned()),
        MemberResponse {
            roles: roles.iter().map(|role| (*role).into()).collect(),
            held,
        },
    );
}

fn expire_actor(db: &rusqlite::Connection) {
    assert_eq!(
        db.execute(
            "UPDATE messaging_actor_evidence SET data=json_set(data,'$.evidence.expires_at',0)
         WHERE actor='7'",
            [],
        )
        .unwrap(),
        1
    );
}

#[tokio::test]
async fn discord_refreshes_expired_guild_evidence_for_a_waiting_native_claim() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    set_member(&fixture, &["41"], None);
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), guild_settings())],
        )
        .await;
    let wire = open_replaying(&mut fixture, &gateway).await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        guild_message("453", "recover current role"),
    )
    .await;
    wait_status(&gateway, |status| status["input"][0]["state"] == "pending").await;
    assert!(model.requests().is_empty());
    expire_actor(&hermes_engine::db::open(root.path()).unwrap());

    wire.dispatch(3, "RESUMED", json!({})).await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!();
    };
    assert_eq!(channel, "22");
    assert_eq!(payload["content"], "answer recover current role");
    wait_reply(&gateway, "discord-message-453").await;
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_ignores_a_membership_lookup_that_races_a_role_revocation() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    set_member(&fixture, &["41"], Some((entered.clone(), release.clone())));
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), guild_settings())],
        )
        .await;
    let wire = open_replaying(&mut fixture, &gateway).await;
    let original = guild_message("454", "must not execute");
    wire.dispatch(2, "MESSAGE_CREATE", original.clone()).await;
    wait_status(&gateway, |status| status["input"][0]["state"] == "pending").await;
    let db = hermes_engine::db::open(root.path()).unwrap();
    expire_actor(&db);
    wire.dispatch(3, "RESUMED", json!({})).await;
    timeout(LIMIT, entered.notified())
        .await
        .expect("membership lookup started");

    wire.dispatch(
        4,
        "GUILD_MEMBER_UPDATE",
        json!({"guild_id":"11","user":{"id":"7"},"roles":[]}),
    )
    .await;
    wire.dispatch(5, "MESSAGE_CREATE", original).await;
    // An unrelated callback proceeds even while the old REST response is held.
    let id = ((now() - 1_420_070_400_000) as u64) << 22;
    wire.dispatch(
        6,
        "INTERACTION_CREATE",
        json!({
            "id":id.to_string(),"application_id":"90","type":2,
            "token":"membership-fixture-callback","channel_id":"20",
            "user":{"id":"8"},"data":{"name":"status"},
        }),
    )
    .await;
    let Write::Callback(ack) = fixture
        .next_write("status acknowledgement during membership lookup", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(ack["type"], 5);
    assert!(model.requests().is_empty());

    set_member(&fixture, &[], None);
    release.add_permits(1);
    timeout(LIMIT, async {
        loop {
            let refreshed_without_role: bool = db
                .query_row(
                    "SELECT EXISTS (SELECT 1 FROM messaging_actor_evidence a,
                      json_each(a.data,'$.evidence.memberships') m
                  WHERE a.actor='7' AND json_extract(m.value,'$.room.thread')='22'
                      AND json_array_length(m.value,'$.roles')=0)",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(model.requests().is_empty());
            if refreshed_without_role {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the next lookup observed the revoked role");
    let status = gateway.messaging_status().await.unwrap();
    assert!(status["input"].as_array().unwrap().iter().any(|row| {
        row["id"] == "discord-message-454" && row["state"] == "pending" && row["conflict"] == false
    }));
    assert!(status["deliveries"].as_array().unwrap().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_requires_the_exact_dm_recipient_to_resume_pending_delivery() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "membership-dm").await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("455", "20", "steer wait", false),
    )
    .await;
    wait_for_model(&model).await;

    let reads = fixture.transport.direct_reads.load(Ordering::SeqCst);
    *crate::lock(&fixture.transport.direct_recipient) = "8".into();
    expire_actor(&hermes_engine::db::open(root.path()).unwrap());
    model.release_waiting_turn();
    let status = wait_status(&gateway, |status| {
        status["input"][0]["state"] == "done"
            && fixture.transport.direct_reads.load(Ordering::SeqCst) > reads
    })
    .await;
    let id = crate::messaging::storage::key(
        "reply",
        &crate::messaging::storage::key(
            "channel-input",
            &(
                status["input"][0]["connection"].as_str().unwrap(),
                "discord-message-455",
            ),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        status["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|delivery| delivery["id"] == id && delivery["state"] == "pending")
    );

    *crate::lock(&fixture.transport.direct_recipient) = "7".into();
    wait_reply(&gateway, "discord-message-455").await;
    assert_eq!(model.requests().len(), 1);
    assert!(
        crate::lock(&fixture.transport.messages)
            .values()
            .any(|message| message["channel_id"] == "20" && message["content"] == "Working")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_renews_both_the_prepared_thread_and_its_original_parent_before_delivery() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    set_member(&fixture, &["41"], None);
    let mut settings = guild_settings();
    settings["auto_thread"] = json!(true);
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), settings)],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "membership-prepared-thread").await;
    let mut starter = guild_message("456", "steer wait");
    starter["channel_id"] = json!("21");
    wire.dispatch(2, "MESSAGE_CREATE", starter).await;
    wait_for_model(&model).await;

    let db = hermes_engine::db::open(root.path()).unwrap();
    expire_actor(&db);
    model.release_waiting_turn();
    wait_reply(&gateway, "discord-message-456").await;
    let exact_memberships: i64 = db
        .query_row(
            "SELECT count(*) FROM messaging_actor_evidence a,
             json_each(a.data,'$.evidence.memberships') m
         WHERE a.actor='7' AND json_extract(m.value,'$.room.chat')='21'
            AND (json_extract(m.value,'$.room.thread') IS NULL
                 OR json_extract(m.value,'$.room.thread')='456')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(exact_memberships, 2);
    assert!(
        crate::lock(&fixture.transport.messages)
            .values()
            .any(|message| message["channel_id"] == "456" && message["content"] == "Working")
    );
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

async fn wait_for_model(model: &Model) {
    timeout(LIMIT, async {
        while model.requests().is_empty() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native claim");
}
