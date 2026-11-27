use super::*;
use crate::messaging::storage::key;

fn background_settings() -> Value {
    json!({
        "grants":{"users":["7","8"]},
        "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
        "commands":{"sync":false,"guild_roles":{"11":["31"]}},
    })
}

fn background_interaction(offset: u64, actor: &str, roles: &[&str], text: &str) -> Value {
    let id = (((now() - 1_420_070_400_000) as u64) << 22) + offset;
    json!({
        "id":id.to_string(),"application_id":"90","type":2,"token":format!("bg-{id}"),
        "guild_id":"11","channel_id":"22","app_permissions":((1_u64 << 38) | (1 << 10)).to_string(),
        "member":{"roles":roles,"user":{"id":actor}},
        "data":{"name":"bg","options":[{"name":"text","value":text}]},
    })
}

fn event_id(interaction: &Value) -> String {
    format!(
        "discord-interaction-{}",
        interaction["id"].as_str().unwrap()
    )
}

fn input<'a>(status: &'a Value, id: &str) -> Option<&'a Value> {
    status["input"]
        .as_array()?
        .iter()
        .find(|row| row["id"] == id)
}

fn replies_to(status: &Value, event: &str) -> Option<String> {
    let connection = input(status, event)?["connection"].as_str()?;
    let submission = key("channel-input", &(connection, event)).unwrap();
    Some(key("reply", &submission).unwrap())
}

async fn accepted(fixture: &mut Fixtures) {
    let Write::Callback(callback) = fixture
        .next_write("private background acknowledgement", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(callback["type"], 5);
    assert_eq!(callback["data"]["flags"], 64);
    let Write::Followup(outcome) = fixture
        .next_write("native background receipt", |write| {
            matches!(write, Write::Followup(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(
        outcome["content"],
        "Background work queued. The result will arrive in this conversation."
    );
}

async fn resume(fixture: &mut Fixtures, wire: &Wire, sequence: u64) -> Wire {
    wire.data(json!({"op":7})).await;
    let mut resumed = fixture.wire().await;
    assert_eq!(resumed.authentication(6).await["d"]["seq"], sequence);
    resumed
}

async fn wait_model(model: &Model, text: &str) {
    timeout(LIMIT, async {
        while !model.requests().iter().any(|request| {
            request["messages"]
                .as_array()
                .and_then(|messages| messages.last())
                .is_some_and(|message| message["content"] == text)
        }) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native model request");
}

#[tokio::test]
async fn discord_background_runs_in_a_distinct_native_session_and_returns_to_its_thread() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let config = fixture.configuration_with(root.path(), background_settings());
    let gateway = model.gateway(root.path(), vec![config]).await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "background-foreground").await;
    wire.dispatch(2, "MESSAGE_CREATE", message("450", "22", "<@9> hold", true))
        .await;
    wait_model(&model, "hold").await;

    let refused = background_interaction(3, "8", &[], "role needed");
    let refused_id = event_id(&refused);
    wire.dispatch(3, "INTERACTION_CREATE", refused).await;
    let Write::Callback(response) = fixture
        .next_write("private refusal", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!();
    };
    assert_eq!(response["type"], 4);
    assert_eq!(response["data"]["flags"], 64);

    let invocation = background_interaction(4, "7", &["31"], "separate task");
    let id = event_id(&invocation);
    wire.dispatch(4, "INTERACTION_CREATE", invocation).await;
    accepted(&mut fixture).await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!();
    };
    assert_eq!(channel, "22");
    assert_eq!(payload["content"], "answer separate task");
    assert_eq!(payload["allowed_mentions"]["parse"], json!([]));
    wait_reply(&gateway, &id).await;

    let status = gateway.messaging_status().await.unwrap();
    let foreground = input(&status, "discord-message-450").unwrap();
    let background = input(&status, &id).unwrap();
    assert_eq!(foreground["state"], "admitted");
    assert_eq!(background["state"], "done");
    assert_ne!(foreground["route"], background["route"]);
    assert_ne!(foreground["session"], background["session"]);
    assert_eq!(input(&status, &refused_id).unwrap()["state"], "refused");
    let requests = model.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["content"] != "hold")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_background_recovers_the_original_checkpoint_and_executes_one_job() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), background_settings())],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "background-reopen").await;
    wait_status(&gateway, |status| {
        status["connections"][0]["health"] == "ready"
    })
    .await;
    let wire = resume(&mut fixture, &wire, 1).await;

    let invocation = background_interaction(2, "7", &["31"], "recover this job");
    let id = event_id(&invocation);
    // During RESUME, replayed dispatches commit before the live transport
    // permits native execution.
    wire.dispatch(2, "INTERACTION_CREATE", invocation.clone())
        .await;
    accepted(&mut fixture).await;
    let status = gateway.messaging_status().await.unwrap();
    assert_eq!(input(&status, &id).unwrap()["state"], "pending");
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), background_settings())],
        )
        .await;
    let mut reopened = fixture.wire().await;
    let resume = reopened.authentication(6).await;
    assert_eq!(resume["d"]["seq"], 2);
    assert_eq!(resume["d"]["session_id"], "background-reopen");
    reopened
        .dispatch(3, "INTERACTION_CREATE", invocation.clone())
        .await;
    reopened.dispatch(4, "RESUMED", json!({})).await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!();
    };
    assert_eq!(channel, "22");
    assert_eq!(payload["content"], "answer recover this job");
    wait_reply(&gateway, &id).await;

    // Reusing the immutable interaction identity with different text records
    // a conflict and leaves the committed job and result alone.
    let mut conflict = invocation;
    conflict["data"]["options"][0]["value"] = json!("different job");
    reopened.dispatch(5, "INTERACTION_CREATE", conflict).await;
    let status = wait_status(&gateway, |status| {
        input(status, &id).is_some_and(|row| row["conflict"] == true)
    })
    .await;
    assert_eq!(input(&status, &id).unwrap()["state"], "done");
    assert_eq!(status["deliveries"].as_array().unwrap().len(), 1);
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_background_rechecks_command_roles_at_native_claim_and_delivery() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), background_settings())],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "background-revocation").await;
    wait_status(&gateway, |status| {
        status["connections"][0]["health"] == "ready"
    })
    .await;
    let wire = resume(&mut fixture, &wire, 1).await;
    let request = background_interaction(2, "7", &["31"], "revoked before claim");
    let queued = event_id(&request);
    wire.dispatch(2, "INTERACTION_CREATE", request.clone())
        .await;
    accepted(&mut fixture).await;
    let unprivileged = json!({"guild_id":"11","user":{"id":"7"},"roles":[]});
    wire.dispatch(3, "GUILD_MEMBER_UPDATE", unprivileged.clone())
        .await;
    // This stale copy must not restore the lost command grant.
    wire.dispatch(4, "INTERACTION_CREATE", request).await;
    wire.dispatch(5, "RESUMED", json!({})).await;
    wire.dispatch(
        6,
        "MESSAGE_CREATE",
        message("451", "22", "<@9> chat still allowed", true),
    )
    .await;
    wait_reply(&gateway, "discord-message-451").await;
    assert_eq!(model.requests().len(), 1);
    let status = gateway.messaging_status().await.unwrap();
    assert_eq!(input(&status, &queued).unwrap()["state"], "pending");

    let request = background_interaction(7, "8", &["31"], "steer wait");
    let running = event_id(&request);
    wire.dispatch(7, "INTERACTION_CREATE", request).await;
    wait_model(&model, "steer wait").await;
    wire.dispatch(
        8,
        "GUILD_MEMBER_UPDATE",
        json!({"guild_id":"11","user":{"id":"8"},"roles":[]}),
    )
    .await;
    let mut next_chat = message("452", "22", "<@9> another allowed chat", true);
    next_chat["author"]["id"] = json!("8");
    wire.dispatch(9, "MESSAGE_CREATE", next_chat).await;
    wait_reply(&gateway, "discord-message-452").await;

    model.release_waiting_turn();
    let status = wait_status(&gateway, |status| {
        input(status, &running).is_some_and(|row| row["state"] == "done")
    })
    .await;
    let id = replies_to(&status, &running).unwrap();
    assert!(
        status["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|delivery| delivery["id"] == id && delivery["state"] == "pending")
    );
    assert!(
        crate::lock(&fixture.transport.messages)
            .values()
            .all(|message| message["content"] != "Working")
    );
    gateway.shutdown().await.unwrap();
}
