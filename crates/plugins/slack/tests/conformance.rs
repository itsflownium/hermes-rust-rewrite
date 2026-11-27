mod fixtures;

use axum::http::StatusCode;
use fixtures::{ACTOR, APP, FIRST, Fixture, Peer, SECOND, bot, event, state, until};
use hermes_gateway_core::{
    RunningGateway,
    messaging::{
        DeliveryResolution,
        test_support::{Model, dispatch},
    },
};
use hermes_slack_plugin::STOCK_ADAPTER;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Semaphore;

async fn setup() -> (tempfile::TempDir, Model, Fixture, RunningGateway) {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::start().await;
    let model = Model::new().await.with_stock_adapter(STOCK_ADAPTER);
    let gateway = model.gateway(root.path(), vec![]).await;
    (root, model, fixture, gateway)
}

async fn receiver(fixture: &Fixture, gateway: &RunningGateway, mut settings: Value) -> Peer {
    settings["receiver_owner"] = json!(true);
    fixture
        .create(gateway, "slack-one", FIRST, true, settings)
        .await;
    let peer = fixture.peer().await;
    state(gateway, |state| {
        state["verification"]["slack-one"]["socket"] == "ready"
    })
    .await;
    peer
}

#[tokio::test]
async fn peer_handoff_requires_the_configured_bot_a_fresh_mention_and_the_correct_actor() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["peer_bots"] = json!({"BPEER":"mention"});
    settings["continue_threads"] = json!(true);
    settings["access"]["users"] = json!([ACTOR, "UPEER"]);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    for (ordinal, actor, bot, text) in [
        (1, "UPEER", "BPEER", "<@UBOT1> peer handoff"),
        (2, "UPEER", "BPEER", "quiet follow-up"),
        (3, "UPEER", "BBOT1", "<@UBOT1> self echo"),
        (4, ACTOR, "BPEER", "<@UBOT1> mismatched bot identity"),
    ] {
        let mut msg = event(
            FIRST,
            &format!("peer-{ordinal}"),
            &format!("EvPeer{ordinal}"),
            "CPUBLIC",
            ordinal,
            text,
        );
        let source = &mut msg["payload"]["event"];
        source["user"] = json!(actor);
        source["bot_id"] = json!(bot);
        source["subtype"] = json!("bot_message");
        source["thread_ts"] = json!("1730000000.000001");
        peer.ack(msg).await;
        if ordinal == 1 {
            until(|| fixture.calls("chat.postMessage").len() == 1).await;
        }
    }
    until(|| {
        fixture
            .calls("bots.info")
            .iter()
            .filter(|call| call.body["bot"] == "BPEER")
            .count()
            == 2
    })
    .await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["text"],
        "answer peer handoff"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn reaction_checks_both_audiences_and_never_copies_context_into_a_different_room() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    let private = json!({"kind":"group","chat":"GPRIVATE","thread":null});
    let public = json!({"kind":"group","chat":"CPUBLIC","thread":null});
    settings["reaction_triggers"] = json!({
        "local":{"source":private,"destination":private,"prompt":"Summarize in place"},
        "forward":{"source":private,"destination":public,"prompt":"Send the approved update"},
        "blocked":{"source":private,"destination":public,"prompt":"Disallowed reaction"},
    });
    settings["access"]["scope"]["events"] = json!({
        "slack-reaction":{"actors":[ACTOR],"resources":["GPRIVATE/local","GPRIVATE/forward"],"destinations":[private,public]},
    });
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let reaction = |name: &str, envelope: &str| {
        let mut msg = event(
            FIRST,
            envelope,
            &format!("EvReaction{name}"),
            "GPRIVATE",
            14,
            "",
        );
        msg["payload"]["event"] = json!({
            "type":"reaction_added","user":ACTOR,"reaction":name,
            "item":{"type":"message","channel":"GPRIVATE","ts":"1730000000.000014"},
        });
        msg
    };
    peer.ack(reaction("local", "local")).await;
    peer.ack(reaction("forward", "forward")).await;
    peer.ack(reaction("blocked", "blocked")).await;
    peer.ack(reaction("forward", "retry-forward")).await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    assert_eq!(model.requests().len(), 2);
    assert_eq!(fixture.calls("reactions.get").len(), 2);
    let posts = fixture.calls("chat.postMessage");
    assert!(posts.iter().any(|call| {
        call.body["channel"] == "GPRIVATE"
            && call.body["text"]
                .as_str()
                .unwrap()
                .contains("private Slack source")
    }));
    assert!(posts.iter().any(|call| call.body["channel"] == "CPUBLIC"
        && call.body["text"] == "answer Send the approved update"));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_broadcast_requires_notifications_in_the_thread_and_parent_room() {
    let (_root, _model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["broadcast_channels"] = json!(["CPUBLIC", "GPRIVATE"]);
    settings["access"]["scope"]["rooms"]["CPUBLIC"]["delivery"] = json!({"notifications":true});
    settings["access"]["scope"]["threads"]["GPRIVATE"]["1730000000.000002"]["delivery"] =
        json!({"notifications":true});
    let mut peer = receiver(&fixture, &gateway, settings).await;
    peer.ack(event(
        FIRST,
        "allow-broadcast",
        "EvBroadcast",
        "CPUBLIC",
        1,
        "<@UBOT1> wide reply",
    ))
    .await;
    peer.ack(event(
        FIRST,
        "deny-broadcast",
        "EvNoBroadcast",
        "GPRIVATE",
        2,
        "<@UBOT1> narrow reply",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    let replies = fixture.calls("chat.postMessage");
    assert!(
        replies
            .iter()
            .any(|call| call.body["thread_ts"] == "1730000000.000001"
                && call.body["reply_broadcast"] == true)
    );
    assert!(
        replies
            .iter()
            .any(|call| call.body["thread_ts"] == "1730000000.000002"
                && call.body["reply_broadcast"].is_null())
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn member_home_uses_the_exact_verified_dm_for_native_manual_sends() {
    let (_root, _model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["home_member"] = json!(ACTOR);
    settings["access"]["scope"]["direct"]["delivery"] = json!({"notifications":true});
    let _peer = receiver(&fixture, &gateway, settings).await;
    let ready = state(&gateway, |s| {
        s["verification"]["slack-one"]["home_route"].is_string()
    })
    .await;
    let route = ready["verification"]["slack-one"]["home_route"]
        .as_str()
        .unwrap();
    dispatch(
        &gateway,
        "messaging.send",
        &json!({
            "route":route, "request_id":"slack-home-test", "text":"owner notification"
        }),
        true,
    )
    .await
    .unwrap();
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    assert_eq!(fixture.calls("conversations.open")[0].body["users"], ACTOR);
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["channel"],
        "DMEMBER"
    );
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["text"],
        "owner notification"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduler_signed_webhook_and_background_use_the_verified_slack_thread() {
    let (root, _model, fixture, gateway) = setup().await;
    let thread = "1730000000.000061";
    let mut settings = fixture.settings(FIRST, true);
    settings["home"] = json!({"kind":"group","chat":"CPUBLIC","thread":thread});
    settings["access"]["scope"]["rooms"]["CPUBLIC"]["delivery"] = json!({"notifications":true});
    fixture.service.lock().unwrap().history = vec![json!({
        "ts":thread,"user":ACTOR,"text":"authorized notification thread",
    })];
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let verification = state(&gateway, |s| {
        s["verification"]["slack-one"]["home_route"].is_string()
    })
    .await;
    let route = verification["verification"]["slack-one"]["home_route"]
        .as_str()
        .unwrap();
    let destination = format!("channel:{route}");
    let job = dispatch(
        &gateway,
        "cron.manage",
        &json!({
            "action":"create","name":"Slack producer fixture","schedule":"every 1h",
            "prompt":"scheduled Slack result","deliver":destination,
        }),
        true,
    )
    .await
    .unwrap();
    dispatch(
        &gateway,
        "cron.manage",
        &json!({"action":"run","name":job["job_id"]}),
        true,
    )
    .await
    .unwrap();
    until(|| {
        fixture
            .calls("chat.postMessage")
            .iter()
            .any(|call| call.body["text"] == "answer scheduled Slack result")
    })
    .await;

    let hook = hermes_library::Library::open(root.path())
        .unwrap()
        .create_webhook(hermes_library::webhooks::Webhook {
            name: "slack-producer-fixture".into(),
            prompt: "signed Slack result".into(),
            deliver: destination,
            ..Default::default()
        })
        .unwrap();
    let body = r#"{"event":"slack-producer-fixture"}"#;
    let signature = ring::hmac::sign(
        &ring::hmac::Key::new(ring::hmac::HMAC_SHA256, hook.secret.as_bytes()),
        body.as_bytes(),
    );
    let signed: String = signature
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let http = reqwest::Client::new();
    for _ in 0..2 {
        let response = http
            .post(format!("{}/webhooks/{}", gateway.http_url(), hook.name))
            .header("x-hermes-signature", &signed)
            .header("x-hermes-delivery-id", "slack-producer-delivery")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 202);
    }
    let mut background = event(
        FIRST,
        "slack-bg",
        "EvSlackBg",
        "CPUBLIC",
        62,
        "!bg isolated Slack task",
    );
    background["payload"]["event"]["thread_ts"] = json!(thread);
    peer.ack(background).await;
    until(|| {
        let posts = fixture.calls("chat.postMessage");
        posts
            .iter()
            .any(|call| call.body["text"] == "answer isolated Slack task")
            && posts.iter().any(|call| {
                call.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("answer signed Slack result"))
            })
    })
    .await;
    let replies = fixture.calls("chat.postMessage");
    assert!(
        replies
            .iter()
            .all(|call| call.body["channel"] == "CPUBLIC" && call.body["thread_ts"] == thread)
    );
    assert_eq!(
        replies
            .iter()
            .filter(|call| {
                call.body["text"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("answer signed Slack result"))
            })
            .count(),
        1
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_socket_routes_overlapping_destinations_to_verified_workspaces() {
    let (_root, model, fixture, gateway) = setup().await;
    fixture
        .create(
            &gateway,
            "slack-two",
            SECOND,
            false,
            fixture.settings(SECOND, false),
        )
        .await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    state(&gateway, |state| {
        state["verification"]["slack-two"]["socket"] == "ready"
    })
    .await;
    peer.ack(event(
        FIRST,
        "envelope-1",
        "EvWorkspace1",
        "DSAME",
        1,
        "first workspace",
    ))
    .await;
    // Slack Connect can show a source workspace unrelated to the app's installation.
    let mut shared = event(
        SECOND,
        "envelope-2",
        "EvWorkspace2",
        "CPUBLIC",
        1,
        "<@UBOT2> shared message",
    );
    shared["payload"]["team_id"] = json!("TEXTERNAL");
    shared["payload"]["event"]["user_team"] = json!("TEXTERNAL");
    peer.ack(shared).await;
    peer.ack(event(
        SECOND,
        "envelope-3",
        "EvWorkspace3",
        "DSAME",
        1,
        "second workspace",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 3).await;
    let replies = fixture.calls("chat.postMessage");
    for (team, text) in [
        (FIRST, "answer first workspace"),
        (SECOND, "answer second workspace"),
        (SECOND, "answer shared message"),
    ] {
        assert!(
            replies
                .iter()
                .any(|call| call.team == team && call.body["text"] == text)
        );
    }
    let shared = replies
        .iter()
        .find(|reply| reply.body["channel"] == "CPUBLIC")
        .unwrap();
    assert_eq!(shared.body["thread_ts"], "1730000000.000001");
    assert_eq!(fixture.service.lock().unwrap().opens, 1);
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn ack_commits_deferred_work_and_aliases_resume_once_after_restart() {
    let (root, model, fixture, gateway) = setup().await;
    let pause = Arc::new(Semaphore::new(0));
    fixture.service.lock().unwrap().pause_read = Some(pause.clone());
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    let first = event(
        FIRST,
        "source",
        "EvMessage",
        "CPUBLIC",
        20,
        "<@UBOT1> durable work",
    );
    peer.ack(first.clone()).await;
    let committed = gateway.messaging_status().await.unwrap();
    assert_eq!(committed["input"][0]["state"], "preparing");
    assert!(model.requests().is_empty());
    assert!(fixture.calls("chat.postMessage").is_empty());
    gateway.shutdown().await.unwrap();
    drop(peer);
    fixture.service.lock().unwrap().pause_read = None;
    pause.add_permits(2);

    let gateway = model.gateway(root.path(), vec![]).await;
    let mut peer = fixture.peer().await;
    let mut mention = first.clone();
    mention["envelope_id"] = json!("alias");
    mention["payload"]["event_id"] = json!("EvMention");
    mention["payload"]["event"]["type"] = json!("app_mention");
    peer.ack(mention).await;
    let mut repeated = first.clone();
    repeated["envelope_id"] = json!("retry-with-new-envelope");
    peer.ack(repeated).await;
    let mut conflicting = first;
    conflicting["envelope_id"] = json!("conflicting");
    conflicting["payload"]["event"]["text"] = json!("<@UBOT1> different content");
    peer.ack(conflicting).await;
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["text"],
        "answer durable work"
    );
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["thread_ts"],
        "1730000000.000020"
    );
    assert_eq!(model.requests().len(), 1);
    let sql = rusqlite::Connection::open(root.path().join("mercury.db")).unwrap();
    let conflicts: i64 = sql
        .query_row(
            "SELECT count(*) FROM messaging_slack_wire_conflicts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(conflicts, 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn ambiguous_installation_and_denied_dm_are_acknowledged_without_admitting_turns() {
    let (_root, model, fixture, gateway) = setup().await;
    fixture
        .create(
            &gateway,
            "slack-two",
            SECOND,
            false,
            fixture.settings(SECOND, false),
        )
        .await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    state(&gateway, |state| {
        state["verification"]["slack-two"]["socket"] == "ready"
    })
    .await;
    let mut ambiguous = event(
        FIRST,
        "ambiguous",
        "EvAmbiguous",
        "DSAME",
        5,
        "wrong target",
    );
    ambiguous["payload"]["authorizations"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "team_id":SECOND,"user_id":"UBOT2","is_bot":true,
        }));
    peer.ack(ambiguous).await;
    let mut refused = event(FIRST, "denied", "EvDenied", "DENIED", 6, "wrong actor");
    refused["payload"]["event"]["user"] = json!("UPEER");
    peer.ack(refused).await;
    peer.ack(event(
        FIRST,
        "allowed",
        "EvAllowed",
        "DALLOWED",
        7,
        "normal actor",
    ))
    .await;
    until(|| !fixture.calls("chat.postMessage").is_empty()).await;
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["text"],
        "answer normal actor"
    );
    assert_eq!(model.requests().len(), 1);
    assert!(
        !fixture
            .calls("conversations.info")
            .iter()
            .any(|call| call.body["channel"] == "DENIED")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreign_app_enrollment_and_wrong_socket_hello_are_refused() {
    let (_root, model, fixture, gateway) = setup().await;
    dispatch(
        &gateway,
        "messaging.profile.slack.create",
        &json!({"name":"slack-one","settings":fixture.settings(FIRST,true)}),
        false,
    )
    .await
    .unwrap_err();
    dispatch(
        &gateway,
        "messaging.profile.slack.create",
        &json!({"name":"slack-one","settings":fixture.settings(FIRST,true)}),
        true,
    )
    .await
    .unwrap();
    fixture.service.lock().unwrap().wrong_install = Some("AOTHER".into());
    assert!(
        fixture
            .enroll(&gateway, "slack-one", FIRST, true)
            .await
            .is_err()
    );
    assert_eq!(fixture.service.lock().unwrap().opens, 0);
    {
        let mut service = fixture.service.lock().unwrap();
        service.wrong_install = None;
        service.alternate_app = Some("AOTHER".into());
    }
    fixture
        .enroll(&gateway, "slack-one", FIRST, true)
        .await
        .unwrap();
    state(&gateway, |state| {
        state["verification"]["slack-one"]["socket"] == "wrong_app"
    })
    .await;
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_socket_lease_fences_a_competing_profile() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::start().await;
    let model = Model::new().await.with_stock_adapter(STOCK_ADAPTER);
    let first = model
        .gateway(&root.path().join("profiles/first"), vec![])
        .await;
    let second = model
        .gateway(&root.path().join("profiles/second"), vec![])
        .await;
    let _peer = receiver(&fixture, &first, fixture.settings(FIRST, true)).await;
    fixture
        .create(
            &second,
            "slack-competing",
            SECOND,
            true,
            fixture.settings(SECOND, true),
        )
        .await;
    state(&second, |state| {
        state["verification"]["slack-competing"]["socket"] == "receiver_in_use"
    })
    .await;
    assert_eq!(fixture.service.lock().unwrap().opens, 1);
    second.shutdown().await.unwrap();
    first.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconnects_and_rotates_without_replaying_a_turn() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    let mut first = event(FIRST, "source", "EvSource", "DROOM", 9, "once");
    peer.ack(first.clone()).await;
    until(|| !fixture.calls("chat.postMessage").is_empty()).await;
    peer.send(json!({"type":"disconnect","reason":"refresh_requested"}))
        .await;
    let mut peer = fixture.peer().await;
    first["envelope_id"] = json!("retry");
    peer.ack(first.clone()).await;
    fixture
        .enroll(&gateway, "slack-one", FIRST, true)
        .await
        .unwrap();
    let mut peer = fixture.peer().await;
    first["envelope_id"] = json!("retry-after-rotation");
    peer.ack(first).await;
    peer.ack(event(FIRST, "new", "EvNew", "DROOM", 10, "after rotation"))
        .await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    assert_eq!(model.requests().len(), 2);
    assert_eq!(fixture.service.lock().unwrap().opens, 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn slash_result_stays_ephemeral_and_respects_requester_grants() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    let slash = |user, envelope, trigger| {
        json!({"type":"slash_commands","envelope_id":envelope,"accepts_response_payload":true,
        "payload":{"api_app_id":APP,"team_id":FIRST,"channel_id":"DROOM","user_id":user,
            "trigger_id":trigger,"command":"/mercury-status","text":""}})
    };
    let accepted = peer.ack(slash(ACTOR, "command-one", "trigger-1")).await;
    assert_eq!(
        accepted["payload"],
        json!({"response_type":"ephemeral","text":"Mercury is connected."})
    );
    let repeat = peer.ack(slash(ACTOR, "command-retry", "trigger-1")).await;
    assert_eq!(repeat["payload"], accepted["payload"]);
    let denied = peer.ack(slash("UPEER", "command-two", "trigger-2")).await;
    assert_eq!(
        denied["payload"]["text"],
        "This Mercury command is unavailable here."
    );
    assert!(model.requests().is_empty());
    assert!(fixture.calls("chat.postMessage").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_slash_response_uses_the_scoped_handle_at_most_once() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    let slash = |user: &str, envelope: &str, trigger: &str, endpoint: &str| {
        json!({"type":"slash_commands","envelope_id":envelope,"accepts_response_payload":false,
            "payload":{"api_app_id":APP,"team_id":FIRST,"channel_id":"DROOM","user_id":user,
                "trigger_id":trigger,"command":"/mercury-status","response_url":endpoint}})
    };
    let active = format!("{}actions/active-fixture", fixture.origin);
    let expired = format!("{}actions/expired-fixture", fixture.origin);
    let forbidden = fixture.origin.replace("127.0.0.1", "localhost") + "actions/wrong-origin";
    for (user, env, trigger, endpoint) in [
        (ACTOR, "active", "deferred-first", active.as_str()),
        (ACTOR, "repeat-active", "deferred-first", active.as_str()),
        ("UDENIED", "denied", "deferred-denied", active.as_str()),
        (ACTOR, "other-origin", "deferred-origin", forbidden.as_str()),
        (ACTOR, "expired", "deferred-expired", expired.as_str()),
        (
            ACTOR,
            "repeat-expired",
            "deferred-expired",
            expired.as_str(),
        ),
    ] {
        let ack = peer.ack(slash(user, env, trigger, endpoint)).await;
        assert!(ack["payload"].is_null());
    }
    until(|| fixture.calls("response_url").len() == 2).await;
    let calls = fixture.calls("response_url");
    assert_eq!(calls[0].team, "active-fixture");
    assert_eq!(calls[1].team, "expired-fixture");
    assert!(calls.iter().all(|call| call.body
        == json!({
            "response_type":"ephemeral","replace_original":false,"text":"Mercury is connected."
        })));
    assert!(fixture.calls("chat.postMessage").is_empty());
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_pairing_uses_one_private_challenge_and_requires_owner_approval() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["access"]["users"] = json!([]);
    settings["access"]["scope"]["pair_direct"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let pair = |channel, envelope, trigger| {
        json!({"type":"slash_commands","envelope_id":envelope,"accepts_response_payload":true,
            "payload":{"api_app_id":APP,"team_id":FIRST,"channel_id":channel,"user_id":ACTOR,
                "trigger_id":trigger,"command":"/mercury-pair"}})
    };
    let channel = peer
        .ack(pair("CPUBLIC", "group-pair", "pair-trigger-1"))
        .await;
    assert!(channel["payload"].is_null());
    let direct = peer
        .ack(pair("DPAIR", "direct-pair", "pair-trigger-2"))
        .await;
    let retry = peer
        .ack(pair("DPAIR", "retry-pair", "pair-trigger-2"))
        .await;
    assert_eq!(retry["payload"], direct["payload"]);
    assert_eq!(direct["payload"]["response_type"], "ephemeral");
    let code = direct["payload"]["text"]
        .as_str()
        .unwrap()
        .split('`')
        .nth(1)
        .unwrap();
    peer.ack(event(
        FIRST,
        "before-pair",
        "EvBeforePair",
        "DPAIR",
        1,
        "before approval",
    ))
    .await;
    let result = dispatch(
        &gateway,
        "messaging.profile.pair",
        &json!({"name":"slack-one","code":code}),
        true,
    )
    .await
    .unwrap();
    assert_eq!(result["paired"], true);
    peer.ack(event(
        FIRST,
        "after-pair",
        "EvAfterPair",
        "DPAIR",
        2,
        "after approval",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(
        fixture.calls("chat.postMessage")[0].body["text"],
        "answer after approval"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn agent_context_labels_only_the_view_without_hydrating_it() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["agent"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let mut view = event(FIRST, "context", "EvView", "DAGENT", 1, "");
    view["payload"]["event"] = json!({
        "type":"app_context_changed","user":ACTOR,"event_ts":"1730000000.000001",
        "context":{"entities":[
            {"type":"slack#/types/channel_id","value":"CFOREIGN","team_id":SECOND},
            {"type":"slack#/types/channel_id","value":"GPRIVATE","team_id":FIRST},
        ]},
    });
    peer.ack(view).await;
    let info = state(&gateway, |s| {
        s["verification"]["slack-one"]["agent_context"]["channel"] == "GPRIVATE"
    })
    .await;
    assert_eq!(
        info["verification"]["slack-one"]["agent_context"]["actor"],
        ACTOR
    );
    assert!(fixture.calls("conversations.info").is_empty());
    assert!(fixture.calls("conversations.replies").is_empty());
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn card_callback_is_actor_and_message_bound_and_retires_the_original_card() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    peer.ack(event(FIRST, "clarify", "EvClarify", "DROOM", 12, "clarify"))
        .await;
    state(&gateway, |state| {
        state["deliveries"].as_array().unwrap().iter().any(|item| {
            item["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("action-"))
                && item["part_state"] == "sent"
        })
    })
    .await;
    let (card, token) = {
        let service = fixture.service.lock().unwrap();
        let (key, value) = service
            .sent
            .iter()
            .find(|(_, value)| value["blocks"][1]["type"] == "actions")
            .unwrap();
        (
            key.1.clone(),
            value["blocks"][1]["elements"][0]["value"]
                .as_str()
                .unwrap()
                .to_owned(),
        )
    };
    let tap = |actor, envelope, trigger| {
        json!({"type":"interactive","envelope_id":envelope,"accepts_response_payload":true,
        "payload":{"type":"block_actions","api_app_id":APP,"team":{"id":FIRST},"channel":{"id":"DROOM"},"user":{"id":actor},
            "trigger_id":trigger,"container":{"channel_id":"DROOM","message_ts":card},
            "message":{"ts":card,"bot_id":bot(FIRST)},"actions":[{"action_id":"mercury-respond-0","value":token}]}})
    };
    assert!(
        peer.ack(tap("UPEER", "tap-wrong", "trigger-wrong"))
            .await
            .get("payload")
            .is_none()
    );
    let accepted = peer.ack(tap(ACTOR, "tap-right", "trigger-right")).await;
    assert_eq!(accepted["payload"]["response_type"], "ephemeral");
    peer.ack(tap(ACTOR, "tap-replay", "trigger-right")).await;
    until(|| fixture.calls("chat.update").len() == 1 && model.requests().len() >= 2).await;
    assert_eq!(fixture.calls("chat.update")[0].body["ts"], card);
    assert_eq!(fixture.calls("chat.update")[0].body["blocks"], json!([]));
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_send_remains_fenced_across_restart_while_another_route_works() {
    let (root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    fixture.fail_next(FIRST, "chat.postMessage", StatusCode::BAD_GATEWAY);
    peer.ack(event(FIRST, "lost", "EvLost", "DLOST", 30, "lost reply"))
        .await;
    state(&gateway, |state| {
        state["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["part_state"] == "uncertain")
    })
    .await;
    gateway.shutdown().await.unwrap();
    drop(peer);
    let gateway = model.gateway(root.path(), vec![]).await;
    let mut peer = fixture.peer().await;
    peer.ack(event(FIRST, "retry", "EvLost", "DLOST", 30, "lost reply"))
        .await;
    peer.ack(event(
        FIRST,
        "later",
        "EvLater",
        "DLOST",
        31,
        "same destination",
    ))
    .await;
    peer.ack(event(
        FIRST,
        "other",
        "EvOther",
        "DOTHER",
        32,
        "another destination",
    ))
    .await;
    until(|| {
        fixture
            .calls("chat.postMessage")
            .iter()
            .any(|call| call.body["channel"] == "DOTHER")
    })
    .await;
    assert_eq!(
        fixture
            .calls("chat.postMessage")
            .iter()
            .filter(|call| call.body["channel"] == "DLOST")
            .count(),
        1
    );
    let status = gateway.messaging_status().await.unwrap();
    let unknown = status["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["part_state"] == "uncertain")
        .unwrap();
    assert!(
        gateway
            .resolve_delivery(
                unknown["connection"].as_str().unwrap().into(),
                unknown["id"].as_str().unwrap().into(),
                unknown["part"].as_u64().unwrap() as u32,
                unknown["attempt"].as_u64().unwrap() as u32,
                DeliveryResolution::Delivered
            )
            .await
            .unwrap()
    );
    until(|| {
        fixture
            .calls("chat.postMessage")
            .iter()
            .any(|call| call.body["text"] == "answer same destination")
    })
    .await;
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn membership_is_rechecked_after_ack_before_native_claim() {
    let (_root, model, fixture, gateway) = setup().await;
    let pause = Arc::new(Semaphore::new(0));
    fixture.service.lock().unwrap().pause_read = Some(pause.clone());
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    peer.ack(event(
        FIRST,
        "before-revocation",
        "EvRevocation",
        "DROOM",
        33,
        "allowed at receipt",
    ))
    .await;
    state(&gateway, |state| state["input"][0]["state"] == "preparing").await;
    fixture.service.lock().unwrap().denied_channel = Some("DROOM".into());
    pause.add_permits(1);
    state(&gateway, |state| state["input"][0]["state"] == "refused").await;
    assert!(model.requests().is_empty());
    assert!(fixture.calls("chat.postMessage").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn thread_history_is_attributed_and_uses_a_durable_delta_after_restart() {
    let (root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["thread_history"] = json!(true);
    settings["files"] = json!(true);
    let thread = "1730000000.000040";
    {
        let mut slack = fixture.service.lock().unwrap();
        slack.history = vec![
            json!({"ts":thread,"user":"UPEER","text":"Prior thread context",
            "files":[{"id":"FROOT"}]}),
        ];
        slack
            .files
            .insert("FROOT".into(), b"private thread attachment".to_vec());
    }
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let mut first = event(
        FIRST,
        "thread-first",
        "EvContext1",
        "CPUBLIC",
        41,
        "<@UBOT1> first engagement",
    );
    first["payload"]["event"]["thread_ts"] = json!(thread);
    peer.ack(first).await;
    until(|| !fixture.calls("chat.postMessage").is_empty()).await;
    assert!(
        model.requests()[0]
            .to_string()
            .contains("Prior thread context")
    );
    assert!(model.requests()[0].to_string().contains("UPEER"));
    assert_eq!(
        fixture.service.lock().unwrap().downloaded,
        [(FIRST.into(), "FROOT".into())]
    );
    assert_eq!(fixture.calls("conversations.replies")[0].body["ts"], thread);
    gateway.shutdown().await.unwrap();
    drop(peer);

    let gateway = model.gateway(root.path(), vec![]).await;
    let mut peer = fixture.peer().await;
    fixture.service.lock().unwrap().history.push(json!({
        "ts":"1730000000.000042","thread_ts":thread,"user":"UPEER","text":"New authorized context",
    }));
    let mut second = event(
        FIRST,
        "thread-second",
        "EvContext2",
        "CPUBLIC",
        43,
        "<@UBOT1> second engagement",
    );
    second["payload"]["event"]["thread_ts"] = json!(thread);
    peer.ack(second).await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    let history = fixture.calls("conversations.replies");
    assert_eq!(history[1].body["oldest"], "1730000000.000041");
    assert!(
        model.requests()[1]
            .to_string()
            .contains("New authorized context")
    );
    assert_eq!(model.requests().len(), 2);
    assert_eq!(fixture.service.lock().unwrap().downloaded.len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn rate_limit_on_one_workspace_method_preserves_other_workspace_delivery() {
    let (_root, _model, fixture, gateway) = setup().await;
    fixture
        .create(
            &gateway,
            "slack-two",
            SECOND,
            false,
            fixture.settings(SECOND, false),
        )
        .await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    state(&gateway, |state| {
        state["verification"]["slack-two"]["socket"] == "ready"
    })
    .await;
    fixture.fail_next(FIRST, "chat.postMessage", StatusCode::TOO_MANY_REQUESTS);
    peer.ack(event(FIRST, "rate", "EvRate", "DROOM", 45, "rate limited"))
        .await;
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    peer.ack(event(
        SECOND,
        "second",
        "EvRateOther",
        "DROOM",
        45,
        "still eligible",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 3).await;
    let calls = fixture.calls("chat.postMessage");
    assert_eq!(
        calls
            .iter()
            .map(|call| call.team.as_str())
            .collect::<Vec<_>>(),
        [FIRST, SECOND, FIRST]
    );
    assert!(calls.iter().all(|call| call.body["channel"] == "DROOM"));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_stream_start_append_and_stop_use_the_same_scoped_timestamp() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["agent"] = json!(true);
    settings["native_streams"] = json!(true);
    settings["feedback_buttons"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    peer.ack(event(
        FIRST,
        "stream",
        "EvStream",
        "DROOM",
        51,
        "steer wait",
    ))
    .await;
    until(|| fixture.calls("chat.startStream").len() == 1).await;
    model.release_waiting_turn();
    until(|| fixture.calls("chat.stopStream").len() == 1).await;
    let started = &fixture.calls("chat.startStream")[0];
    let appended = &fixture.calls("chat.appendStream")[0];
    let stopped = &fixture.calls("chat.stopStream")[0];
    assert_eq!(started.body["thread_ts"], "1730000000.000051");
    assert_eq!(started.body["channel"], "DROOM");
    assert_eq!(appended.body["ts"], stopped.body["ts"]);
    assert_eq!(appended.body["markdown_text"], "Working");
    assert_eq!(
        stopped.body["blocks"][0]["elements"][0]["type"],
        "feedback_buttons"
    );
    assert!(fixture.calls("chat.postMessage").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn feedback_requires_the_original_reply_and_recipient() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["feedback_buttons"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    peer.ack(event(
        FIRST,
        "feedback-input",
        "EvFeedback",
        "DFEEDBACK",
        75,
        "feedback example",
    ))
    .await;
    state(&gateway, |s| {
        s["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["part_state"] == "sent"))
    })
    .await;
    let message = {
        let service = fixture.service.lock().unwrap();
        let ((_, receipt), post) = service.sent.first_key_value().unwrap();
        assert_eq!(
            post["blocks"][1]["elements"][0]["action_id"],
            "mercury-feedback"
        );
        receipt.clone()
    };
    let click = |actor: &str, chat: &str, envelope: &str, trigger: &str| {
        json!({
            "type":"interactive","envelope_id":envelope,"accepts_response_payload":true,
            "payload":{"type":"block_actions","api_app_id":APP,"team":{"id":FIRST},
                "user":{"id":actor},"channel":{"id":chat},
                "container":{"message_ts":message,"channel_id":chat},
                "message":{"ts":message,"bot_id":bot(FIRST)},
                "trigger_id":trigger,"actions":[{"action_id":"mercury-feedback","value":"positive"}]}
        })
    };
    let wrong_actor = peer
        .ack(click("UPEER", "DFEEDBACK", "bad-actor", "feedback-1"))
        .await;
    assert!(wrong_actor["payload"].is_null());
    let wrong_room = peer
        .ack(click(ACTOR, "DOTHER", "bad-room", "feedback-2"))
        .await;
    assert!(wrong_room["payload"].is_null());
    let valid = peer
        .ack(click(ACTOR, "DFEEDBACK", "feedback", "feedback-3"))
        .await;
    assert_eq!(valid["payload"]["response_type"], "ephemeral");
    let retry = peer
        .ack(click(ACTOR, "DFEEDBACK", "retry-feedback", "feedback-3"))
        .await;
    assert_eq!(retry["payload"], valid["payload"]);
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn ambiguous_stream_append_fences_later_effects_through_a_restart() {
    let (root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["agent"] = json!(true);
    settings["native_streams"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    fixture.fail_next(
        FIRST,
        "chat.appendStream",
        StatusCode::INTERNAL_SERVER_ERROR,
    );
    peer.ack(event(
        FIRST,
        "uncertain-stream",
        "EvUncertainStream",
        "DRAIN",
        71,
        "steer wait",
    ))
    .await;
    until(|| fixture.calls("chat.startStream").len() == 1).await;
    model.release_waiting_turn();
    let uncertain = |s: &Value| {
        s["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["part_state"] == "uncertain"))
    };
    state(&gateway, uncertain).await;
    gateway.shutdown().await.unwrap();
    drop(peer);
    let gateway = model.gateway(root.path(), vec![]).await;
    let mut peer = fixture.peer().await;
    state(&gateway, uncertain).await;
    peer.ack(event(
        FIRST,
        "retry-uncertain",
        "EvUncertainStream",
        "DRAIN",
        71,
        "steer wait",
    ))
    .await;
    peer.ack(event(
        FIRST,
        "independent",
        "EvStreamIndependent",
        "DSEPARATE",
        72,
        "independent route",
    ))
    .await;
    until(|| {
        fixture
            .calls("chat.postMessage")
            .iter()
            .chain(fixture.calls("chat.appendStream").iter())
            .any(|call| call.body["channel"] == "DSEPARATE")
    })
    .await;
    assert_eq!(
        fixture
            .calls("chat.appendStream")
            .iter()
            .filter(|call| call.body["channel"] == "DRAIN")
            .count(),
        1
    );
    assert!(
        fixture
            .calls("chat.postMessage")
            .iter()
            .chain(fixture.calls("chat.stopStream").iter())
            .all(|call| call.body["channel"] != "DRAIN")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_transfer_is_not_repeated_when_a_share_retries_after_restart() {
    let (root, model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["files"] = json!(true);
    settings["access"]["scope"]["direct"]["turn"] = json!({"approval":"off"});
    let mut peer = receiver(&fixture, &gateway, settings).await;
    fixture.fail_next(
        FIRST,
        "files.completeUploadExternal",
        StatusCode::TOO_MANY_REQUESTS,
    );
    peer.ack(event(
        FIRST,
        "artifact",
        "EvArtifact",
        "DROOM",
        72,
        "write artifact",
    ))
    .await;
    until(|| fixture.calls("files.completeUploadExternal").len() == 1).await;
    state(&gateway, |state| {
        state["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["part_state"] == "pending" && row["error"] == "rate_limited")
    })
    .await;
    let before = fixture.service.lock().unwrap().transfers.clone();
    assert_eq!(before.len(), 1);
    assert_eq!(before.values().next().unwrap(), b"scoped artifact");
    gateway.shutdown().await.unwrap();
    drop(peer);
    let gateway = model.gateway(root.path(), vec![]).await;
    let _peer = fixture.peer().await;
    until(|| fixture.calls("files.completeUploadExternal").len() == 2).await;
    assert_eq!(fixture.calls("files.getUploadURLExternal").len(), 1);
    assert_eq!(fixture.service.lock().unwrap().transfers, before);
    let calls = fixture.calls("files.completeUploadExternal");
    assert_eq!(calls[0].body, calls[1].body);
    assert_eq!(calls[1].body["channel_id"], "DROOM");
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn private_media_uses_the_installation_credential_and_refuses_other_origins() {
    let (_root, model, fixture, gateway) = setup().await;
    fixture
        .service
        .lock()
        .unwrap()
        .files
        .insert("FFIXTURE".into(), b"file text".to_vec());
    let mut settings = fixture.settings(SECOND, false);
    settings["files"] = json!(true);
    fixture
        .create(&gateway, "slack-two", SECOND, false, settings)
        .await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    state(&gateway, |state| {
        state["verification"]["slack-two"]["socket"] == "ready"
    })
    .await;
    let mut file = event(SECOND, "file", "EvFile", "DROOM", 81, "read private file");
    file["payload"]["event"]["subtype"] = json!("file_share");
    file["payload"]["event"]["files"] = json!([{"id":"FFIXTURE","file_access":"check_file_info"}]);
    peer.ack(file.clone()).await;
    until(|| !fixture.calls("chat.postMessage").is_empty()).await;
    assert_eq!(
        fixture.service.lock().unwrap().downloaded,
        [(SECOND.into(), "FFIXTURE".into())]
    );
    assert_eq!(fixture.calls("files.info")[0].team, SECOND);
    fixture.service.lock().unwrap().forced_url = Some("https://other-host.invalid/private".into());
    file["envelope_id"] = json!("file-other");
    file["payload"]["event_id"] = json!("EvFileOther");
    file["payload"]["event"]["ts"] = json!("1730000000.000082");
    peer.ack(file).await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    assert_eq!(fixture.service.lock().unwrap().downloaded.len(), 1);
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_dm_history_preserves_each_thread_as_a_reply_destination() {
    let (_root, _model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["shared_dm_history"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    for (ordinal, thread, text) in [
        (81, "1730000000.000051", "first thread"),
        (82, "1730000000.000061", "second thread"),
    ] {
        let mut input = event(
            FIRST,
            &format!("envelope-{ordinal}"),
            &format!("EvShared{ordinal}"),
            "DMERGED",
            ordinal,
            text,
        );
        input["payload"]["event"]["thread_ts"] = json!(thread);
        peer.ack(input).await;
    }
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    let calls = fixture.calls("chat.postMessage");
    assert!(
        calls
            .iter()
            .any(|call| call.body["thread_ts"] == "1730000000.000051"
                && call.body["text"] == "answer first thread")
    );
    assert!(
        calls
            .iter()
            .any(|call| call.body["thread_ts"] == "1730000000.000061"
                && call.body["text"] == "answer second thread")
    );
    let snapshot = gateway.messaging_status().await.unwrap();
    let inputs = snapshot["input"].as_array().unwrap();
    assert!(inputs[0]["session"].is_string());
    assert_eq!(inputs[0]["session"], inputs[1]["session"]);
    assert_ne!(inputs[0]["route"], inputs[1]["route"]);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn opting_into_shared_dm_history_preserves_existing_thread_sessions() {
    let (_root, _model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    let mut peer = receiver(&fixture, &gateway, settings.clone()).await;
    let thread = "1730000000.000089";
    let input = |ordinal, envelope, id, text| {
        let mut msg = event(FIRST, envelope, id, "DKEEP", ordinal, text);
        msg["payload"]["event"]["thread_ts"] = json!(thread);
        msg
    };
    peer.ack(input(90, "independent", "EvKeep", "original session"))
        .await;
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    let before = gateway.messaging_status().await.unwrap()["input"][0]["session"].clone();
    assert!(before.is_string());
    let config = dispatch(&gateway, "messaging.profile.slack.list", &json!({}), true)
        .await
        .unwrap();
    settings["shared_dm_history"] = json!(true);
    dispatch(
        &gateway,
        "messaging.profile.slack.update",
        &json!({
            "name":"slack-one","revision":config["connections"][0]["revision"],"settings":settings,
        }),
        true,
    )
    .await
    .unwrap();
    drop(peer);
    let mut peer = fixture.peer().await;
    peer.ack(input(91, "same-thread", "EvKeepAgain", "continue original"))
        .await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    assert_eq!(
        fixture.calls("chat.postMessage")[1].body["thread_ts"],
        thread
    );
    let snapshot = gateway.messaging_status().await.unwrap();
    assert!(
        snapshot["input"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["session"] == before)
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn agent_stop_requires_the_exact_live_stream_and_operator() {
    let (_root, _model, fixture, gateway) = setup().await;
    let mut settings = fixture.settings(FIRST, true);
    settings["agent"] = json!(true);
    settings["native_streams"] = json!(true);
    let mut peer = receiver(&fixture, &gateway, settings).await;
    peer.ack(event(
        FIRST,
        "running",
        "EvRunning",
        "DROOM",
        90,
        "steer wait",
    ))
    .await;
    state(&gateway, |state| {
        state["deliveries"].as_array().unwrap().iter().any(|row| {
            row["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("progress-"))
                && row["part_state"] == "sent"
        })
    })
    .await;
    let recorded = fixture.service.lock().unwrap().streamed[0].clone();
    let stop = |actor, stream: &str, occurrence| {
        let mut input = event(FIRST, occurrence, occurrence, "DROOM", 92, "");
        input["payload"]["event"] = json!({"type":"agent_session_stopped","channel":"DROOM",
            "thread_ts":"1730000000.000090","streaming_message_ts":[stream],"user":actor});
        input
    };
    peer.ack(stop("UPEER", &recorded, "EvWrongActor")).await;
    peer.ack(stop(ACTOR, "1740000000.999999", "EvWrongStream"))
        .await;
    assert!(fixture.calls("chat.stopStream").is_empty());
    peer.ack(stop(ACTOR, &recorded, "EvAuthorizedStop")).await;
    until(|| fixture.calls("chat.stopStream").len() == 1).await;
    assert_eq!(fixture.calls("chat.stopStream")[0].body["ts"], recorded);
    assert_eq!(
        fixture.calls("chat.stopStream")[0].body["session_status"],
        "active"
    );
    gateway.shutdown().await.unwrap();
}

struct Speech;

impl hermes_gateway_core::messaging::AudioService for Speech {
    fn transcribe<'a>(
        &'a self,
        _: &'a std::path::Path,
        wav: &'a [u8],
    ) -> hermes_gateway_core::messaging::AdapterFuture<
        'a,
        Result<String, hermes_gateway_core::messaging::GatewayError>,
    > {
        Box::pin(async move {
            assert!(wav.starts_with(b"RIFF"));
            Ok("fixture speech".into())
        })
    }

    fn synthesize<'a>(
        &'a self,
        _: &'a std::path::Path,
        text: &'a str,
    ) -> hermes_gateway_core::messaging::AdapterFuture<
        'a,
        Result<Vec<u8>, hermes_gateway_core::messaging::GatewayError>,
    > {
        Box::pin(async move {
            assert!(text.contains("fixture speech"));
            Ok(hermes_gateway_core::messaging::voice_codec::pcm_wave(
                &[512; 320],
            ))
        })
    }
}

#[tokio::test]
async fn authorized_audio_uses_profile_transcription_and_uploads_the_spoken_reply() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(STOCK_ADAPTER);
    let fixture = Fixture::start().await;
    let gateway = model
        .gateway_with_audio(root.path(), vec![], Some(Arc::new(Speech)))
        .await;
    let mut settings = fixture.settings(FIRST, true);
    settings["files"] = json!(true);
    settings["access"]["direct_spoken_replies"] = json!(true);
    settings["access"]["scope"]["spoken_reply_default"] = json!("voice_notes");
    let mut peer = receiver(&fixture, &gateway, settings).await;
    let wav = hermes_gateway_core::messaging::voice_codec::pcm_wave(&[512; 320]);
    fixture
        .service
        .lock()
        .unwrap()
        .files
        .insert("FVOICE".into(), wav);
    let mut audio = event(FIRST, "voice", "EvVoice", "DROOM", 95, "");
    audio["payload"]["event"]["files"] = json!([{"id":"FVOICE","subtype":"slack_audio"}]);
    peer.ack(audio).await;
    until(|| fixture.calls("files.completeUploadExternal").len() == 1).await;
    assert_eq!(
        fixture.calls("files.getUploadURLExternal")[0].body["filename"],
        "mercury-reply.wav"
    );
    assert!(
        fixture
            .service
            .lock()
            .unwrap()
            .transfers
            .values()
            .all(|bytes| bytes.starts_with(b"RIFF"))
    );
    assert!(model.requests()[0].to_string().contains("fixture speech"));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn uninstall_revokes_the_old_credential_and_preserves_the_verified_workspace_history() {
    let (_root, model, fixture, gateway) = setup().await;
    let mut peer = receiver(&fixture, &gateway, fixture.settings(FIRST, true)).await;
    peer.ack(event(
        FIRST,
        "prior",
        "EvPrior",
        "DROOM",
        101,
        "before uninstall",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 1).await;
    let mut removed = event(FIRST, "uninstall", "EvUninstall", "DROOM", 102, "");
    removed["payload"]["event"] = json!({"type":"app_uninstalled"});
    peer.ack(removed).await;
    state(&gateway, |state| {
        state["verification"]["slack-one"]["socket"] == "uninstalled"
    })
    .await;
    peer.ack(event(
        FIRST,
        "revoked",
        "EvRevoked",
        "DROOM",
        103,
        "revoked",
    ))
    .await;
    assert_eq!(model.requests().len(), 1);

    fixture
        .enroll(&gateway, "slack-one", FIRST, true)
        .await
        .unwrap();
    let _revoked_socket = fixture.peer().await;
    state(&gateway, |state| {
        state["verification"]["slack-one"]["socket"] == "uninstalled"
    })
    .await;
    let lease = gateway
        .begin_adapter_enrollment("slack-one", std::time::Duration::from_secs(15))
        .await
        .unwrap();
    gateway
        .complete_adapter_enrollment("slack-one", &lease.id, fixtures::token_bundle('3', true))
        .await
        .unwrap();
    let mut peer = fixture.peer().await;
    state(&gateway, |state| {
        state["verification"]["slack-one"]["socket"] == "ready"
    })
    .await;
    peer.ack(event(
        FIRST,
        "renewed",
        "EvRenewed",
        "DROOM",
        104,
        "after reinstallation",
    ))
    .await;
    until(|| fixture.calls("chat.postMessage").len() == 2).await;
    let status = gateway.messaging_status().await.unwrap();
    let inputs = status["input"].as_array().unwrap();
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0]["session"], inputs[1]["session"]);
    gateway.shutdown().await.unwrap();
}
