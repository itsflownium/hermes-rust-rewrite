use super::*;
use crate::{Config, Gateway, GatewayConfig, RunningGateway, client::GatewayClient};
use axum::{Json, Router, http::header, routing::post};
use hermes_telegram::{
    Error, Incoming, Method, RequestFuture, Settings as TelegramSettings, Transport,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::Mutex,
};
use tokio::{net::TcpListener, task::JoinHandle};

const BOT_ID: i64 = 123456;
type Requests = Arc<Mutex<Vec<Value>>>;

#[derive(Default)]
struct Telegram {
    updates: Mutex<VecDeque<Value>>,
    offsets: Mutex<Vec<i64>>,
    sends: Mutex<Vec<(i64, Value)>>,
    failures: Mutex<HashMap<i64, VecDeque<Error>>>,
}

impl Transport for Telegram {
    fn request<'a>(&'a self, _: &'a Token, method: Method, body: Value) -> RequestFuture<'a> {
        Box::pin(async move {
            match method {
                Method::GetMe => Ok(json!({"id":BOT_ID,"is_bot":true,"username":"fixture_bot"})),
                Method::GetWebhookInfo => Ok(json!({"url":""})),
                Method::GetUpdates => {
                    self.offsets
                        .lock()
                        .unwrap()
                        .push(body["offset"].as_i64().unwrap());
                    let batch = self.updates.lock().unwrap().pop_front();
                    if let Some(batch) = batch {
                        Ok(batch)
                    } else {
                        time::sleep(Duration::from_millis(50)).await;
                        Ok(json!([]))
                    }
                }
                Method::SendMessage => {
                    let chat = body["chat_id"].as_i64().unwrap();
                    let message_id = {
                        let mut sends = self.sends.lock().unwrap();
                        sends.push((now(), body));
                        1000 + sends.len()
                    };
                    if let Some(error) = self
                        .failures
                        .lock()
                        .unwrap()
                        .get_mut(&chat)
                        .and_then(VecDeque::pop_front)
                    {
                        return Err(error);
                    }
                    Ok(json!({"message_id":message_id, "chat":{"id":chat}, "date":1234}))
                }
                _ => panic!("The predecessor host requested an unexpected Telegram method"),
            }
        })
    }
}

struct Model {
    config: Config,
    requests: Requests,
    task: JoinHandle<()>,
}

impl Drop for Model {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Model {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Requests::default();
        let recorded = requests.clone();
        let task = tokio::spawn(async move {
            let router = Router::new().route("/v1/chat/completions", post(move |Json(body): Json<Value>| {
                let recorded = recorded.clone();
                async move {
                    let prompt = body["messages"].as_array().unwrap().last().unwrap()["content"].as_str().unwrap();
                    let (delta, finish) = if prompt == "unexpected-call" {
                        (json!({"content":"This must not be delivered","tool_calls":[{"index":0,"id":"fixture-call",
                            "function":{"name":"terminal","arguments":"{}"}}]}), "tool_calls")
                    } else {
                        (json!({"content":format!("Reply to {prompt}")}), "stop")
                    };
                    recorded.lock().unwrap().push(body);
                    let event = json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
                    ([(header::CONTENT_TYPE, "text/event-stream")], format!("data: {event}\n\ndata: [DONE]\n\n"))
                }
            }));
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            config: Config {
                base_url: format!("http://{address}/v1"),
                model: "fixture-model".into(),
                api_key: String::new(),
            },
            requests,
            task,
        }
    }

    async fn gateway(&self, root: &Path, telegram: Arc<Telegram>) -> RunningGateway {
        let mut config = GatewayConfig::new(root, self.config.clone());
        config.telegram_transport = Some(telegram);
        Gateway::start(config).await.unwrap()
    }
}

fn identity() -> Identity {
    Identity {
        id: BOT_ID,
        username: "fixture_bot".into(),
        name: "Telegram".into(),
    }
}

fn token() -> Token {
    Token::parse("123456:abcdefghijklmnopqrstuvwxyz1234567890").unwrap()
}

fn setup(root: &Path, allowed: &[i64]) -> Store {
    let store = Store::new(root);
    store
        .save_settings(&TelegramSettings {
            version: 1,
            token: Some(token()),
            allowed: BTreeSet::from_iter(allowed.iter().copied()),
            bot: Some(identity()),
            enabled: true,
            agent_replies: true,
        })
        .unwrap();
    store
}

fn set_enabled(store: &Store, enabled: bool) {
    let mut settings = store.load_settings().unwrap().unwrap();
    settings.enabled = enabled;
    store.save_settings(&settings).unwrap();
}

fn incoming(update_id: i64, chat_id: i64, text: &str) -> Incoming {
    Incoming {
        bot_id: BOT_ID,
        update_id,
        chat_id,
        user_id: chat_id,
        message_id: update_id,
        text: text.into(),
        sender: "Fixture".into(),
        timestamp: 1234,
    }
}

fn message(id: i64, chat: i64, text: &str) -> Value {
    json!({"update_id":id,"message":{"message_id":id,"date":1234,
        "from":{"id":chat,"is_bot":false},"chat":{"id":chat,"type":"private"},"text":text}})
}

async fn expect_work(gateway: &RunningGateway, predicate: impl Fn(&Value) -> bool) -> Value {
    let mut last = Value::Null;
    match time::timeout(Duration::from_secs(10), async {
        loop {
            last = gateway.shared.telegram.status().await.unwrap();
            if predicate(&last["work"]) {
                break;
            }
            time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    {
        Ok(()) => last["work"].clone(),
        Err(_) => panic!("Telegram work did not reach expected state: {last}"),
    }
}

async fn local_client(gateway: &RunningGateway) -> GatewayClient {
    let client = GatewayClient::default();
    let ticket = gateway.shared.tickets.issue_native().unwrap();
    client
        .connect_local_grant(&gateway.http_url(), &ticket)
        .await
        .unwrap();
    client
}

fn head_connector(store: Store, telegram: Arc<Telegram>) -> Connector {
    Connector {
        store,
        client: Client::with_transport(token(), telegram),
        bot: identity(),
        cancel: CancellationToken::new(),
    }
}

#[tokio::test]
async fn telegram_deduplicates_routes_dms_and_delivers_only_committed_text() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path(), &[42, 43, 44]);
    let library = hermes_library::Library::open(dir.path()).unwrap();
    library
        .create_background_session("Unrelated conversation", "telegram-route:123456:44", |_| {
            Ok(())
        })
        .unwrap();
    std::fs::write(dir.path().join("SOUL.md"), "Profile voice fixture.").unwrap();
    let telegram = Arc::new(Telegram::default());
    let first = message(10, 42, "@file:missing.txt first");
    let mut topic = message(15, 42, "private topic");
    topic["message"]["message_thread_id"] = json!(2);
    let mut bot = message(17, 42, "from another bot");
    bot["message"]["from"]["is_bot"] = json!(true);
    telegram.updates.lock().unwrap().push_back(json!([
        message(12, 42, "second"), first, first, message(10, 42, "conflicting payload"),
        message(11, 43, "separate chat"), message(13, 99, "outside allowlist"),
        message(14, 42, "unexpected-call"), topic, {"update_id":16,"edited_message":first["message"]}, bot,
        message(18, 44, "conflicting route"),
    ]));
    let model = Model::start().await;
    let gateway = model.gateway(dir.path(), telegram.clone()).await;
    let work = expect_work(&gateway, |work| {
        work["counts"]["sent"] == 3
            && work["counts"]["failed"] == 1
            && work["counts"]["blocked"] == 1
    })
    .await;
    assert_eq!(work["conflicts"], 1);
    assert_eq!(store.load_inbox(&identity()).unwrap().cursor, 19);
    assert!(store.route(&identity(), 44).unwrap().is_none());

    let ids = [
        store.route(&identity(), 42).unwrap().unwrap(),
        store.route(&identity(), 43).unwrap().unwrap(),
    ];
    assert_ne!(ids[0], ids[1]);
    assert!(gateway.shared.library.active_session().unwrap().is_none());
    let perm_chat = gateway.shared.library.session(&ids[0]).unwrap();
    assert!(perm_chat.project_id.is_none());
    let (history, _) = hermes_engine::read_conversation(&perm_chat.directory).unwrap();
    assert_eq!(history.turns.len(), 3);
    assert!(
        history
            .turns
            .iter()
            .all(|turn| turn.context.is_empty() && turn.attachments.is_empty())
    );
    assert!(matches!(
        history.turns[2].status,
        hermes_engine::Status::Failed(_)
    ));
    let perm_other = gateway.shared.library.session(&ids[1]).unwrap();
    assert_eq!(
        hermes_engine::read_conversation(&perm_other.directory)
            .unwrap()
            .0
            .turns
            .len(),
        1
    );

    {
        let requests = model.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        for request in requests.iter() {
            assert!(request["tools"].is_null() || request["tools"] == json!([]));
            assert!(
                request["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .contains("Profile voice fixture.")
            );
        }
        let second = requests
            .iter()
            .find(|request| {
                request["messages"].as_array().unwrap().last().unwrap()["content"] == "second"
            })
            .unwrap();
        assert_eq!(second["messages"][1]["content"], "@file:missing.txt first");
        assert_eq!(
            second["messages"][2]["content"],
            "Reply to @file:missing.txt first"
        );
    }
    {
        let sends = telegram.sends.lock().unwrap();
        let chat = sends
            .iter()
            .filter(|(_, body)| body["chat_id"] == 42)
            .map(|(_, body)| body["text"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            chat,
            ["Reply to @file:missing.txt first", "Reply to second"]
        );
        assert!(sends.iter().all(
            |(_, body)| body["link_preview_options"]["is_disabled"] == true
                && body["parse_mode"].is_null()
        ));
    }
    gateway.shutdown().await.unwrap();

    // Return an already acknowledged update even when asked for a later offset.
    // Telegram normally filters this itself, and the gateway still does not replay it.
    telegram.updates.lock().unwrap().push_back(json!([first]));
    let before_restart = telegram.offsets.lock().unwrap().len();
    let restarted = model.gateway(dir.path(), telegram.clone()).await;
    time::timeout(Duration::from_secs(3), async {
        loop {
            if telegram.offsets.lock().unwrap().len() >= before_restart + 2 {
                break;
            }
            time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        &telegram.offsets.lock().unwrap()[before_restart..before_restart + 2],
        &[19, 19]
    );
    assert_eq!(store.load_inbox(&identity()).unwrap().messages.len(), 8);
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    assert_eq!(telegram.sends.lock().unwrap().len(), 3);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn telegram_restart_resumes_received_queued_and_complete_but_fences_started() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path(), &[42, 43, 44, 45]);
    let inputs = [
        incoming(10, 42, "received"),
        incoming(11, 43, "queued"),
        incoming(12, 44, "complete"),
        incoming(13, 45, "started"),
        incoming(14, 45, "after the uncertain turn"),
    ];
    store
        .commit_poll(&identity(), &token(), &inputs, 0, 15, now())
        .unwrap();
    set_enabled(&store, false);

    let model = Model::start().await;
    let telegram = Arc::new(Telegram::default());
    let gateway = model.gateway(dir.path(), telegram.clone()).await;
    let connector = head_connector(store.clone(), telegram.clone());
    for input in &inputs[1..4] {
        let Route::Session(session) = connector
            .session_for(&gateway.shared, input.chat_id, true)
            .await
            .unwrap()
        else {
            panic!("invalid fixture route");
        };
        let submission = submission_key(BOT_ID, input.update_id);
        gateway
            .shared
            .submit_telegram(session.clone(), input.text.clone(), submission.clone())
            .await
            .unwrap();
        if input.update_id == 11 {
            continue;
        }
        let claim = gateway
            .shared
            .claim_telegram_turn(&session)
            .unwrap()
            .unwrap();
        if input.update_id == 12 {
            gateway.shared.spawn_worker(session.clone(), claim).await;
            time::timeout(Duration::from_secs(5), async {
                loop {
                    let receipt = gateway
                        .shared
                        .submission_receipt(&session, &submission)
                        .unwrap();
                    if receipt["status"] == "terminal" {
                        assert_eq!(receipt["outcome"], "complete");
                        assert_eq!(receipt["result"]["text"], "Reply to complete");
                        break;
                    }
                    time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        // For update 13, only the durable claim has happened when the owner stops.
    }
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    assert!(telegram.sends.lock().unwrap().is_empty());
    gateway.shutdown().await.unwrap();

    set_enabled(&store, true);
    let restarted = model.gateway(dir.path(), telegram.clone()).await;
    let work = expect_work(&restarted, |work| {
        work["counts"]["sent"] == 3 && work["counts"]["turn_unknown"] == 1
    })
    .await;
    assert_eq!(work["issues"][0]["update_id"], 13);
    assert_eq!(model.requests.lock().unwrap().len(), 3);
    {
        let sent = telegram.sends.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert!(sent.iter().all(|(_, body)| body["chat_id"] != 45));
    }
    let local = local_client(&restarted).await;
    let status = local.request("telegram.status", json!({})).await.unwrap();
    let issue = &status["work"]["issues"][0];
    assert_eq!(issue["update_id"], 13);
    let session = issue["session_id"].as_str().unwrap();
    let admission = issue["admission_id"].as_str().unwrap();
    let generation = issue["execution_generation"].as_u64().unwrap();
    local
        .request(
            "session.resume",
            json!({"session_id":session,"omit_messages":true}),
        )
        .await
        .unwrap();
    let resolved = local
        .request(
            "prompt.resolve_unknown",
            json!({
                "session_id":session,"admission_id":admission,"execution_generation":generation,
            }),
        )
        .await
        .unwrap();
    assert_eq!(resolved["outcome"], "interrupted");
    expect_work(&restarted, |work| {
        work["counts"]["sent"] == 4 && work["counts"]["failed"] == 1
    })
    .await;
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    assert_eq!(
        telegram.sends.lock().unwrap().last().unwrap().1["text"],
        "Reply to after the uncertain turn"
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn telegram_lost_ack_and_interrupted_send_never_resend_and_fence_only_their_chat() {
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path(), &[42, 43, 44]);
    let telegram = Arc::new(Telegram::default());
    telegram.failures.lock().unwrap().insert(
        42,
        vec![Error {
            kind: ErrorKind::Retryable,
            message: "fake accepted the message and then lost the reply".into(),
            retry_after: Some(1),
        }]
        .into(),
    );
    telegram.updates.lock().unwrap().push_back(json!([
        message(10, 42, "lost acknowledgement"),
        message(11, 42, "same chat later"),
        message(12, 43, "other chat"),
        message(13, 44, "interrupted delivery"),
    ]));
    let model = Model::start().await;
    let gateway = model.gateway(dir.path(), telegram.clone()).await;
    expect_work(&gateway, |work| {
        work["counts"]["unknown"] == 1 && work["counts"]["sent"] == 2
    })
    .await;
    assert_eq!(
        telegram
            .sends
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, body)| body["chat_id"] == 42)
            .count(),
        1
    );
    gateway.shutdown().await.unwrap();

    // Advance a separate update to a durable sending state, with no durable outcome.
    store
        .commit_poll(
            &identity(),
            &token(),
            &[incoming(14, 44, "crash after send claim")],
            14,
            15,
            now(),
        )
        .unwrap();
    store
        .record_turn(
            &identity(),
            14,
            TurnOutcome::Reply("Previously committed".into()),
        )
        .unwrap();
    let head = store
        .ready(&identity(), now())
        .unwrap()
        .into_iter()
        .find(|work| work.chat_id == 44)
        .unwrap();
    assert_eq!(
        store
            .begin_send(&identity(), &token(), &head, now())
            .unwrap(),
        Some(1)
    );

    let restarted = model.gateway(dir.path(), telegram.clone()).await;
    let work = expect_work(&restarted, |work| work["counts"]["unknown"] == 2).await;
    assert!(
        work["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["update_id"] == 14 && issue["reason"] == "send_interrupted")
    );
    let local = local_client(&restarted).await;
    assert!(
        local
            .request(
                "telegram.resolve_delivery",
                json!({
                    "bot_id":BOT_ID,"update_id":10,"attempt":2,"resolution":"delivered",
                })
            )
            .await
            .is_err()
    );
    let resolution = local
        .request(
            "telegram.resolve_delivery",
            json!({
                "bot_id":BOT_ID,"update_id":10,"attempt":1,"resolution":"delivered",
            }),
        )
        .await
        .unwrap();
    assert_eq!(resolution["resolved"], true);
    expect_work(&restarted, |work| {
        work["counts"]["sent"] == 4 && work["counts"]["unknown"] == 1
    })
    .await;
    {
        let sent = telegram.sends.lock().unwrap();
        let chat = sent
            .iter()
            .filter(|(_, body)| body["chat_id"] == 42)
            .map(|(_, body)| body["text"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            chat,
            ["Reply to lost acknowledgement", "Reply to same chat later"]
        );
        assert_eq!(sent.len(), 4);
    }
    assert_eq!(model.requests.lock().unwrap().len(), 4);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn telegram_retries_explicit_rate_limit_after_restart_without_regenerating() {
    let dir = tempfile::tempdir().unwrap();
    let _store = setup(dir.path(), &[42]);
    let telegram = Arc::new(Telegram::default());
    telegram
        .updates
        .lock()
        .unwrap()
        .push_back(json!([message(10, 42, "rate limited")]));
    telegram.failures.lock().unwrap().insert(
        42,
        vec![Error {
            kind: ErrorKind::RateLimited,
            message: "fake definite 429 response".into(),
            retry_after: Some(2),
        }]
        .into(),
    );
    let model = Model::start().await;
    let gateway = model.gateway(dir.path(), telegram.clone()).await;
    let limited = expect_work(&gateway, |work| {
        work["issues"][0]["reason"] == "rate_limited"
    })
    .await;
    let retry_at = limited["issues"][0]["retry_at"].as_i64().unwrap();
    assert_eq!(telegram.sends.lock().unwrap().len(), 1);
    gateway.shutdown().await.unwrap();

    let restarted = model.gateway(dir.path(), telegram.clone()).await;
    expect_work(&restarted, |work| work["counts"]["sent"] == 1).await;
    {
        let sent = telegram.sends.lock().unwrap();
        assert_eq!(sent.len(), 2);
        assert!(sent[1].0 >= retry_at);
        assert_eq!(sent[0].1, sent[1].1);
    }
    assert_eq!(model.requests.lock().unwrap().len(), 1);
    restarted.shutdown().await.unwrap();
}
