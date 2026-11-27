use axum::{
    Json, Router,
    extract::{Multipart, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use hermes_gateway_core::{
    RunningGateway,
    messaging::{
        AdapterFuture, AudioService, EnrollmentCredential, GatewayError,
        test_support::{Model, dispatch},
        voice_codec,
    },
};
use hermes_telegram_plugin::STOCK_ADAPTER;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

const BOT: i64 = 76012;
const NAME: &str = "telegram-fixture";
const SECRET_HEADER: &str = "x-telegram-bot-api-secret-token";

#[derive(Default)]
struct BotApi {
    calls: Vec<(String, Value)>,
    batches: VecDeque<Vec<Value>>,
    webhook: String,
    secret: String,
    next_message: i64,
    poll_conflict: bool,
    lose_send: Option<i64>,
    files: BTreeMap<String, Vec<u8>>,
    sent: BTreeMap<i64, Value>,
}

struct Fixture {
    api: Arc<Mutex<BotApi>>,
    url: String,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn start() -> Self {
        let api = Arc::new(Mutex::new(BotApi {
            next_message: 1_000,
            ..BotApi::default()
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = Router::new()
            .route("/{bot}/{method}", post(bot_api))
            .route("/{bot}/sendVoice", post(voice_upload))
            .route("/file/{bot}/documents/{file}", get(file_bytes))
            .with_state(api.clone());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { api, url, server }
    }

    fn settings(&self, webhook: bool) -> Value {
        let intake = if webhook {
            json!({"mode":"webhook","public_origin":"https://testing.invalid"})
        } else {
            json!({"mode":"polling"})
        };
        json!({
            "bot_id": BOT, "api_origin": self.url, "intake": intake,
            "access": {
                "users":["42","73"],"direct_tools":true,
                "rooms":{"-100123":{"mention_required":true,"tools":true}},
            },
        })
    }

    async fn create(&self, gateway: &RunningGateway, settings: Value) {
        let request = json!({"name":NAME,"settings":settings});
        assert_eq!(
            dispatch(
                gateway,
                "messaging.profile.telegram.create",
                &request,
                false
            )
            .await,
            Err(4403)
        );
        dispatch(gateway, "messaging.profile.telegram.create", &request, true)
            .await
            .unwrap();
        self.enroll(gateway, 'a').await;
        status(gateway, |value| {
            value["connections"][0]["health"] == "ready"
        })
        .await;
    }

    async fn enroll(&self, gateway: &RunningGateway, suffix: char) {
        let credential = format!("{BOT}:{}", suffix.to_string().repeat(40));
        let lease = gateway
            .begin_adapter_enrollment(NAME, Duration::from_secs(30))
            .await
            .unwrap();
        gateway
            .complete_adapter_enrollment(
                NAME,
                &lease.id,
                EnrollmentCredential::new(credential).unwrap(),
            )
            .await
            .unwrap();
    }

    fn queue(&self, updates: Vec<Value>) {
        self.api.lock().unwrap().batches.push_back(updates);
    }

    fn requests(&self, method: &str) -> Vec<Value> {
        self.api
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter_map(|(name, body)| (name == method).then(|| body.clone()))
            .collect()
    }

    async fn receive(&self, gateway: &RunningGateway, update: &Value, secret: bool) -> StatusCode {
        let (url, token) = {
            let api = self.api.lock().unwrap();
            (
                reqwest::Url::parse(&api.webhook).unwrap(),
                api.secret.clone(),
            )
        };
        reqwest::Client::new()
            .post(format!("{}{}", gateway.http_url(), url.path()))
            .header(
                SECRET_HEADER,
                if secret { token.as_str() } else { "invalid" },
            )
            .json(update)
            .send()
            .await
            .unwrap()
            .status()
    }
}

fn answer(value: Value) -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({"ok":true,"result":value})))
}

async fn bot_api(
    State(api): State<Arc<Mutex<BotApi>>>,
    Path((_bot, method)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut api = api.lock().unwrap();
    api.calls.push((method.clone(), body.clone()));
    match method.as_str() {
        "getMe" => answer(json!({
            "id":BOT,"is_bot":true,"username":"fixture_bot","first_name":"Fixture",
            "can_join_groups":true,"can_read_all_group_messages":true,
            "has_topics_enabled":true,"allows_users_to_create_topics":true,
        })),
        "getWebhookInfo" => answer(json!({"url":api.webhook,"pending_update_count":0})),
        "setWebhook" => {
            assert_eq!(body["drop_pending_updates"], false);
            api.webhook = body["url"].as_str().unwrap().into();
            api.secret = body["secret_token"].as_str().unwrap().into();
            answer(json!(true))
        }
        "deleteWebhook" => {
            assert_eq!(body["drop_pending_updates"], false);
            api.webhook.clear();
            api.secret.clear();
            answer(json!(true))
        }
        "getUpdates" if std::mem::take(&mut api.poll_conflict) => (
            StatusCode::CONFLICT,
            Json(json!({"ok":false,"error_code":409,"description":"another poller"})),
        ),
        "getUpdates" => answer(json!(api.batches.pop_front().unwrap_or_default())),
        "sendMessage" | "sendRichMessage" | "editMessageText" | "editMessageReplyMarkup" => {
            let chat = body["chat_id"].as_i64().unwrap();
            if method == "sendMessage" && api.lose_send.take_if(|id| *id == chat).is_some() {
                return (StatusCode::BAD_GATEWAY, Json(json!({})));
            }
            api.next_message += 1;
            let id = body["message_id"].as_i64().unwrap_or(api.next_message);
            let mut message = api.sent.get(&id).cloned().unwrap_or_else(|| {
                json!({
                    "message_id":id,
                    "chat":{"id":chat,"type":if chat < 0 {"supergroup"} else {"private"}},
                })
            });
            if let Some(thread) = body.get("message_thread_id") {
                message["message_thread_id"] = thread.clone();
            }
            if let Some(topic) = body.get("direct_messages_topic_id") {
                message["direct_messages_topic"] = json!({"topic_id":topic});
            }
            api.sent.insert(id, message.clone());
            answer(message)
        }
        "getFile" => {
            let id = body["file_id"].as_str().unwrap();
            let bytes = &api.files[id];
            answer(
                json!({"file_id":id,"file_size":bytes.len(),"file_path":format!("documents/{id}")}),
            )
        }
        "getChat" => answer(json!({"id":body["chat_id"],"type":"private"})),
        "setMyCommands" | "answerCallbackQuery" | "sendMessageDraft" | "sendChatAction" => {
            answer(json!(true))
        }
        _ => (StatusCode::NOT_IMPLEMENTED, Json(json!({}))),
    }
}

async fn file_bytes(
    State(api): State<Arc<Mutex<BotApi>>>,
    Path((_bot, file)): Path<(String, String)>,
) -> Vec<u8> {
    api.lock().unwrap().files[&file].clone()
}

async fn voice_upload(
    State(api): State<Arc<Mutex<BotApi>>>,
    mut form: Multipart,
) -> (StatusCode, Json<Value>) {
    let mut chat = None;
    let mut voice = false;
    while let Some(field) = form.next_field().await.unwrap() {
        match field.name() {
            Some("chat_id") => chat = Some(field.text().await.unwrap().parse::<i64>().unwrap()),
            Some("voice") => {
                assert_eq!(field.content_type(), Some("audio/ogg"));
                let body = field.bytes().await.unwrap();
                assert!(body.starts_with(b"OggS"));
                assert!(voice_codec::decode_voice_note(&body).is_ok());
                voice = true;
            }
            _ => {}
        }
    }
    assert!(voice);
    let chat = chat.unwrap();
    let mut api = api.lock().unwrap();
    api.calls
        .push(("sendVoice".into(), json!({"chat_id":chat})));
    api.next_message += 1;
    answer(json!({"message_id":api.next_message,"chat":{"id":chat,"type":"private"}}))
}

fn message(update: i64, user: i64, text: &str) -> Value {
    json!({
        "update_id": update,
        "message": {
            "message_id":update + 200,"from":{"id":user,"is_bot":false},
            "chat":{"id":user,"type":"private"},"text":text,
        },
    })
}

fn forum_message(update: i64, thread: i64, text: &str) -> Value {
    let mut message = message(update, 42, text);
    message["message"]["chat"] = json!({"id":-100123,"type":"supergroup","is_forum":true});
    message["message"]["message_thread_id"] = json!(thread);
    message
}

async fn until(mut ready: impl FnMut() -> bool) {
    timeout(Duration::from_secs(12), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("fixture condition");
}

async fn status(gateway: &RunningGateway, test: impl Fn(&Value) -> bool) -> Value {
    let outcome = timeout(Duration::from_secs(12), async {
        loop {
            let status = gateway.messaging_status().await.unwrap();
            if test(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    })
    .await;
    match outcome {
        Ok(value) => value,
        Err(_) => panic!(
            "gateway status condition: {}",
            gateway.messaging_status().await.unwrap()
        ),
    }
}

// The Bot API fixture records requests before the gateway saves their receipts.
async fn wait_for_sent_replies(gateway: &RunningGateway, count: usize) {
    status(gateway, |value| {
        value["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| {
                row["part"] == 0
                    && row["state"] == "sent"
                    && row["id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("reply-"))
            })
            .count()
            >= count
    })
    .await;
}

async fn opening() -> (tempfile::TempDir, Model, Fixture, RunningGateway) {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(STOCK_ADAPTER);
    let fixture = Fixture::start().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    (root, model, fixture, gateway)
}

#[tokio::test]
async fn polling_commits_before_advancing_and_recovers_replays_after_restart() {
    let (root, model, fixture, gateway) = opening().await;
    let first = message(20, 42, "first");
    fixture.api.lock().unwrap().poll_conflict = true;
    fixture.queue(vec![
        first.clone(),
        first.clone(),
        message(20, 42, "conflicting body"),
        message(21, 900, "denied"),
    ]);
    fixture.create(&gateway, fixture.settings(false)).await;
    status(&gateway, |_| {
        fixture
            .requests("getUpdates")
            .iter()
            .any(|call| call["offset"] == 22)
    })
    .await;
    let committed = gateway.messaging_status().await.unwrap();
    assert!(
        committed["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == "telegram-update-0-20" && row["conflict"] == true)
    );
    assert!(
        committed["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == "telegram-update-0-21" && row["state"] == "refused")
    );
    wait_for_sent_replies(&gateway, 1).await;
    gateway.shutdown().await.unwrap();

    let polled = fixture.requests("getUpdates").len();
    fixture.queue(vec![first, message(22, 42, "after restart")]);
    let gateway = model.gateway(root.path(), vec![]).await;
    wait_for_sent_replies(&gateway, 2).await;
    let polls = fixture.requests("getUpdates");
    assert_eq!(polls[0]["offset"], 0);
    assert_eq!(polls[polled]["offset"], 22);
    let replies = fixture.requests("sendMessage");
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0]["text"], "answer first");
    assert_eq!(replies[1]["text"], "answer after restart");
    assert_eq!(replies[0]["reply_parameters"]["message_id"], 220);
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn webhook_authenticates_and_commits_out_of_order_updates_then_switches_to_polling() {
    let (root, model, fixture, gateway) = opening().await;
    fixture.create(&gateway, fixture.settings(true)).await;
    status(&gateway, |value| {
        value["connections"][0]["health"] == "ready"
    })
    .await;
    let newer = message(81, 42, "newer");
    let prior = message(80, 73, "prior");
    assert_eq!(
        fixture.receive(&gateway, &newer, false).await,
        StatusCode::FORBIDDEN
    );
    assert!(
        gateway.messaging_status().await.unwrap()["input"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    for update in [&newer, &prior, &newer] {
        assert_eq!(
            fixture.receive(&gateway, update, true).await,
            StatusCode::NO_CONTENT
        );
    }
    wait_for_sent_replies(&gateway, 2).await;
    assert_eq!(fixture.requests("sendMessage").len(), 2);
    gateway.shutdown().await.unwrap();

    let gateway = model.gateway(root.path(), vec![]).await;
    status(&gateway, |value| {
        value["connections"][0]["health"] == "ready"
    })
    .await;
    assert_eq!(
        fixture.receive(&gateway, &prior, true).await,
        StatusCode::NO_CONTENT
    );
    let saved = gateway.adapter_settings().await.unwrap().remove(0);
    let change = json!({"name":NAME,"revision":saved.revision,"settings":fixture.settings(false)});
    fixture.queue(vec![prior, newer, message(82, 73, "after switching")]);
    dispatch(&gateway, "messaging.profile.telegram.update", &change, true)
        .await
        .unwrap();
    until(|| {
        fixture
            .requests("sendMessage")
            .iter()
            .any(|call| call["text"] == "answer after switching")
    })
    .await;
    assert_eq!(fixture.requests("deleteWebhook").len(), 1);
    assert_eq!(fixture.requests("getUpdates")[0]["offset"], 0);
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_ambiguous_send_blocks_its_route_across_restart_while_another_route_delivers() {
    let (root, model, fixture, gateway) = opening().await;
    fixture.api.lock().unwrap().lose_send = Some(42);
    fixture.queue(vec![message(11, 42, "lost receipt")]);
    fixture.create(&gateway, fixture.settings(false)).await;
    status(&gateway, |value| {
        value["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["part_state"] == "uncertain")
    })
    .await;
    gateway.shutdown().await.unwrap();

    fixture.queue(vec![
        message(11, 42, "lost receipt"),
        message(12, 42, "same route"),
        message(13, 73, "other route"),
    ]);
    let gateway = model.gateway(root.path(), vec![]).await;
    until(|| {
        fixture
            .requests("sendMessage")
            .iter()
            .any(|call| call["text"] == "answer other route")
    })
    .await;
    assert_eq!(
        fixture
            .requests("sendMessage")
            .iter()
            .filter(|call| call["chat_id"] == 42)
            .count(),
        1
    );
    let held = status(&gateway, |value| {
        value["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["part_state"] == "uncertain")
    })
    .await;
    let held = held["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["part_state"] == "uncertain")
        .unwrap();
    assert!(
        gateway
            .resolve_delivery(
                held["connection"].as_str().unwrap().into(),
                held["id"].as_str().unwrap().into(),
                held["part"].as_u64().unwrap() as u32,
                held["attempt"].as_u64().unwrap() as u32,
                hermes_gateway_core::messaging::DeliveryResolution::Delivered,
            )
            .await
            .unwrap()
    );
    until(|| {
        fixture
            .requests("sendMessage")
            .iter()
            .any(|call| call["text"] == "answer same route")
    })
    .await;
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn reply_routes_keep_forum_general_group_topic_and_private_topic_separate() {
    let (_root, model, fixture, gateway) = opening().await;
    let addressed = |id, thread, text: &str| {
        let mut item = forum_message(id, thread, text);
        item["message"]["entities"] = json!([{"type":"mention","offset":3,"length":12}]);
        item
    };
    let mut private = message(34, 42, "private topic");
    private["message"]["message_thread_id"] = json!(88);
    fixture.queue(vec![
        addressed(30, 1, "🌞 @fixture_bot General"),
        addressed(31, 9, "🌞 @fixture_bot topic nine"),
        forum_message(32, 9, "unaddressed"),
        addressed(33, 9, "🌞 @another_bot denied"),
        private,
    ]);
    fixture.create(&gateway, fixture.settings(false)).await;
    wait_for_sent_replies(&gateway, 3).await;
    let writes = fixture.requests("sendMessage");
    let general = writes
        .iter()
        .find(|call| call["text"].as_str().unwrap().ends_with("General"))
        .unwrap();
    let topic = writes
        .iter()
        .find(|call| call["text"].as_str().unwrap().ends_with("topic nine"))
        .unwrap();
    let private = writes
        .iter()
        .find(|call| call["text"] == "answer private topic")
        .unwrap();
    assert_eq!(general["chat_id"], -100123);
    assert!(general.get("message_thread_id").is_none());
    assert_eq!(topic["message_thread_id"], 9);
    assert_eq!(topic["chat_id"], -100123);
    assert_eq!(private["chat_id"], 42);
    assert_eq!(private["message_thread_id"], 88);
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn rotation_for_the_same_bot_preserves_admissions_and_does_not_wait_for_its_own_lease() {
    let (_root, model, fixture, gateway) = opening().await;
    let original = message(50, 42, "before rotation");
    fixture.queue(vec![original.clone()]);
    fixture.create(&gateway, fixture.settings(false)).await;
    wait_for_sent_replies(&gateway, 1).await;
    fixture.enroll(&gateway, 'b').await;
    fixture.queue(vec![original, message(51, 42, "after rotation")]);
    until(|| {
        fixture
            .requests("sendMessage")
            .iter()
            .any(|call| call["text"] == "answer after rotation")
    })
    .await;
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn media_rechecks_access_before_contacting_the_file_api() {
    let (_root, model, fixture, gateway) = opening().await;
    fixture
        .api
        .lock()
        .unwrap()
        .files
        .insert("fixture-file".into(), b"attachment content".to_vec());
    let document = |id, user| {
        let mut item = message(id, user, "read the file");
        item["message"].as_object_mut().unwrap().remove("text");
        item["message"]["caption"] = json!("read the file");
        item["message"]["document"] =
            json!({"file_id":"fixture-file","file_name":"fixture.txt","file_size":18});
        item
    };
    fixture.queue(vec![document(40, 900), document(41, 42)]);
    fixture.create(&gateway, fixture.settings(false)).await;
    until(|| !fixture.requests("sendMessage").is_empty()).await;
    assert_eq!(fixture.requests("getFile").len(), 1);
    assert_eq!(model.requests().len(), 1);
    let status = gateway.messaging_status().await.unwrap();
    assert!(
        status["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == "telegram-update-0-40" && row["state"] == "refused")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn callbacks_match_the_persisted_card_and_actor_then_retire_its_buttons() {
    let (_root, model, fixture, gateway) = opening().await;
    let mut settings = fixture.settings(true);
    settings["sync_commands"] = json!(true);
    fixture.create(&gateway, settings).await;
    assert_eq!(
        fixture
            .receive(&gateway, &message(70, 42, "clarify"), true)
            .await,
        StatusCode::NO_CONTENT
    );
    sent_prompt(&gateway).await;
    let card = fixture
        .requests("sendMessage")
        .into_iter()
        .find(|call| call["reply_markup"].is_object())
        .unwrap();
    assert_eq!(card["text"], "Choose a release branch");
    assert_eq!(
        card["reply_markup"]["inline_keyboard"][0][0]["text"],
        "stable"
    );
    let button = &card["reply_markup"]["inline_keyboard"][0][0]["callback_data"];
    assert!(button.as_str().unwrap().len() <= 64);
    assert!(
        fixture.requests("setMyCommands")[0]["commands"]
            .as_array()
            .unwrap()
            .len()
            <= 100
    );
    let tap = |id, chat| {
        json!({
            "update_id":id,"callback_query":{
                "id":format!("tap-{id}"),"data":button,"from":{"id":42,"is_bot":false},
                "message":{"message_id":1001,"from":{"id":BOT,"is_bot":true},"chat":{"id":chat,"type":"private"}},
            },
        })
    };
    assert_eq!(
        fixture.receive(&gateway, &tap(71, 73), true).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        fixture.receive(&gateway, &tap(72, 42), true).await,
        StatusCode::NO_CONTENT
    );
    until(|| fixture.requests("editMessageReplyMarkup").len() == 1).await;
    assert_eq!(
        fixture.requests("editMessageReplyMarkup")[0]["message_id"],
        1001
    );
    assert_eq!(
        fixture.requests("editMessageReplyMarkup")[0]["reply_markup"]["inline_keyboard"],
        json!([])
    );
    assert_eq!(
        fixture.receive(&gateway, &tap(72, 42), true).await,
        StatusCode::NO_CONTENT
    );
    until(|| {
        fixture
            .requests("answerCallbackQuery")
            .iter()
            .any(|call| call["text"] == "Response recorded")
    })
    .await;
    until(|| model.requests().len() >= 2).await;
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_typed_reply_only_resolves_its_originating_clarification() {
    let (_root, model, fixture, gateway) = opening().await;
    let mut settings = fixture.settings(false);
    settings["typed_clarification"] = json!(true);
    fixture.queue(vec![message(70, 42, "clarify")]);
    fixture.create(&gateway, settings).await;
    sent_prompt(&gateway).await;
    let mut reply = message(71, 42, "custom branch");
    reply["message"]["reply_to_message"] =
        json!({"message_id":1001,"from":{"id":BOT,"is_bot":true}});
    fixture.queue(vec![reply.clone(), reply]);
    until(|| fixture.requests("editMessageReplyMarkup").len() == 1).await;
    until(|| model.requests().len() >= 2).await;
    assert_eq!(model.requests().len(), 2);
    assert!(model.requests()[1].to_string().contains("custom branch"));
    gateway.shutdown().await.unwrap();
}

async fn sent_prompt(gateway: &RunningGateway) {
    status(gateway, |value| {
        value["deliveries"].as_array().unwrap().iter().any(|row| {
            row["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("action-"))
                && row["part_state"] == "sent"
        })
    })
    .await;
}

#[tokio::test]
async fn a_staged_album_resumes_as_one_turn_with_files_in_message_order() {
    let (root, model, fixture, gateway) = opening().await;
    {
        let mut api = fixture.api.lock().unwrap();
        api.files.insert("second-file".into(), b"second".to_vec());
        api.files.insert("first-file".into(), b"first".to_vec());
    }
    let photo = |update, id, file, size, caption| {
        let mut item = message(update, 42, "");
        let media = item["message"].as_object_mut().unwrap();
        media.remove("text");
        media.insert("message_id".into(), json!(id));
        media.insert("photo".into(), json!([{"file_id":file,"file_size":size}]));
        media.insert("media_group_id".into(), json!("fixture-album"));
        media.insert("caption".into(), json!(caption));
        item
    };
    let second = photo(60, 961, "second-file", 6, "second caption");
    let first = photo(61, 960, "first-file", 5, "first caption");
    fixture.queue(vec![second, first.clone()]);
    fixture.create(&gateway, fixture.settings(false)).await;
    until(|| {
        fixture
            .requests("getUpdates")
            .iter()
            .any(|call| call["offset"] == 62)
    })
    .await;
    gateway.shutdown().await.unwrap();
    fixture.queue(vec![first]);
    let gateway = model.gateway(root.path(), vec![]).await;
    until(|| !fixture.requests("sendMessage").is_empty()).await;
    let opened: Vec<_> = fixture
        .requests("getFile")
        .into_iter()
        .map(|req| req["file_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(opened, ["first-file", "second-file"]);
    assert_eq!(model.requests().len(), 1);
    assert_eq!(
        fixture.requests("sendMessage")[0]["reply_parameters"]["message_id"],
        961
    );
    gateway.shutdown().await.unwrap();
}

struct Speech;

impl AudioService for Speech {
    fn transcribe<'a>(
        &'a self,
        _: &'a std::path::Path,
        wav: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            assert!(wav.starts_with(b"RIFF"));
            Ok("fixture transcription".into())
        })
    }

    fn synthesize<'a>(
        &'a self,
        _: &'a std::path::Path,
        text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async move {
            assert!(text.contains("fixture transcription"));
            Ok(voice_codec::pcm_wave(&[512; 320]))
        })
    }
}

#[tokio::test]
async fn voice_note_uses_profile_speech_in_both_directions_and_sends_ogg_opus() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(STOCK_ADAPTER);
    let fixture = Fixture::start().await;
    let gateway = model
        .gateway_with_audio(root.path(), vec![], Some(Arc::new(Speech)))
        .await;
    let mut settings = fixture.settings(false);
    settings["access"]["direct_spoken_replies"] = json!(true);
    settings["access"]["scope"]["spoken_reply_default"] = json!("voice_notes");
    let voice = voice_codec::encode_voice_note(&voice_codec::pcm_wave(&[512; 320]))
        .unwrap()
        .bytes;
    let mut input = message(90, 42, "");
    input["message"].as_object_mut().unwrap().remove("text");
    input["message"]["voice"] = json!({"file_id":"voice","file_size":voice.len()});
    fixture
        .api
        .lock()
        .unwrap()
        .files
        .insert("voice".into(), voice);
    fixture.queue(vec![input]);
    fixture.create(&gateway, settings).await;
    status(&gateway, |_| !fixture.requests("sendVoice").is_empty()).await;
    assert_eq!(fixture.requests("sendVoice")[0]["chat_id"], 42);
    assert!(fixture.requests("sendMessage").is_empty());
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn previous_sqlite_inbox_prevents_the_stock_adapter_from_acknowledging_the_bot() {
    let (root, _model, fixture, gateway) = opening().await;
    let store = hermes_telegram::Store::new(root.path());
    store
        .load_inbox(&hermes_telegram::telegram::Identity {
            id: BOT,
            username: "fixture_bot".into(),
            name: "Fixture".into(),
        })
        .unwrap();
    fixture.queue(vec![message(35, 42, "needs upgrade")]);
    dispatch(
        &gateway,
        "messaging.profile.telegram.create",
        &json!({"name":NAME,"settings":fixture.settings(false)}),
        true,
    )
    .await
    .unwrap();
    fixture.enroll(&gateway, 'a').await;
    status(&gateway, |value| {
        value["verification"][NAME]["issue"] == "migration_required"
    })
    .await;
    assert!(fixture.requests("getUpdates").is_empty());
    assert!(fixture.requests("sendMessage").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_unmanaged_webhook_is_never_deleted_or_replaced() {
    let (_root, _model, fixture, gateway) = opening().await;
    fixture.api.lock().unwrap().webhook = "https://other-integration.invalid/listener".into();
    dispatch(
        &gateway,
        "messaging.profile.telegram.create",
        &json!({"name":NAME,"settings":fixture.settings(true)}),
        true,
    )
    .await
    .unwrap();
    fixture.enroll(&gateway, 'a').await;
    status(&gateway, |value| {
        value["connections"][0]["health"] == "configuration"
    })
    .await;
    assert!(fixture.requests("setWebhook").is_empty());
    assert!(fixture.requests("deleteWebhook").is_empty());
    assert!(fixture.requests("getUpdates").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn rich_replies_finalize_a_private_topic_draft_with_its_reply_anchor() {
    let (_root, model, fixture, gateway) = opening().await;
    let mut settings = fixture.settings(false);
    settings["drafts"] = json!(true);
    settings["rich_messages"] = json!(true);
    let mut source = message(101, 42, "steer wait");
    source["message"]["message_thread_id"] = json!(7);
    fixture.queue(vec![source]);
    fixture.create(&gateway, settings).await;
    until(|| !fixture.requests("sendMessageDraft").is_empty()).await;
    let drafts = fixture.requests("sendMessageDraft");
    assert!(drafts[0]["draft_id"].as_i64().is_some_and(|id| id > 0));
    assert_eq!(drafts[0]["message_thread_id"], 7);
    model.release_waiting_turn();
    status(&gateway, |_| {
        !fixture.requests("sendRichMessage").is_empty()
    })
    .await;
    let writes = fixture.requests("sendRichMessage");
    assert_eq!(writes[0]["chat_id"], 42);
    assert_eq!(writes[0]["message_thread_id"], 7);
    assert_eq!(writes[0]["reply_parameters"]["message_id"], 301);
    assert_eq!(writes[0]["rich_message"]["html"], "<p>Working</p>");
    let mut markup = message(102, 42, "**rich**");
    markup["message"]["message_thread_id"] = json!(7);
    fixture.queue(vec![markup]);
    until(|| fixture.requests("sendRichMessage").len() == 2).await;
    assert_eq!(
        fixture.requests("sendRichMessage")[1]["rich_message"]["html"],
        "<p>answer <b>rich</b></p>"
    );
    assert!(fixture.requests("sendMessage").is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_owner_notification_can_use_a_verified_home_without_starting_a_turn() {
    let (_root, model, fixture, gateway) = opening().await;
    let mut settings = fixture.settings(false);
    settings["home"] = json!({"kind":"direct","chat":"42","thread":null});
    fixture.create(&gateway, settings).await;
    let status = gateway.messaging_status().await.unwrap();
    let home = status["verification"][NAME]["home_route"].as_str().unwrap();
    gateway
        .send_messaging_notification(home, "owner-fixture", "hello from the owner".into())
        .unwrap();
    until(|| {
        fixture
            .requests("sendMessage")
            .iter()
            .any(|call| call["text"] == "hello from the owner")
    })
    .await;
    assert_eq!(fixture.requests("sendMessage")[0]["chat_id"], 42);
    assert!(
        fixture.requests("sendMessage")[0]
            .get("reply_parameters")
            .is_none()
    );
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}
