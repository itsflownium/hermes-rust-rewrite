use super::{DiscordSettings, settings::DiscordFactory};
use crate::{
    RunningGateway,
    messaging::{AdapterConfig, EnrollmentCredential, runtime::now, test_support::{self, Model}},
};
use axum::{
    Json, Router,
    body::{Bytes, to_bytes},
    extract::{
        FromRequest, Multipart, Request, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use reqwest::Url;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Notify, Semaphore, mpsc},
    time::{sleep, timeout},
};

#[path = "burst_fixtures.rs"]
mod burst_fixtures;

#[path = "history_fixtures.rs"]
mod history_fixtures;

#[path = "feedback_fixtures.rs"]
mod feedback_fixtures;

#[path = "command_fixtures.rs"]
mod command_fixtures;
#[path = "background_fixtures.rs"]
mod background_fixtures;
#[path = "manual_thread_fixtures.rs"]
mod manual_thread_fixtures;
#[path = "voice_note_fixtures.rs"]
mod voice_note_fixtures;
#[path = "voice_cue_fixtures.rs"]
mod voice_cue_fixtures;
#[path = "membership_fixtures.rs"]
mod membership_fixtures;
#[path = "thread_update_fixtures.rs"]
mod thread_update_fixtures;

const LIMIT: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct Server {
    gateway: String,
    heartbeat_interval_ms: u64,
    incoming: mpsc::Sender<Wire>,
    writes: mpsc::Sender<Write>,
    counter: Arc<AtomicU64>,
    messages: Arc<Mutex<BTreeMap<(String, String), Value>>>,
    history: Arc<Mutex<BTreeMap<String, Vec<Value>>>>,
    threads: Arc<Mutex<BTreeMap<String, Value>>>,
    thread_creates: Arc<AtomicU64>,
    thread_requester_permissions: Arc<Mutex<Option<u64>>>,
    thread_bot_permissions: Arc<Mutex<Option<u64>>>,
    thread_update_writes: Arc<Mutex<Vec<(String, Option<String>)>>>,
    lose_thread_update_responses: Arc<AtomicBool>,
    limit_next_pin: Arc<AtomicBool>,
    callback_paths: Arc<Mutex<Vec<String>>>,
    voice_outcome: Arc<AtomicU64>,
    suppress_progress: Arc<AtomicBool>,
    feedbacks: Arc<Mutex<Vec<String>>>,
    cdn: Arc<Mutex<BTreeMap<String, Bytes>>>,
    direct_recipient: Arc<Mutex<String>>,
    direct_reads: Arc<AtomicU64>,
    member_responses: Arc<Mutex<BTreeMap<(String, String), MemberResponse>>>,
}

#[derive(Clone)]
struct MemberResponse {
    roles: Vec<String>,
    held: Option<(Arc<Notify>, Arc<Semaphore>)>,
}

struct Wire {
    to_bot: mpsc::Sender<WireEvent>,
    from_bot: mpsc::Receiver<Value>,
}

enum WireEvent {
    Json(Value),
    NoHeartbeatAcks,
    Close(u16),
}

impl Wire {
    async fn data(&self, value: Value) {
        self.to_bot.send(WireEvent::Json(value)).await.unwrap();
    }

    async fn dispatch(&self, sequence: u64, kind: &str, data: Value) {
        self.data(json!({"op":0,"t":kind,"s":sequence,"d":data}))
            .await;
    }

    async fn authentication(&mut self, code: u64) -> Value {
        timeout(LIMIT, async {
            while let Some(event) = self.from_bot.recv().await {
                if event["op"] == code {
                    return event;
                }
            }
            panic!("Discord did not authenticate");
        })
        .await
        .unwrap()
    }
}

enum Write {
    Message {
        channel: String,
        payload: Value,
        id: String,
    },
    VoiceNote {
        channel: String,
        payload: Value,
        audio: Bytes,
        message: Value,
    },
    Callback(Value),
    Followup(Value),
    Thread {
        parent: String,
        starter: String,
        payload: Value,
    },
}

struct Fixtures {
    transport: Server,
    api: Url,
    wires: mpsc::Receiver<Wire>,
    writes: mpsc::Receiver<Write>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Fixtures {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Fixtures {
    async fn start() -> Self {
        Self::start_with_heartbeat(5_000).await
    }

    async fn start_with_heartbeat(heartbeat_interval_ms: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (incoming, wires) = mpsc::channel(16);
        let (writes_tx, writes) = mpsc::channel(32);
        let transport = Server {
            gateway: format!("ws://{address}/"),
            heartbeat_interval_ms,
            incoming,
            writes: writes_tx,
            counter: Arc::new(AtomicU64::new(500)),
            messages: Arc::default(),
            history: Arc::default(),
            threads: Arc::default(),
            thread_creates: Arc::default(),
            thread_requester_permissions: Arc::default(),
            thread_bot_permissions: Arc::default(),
            thread_update_writes: Arc::default(),
            lose_thread_update_responses: Arc::default(),
            limit_next_pin: Arc::default(),
            callback_paths: Arc::default(),
            voice_outcome: Arc::default(),
            suppress_progress: Arc::default(),
            feedbacks: Arc::default(),
            cdn: Arc::default(),
            direct_recipient: Arc::new(Mutex::new("7".into())),
            direct_reads: Arc::default(),
            member_responses: Arc::default(),
        };
        let endpoint = transport.clone();
        let router = Router::new()
            .route("/", axum::routing::get(upgrade))
            .fallback(rest)
            .with_state(endpoint);
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            transport,
            api: Url::parse(&format!("http://{address}/api/v10/")).unwrap(),
            wires,
            writes,
            task,
        }
    }

    async fn wire(&mut self) -> Wire {
        timeout(LIMIT, self.wires.recv())
            .await
            .expect("Discord socket connection")
            .unwrap()
    }

    async fn write(&mut self) -> Write {
        self.next_write("Discord REST write", |_| true).await
    }

    async fn next_write(&mut self, expected: &str, accepts: impl Fn(&Write) -> bool) -> Write {
        timeout(LIMIT, async {
            while let Some(write) = self.writes.recv().await {
                if accepts(&write) {
                    return write;
                }
            }
            panic!("Discord REST fixture stopped");
        })
        .await
        .unwrap_or_else(|_| panic!("Missing {expected}"))
    }

    async fn reply(&mut self) -> Write {
        self.next_write("a final Discord reply", |write| {
            matches!(write, Write::Message { payload, .. } if payload["content"].as_str().is_some_and(|text| text.starts_with("answer")))
        }).await
    }

    async fn ready(&self, wire: &Wire, id: &str) {
        wire.dispatch(
            1,
            "READY",
            json!({
                "user":{"id":"9","bot":true},"application":{"id":"90"},
                "session_id":id,"resume_gateway_url":self.transport.gateway,
            }),
        )
        .await;
    }

    fn configuration(&self, root: &Path) -> AdapterConfig {
        self.configuration_with(
            root,
            json!({
                "grants":{"users":["7"]},
                "operators":["7"],
                "direct":{"rules":{"turn":{"tools":true,"approval":"manual"}}},
                "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
                "commands":{"enabled":true,"sync":false},
            }),
        )
    }

    fn configuration_with(&self, root: &Path, settings: Value) -> AdapterConfig {
        let settings: DiscordSettings = serde_json::from_value(settings).unwrap();
        AdapterConfig {
            name: "fixture-discord".into(),
            policy: settings.host_policy(),
            factory: Arc::new(
                DiscordFactory::for_fixture(root.to_owned(), settings, self.api.clone()).unwrap(),
            ),
        }
    }

    async fn activate(&self, gateway: &RunningGateway) {
        let enrollment = gateway
            .begin_adapter_enrollment("fixture-discord", Duration::from_secs(30))
            .await
            .unwrap();
        gateway
            .complete_adapter_enrollment(
                "fixture-discord",
                &enrollment.id,
                EnrollmentCredential::new("fixture-bot-secret".into()).unwrap(),
            )
            .await
            .unwrap();
    }
}

async fn upgrade(State(state): State<Server>, websocket: WebSocketUpgrade) -> Response {
    websocket.on_upgrade(move |socket| run_wire(state, socket))
}

async fn run_wire(state: Server, mut socket: WebSocket) {
    let (to_bot, mut outbound) = mpsc::channel(16);
    let (inbound, from_bot) = mpsc::channel(16);
    if state
        .incoming
        .send(Wire { to_bot, from_bot })
        .await
        .is_err()
    {
        return;
    }
    let hello = json!({"op":10,"d":{"heartbeat_interval":state.heartbeat_interval_ms}});
    if socket
        .send(Message::Text(hello.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    let mut ack = true;
    loop {
        tokio::select! {
            operation = outbound.recv() => match operation {
                Some(WireEvent::Json(value)) => {
                    if socket.send(Message::Text(value.to_string().into())).await.is_err() { return }
                }
                Some(WireEvent::NoHeartbeatAcks) => ack = false,
                Some(WireEvent::Close(code)) => {
                    let _ = socket.send(Message::Close(Some(CloseFrame { code, reason: "fixture".into() }))).await;
                    return;
                }
                None => return,
            },
            received = socket.recv() => match received {
                Some(Ok(Message::Text(text))) => {
                    let event: Value = serde_json::from_str(&text).unwrap();
                    if event["op"] == 1 {
                        if ack && socket.send(Message::Text(r#"{"op":11,"d":null}"#.into())).await.is_err() { return }
                    } else if inbound.send(event).await.is_err() { return }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                _ => {},
            }
        }
    }
}

async fn rest(State(state): State<Server>, request: Request) -> Response {
    let path = request
        .uri()
        .path()
        .strip_prefix("/api/v10/")
        .unwrap_or("")
        .to_owned();
    let method = request.method().clone();
    let auth = request
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok());
    if path.starts_with("interactions/") || path.starts_with("webhooks/") {
        assert!(
            auth.is_none(),
            "Discord bot token must stay out of callback requests"
        );
        crate::lock(&state.callback_paths).push(path.clone());
        let bytes = to_bytes(request.into_body(), 64 * 1024).await.unwrap();
        let payload = serde_json::from_slice::<Value>(&bytes).unwrap();
        let has_model_picker =
            payload.pointer("/components/0/components/0/type") == Some(&json!(3));
        let sent = if path.starts_with("interactions/") {
            Write::Callback(payload)
        } else {
            Write::Followup(payload)
        };
        let _ = state.writes.send(sent).await;
        if has_model_picker {
            return Json(json!({"id":"600","author":{"id":"9","bot":true},"channel_id":"20"}))
                .into_response();
        }
        return StatusCode::NO_CONTENT.into_response();
    }
    if request.uri().path().starts_with("/attachments/") && method == Method::GET {
        assert!(auth.is_none(), "Discord CDN must not receive bot credentials");
        let file = crate::lock(&state.cdn).get(request.uri().path()).cloned();
        return file.map_or_else(|| StatusCode::NOT_FOUND.into_response(), IntoResponse::into_response);
    }
    if auth != Some("Bot fixture-bot-secret") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let segments = path.split('/').collect::<Vec<_>>();
    let document = match (method, segments.as_slice()) {
        (Method::GET, ["users", "@me"]) => json!({"id":"9","bot":true}),
        (Method::GET, ["applications", "@me"]) => {
            json!({"id":"90","bot":{"id":"9"},"flags":835584})
        }
        (Method::GET, ["gateway", "bot"]) => json!({
            "url":state.gateway,"shards":1,
            "session_start_limit":{"total":1000,"remaining":1000,"reset_after":1000,"max_concurrency":1}
        }),
        (Method::GET, ["guilds", "11"]) => json!({"id":"11","owner_id":"99"}),
        (Method::GET, ["guilds", "11", "threads", "active"]) => json!({
            "threads":crate::lock(&state.threads).values().cloned().collect::<Vec<_>>(),
            "members":[]
        }),
        (Method::GET, ["guilds", "11", "members", "9"]) => {
            json!({"user":{"id":"9","bot":true},"roles":["80"]})
        }
        (Method::GET, ["guilds", "11", "roles"]) => {
            let requester = crate::lock(&state.thread_requester_permissions).unwrap_or(8);
            let bot = crate::lock(&state.thread_bot_permissions).unwrap_or(8);
            json!([
                {"id":"11","permissions":requester.to_string()},{"id":"80","permissions":bot.to_string()},
                {"id":"31","permissions":"0"},{"id":"41","permissions":"0"}
            ])
        }
        (Method::POST, ["channels", _, "typing"]) => {
            crate::lock(&state.feedbacks).push(path);
            return StatusCode::NO_CONTENT.into_response();
        }
        (Method::PUT, ["channels", _, "messages", _, "reactions", _, "@me"]) => {
            crate::lock(&state.feedbacks).push(path);
            return StatusCode::BAD_GATEWAY.into_response();
        }
        (Method::GET, ["guilds", guild, "members", actor]) => {
            let found = crate::lock(&state.member_responses)
                .get(&(guild.to_string(), actor.to_string())).cloned();
            let Some(response) = found else {
                return if *guild == "11" && *actor == "7" {
                    Json(json!({"user":{"id":"7","bot":false},"roles":[]})).into_response()
                } else {
                    StatusCode::NOT_FOUND.into_response()
                };
            };
            if let Some((entered, release)) = response.held {
                entered.notify_one();
                release.acquire().await.unwrap().forget();
            }
            json!({"user":{"id":actor},"roles":response.roles})
        }
        (Method::GET, ["channels", "20"]) => {
            state.direct_reads.fetch_add(1, Ordering::SeqCst);
            json!({"id":"20","type":1,"recipients":[{"id":crate::lock(&state.direct_recipient).clone()}]})
        }
        (Method::GET, ["channels", "21"]) => json!({
            "id":"21","type":0,"guild_id":"11","permission_overwrites":[]
        }),
        (Method::GET, ["channels", "22"]) => {
            json!({"id":"22","type":11,"guild_id":"11","parent_id":"21"})
        }
        (Method::GET, ["channels", "23"]) => json!({"id":"23","type":0,"guild_id":"12"}),
        (Method::GET, ["channels", id]) => {
            let found = crate::lock(&state.threads).get(*id).cloned();
            let Some(found) = found else {
                return (StatusCode::NOT_FOUND, Json(json!({"code":10003}))).into_response();
            };
            found
        }
        (Method::PATCH, ["channels", thread]) => {
            let bytes = to_bytes(request.into_body(), 64 * 1024).await.unwrap();
            let payload = serde_json::from_slice::<Value>(&bytes).unwrap();
            let name = payload["name"].as_str().expect("thread update name");
            let actual = {
                let mut threads = crate::lock(&state.threads);
                let Some(actual) = threads.get_mut(*thread) else {
                    return StatusCode::NOT_FOUND.into_response();
                };
                actual["name"] = json!(name);
                actual.clone()
            };
            crate::lock(&state.thread_update_writes).push((path, Some(name.to_owned())));
            if state.lose_thread_update_responses.load(Ordering::SeqCst) {
                return StatusCode::BAD_GATEWAY.into_response();
            }
            actual
        }
        (Method::PUT, ["channels", thread, "messages", "pins", id]) => {
            if state.limit_next_pin.swap(false, Ordering::SeqCst) {
                return (StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({"retry_after":2.75,"global":true}))).into_response();
            }
            {
                let mut messages = crate::lock(&state.messages);
                let Some(message) = messages.get_mut(&(thread.to_string(), id.to_string())) else {
                    return StatusCode::NOT_FOUND.into_response();
                };
                message["pinned"] = json!(true);
            }
            crate::lock(&state.thread_update_writes).push((path, None));
            return if state.lose_thread_update_responses.load(Ordering::SeqCst) {
                StatusCode::BAD_GATEWAY.into_response()
            } else {
                StatusCode::NO_CONTENT.into_response()
            };
        }
        (Method::POST, ["channels", parent, "messages", starter, "threads"]) => {
            let bytes = to_bytes(request.into_body(), 64 * 1024).await.unwrap();
            let payload = serde_json::from_slice::<Value>(&bytes).unwrap();
            let name = payload["name"].clone();
            state.thread_creates.fetch_add(1, Ordering::SeqCst);
            let _ = state
                .writes
                .send(Write::Thread {
                    parent: parent.to_string(),
                    starter: starter.to_string(),
                    payload,
                })
                .await;
            match *starter {
                "400" => return StatusCode::BAD_GATEWAY.into_response(),
                "401" => return StatusCode::FORBIDDEN.into_response(),
                _ => {
                    let thread = json!({"id":starter,"type":11,"parent_id":parent,"owner_id":"9","guild_id":"11",
                        "name":name,"thread_metadata":{"archived":false,"locked":false}});
                    crate::lock(&state.threads).insert(starter.to_string(), thread.clone());
                    thread
                }
            }
        }
        (Method::GET, ["channels", channel, "messages"]) => {
            json!(
                crate::lock(&state.history)
                    .get(*channel)
                    .cloned()
                    .unwrap_or_default()
            )
        }
        (Method::GET, ["channels", channel, "messages", id]) => {
            let message = crate::lock(&state.messages)
                .get(&(channel.to_string(), id.to_string()))
                .cloned();
            let Some(mut message) = message else {
                return StatusCode::NOT_FOUND.into_response();
            };
            if message["pinned"].is_null() {
                message["pinned"] = json!(false);
            }
            message
        }
        (Method::POST, ["channels", channel, "messages"]) => {
            let multipart = request.headers().get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("multipart/form-data"));
            if multipart {
                return voice_note_fixtures::record_voice(&state, channel, request).await;
            }
            let bytes = to_bytes(request.into_body(), 256 * 1024).await.unwrap();
            let payload = serde_json::from_slice::<Value>(&bytes).unwrap();
            if state.suppress_progress.load(Ordering::SeqCst) && payload["content"] == "Working…" {
                return StatusCode::FORBIDDEN.into_response();
            }
            let id = state.counter.fetch_add(1, Ordering::SeqCst).to_string();
            let message = json!({"id":id,"channel_id":channel,"author":{"id":"9","bot":true},
                "nonce":payload["nonce"],"content":payload["content"]});
            crate::lock(&state.messages).insert((channel.to_string(), id.clone()), message.clone());
            let _ = state
                .writes
                .send(Write::Message {
                    channel: channel.to_string(),
                    payload,
                    id,
                })
                .await;
            message
        }
        (Method::PATCH, ["channels", channel, "messages", id]) => {
            let bytes = to_bytes(request.into_body(), 256 * 1024).await.unwrap();
            let payload = serde_json::from_slice::<Value>(&bytes).unwrap();
            let message = json!({"id":id,"channel_id":channel,"author":{"id":"9","bot":true},"content":payload["content"]});
            crate::lock(&state.messages)
                .insert((channel.to_string(), id.to_string()), message.clone());
            let _ = state
                .writes
                .send(Write::Message {
                    channel: channel.to_string(),
                    payload,
                    id: id.to_string(),
                })
                .await;
            message
        }
        (Method::DELETE, ["channels", channel, "messages", id]) => {
            crate::lock(&state.messages).remove(&(channel.to_string(), id.to_string()));
            return StatusCode::NO_CONTENT.into_response();
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    Json(document).into_response()
}

fn message(id: &str, channel: &str, content: &str, mentioned: bool) -> Value {
    let mut event = json!({"id":id,"channel_id":channel,"content":content,"author":{"id":"7"},"type":0,
        "mentions":if mentioned { vec![json!({"id":"9","bot":true})] } else { Vec::new() }});
    if channel != "20" {
        event["guild_id"] = json!("11");
        event["member"] = json!({"roles":[]});
    }
    event
}

async fn wait_status(gateway: &RunningGateway, condition: impl Fn(&Value) -> bool) -> Value {
    timeout(LIMIT, async {
        loop {
            let status = gateway.messaging_status().await.unwrap();
            if condition(&status) {
                return status;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("gateway status transition")
}

async fn wait_reply(gateway: &RunningGateway, event: &str) {
    use crate::messaging::storage::key;
    wait_status(gateway, |state| {
        let Some(source) = state["input"]
            .as_array()
            .and_then(|inputs| inputs.iter().find(|input| input["id"] == event))
        else {
            return false;
        };
        let connection = source["connection"].as_str().unwrap();
        let admission = key("channel-input", &(connection, event)).unwrap();
        let id = key("reply", &admission).unwrap();
        state["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|delivery| delivery["id"] == id && delivery["state"] == "sent")
    })
    .await;
}

#[tokio::test]
async fn discord_identifies_resumes_deduplicates_and_routes_threads_through_the_native_gateway() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    assert_eq!(
        wire.authentication(2).await["d"]["token"],
        "fixture-bot-secret"
    );
    assert_eq!(
        gateway.messaging_status().await.unwrap()["connections"][0]["health"],
        "connecting"
    );
    fixture.ready(&wire, "durable-session").await;
    wire.dispatch(2, "MESSAGE_CREATE", message("100", "20", "first", false))
        .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        panic!("expected direct reply")
    };
    assert_eq!(channel, "20");
    assert_eq!(payload["content"], "answer first");
    assert_eq!(payload["allowed_mentions"]["parse"], json!([]));
    wire.dispatch(
        3,
        "MESSAGE_CREATE",
        message("101", "22", "not addressed", false),
    )
    .await;
    wire.dispatch(
        4,
        "MESSAGE_CREATE",
        message("102", "22", "<@9> threaded", true),
    )
    .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        panic!("expected thread reply")
    };
    assert_eq!(channel, "22");
    assert_eq!(payload["content"], "answer threaded");
    wait_reply(&gateway, "discord-message-102").await;
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    let mut wire = fixture.wire().await;
    let resume = wire.authentication(6).await;
    assert_eq!(resume["d"]["session_id"], "durable-session");
    assert_eq!(resume["d"]["seq"], 4);
    wire.dispatch(5, "RESUMED", json!({})).await;
    wire.dispatch(6, "MESSAGE_CREATE", message("100", "20", "first", false))
        .await;
    wire.dispatch(7, "MESSAGE_CREATE", message("103", "20", "next", false))
        .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        panic!("expected second direct reply")
    };
    assert_eq!(channel, "20");
    assert_eq!(payload["content"], "answer next");
    assert_eq!(model.requests().len(), 3);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_verified_thread_receives_routine_and_signed_webhook_without_a_chat() {
    use hermes_engine::cron::{Draft, RunStatus, Store};
    use hermes_library::webhooks::Webhook;

    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let config = fixture.configuration_with(
        root.path(),
        json!({
            "guilds":{"11":{"channels":{"21":{"threads":{"22":{}}}}}},
            "commands":{"enabled":true,"sync":false},
            "default_delivery_target":{"guild":"11","channel":"21","thread":"22"},
        }),
    );
    let gateway = model.gateway(root.path(), vec![config]).await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "proactive").await;
    wait_status(&gateway, |status| {
        status["connections"][0]["health"] == "ready"
    })
    .await;

    let db = hermes_engine::db::open(root.path()).unwrap();
    let route: String = db.query_row(
        "SELECT r.id FROM messaging_routes r JOIN messaging_verified_destinations d ON d.route=r.id
         WHERE json_extract(r.room,'$.thread')='22' AND r.session IS NULL",
        [], |row| row.get(0),
    ).unwrap();
    let destination = format!("channel:{route}");
    let library = hermes_library::Library::open(root.path()).unwrap();
    let hook = library
        .create_webhook(Webhook {
            name: "fixture-hook".into(),
            prompt: "Deploy notification".into(),
            deliver: destination.clone(),
            deliver_only: true,
            ..Default::default()
        })
        .unwrap();
    let payload = br#"{"ref":"fixture-revision"}"#.to_vec();
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, hook.secret.as_bytes());
    let signed = ring::hmac::sign(&key, &payload);
    let hex: String = signed
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let signature = format!("sha256={hex}");
    let client = reqwest::Client::new();
    let endpoint = format!("{}/webhooks/fixture-hook", gateway.http_url());
    let post = |sig: &str| {
        client
            .post(&endpoint)
            .header("x-hub-signature-256", sig)
            .header("x-hermes-delivery-id", "fixture-delivery")
            .body(payload.clone())
            .send()
    };
    assert_eq!(
        post("sha256=00").await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    for _ in 0..2 {
        assert_eq!(
            post(&signature).await.unwrap().status(),
            StatusCode::ACCEPTED
        );
    }
    let Write::Message {
        channel, payload, ..
    } = fixture
        .next_write("a signed webhook notification", |write| {
            matches!(write, Write::Message { .. })
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!(channel, "22");
    assert!(
        payload["content"]
            .as_str()
            .unwrap()
            .contains("fixture-revision")
    );
    assert_eq!(payload["allowed_mentions"]["parse"], json!([]));

    let jobs = Store::open_view(root.path()).unwrap();
    let job = jobs
        .create(
            Draft {
                name: "Fixture routine".into(),
                prompt: "routine delivery".into(),
                deliver: destination,
                workdir: Some(root.path().to_owned()),
                paused: true,
                ..Draft::default()
            },
            chrono::Utc::now(),
        )
        .unwrap();
    let request = json!({"job_id":job.id,"request_id":"single-trigger"});
    let trigger = || test_support::dispatch(&gateway, "cron.trigger", &request, true);
    assert_eq!(trigger().await.unwrap()["run_id"], trigger().await.unwrap()["run_id"]);
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!()
    };
    assert_eq!(channel, "22");
    assert_eq!(payload["content"], "answer routine delivery");
    let status = wait_status(&gateway, |status| {
        status["deliveries"].as_array().is_some_and(|deliveries| {
            deliveries.len() == 2
                && deliveries
                    .iter()
                    .all(|delivery| delivery["state"] == "sent" && delivery["route"] == route)
        })
    })
    .await;
    assert_eq!(status["input"], json!([]));
    assert_eq!(model.requests().len(), 1);
    assert_eq!(jobs.runs(&job.id).unwrap()[0].status, RunStatus::Completed);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_auto_thread_converges_after_an_unknown_create_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let configure = |fixture: &Fixtures| {
        fixture.configuration_with(
            root.path(),
            json!({
                "grants":{"users":["7"]},
                "guilds":{"11":{"channels":{"21":{}}}},
                "auto_thread":true,
                "auto_thread_inline_fallback":true,
                "commands":{"enabled":true,"sync":false},
            }),
        )
    };
    let gateway = model.gateway(root.path(), vec![configure(&fixture)]).await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "thread-restart").await;
    let starter = message("400", "21", "<@9> open a thread", true);
    wire.dispatch(2, "MESSAGE_CREATE", starter.clone()).await;
    let Write::Thread {
        parent,
        starter: requested,
        ..
    } = fixture
        .next_write("a thread provision attempt", |write| {
            matches!(write, Write::Thread { .. })
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!((parent.as_str(), requested.as_str()), ("21", "400"));
    wait_status(&gateway, |status| {
        status["deliveries"].as_array().is_some_and(|deliveries| {
            deliveries
                .iter()
                .any(|delivery| delivery["part_state"] == "uncertain")
        })
    })
    .await;
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();

    let gateway = model.gateway(root.path(), vec![configure(&fixture)]).await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 2);
    wire.dispatch(3, "RESUMED", json!({})).await;
    wire.dispatch(4, "MESSAGE_CREATE", starter).await;
    crate::lock(&fixture.transport.threads).insert(
        "400".into(),
        json!({
            "id":"400","type":11,"guild_id":"11","parent_id":"23","owner_id":"9",
        }),
    );
    sleep(Duration::from_millis(2200)).await;
    assert!(model.requests().is_empty());
    let evidence = json!({"id":"400","type":11,"guild_id":"11","parent_id":"21","owner_id":"9"});
    crate::lock(&fixture.transport.threads).insert("400".into(), evidence.clone());
    wire.dispatch(5, "THREAD_CREATE", evidence).await;
    wire.dispatch(
        6,
        "MESSAGE_CREATE",
        message("400", "400", "<@9> open a thread", true),
    )
    .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        unreachable!()
    };
    assert_eq!(channel, "400");
    assert_eq!(payload["content"], "answer open a thread");
    wait_reply(&gateway, "discord-message-400").await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
    let status = gateway.messaging_status().await.unwrap();
    assert_eq!(status["input"].as_array().unwrap().len(), 1);
    assert_eq!(status["input"][0]["conflict"], false);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_auto_thread_uses_inline_only_for_a_definite_refusal() {
    for fallback in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
        let mut fixture = Fixtures::start().await;
        let config = fixture.configuration_with(
            root.path(),
            json!({
                "grants":{"users":["7"]},
                "guilds":{"11":{"channels":{"21":{}}}},
                "auto_thread":true,
                "auto_thread_inline_fallback":fallback,
                "commands":{"enabled":true,"sync":false},
            }),
        );
        let gateway = model.gateway(root.path(), vec![config]).await;
        fixture.activate(&gateway).await;
        let mut wire = fixture.wire().await;
        wire.authentication(2).await;
        fixture.ready(&wire, "thread-refusal").await;
        wire.dispatch(
            2,
            "MESSAGE_CREATE",
            message("401", "21", "<@9> inline", true),
        )
        .await;
        if fallback {
            let Write::Message {
                channel, payload, ..
            } = fixture.reply().await
            else {
                unreachable!()
            };
            assert_eq!(channel, "21");
            assert_eq!(payload["content"], "answer inline");
            wait_reply(&gateway, "discord-message-401").await;
            assert_eq!(model.requests().len(), 1);
        } else {
            wait_status(&gateway, |status| {
                status["input"][0]["reason"] == "thread_provision_refused"
            })
            .await;
            assert!(model.requests().is_empty());
        }
        assert_eq!(fixture.transport.thread_creates.load(Ordering::SeqCst), 1);
        gateway.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn discord_missing_heartbeat_ack_resumes_then_invalid_session_reports_gap() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start_with_heartbeat(125).await;
    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "before-gap").await;
    wait_status(&gateway, |state| {
        state["connections"][0]["health"] == "ready"
    })
    .await;
    wire.to_bot.send(WireEvent::NoHeartbeatAcks).await.unwrap();

    let mut resumed = fixture.wire().await;
    assert_eq!(resumed.authentication(6).await["d"]["seq"], 1);
    resumed.data(json!({"op":9,"d":false})).await;
    let mut fresh = fixture.wire().await;
    fresh.authentication(2).await;
    fixture.ready(&fresh, "after-gap").await;
    wait_status(&gateway, |state| {
        state["connections"][0]["health"] == "ready" && state["connections"][0]["gap"] == true
    })
    .await;
    fresh.to_bot.send(WireEvent::Close(4014)).await.unwrap();
    wait_status(&gateway, |state| {
        state["connections"][0]["health"] == "configuration"
    })
    .await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_fresh_sessions_repair_bounded_history_without_admitting_overlap() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7"]},
        "guilds":{"11":{"channels":{"21":{}}}},
        "commands":{"sync":false},
        "recovery":{"history_window_secs":120,"max_surfaces":2,"max_messages":10}
    });
    let gateway = model
        .gateway(
            root.path(),
            vec![fixture.configuration_with(root.path(), settings)],
        )
        .await;
    fixture.activate(&gateway).await;
    let mut first = fixture.wire().await;
    first.authentication(2).await;
    fixture.ready(&first, "known-history").await;
    let base = ((now() - 1_420_070_400_000) as u64) << 22;
    let original_id = (base + 1).to_string();
    let original = message(&original_id, "21", "<@9> original", true);
    first.dispatch(2, "MESSAGE_CREATE", original.clone()).await;
    fixture.reply().await;
    wait_reply(&gateway, &format!("discord-message-{original_id}")).await;
    let missing_id = (base + 2).to_string();
    let missing = message(&missing_id, "21", "<@9> recovered", true);
    let thread_id = (base + 3).to_string();
    let in_thread = message(&thread_id, "29", "<@9> recovered active thread", true);
    crate::lock(&fixture.transport.history).insert("21".into(), vec![missing, original]);
    crate::lock(&fixture.transport.history).insert("29".into(), vec![in_thread]);
    crate::lock(&fixture.transport.threads).extend([
        ("28".into(), json!({"id":"28","type":12,"guild_id":"11","parent_id":"21"})),
        ("29".into(), json!({"id":"29","type":11,"guild_id":"11","parent_id":"21"})),
    ]);
    first.data(json!({"op":9,"d":false})).await;
    let mut replacement = fixture.wire().await;
    replacement.authentication(2).await;
    fixture.ready(&replacement, "after-history").await;
    let mut remaining = BTreeMap::from([
        ("21", "answer recovered"),
        ("29", "answer recovered active thread"),
    ]);
    for _ in 0..remaining.len() {
        let Write::Message { channel, payload, .. } = fixture.reply().await else {
            panic!("expected reply to recovered Discord history")
        };
        let expected = remaining
            .remove(channel.as_str())
            .expect("one reply per recovered destination");
        assert_eq!(payload["content"], expected);
    }
    wait_reply(&gateway, &format!("discord-message-{missing_id}")).await;
    wait_reply(&gateway, &format!("discord-message-{thread_id}")).await;
    assert_eq!(model.requests().len(), 3);
    assert_eq!(
        gateway.messaging_status().await.unwrap()["connections"][0]["gap"],
        true
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_role_updates_revoke_guild_input_without_blocking_an_independent_dm() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7"]},
        "guilds":{"11":{"rules":{"grants":{"roles":["41"]}},"channels":{"21":{"threads":{"22":{}}}}}},
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
    fixture.ready(&wire, "role-revision").await;
    let mut admitted = message("160", "22", "<@9> authorized by guild role", true);
    admitted["member"]["roles"] = json!(["41"]);
    wire.dispatch(2, "MESSAGE_CREATE", admitted).await;
    let Write::Message { channel, .. } = fixture.reply().await else {
        panic!("expected guild reply")
    };
    assert_eq!(channel, "22");
    wait_reply(&gateway, "discord-message-160").await;
    wire.dispatch(
        3,
        "GUILD_MEMBER_UPDATE",
        json!({"guild_id":"11","user":{"id":"7"},"roles":[]}),
    )
    .await;
    wire.dispatch(
        4,
        "MESSAGE_CREATE",
        message("161", "22", "<@9> former role", true),
    )
    .await;
    wait_status(&gateway, |state| {
        state["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|input| input["id"] == "discord-message-161" && input["state"] == "refused")
    })
    .await;
    wire.dispatch(
        5,
        "MESSAGE_CREATE",
        message("162", "20", "independent direct", false),
    )
    .await;
    let Write::Message {
        channel, payload, ..
    } = fixture.reply().await
    else {
        panic!("expected direct reply")
    };
    assert_eq!(channel, "20");
    assert_eq!(payload["content"], "answer independent direct");
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_resume_never_skips_a_dispatch_that_failed_to_commit() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    fixture.activate(&gateway).await;
    let mut first = fixture.wire().await;
    first.authentication(2).await;
    fixture.ready(&first, "failed-commit").await;
    wait_status(&gateway, |state| {
        state["connections"][0]["health"] == "ready"
    })
    .await;
    let db = hermes_engine::db::open(root.path()).unwrap();
    db.execute_batch(
        "CREATE TRIGGER reject_test_dispatch BEFORE INSERT ON messaging_ingress
         WHEN NEW.event = 'discord-message-300'
         BEGIN SELECT RAISE(ABORT, 'fixture commit interruption'); END;",
    )
    .unwrap();
    let event = message("300", "20", "retry after a failed commit", false);
    first.dispatch(2, "MESSAGE_CREATE", event.clone()).await;
    let mut resumed = fixture.wire().await;
    assert_eq!(resumed.authentication(6).await["d"]["seq"], 1);
    assert!(model.requests().is_empty());
    db.execute_batch("DROP TRIGGER reject_test_dispatch;")
        .unwrap();
    resumed.dispatch(2, "MESSAGE_CREATE", event).await;
    resumed.dispatch(3, "RESUMED", json!({})).await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        panic!("expected the recovered reply")
    };
    assert_eq!(payload["content"], "answer retry after a failed commit");
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_requester_commands_answer_privately_without_creating_a_turn() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let settings = json!({
        "grants":{"users":["7"]},
        "guilds":{
            "11":{"operator_roles":["31"],"channels":{"21":{}}},
            "12":{"channels":{"23":{}}}
        },
        "commands":{"sync":false,"guild_roles":{"11":["31"]}},
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
    fixture.ready(&wire, "requester").await;
    let interaction = |offset: u64, command: &str, channel: &str, guild: &str, roles: &[&str]| {
        let id = (((now() - 1_420_070_400_000) as u64) << 22) + offset;
        json!({
            "id":id.to_string(), "application_id":"90", "type":2,
            "token":format!("callback-{id}"), "channel_id":channel, "guild_id":guild,
            "member":{"roles":roles,"user":{"id":"8"}}, "data":{"name":command},
        })
    };
    let allowed = interaction(1, "status", "21", "11", &["31"]);
    let event = format!("discord-interaction-{}", allowed["id"].as_str().unwrap());
    wire.dispatch(2, "INTERACTION_CREATE", allowed).await;
    let Write::Callback(response) = fixture.write().await else {
        panic!("expected requester callback")
    };
    assert_eq!(response["type"], 4);
    assert_eq!(response["data"]["flags"], 64);
    assert_eq!(
        response["data"]["content"],
        "Mercury is ready. Operator role verified."
    );
    let mut chat = message("300", "21", "stay outside chat", true);
    chat["author"]["id"] = json!("8");
    chat["member"]["roles"] = json!(["31"]);
    wire.dispatch(3, "MESSAGE_CREATE", chat).await;

    for (position, command, channel, guild, roles) in [
        (4, "queue", "21", "11", &["31"][..]),
        (5, "status", "23", "12", &["31"][..]),
        (6, "status", "21", "11", &[][..]),
    ] {
        let denied = interaction(position, command, channel, guild, roles);
        wire.dispatch(position, "INTERACTION_CREATE", denied).await;
        let Write::Callback(response) = fixture.write().await else {
            panic!("expected private refusal")
        };
        assert_eq!(response["type"], 4);
        assert_eq!(response["data"]["flags"], 64);
        assert!(
            !response["data"]["content"]
                .as_str()
                .unwrap()
                .contains("Mercury is ready")
        );
    }

    let status = gateway.messaging_status().await.unwrap();
    let input = status["input"].as_array().unwrap();
    let invocation = input.iter().find(|entry| entry["id"] == event).unwrap();
    assert_eq!(invocation["state"], "done");
    assert!(invocation["route"].is_null());
    assert!(
        input
            .iter()
            .any(|entry| entry["id"] == "discord-message-300" && entry["state"] == "refused")
    );
    assert!(model.requests().is_empty());
    assert_eq!(status["deliveries"], json!([]));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_approval_is_bound_to_the_exact_bot_message_and_acks_committed_control() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let gateway = model
        .gateway(root.path(), vec![fixture.configuration(root.path())])
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "approvals").await;
    wire.dispatch(2, "MESSAGE_CREATE", message("200", "20", "approval", false))
        .await;
    let Write::Message { payload, id, .. } = fixture.next_write("an approval prompt", |write| {
        matches!(write, Write::Message { payload, .. } if payload["components"][0]["components"][0]["custom_id"].is_string())
    }).await else {
        panic!("expected an approval prompt")
    };
    let button = payload["components"][0]["components"][0]["custom_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let action = button.split(':').nth(1).unwrap();
    wait_status(&gateway, |state| {
        state["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|delivery| delivery["id"] == action && delivery["state"] == "sent")
    })
    .await;
    let id_base = ((now() - 1_420_070_400_000) as u64) << 22;
    let click = |id: u64, message: &str| {
        json!({
            "id":id.to_string(),"application_id":"90","token":format!("callback-{id}"),"type":3,
            "channel_id":"20","user":{"id":"7"},"data":{"custom_id":button},
            "message":{"id":message,"author":{"id":"9","bot":true}}
        })
    };
    wire.dispatch(3, "INTERACTION_CREATE", click(id_base + 1, "888"))
        .await;
    let Write::Callback(rejected) = fixture
        .next_write("a private component refusal", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        panic!("expected private refusal")
    };
    assert_eq!(rejected["type"], 4);
    assert_eq!(rejected["data"]["flags"], 64);
    let expired = id_base - (5_000_u64 << 22);
    wire.dispatch(4, "INTERACTION_CREATE", click(expired, &id))
        .await;
    wait_status(&gateway, |state| {
        state["input"].as_array().unwrap().iter().any(|input| {
            input["id"] == format!("discord-interaction-{expired}") && input["state"] == "refused"
        })
    })
    .await;
    assert_eq!(model.requests().len(), 1);
    let event = (((now() - 1_420_070_400_000) as u64) << 22) + 2;
    wire.dispatch(5, "INTERACTION_CREATE", click(event, &id))
        .await;
    let mut acknowledged = false;
    let mut applied = false;
    let mut replied = false;
    while !acknowledged || !applied || !replied {
        match fixture.write().await {
            Write::Callback(value) => {
                assert_eq!(value["type"], 5);
                let status = gateway.messaging_status().await.unwrap();
                assert!(
                    status["input"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|input| input["id"] == format!("discord-interaction-{event}"))
                );
                acknowledged = true;
            }
            Write::Followup(value) => {
                assert_eq!(value["content"], "Applied.");
                applied = true;
            }
            Write::Message {
                channel, payload, ..
            } => {
                assert_eq!(channel, "20");
                if payload["content"]
                    .as_str()
                    .is_some_and(|text| text.starts_with("answer"))
                {
                    replied = true;
                }
            }
            Write::Thread { .. } => panic!("approval unexpectedly provisioned a thread"),
            Write::VoiceNote { .. } => panic!("approval unexpectedly sent a voice note"),
        }
    }
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}
