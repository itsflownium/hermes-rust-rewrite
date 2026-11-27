use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use hermes_gateway_core::{
    RunningGateway,
    messaging::{EnrollmentCredential, test_support::dispatch},
};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, Semaphore, mpsc},
    task::JoinHandle,
    time::timeout,
};

pub const APP: &str = "AAPP1";
pub const FIRST: &str = "TTEAM1";
pub const SECOND: &str = "TTEAM2";
pub const ACTOR: &str = "UACTOR";

pub fn bot(team: &str) -> String {
    format!("BBOT{}", team.as_bytes().last().copied().unwrap() as char)
}
pub fn bot_user(team: &str) -> String {
    format!("UBOT{}", team.as_bytes().last().copied().unwrap() as char)
}

fn token(prefix: &str, suffix: char) -> String {
    format!("{prefix}-{}", suffix.to_string().repeat(40))
}
fn authorization(prefix: &str, suffix: char) -> String {
    format!("Bearer {}", token(prefix, suffix))
}

pub fn credential(team: &str, receiver: bool) -> EnrollmentCredential {
    token_bundle(team.as_bytes().last().copied().unwrap() as char, receiver)
}

pub fn token_bundle(suffix: char, receiver: bool) -> EnrollmentCredential {
    let mut value = json!({"bot_token":token("xoxb",suffix)});
    if receiver {
        value["app_token"] = json!(token("xapp", '1'));
    }
    EnrollmentCredential::new(value.to_string()).unwrap()
}

#[derive(Clone)]
pub struct Call {
    pub team: String,
    pub method: String,
    pub body: Value,
}

#[derive(Default)]
pub struct Service {
    pub calls: Vec<Call>,
    pub opens: usize,
    pub alternate_app: Option<String>,
    pub wrong_install: Option<String>,
    pub failures: BTreeMap<(String, String), VecDeque<StatusCode>>,
    pub denied_channel: Option<String>,
    pub pause_read: Option<Arc<Semaphore>>,
    pub sent: BTreeMap<(String, String), Value>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub forced_url: Option<String>,
    pub downloaded: Vec<(String, String)>,
    pub transfers: BTreeMap<String, Vec<u8>>,
    pub history: Vec<Value>,
    pub streamed: Vec<String>,
    serial: usize,
}

#[derive(Clone)]
struct Context {
    service: Arc<Mutex<Service>>,
    address: String,
    sockets: mpsc::Sender<Peer>,
}

pub struct Fixture {
    pub service: Arc<Mutex<Service>>,
    pub origin: String,
    sockets: AsyncMutex<mpsc::Receiver<Peer>>,
    server: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    pub async fn start() -> Self {
        let service = Arc::new(Mutex::new(Service::default()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (tx, rx) = mpsc::channel(16);
        let context = Context {
            address: address.clone(),
            service: service.clone(),
            sockets: tx,
        };
        let router = Router::new()
            .route("/socket", get(upgrade))
            .route("/api/{method}", post(api))
            .route("/actions/{handle}", post(private_response))
            .route("/upload/{file}", post(transfer))
            .route("/file/{team}/{file}", get(download))
            .with_state(context);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            service,
            origin: format!("http://{address}/"),
            sockets: AsyncMutex::new(rx),
            server,
        }
    }

    pub fn settings(&self, team: &str, receiver: bool) -> Value {
        json!({
            "app_id":APP,"team_id":team,"bot_id":bot(team),"receiver_owner":receiver,
            "api_origin":self.origin,"agent":false,
            "access":{
                "users":[ACTOR],"operators":[ACTOR],"direct_tools":true,
                "rooms":{"CPUBLIC":{"mention_required":true},"GPRIVATE":{"mention_required":true}},
                "scope":{"direct":{"requester_commands":{"users":[ACTOR]}}},
            },
        })
    }

    pub async fn create(
        &self,
        gateway: &RunningGateway,
        name: &str,
        team: &str,
        receiver: bool,
        settings: Value,
    ) {
        dispatch(
            gateway,
            "messaging.profile.slack.create",
            &json!({"name":name,"settings":settings}),
            true,
        )
        .await
        .unwrap();
        self.enroll(gateway, name, team, receiver).await.unwrap();
    }

    pub async fn enroll(
        &self,
        gateway: &RunningGateway,
        name: &str,
        team: &str,
        receiver: bool,
    ) -> anyhow::Result<()> {
        let lease = gateway
            .begin_adapter_enrollment(name, Duration::from_secs(15))
            .await?;
        gateway
            .complete_adapter_enrollment(name, &lease.id, credential(team, receiver))
            .await
    }

    pub async fn peer(&self) -> Peer {
        timeout(Duration::from_secs(10), self.sockets.lock().await.recv())
            .await
            .expect("Socket Mode connection")
            .expect("fixture socket")
    }

    pub fn calls(&self, method: &str) -> Vec<Call> {
        self.service
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|call| call.method == method)
            .cloned()
            .collect()
    }

    pub fn fail_next(&self, team: &str, method: &str, code: StatusCode) {
        self.service
            .lock()
            .unwrap()
            .failures
            .entry((team.into(), method.into()))
            .or_default()
            .push_back(code);
    }
}

pub struct Peer {
    sends: mpsc::Sender<Value>,
    receives: mpsc::Receiver<Value>,
}

impl Peer {
    pub async fn send(&self, value: Value) {
        self.sends.send(value).await.expect("fixture connected");
    }

    pub async fn ack(&mut self, value: Value) -> Value {
        let id = value["envelope_id"].clone();
        self.send(value).await;
        let ack = timeout(Duration::from_secs(4), self.receives.recv())
            .await
            .expect("Slack ack deadline")
            .expect("fixture ack");
        assert_eq!(ack["envelope_id"], id);
        ack
    }
}

async fn upgrade(State(context): State<Context>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |socket| operate(socket, context))
}

async fn operate(mut socket: WebSocket, context: Context) {
    let app = context
        .service
        .lock()
        .unwrap()
        .alternate_app
        .clone()
        .unwrap_or_else(|| APP.into());
    if socket
        .send(Message::Text(
            json!({"type":"hello","connection_info":{"app_id":app}})
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let (send, mut commands) = mpsc::channel(32);
    let (acks, receive) = mpsc::channel(32);
    if context
        .sockets
        .send(Peer {
            sends: send,
            receives: receive,
        })
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { return };
                if socket.send(Message::Text(command.to_string().into())).await.is_err() { return }
            }
            frame = socket.recv() => match frame {
                Some(Ok(Message::Text(text))) => {
                    let ack: Value = serde_json::from_str(&text).unwrap();
                    if acks.send(ack).await.is_err() { return }
                }
                Some(Ok(Message::Ping(body))) => if socket.send(Message::Pong(body)).await.is_err() { return },
                Some(Ok(Message::Pong(_))) => {},
                _ => return,
            },
        }
    }
}

fn response(value: Value) -> Response {
    Json(value).into_response()
}

async fn api(
    State(context): State<Context>,
    Path(method): Path<String>,
    headers: HeaderMap,
    raw: Bytes,
) -> Response {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if method == "apps.connections.open" {
        assert!(supplied == Some(authorization("xapp", '1').as_str()));
        context.service.lock().unwrap().opens += 1;
        return response(json!({"ok":true,"url":format!("ws://{}/socket",context.address)}));
    }
    let team = match supplied {
        Some(value)
            if value == authorization("xoxb", '1') || value == authorization("xoxb", '3') =>
        {
            FIRST
        }
        Some(value) if value == authorization("xoxb", '2') => SECOND,
        _ => return response(json!({"ok":false,"error":"invalid_auth"})),
    };
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let body = match content_type {
        _ if raw.is_empty() => json!({}),
        Some("application/x-www-form-urlencoded") => Value::Object(
            url::form_urlencoded::parse(&raw)
                .map(|(key, value)| (key.into_owned(), json!(value)))
                .collect(),
        ),
        Some("application/json") => match Json::<Map<String, Value>>::from_bytes(&raw) {
            Ok(Json(fields)) => Value::Object(fields),
            Err(error) => return error.into_response(),
        },
        _ => return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response(),
    };
    let pause = {
        let mut state = context.service.lock().unwrap();
        state.calls.push(Call {
            team: team.into(),
            method: method.clone(),
            body: body.clone(),
        });
        if let Some(code) = state
            .failures
            .get_mut(&(team.into(), method.clone()))
            .and_then(VecDeque::pop_front)
        {
            return (
                code,
                [(header::RETRY_AFTER, "1")],
                Json(json!({"ok":false,"error":"fixture"})),
            )
                .into_response();
        }
        (method == "conversations.info")
            .then(|| state.pause_read.clone())
            .flatten()
    };
    if let Some(pause) = pause {
        pause.acquire().await.unwrap().forget();
    }
    let mut state = context.service.lock().unwrap();
    let result = match method.as_str() {
        "auth.test" => {
            json!({"ok":true,"team_id":team,"bot_id":bot(team),"user_id":bot_user(team)})
        }
        "bots.info" if body["bot"] == "BPEER" => {
            json!({"ok":true,"bot":{"id":"BPEER","app_id":"APEER","user_id":"UPEER","deleted":false}})
        }
        "bots.info" => json!({"ok":true,"bot":{
            "id":bot(team),"app_id":state.wrong_install.as_deref().unwrap_or(APP),"user_id":bot_user(team),"deleted":false,
        }}),
        "users.info" => json!({"ok":true,"user":{
            "id":body["user"],"team_id":team,"is_bot":body["user"]=="UPEER","deleted":false,
        }}),
        "conversations.open" => {
            json!({"ok":true,"channel":{"id":"DMEMBER","is_im":true,"user":body["users"]}})
        }
        "conversations.info" => {
            let Some(id) = body["channel"].as_str() else {
                panic!("missing fixture channel")
            };
            let allowed = state.denied_channel.as_deref() != Some(id);
            let direct = id.starts_with('D');
            json!({"ok":true,"channel":{"id":id,"is_im":direct,"is_mpim":id=="GMPIM",
                "is_member":allowed,"is_archived":!allowed,"user":ACTOR}})
        }
        "conversations.members" => {
            json!({"ok":true,"members":[ACTOR,"UPEER"],"response_metadata":{"next_cursor":""}})
        }
        "conversations.replies" => json!({"ok":true,"messages":state.history}),
        "reactions.get" => json!({
            "ok":true,"type":"message","channel":body["channel"],
            "message":{"ts":body["timestamp"],"text":"private Slack source"}
        }),
        "chat.postMessage" => {
            state.serial += 1;
            let ts = format!("1740000000.{:06}", state.serial);
            state.sent.insert((team.into(), ts.clone()), body.clone());
            let mut message = json!({"bot_id":bot(team),"ts":ts});
            if let Some(thread) = body.get("thread_ts") {
                message["thread_ts"] = thread.clone();
            }
            json!({"ok":true,"channel":body["channel"],"ts":ts,"message":message})
        }
        "chat.update" => json!({"ok":true,"channel":body["channel"],"ts":body["ts"]}),
        "chat.startStream" => {
            state.serial += 1;
            let ts = format!("1740000000.{:06}", state.serial);
            state.streamed.push(ts.clone());
            json!({"ok":true,"channel":body["channel"],"ts":ts})
        }
        "chat.appendStream" | "chat.stopStream" => {
            json!({"ok":true,"channel":body["channel"],"ts":body["ts"]})
        }
        "agents.sessions.setStatus" => json!({"ok":true,"agent_status":body["status"]}),
        "files.info" => {
            let file = body["file"].as_str().unwrap();
            let bytes = &state.files[file];
            let address = state
                .forced_url
                .clone()
                .unwrap_or_else(|| format!("http://{}/file/{team}/{file}", context.address));
            json!({"ok":true,"file":{"id":file,"name":"fixture.txt","size":bytes.len(),"url_private":address}})
        }
        "files.getUploadURLExternal" => {
            state.serial += 1;
            let id = format!("FALLOC{}", state.serial);
            json!({"ok":true,"file_id":id,"upload_url":format!("http://{}/upload/{id}",context.address)})
        }
        "files.completeUploadExternal" => {
            assert!(
                body["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|file| state.transfers.contains_key(file["id"].as_str().unwrap()))
            );
            json!({"ok":true,"files":body["files"]})
        }
        _ => return StatusCode::NOT_IMPLEMENTED.into_response(),
    };
    response(result)
}

#[tokio::test]
async fn fixture_rejects_invalid_api_bodies() {
    let fixture = Fixture::start().await;
    let client = reqwest::Client::new();
    let endpoint = format!("{}api/chat.postMessage", fixture.origin);

    for (content_type, body, expected_status) in [
        ("application/json", "{", StatusCode::BAD_REQUEST),
        ("application/json", "[]", StatusCode::UNPROCESSABLE_ENTITY),
        ("text/plain", "{}", StatusCode::UNSUPPORTED_MEDIA_TYPE),
    ] {
        let response = client
            .post(&endpoint)
            .header(header::AUTHORIZATION, authorization("xoxb", '1'))
            .header(header::CONTENT_TYPE, content_type)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
    }

    assert!(fixture.calls("chat.postMessage").is_empty());
}

async fn transfer(
    State(context): State<Context>,
    Path(file): Path<String>,
    headers: HeaderMap,
    raw: Bytes,
) -> StatusCode {
    assert!(headers.get(header::AUTHORIZATION).is_none());
    context
        .service
        .lock()
        .unwrap()
        .transfers
        .insert(file, raw.to_vec());
    StatusCode::OK
}

async fn private_response(
    State(context): State<Context>,
    Path(handle): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    assert!(headers.get(header::AUTHORIZATION).is_none());
    context.service.lock().unwrap().calls.push(Call {
        team: handle.clone(),
        method: "response_url".into(),
        body,
    });
    if handle == "expired-fixture" {
        StatusCode::GONE
    } else {
        StatusCode::OK
    }
}

async fn download(
    State(context): State<Context>,
    Path((team, file)): Path<(String, String)>,
    headers: HeaderMap,
) -> Vec<u8> {
    assert!(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            == Some(
                authorization("xoxb", team.as_bytes().last().copied().unwrap() as char).as_str()
            )
    );
    let mut state = context.service.lock().unwrap();
    state.downloaded.push((team, file.clone()));
    state.files[&file].clone()
}

pub fn event(
    team: &str,
    envelope: &str,
    event: &str,
    channel: &str,
    ordinal: u32,
    text: &str,
) -> Value {
    json!({
        "type":"events_api","envelope_id":envelope,
        "payload":{"type":"event_callback","api_app_id":APP,"event_id":event,"team_id":team,
            "authorizations":[{"team_id":team,"user_id":bot_user(team),"is_bot":true}],
            "event":{"type":"message","user":ACTOR,"channel":channel,"ts":format!("1730000000.{ordinal:06}"),"text":text},
        },
    })
}

pub async fn until(mut ready: impl FnMut() -> bool) {
    timeout(Duration::from_secs(15), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Slack fixture condition");
}

pub async fn state(gateway: &RunningGateway, ready: impl Fn(&Value) -> bool) -> Value {
    let result = timeout(Duration::from_secs(15), async {
        loop {
            let value = gateway.messaging_status().await.unwrap();
            if ready(&value) {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(state) => state,
        Err(_) => panic!(
            "Slack gateway status condition: {}",
            gateway.messaging_status().await.unwrap()
        ),
    }
}
