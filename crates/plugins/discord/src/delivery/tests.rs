use super::*;
use crate::messaging::{
    AccessPolicy, Adapter, AdapterConfig, AdapterFactory, AdapterFuture, Capabilities,
    ConnectionFailure, DeliveryIntent, DeliveryPart, Descriptor, EffectCapabilities,
    EnrollmentCredential, Message, PreparationFailure, ScopePolicy, VerifiedAdapter,
};
use crate::{GatewayConfig, RunningGateway};
use axum::{
    Json, Router,
    body::{Bytes, to_bytes},
    extract::{Request, State},
    response::{IntoResponse, Response},
};
use hermes_engine::Config;
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, oneshot};

const BOT: &str = "900";
const WAIT: Duration = Duration::from_secs(10);

fn direct(channel: &str) -> Room {
    Room {
        kind: RoomKind::Direct,
        chat: channel.into(),
        thread: None,
    }
}

fn intent(room: Room, text: impl Into<String>) -> DeliveryIntent {
    DeliveryIntent {
        id: "intent".into(),
        room,
        reply_anchor: Some("747".into()),
        edit_target: None,
        body: DeliveryBody::Reply(Message::text(text)),
    }
}

#[test]
fn split_preserves_graphemes_and_balances_reopened_code_fences() {
    let grapheme = "👩🏽‍💻";
    let text = format!("intro\n````rust\n{}\n````\ntail", grapheme.repeat(800));
    let parts = render::split_markdown(&text);
    assert!(parts.len() > 2);
    assert!(parts.iter().all(|part| measure(part) <= CONTENT_LIMIT));
    assert!(parts[0].starts_with("intro\n````rust\n"));
    let mut reconstructed = String::new();
    for (index, part) in parts.iter().enumerate() {
        let start = if index == 0 {
            "intro\n````rust\n"
        } else {
            "````rust\n"
        };
        let end = if index + 1 == parts.len() {
            "\n````\ntail"
        } else {
            "\n````"
        };
        let body = part.strip_prefix(start).unwrap().strip_suffix(end).unwrap();
        assert!(body.len() % grapheme.len() == 0);
        reconstructed.push_str(body);
    }
    assert_eq!(reconstructed, grapheme.repeat(800));
}

#[test]
fn rendering_pins_first_reply_and_uses_complete_markdown_for_long_output() {
    let text = "e\u{301}".repeat(2_200);
    let input = intent(direct("301"), &text);
    let normal = prepare(&input).unwrap();
    assert_eq!(normal.len(), 3);
    assert!(references_message(
        normal[0].effect.operation_key.as_deref().unwrap()
    ));
    assert!(
        normal[1..]
            .iter()
            .all(|part| !references_message(part.effect.operation_key.as_deref().unwrap()))
    );
    let file = prepare_with(
        &input,
        PlanOptions {
            max_inline_parts: 1,
            ..PlanOptions::default()
        },
    )
    .unwrap();
    assert_eq!(file.len(), normal.len() + 1);
    assert_eq!(file[0].body, input.body);
    for (chunk, alternative) in normal.iter().zip(&file[1..]) {
        assert_eq!(chunk.body, alternative.body);
        assert_eq!(alternative.effect.attachment_fallback_for, Some(0));
        assert_ne!(
            alternative.effect.operation_key,
            file[0].effect.operation_key
        );
    }
    let off = prepare_with(
        &input,
        PlanOptions {
            reply_mode: ReplyMode::Off,
            ..PlanOptions::default()
        },
    )
    .unwrap();
    assert_ne!(off[0].effect.operation_key, normal[0].effect.operation_key);
    assert_ne!(off[0].effect.operation_key, off[1].effect.operation_key);
}

#[test]
fn discord_retry_after_can_extend_but_not_broaden_a_channel_cooldown() {
    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-bucket", "a-discord-bucket".parse().unwrap());
    headers.insert("retry-after", "1.25".parse().unwrap());
    let local = buckets::Rejection::from_response(&headers, &json!({"retry_after":0.9}));
    assert_eq!(
        local.result(local.fallback_scope(), 1),
        retry(Duration::from_millis(1_250), RetryScope::Destination)
    );
    headers.insert("x-ratelimit-scope", "global".parse().unwrap());
    let global = buckets::Rejection::from_response(&headers, &json!({"retry_after":2.1}));
    assert_eq!(
        global.result(global.fallback_scope(), 1),
        retry(Duration::from_millis(2_100), RetryScope::Account)
    );
    let excessive = buckets::Rejection::from_response(&headers, &json!({"retry_after":1e30}));
    assert_eq!(
        excessive.result(excessive.fallback_scope(), 1),
        DeliveryResult::Rejected
    );
}

#[test]
fn prompts_render_current_grants_and_scoped_component_choices() {
    let (content, rows) = prompt_payload(
        "action-2",
        &crate::messaging::Prompt::Approval {
            text: "@everyone".into(),
            allow_session: true,
            allow_persistent: false,
        },
    )
    .unwrap();
    assert!(content.contains("@everyone"));
    let buttons = rows[0]["components"].as_array().unwrap();
    assert_eq!(buttons.len(), 3);
    assert_eq!(buttons[0]["custom_id"], "mrc:action-2:once");
    assert_eq!(buttons[1]["custom_id"], "mrc:action-2:session");
    assert_eq!(buttons[2]["custom_id"], "mrc:action-2:deny");
    let (_, rows) = prompt_payload(
        "action-3",
        &crate::messaging::Prompt::Clarification {
            question: "Which branch?".into(),
            choices: vec![],
        },
    )
    .unwrap();
    assert_eq!(rows[0]["components"][0]["custom_id"], "mrc:action-3:text");
}

#[derive(Clone)]
struct AdapterFixture {
    client: DiscordClient,
    ready: async_channel::Sender<Ingress>,
    options: PlanOptions,
}

impl AdapterFactory for AdapterFixture {
    fn descriptor(&self) -> Descriptor {
        Descriptor {
            platform: "discord-delivery-fixture".into(),
            version: crate::messaging::CONTRACT_VERSION,
            capabilities: Capabilities {
                threads: true,
                attachments: true,
                progress: true,
                editing: true,
                effects: EffectCapabilities {
                    prepare_resources: true,
                    durable_presentation: true,
                    ..EffectCapabilities::default()
                },
                ..Capabilities::TEXT
            },
        }
    }

    fn connect(
        &self,
        _: CancellationToken,
    ) -> AdapterFuture<'_, Result<VerifiedAdapter, ConnectionFailure>> {
        Box::pin(async {
            Ok(VerifiedAdapter {
                account: BOT.into(),
                adapter: Arc::new(self.clone()),
            })
        })
    }
}

impl Adapter for AdapterFixture {
    fn listen(
        &self,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, ConnectionFailure> {
        Box::pin(async move {
            self.ready.send(ingress).await.unwrap();
            cancel.cancelled().await;
            ConnectionFailure::Stopped
        })
    }

    fn prepare(&self, intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
        prepare_with(intent, self.options)
    }

    fn send(
        &self,
        attempt: SendAttempt,
        ingress: Ingress,
        cancel: CancellationToken,
    ) -> AdapterFuture<'_, DeliveryResult> {
        Box::pin(send(&self.client, BOT, attempt, ingress, cancel))
    }
}

struct RequestSeen {
    method: Method,
    path: String,
    headers: HeaderMap,
    bytes: Bytes,
}

struct Preflight {
    started: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct ForcedLimit {
    bucket: &'static str,
    seconds: f64,
    remaining: u32,
}

struct UploadResponse {
    status: StatusCode,
    body: Value,
    pause: Option<Preflight>,
}

fn multipart_json(bytes: &[u8]) -> Option<Value> {
    let wire = std::str::from_utf8(bytes).ok()?;
    let part = wire
        .split_once("name=\"payload_json\"")?
        .1
        .split_once("\r\n\r\n")?
        .1;
    let json = part.split_once("\r\n")?.0;
    serde_json::from_str(json).ok()
}

#[derive(Clone)]
struct Wire {
    seen: async_channel::Sender<RequestSeen>,
    first_unknown: Arc<Mutex<bool>>,
    preflight: Arc<Mutex<Option<Preflight>>>,
    wrong_message_owner: Arc<AtomicBool>,
    forced_limit: Arc<Mutex<Option<ForcedLimit>>>,
    upload_response: Arc<Mutex<Option<UploadResponse>>>,
    next_receipt: Arc<AtomicU64>,
}

impl Wire {
    async fn handle(State(state): State<Self>, request: Request) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        assert_eq!(request.headers()["authorization"], "Bot fixture-token");
        let headers = request.headers().clone();
        let bytes = to_bytes(request.into_body(), 512 * 1024).await.unwrap();
        let payload: Value = serde_json::from_slice(&bytes)
            .ok()
            .or_else(|| multipart_json(&bytes))
            .unwrap_or_default();
        let markdown = bytes
            .windows(b"filename=\"reply.md\"".len())
            .any(|value| value == b"filename=\"reply.md\"");
        if method != Method::GET {
            state
                .seen
                .send(RequestSeen {
                    method: method.clone(),
                    path: path.clone(),
                    headers,
                    bytes,
                })
                .await
                .unwrap();
        }
        if markdown {
            let response = state.upload_response.lock().await.take();
            if let Some(response) = response {
                if let Some(pause) = response.pause {
                    pause.started.send(()).unwrap();
                    pause.release.await.unwrap();
                }
                return (response.status, Json(response.body)).into_response();
            }
        }
        let read = method == Method::GET;
        let reply = match (method, path.as_str()) {
            (Method::GET, "/channels/301") => {
                let pause = state.preflight.lock().await.take();
                if let Some(pause) = pause {
                    pause.started.send(()).unwrap();
                    pause.release.await.unwrap();
                }
                json!({"id":"301","type":1})
            }
            (Method::GET, "/channels/302") => json!({"id":"302","type":1}),
            (Method::GET, "/channels/311") => json!({"id":"311","type":15}),
            (Method::GET, "/channels/400") => {
                json!({"id":"400","type":11,"owner_id":BOT,"parent_id":"311"})
            }
            (Method::GET, "/channels/400/messages/400") => {
                json!({"id":"400","channel_id":"400","author":{"id":BOT}})
            }
            (Method::POST, "/channels/301/messages") => {
                if let Some(limit) = state.forced_limit.lock().await.as_mut()
                    && limit.remaining > 0
                {
                    limit.remaining -= 1;
                    return (
                        StatusCode::TOO_MANY_REQUESTS,
                        [("x-ratelimit-bucket", limit.bucket)],
                        Json(json!({"retry_after":limit.seconds,"global":false})),
                    )
                        .into_response();
                }
                let mut unknown = state.first_unknown.lock().await;
                if std::mem::take(&mut *unknown) {
                    return StatusCode::BAD_GATEWAY.into_response();
                }
                let id = state
                    .next_receipt
                    .fetch_add(1, Ordering::Relaxed)
                    .to_string();
                json!({"id":id,"channel_id":"301","author":{"id":BOT},"nonce":payload["nonce"]})
            }
            (Method::POST, "/channels/400/messages") => {
                let id = state
                    .next_receipt
                    .fetch_add(1, Ordering::Relaxed)
                    .to_string();
                json!({"id":id,"channel_id":"400","author":{"id":BOT},"nonce":payload["nonce"]})
            }
            (Method::POST, "/channels/302/messages") => {
                json!({"id":"903","channel_id":"302","author":{"id":BOT},"nonce":payload["nonce"]})
            }
            (Method::GET, "/channels/301/messages/902") => {
                let author = if state.wrong_message_owner.load(Ordering::Relaxed) {
                    "999"
                } else {
                    BOT
                };
                json!({"id":"902","channel_id":"301","author":{"id":author}})
            }
            (Method::PATCH, "/channels/301/messages/902") => {
                json!({"id":"902","channel_id":"301","author":{"id":BOT}})
            }
            (Method::DELETE, "/channels/301/messages/902") => {
                return (
                    StatusCode::NO_CONTENT,
                    [("x-ratelimit-bucket", "delete-bucket")],
                )
                    .into_response();
            }
            (Method::POST, "/channels/311/threads") => {
                json!({"id":"400","type":11,"owner_id":BOT,"parent_id":"311",
                       "message":{"id":"400","channel_id":"400","author":{"id":BOT}}})
            }
            _ => return StatusCode::NOT_FOUND.into_response(),
        };
        let bucket = if read { "reads" } else { "message-writes" };
        ([("x-ratelimit-bucket", bucket)], Json(reply)).into_response()
    }
}

struct RestFixture {
    adapter: AdapterFixture,
    ready: async_channel::Receiver<Ingress>,
    sent: async_channel::Receiver<RequestSeen>,
    preflight: Arc<Mutex<Option<Preflight>>>,
    wrong_message_owner: Arc<AtomicBool>,
    forced_limit: Arc<Mutex<Option<ForcedLimit>>>,
    upload_response: Arc<Mutex<Option<UploadResponse>>>,
    task: tokio::task::JoinHandle<()>,
}

impl RestFixture {
    async fn start(options: PlanOptions, first_unknown: bool) -> Self {
        let (ready_tx, ready) = async_channel::bounded(5);
        let (seen, sent) = async_channel::bounded(20);
        let preflight = Arc::new(Mutex::new(None));
        let wrong_message_owner = Arc::new(AtomicBool::new(false));
        let forced_limit = Arc::new(Mutex::new(None));
        let upload_response = Arc::new(Mutex::new(None));
        let state = Wire {
            seen,
            first_unknown: Arc::new(Mutex::new(first_unknown)),
            preflight: preflight.clone(),
            wrong_message_owner: wrong_message_owner.clone(),
            forced_limit: forced_limit.clone(),
            upload_response: upload_response.clone(),
            next_receipt: Arc::new(AtomicU64::new(902)),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = DiscordClient::for_fixture(
            EnrollmentCredential::new("fixture-token".into()).unwrap(),
            format!("http://{}/", listener.local_addr().unwrap())
                .parse()
                .unwrap(),
        )
        .unwrap();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(Wire::handle).with_state(state),
            )
            .await
            .unwrap();
        });
        Self {
            adapter: AdapterFixture {
                client,
                ready: ready_tx,
                options,
            },
            ready,
            sent,
            preflight,
            wrong_message_owner,
            forced_limit,
            upload_response,
            task,
        }
    }

    async fn gateway(&self, path: &Path, policy: AccessPolicy) -> (RunningGateway, Ingress) {
        let mut config = GatewayConfig::new(
            path,
            Config {
                base_url: "http://127.0.0.1:1/v1".into(),
                model: "fixture".into(),
                api_key: String::new(),
            },
        );
        config.adapters.push(AdapterConfig {
            name: "discord-fixture".into(),
            factory: Arc::new(self.adapter.clone()),
            policy,
        });
        let gateway = crate::start_test_gateway(config).await.unwrap();
        let ingress = tokio::time::timeout(WAIT, self.ready.recv())
            .await
            .unwrap()
            .unwrap();
        (gateway, ingress)
    }

    async fn sent(&self) -> RequestSeen {
        tokio::time::timeout(WAIT, self.sent.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

impl Drop for RestFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn status(gateway: &RunningGateway, expected: impl Fn(&Value) -> bool) -> Value {
    tokio::time::timeout(WAIT, async {
        loop {
            let status = gateway.messaging_status().await.unwrap();
            if let Some(row) = status["deliveries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| expected(row))
            {
                break row.clone();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn deferred_write_bucket_survives_restart_and_other_operations_stay_eligible() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(PlanOptions::default(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let room = direct("301");
    let expires = chrono::Utc::now().timestamp_millis() + 60_000;
    let route = ingress
        .verify_destination(room.clone(), expires)
        .await
        .unwrap();
    let other = ingress
        .verify_destination(direct("302"), expires)
        .await
        .unwrap();
    let progress = DeliveryIntent {
        id: "rate-progress".into(),
        body: DeliveryBody::Progress {
            text: "Working".into(),
            reply_id: "unrelated-reply".into(),
        },
        ..intent(room, "Working")
    };
    ingress
        .storage()
        .enqueue(&ingress.storage().route(&route).unwrap().unwrap(), &progress)
        .unwrap();
    fixture.sent().await;
    status(&gateway, |row| {
        row["id"] == progress.id && row["part_state"] == "sent"
    })
    .await;

    let known_bucket = gateway
        .send_messaging_notification(&other, "learn", "learn its bucket".into())
        .unwrap();
    assert_eq!(fixture.sent().await.path, "/channels/302/messages");
    status(&gateway, |row| {
        row["id"] == known_bucket && row["part_state"] == "sent"
    })
    .await;

    *fixture.forced_limit.lock().await = Some(ForcedLimit {
        bucket: "message-writes",
        seconds: 4.0,
        remaining: 1,
    });
    let id = gateway
        .send_messaging_notification(&route, "limited", "limited delivery".into())
        .unwrap();
    let rejected = fixture.sent().await;
    let first: Value = serde_json::from_slice(&rejected.bytes).unwrap();
    status(&gateway, |row| {
        row["id"] == id && row["part_state"] == "pending" && row["attempt"] == 1
    })
    .await;
    gateway.shutdown().await.unwrap();

    let (reopened, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let write = fixture
        .adapter
        .client
        .bot(Method::POST, &["channels", "301", "messages"])
        .build()
        .unwrap();
    let read = fixture
        .adapter
        .client
        .bot(Method::GET, &["channels", "301"])
        .build()
        .unwrap();
    let other_write = fixture
        .adapter
        .client
        .bot(Method::POST, &["channels", "302", "messages"])
        .build()
        .unwrap();
    for (request, limited) in [(write, true), (read, false), (other_write, false)] {
        let route = buckets::Route::from_request(&request).unwrap();
        let wait = buckets::wait(&ingress, Some(&route)).await.unwrap();
        assert_eq!(
            wait.map(|(_, scope)| scope),
            limited.then_some(RetryScope::Part)
        );
    }

    let marker = crate::messaging::MarkerRef {
        delivery: progress.id,
        part: 0,
    };
    assert!(ingress.retire_marker(marker).await.unwrap());
    assert_eq!(fixture.sent().await.method, Method::DELETE);
    let outside = reopened
        .send_messaging_notification(&other, "untouched", "other channel".into())
        .unwrap();
    assert_eq!(fixture.sent().await.path, "/channels/302/messages");
    status(&reopened, |row| {
        row["id"] == outside && row["part_state"] == "sent"
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );

    let retried = fixture.sent().await;
    assert_eq!(retried.path, "/channels/301/messages");
    let last: Value = serde_json::from_slice(&retried.bytes).unwrap();
    assert_eq!(last["nonce"], first["nonce"]);
    status(&reopened, |row| {
        row["id"] == id && row["part_state"] == "sent" && row["attempt"] == 2
    })
    .await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_bucket_falls_back_to_a_finite_number_of_outbox_retries() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(PlanOptions::default(), false).await;
    *fixture.forced_limit.lock().await = Some(ForcedLimit {
        bucket: "not/a/discord/bucket",
        seconds: 0.0,
        remaining: 100,
    });
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let route = ingress
        .verify_destination(
            direct("301"),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .await
        .unwrap();
    let id = gateway
        .send_messaging_notification(&route, "malformed", "send".into())
        .unwrap();
    for _ in 0..8 {
        assert_eq!(fixture.sent().await.path, "/channels/301/messages");
    }
    status(&gateway, |row| {
        row["id"] == id && row["part_state"] == "rejected" && row["attempt"] == 8
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fixture.sent.recv())
            .await
            .is_err()
    );
    let request = fixture
        .adapter
        .client
        .bot(Method::POST, &["channels", "301", "messages"])
        .build()
        .unwrap();
    let key = buckets::Route::from_request(&request).unwrap();
    assert!(buckets::wait(&ingress, Some(&key)).await.unwrap().is_none());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn exact_echo_unblocks_ordered_messages_after_uncertainty_and_restart() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(PlanOptions::default(), true).await;
    let room = direct("301");
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let route = ingress
        .verify_destination(room.clone(), chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    let id = gateway
        .send_messaging_notification(&route, "test", "hello".repeat(500))
        .unwrap();
    let first = fixture.sent().await;
    let message: Value = serde_json::from_slice(&first.bytes).unwrap();
    assert_eq!(first.method, Method::POST);
    assert_eq!(message["allowed_mentions"]["parse"], json!([]));
    assert_eq!(message["allowed_mentions"]["replied_user"], false);
    assert_eq!(message["flags"], QUIET);
    assert_eq!(message["enforce_nonce"], true);
    assert!(is_nonce(message["nonce"].as_str().unwrap()));
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "uncertain"
    })
    .await;
    gateway.shutdown().await.unwrap();

    let (reopened, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let echo = json!({"id":"901","channel_id":"301","author":{"id":BOT},"nonce":message["nonce"]});
    let mut other = echo.clone();
    other["author"]["id"] = json!("999");
    observe_create(&ingress, BOT, &room, &other).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), fixture.sent.recv())
            .await
            .is_err()
    );
    observe_create(&ingress, BOT, &room, &echo).await.unwrap();
    let second = fixture.sent().await;
    let message_two: Value = serde_json::from_slice(&second.bytes).unwrap();
    assert_ne!(message["nonce"], message_two["nonce"]);
    assert_eq!(message_two["content"], "hello".repeat(100));
    status(&reopened, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "sent"
    })
    .await;
    observe_create(&ingress, BOT, &room, &echo).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), fixture.sent.recv())
            .await
            .is_err()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn revocation_during_provider_preflight_prevents_the_message_post() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(PlanOptions::default(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let route = ingress
        .verify_destination(
            direct("301"),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .await
        .unwrap();
    let (started, waiting) = oneshot::channel();
    let (resume, release) = oneshot::channel();
    *fixture.preflight.lock().await = Some(Preflight { started, release });
    let id = gateway
        .send_messaging_notification(&route, "paused", "paused delivery".into())
        .unwrap();
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();

    let mut revoked = AccessPolicy::default();
    revoked.scope.direct.enabled = Some(false);
    gateway
        .set_adapter_policy("discord-fixture", revoked)
        .await
        .unwrap();
    resume.send(()).unwrap();
    status(&gateway, |row| {
        row["id"] == id && row["part_state"] == "pending"
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), fixture.sent.recv())
            .await
            .is_err()
    );

    gateway
        .set_adapter_policy("discord-fixture", AccessPolicy::default())
        .await
        .unwrap();
    assert_eq!(fixture.sent().await.path, "/channels/301/messages");
    status(&gateway, |row| {
        row["id"] == id && row["part_state"] == "sent"
    })
    .await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn upload_uses_the_exact_host_attachment_and_complete_markdown_bytes() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(
        PlanOptions {
            max_inline_parts: 1,
            ..PlanOptions::default()
        },
        false,
    )
    .await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let room = direct("301");
    let verified = ingress
        .verify_destination(room.clone(), chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    let route = ingress.storage().route(&verified).unwrap().unwrap();
    let source = b"artifact for the verified account";
    let artifact = ingress
        .storage()
        .put_attachment("proof.txt".into(), source)
        .unwrap();
    let text = "Full reply.\n".repeat(220);
    let mut message = intent(room, &text);
    if let DeliveryBody::Reply(reply) = &mut message.body {
        reply.attachments.push(artifact);
    }
    ingress.storage().enqueue(&route, &message).unwrap();
    let posted = fixture.sent().await;
    assert_eq!(posted.path, "/channels/301/messages");
    assert!(
        posted.headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data;")
    );
    assert!(
        posted
            .bytes
            .windows(text.len())
            .any(|part| part == text.as_bytes())
    );
    let wire = std::str::from_utf8(&posted.bytes).unwrap();
    assert!(wire.contains("filename=\"reply.md\""));
    let file = fixture.sent().await;
    assert!(file.bytes.windows(source.len()).any(|part| part == source));
    assert!(
        std::str::from_utf8(&file.bytes)
            .unwrap()
            .contains("filename=\"proof.txt\"")
    );
    status(&gateway, |row| {
        row["id"] == message.id && row["state"] == "sent"
    })
    .await;
    gateway.shutdown().await.unwrap();
}

fn markdown_options() -> PlanOptions {
    PlanOptions {
        max_inline_parts: 1,
        ..PlanOptions::default()
    }
}

#[tokio::test]
async fn definite_markdown_refusal_activates_exact_ordered_text_after_restart() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(markdown_options(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let room = direct("301");
    let route = ingress
        .verify_destination(room, chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    let (entered, waiting) = oneshot::channel();
    let (resume, release) = oneshot::channel();
    *fixture.upload_response.lock().await = Some(UploadResponse {
        status: StatusCode::BAD_REQUEST,
        body: json!({"code":50_045}),
        pause: Some(Preflight {
            started: entered,
            release,
        }),
    });
    let text = "Complete reply including @everyone and useful text. ".repeat(115);
    let parts = render::split_markdown(&text);
    let id = gateway
        .send_messaging_notification(&route, "long-reply", text.clone())
        .unwrap();
    let upload = fixture.sent().await;
    let primary = multipart_json(&upload.bytes).unwrap();
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();

    let mut revoked = AccessPolicy::default();
    revoked.scope.direct.enabled = Some(false);
    gateway
        .set_adapter_policy("discord-fixture", revoked)
        .await
        .unwrap();
    resume.send(()).unwrap();
    status(&gateway, |row| {
        row["id"] == id
            && row["part"] == 0
            && row["part_state"] == "skipped"
            && row["error"] == "attachment_not_accepted"
    })
    .await;
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "pending" && row["attempt"] == 0
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();

    let (reopened, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    reopened
        .set_adapter_policy("discord-fixture", AccessPolicy::default())
        .await
        .unwrap();
    let mut recovered = String::new();
    for (index, expected) in parts.iter().enumerate() {
        let sent = fixture.sent().await;
        assert_eq!(sent.method, Method::POST);
        assert_eq!(sent.path, "/channels/301/messages");
        let body: Value = serde_json::from_slice(&sent.bytes).unwrap();
        assert_eq!(body["content"], *expected);
        assert_eq!(body["allowed_mentions"]["parse"], json!([]));
        assert_eq!(body["allowed_mentions"]["replied_user"], false);
        assert_eq!(body["flags"], QUIET);
        assert_eq!(body["nonce"], nonce(&id, (index + 1) as u32, index == 0));
        assert_ne!(body["nonce"], primary["nonce"]);
        recovered.push_str(body["content"].as_str().unwrap());
    }
    assert_eq!(recovered, text);
    status(&reopened, |row| row["id"] == id && row["state"] == "sent").await;
    let receipts = ingress
        .storage()
        .read(|db| {
            let mut query = db.prepare(
                "SELECT receipt FROM messaging_parts WHERE delivery=?1 ORDER BY ordinal",
            )?;
            Ok(query
                .query_map([&id], |row| row.get::<_, Option<String>>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })
        .unwrap();
    assert_eq!(receipts[0], None);
    assert_eq!(receipts.len(), parts.len() + 1);
    for (index, receipt) in receipts[1..].iter().enumerate() {
        assert_eq!(receipt.as_deref(), Some((902 + index).to_string().as_str()));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_markdown_upload_waits_and_late_confirmation_skips_the_text() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(markdown_options(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let room = direct("301");
    let route = ingress
        .verify_destination(room.clone(), chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    *fixture.upload_response.lock().await = Some(UploadResponse {
        status: StatusCode::BAD_GATEWAY,
        body: Value::Null,
        pause: None,
    });
    let id = gateway
        .send_messaging_notification(&route, "lost-upload", "full markdown ".repeat(270))
        .unwrap();
    let sent = fixture.sent().await;
    let primary = multipart_json(&sent.bytes).unwrap();
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "uncertain"
    })
    .await;
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "standby"
    })
    .await;
    gateway.shutdown().await.unwrap();

    let (reopened, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    let echo = json!({"id":"901","channel_id":"301","author":{"id":BOT},"nonce":primary["nonce"]});
    observe_create(&ingress, BOT, &room, &echo).await.unwrap();
    status(&reopened, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "sent"
    })
    .await;
    status(&reopened, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "skipped"
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_early_authenticated_echo_fences_text_even_if_rest_reports_upload_refusal() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(markdown_options(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let room = direct("301");
    let route = ingress
        .verify_destination(room.clone(), chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    let (entered, waiting) = oneshot::channel();
    let (resume, release) = oneshot::channel();
    *fixture.upload_response.lock().await = Some(UploadResponse {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        body: Value::Null,
        pause: Some(Preflight {
            started: entered,
            release,
        }),
    });
    let id = gateway
        .send_messaging_notification(&route, "early-echo", "attachment ".repeat(300))
        .unwrap();
    let primary = multipart_json(&fixture.sent().await.bytes).unwrap();
    tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    let echo = json!({"id":"901","channel_id":"301","author":{"id":BOT},"nonce":primary["nonce"]});
    observe_create(&ingress, BOT, &room, &echo).await.unwrap();
    resume.send(()).unwrap();
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "sent"
    })
    .await;
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "skipped"
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn forum_long_reply_fallback_stays_in_the_one_confirmed_child_thread() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(
        PlanOptions {
            surface: DeliverySurface::ForumPost,
            ..markdown_options()
        },
        false,
    )
    .await;
    let access = AccessPolicy {
        rooms: [("311".into(), Default::default())].into_iter().collect(),
        ..AccessPolicy::default()
    };
    let (gateway, ingress) = fixture.gateway(root.path(), access).await;
    let room = Room {
        kind: RoomKind::Group,
        chat: "311".into(),
        thread: None,
    };
    let route = ingress
        .verify_destination(room, chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    *fixture.upload_response.lock().await = Some(UploadResponse {
        status: StatusCode::FORBIDDEN,
        body: json!({"code":400_001}),
        pause: None,
    });
    let text = "Long forum reply text ".repeat(240);
    let id = gateway
        .send_messaging_notification(&route, "forum-fallback", text.clone())
        .unwrap();
    let starter = fixture.sent().await;
    assert_eq!(starter.path, "/channels/311/threads");
    let payload: Value = serde_json::from_slice(&starter.bytes).unwrap();
    assert_eq!(
        payload["message"]["content"],
        "Reply follows in this thread."
    );
    let upload = fixture.sent().await;
    assert_eq!(upload.path, "/channels/400/messages");
    assert!(multipart_json(&upload.bytes).is_some());
    for expected in render::split_markdown(&text) {
        let sent = fixture.sent().await;
        assert_eq!(sent.path, "/channels/400/messages");
        let body: Value = serde_json::from_slice(&sent.bytes).unwrap();
        assert_eq!(body["content"], expected);
        assert_eq!(body["allowed_mentions"]["parse"], json!([]));
    }
    status(&gateway, |row| row["id"] == id && row["state"] == "sent").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn discord_rate_limit_retries_the_same_attachment_instead_of_the_text_alternative() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(markdown_options(), false).await;
    let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
    let route = ingress
        .verify_destination(
            direct("301"),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .await
        .unwrap();
    *fixture.forced_limit.lock().await = Some(ForcedLimit {
        bucket: "message-writes",
        seconds: 1.0,
        remaining: 1,
    });
    let id = gateway
        .send_messaging_notification(&route, "rate-limited-upload", "attachment ".repeat(300))
        .unwrap();
    let first = fixture.sent().await;
    let primary = multipart_json(&first.bytes).unwrap();
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "pending"
    })
    .await;
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "standby"
    })
    .await;
    let last = fixture.sent().await;
    assert_eq!(
        multipart_json(&last.bytes).unwrap()["nonce"],
        primary["nonce"]
    );
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 0 && row["part_state"] == "sent" && row["attempt"] == 2
    })
    .await;
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "skipped"
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), fixture.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn only_the_exact_owned_progress_message_can_be_edited_or_retired() {
    for wrong_owner in [false, true] {
        let root = TempDir::new().unwrap();
        let fixture = RestFixture::start(PlanOptions::default(), false).await;
        let (gateway, ingress) = fixture.gateway(root.path(), AccessPolicy::default()).await;
        let room = direct("301");
        let id = ingress
            .verify_destination(room.clone(), chrono::Utc::now().timestamp_millis() + 60_000)
            .await
            .unwrap();
        let route = ingress.storage().route(&id).unwrap().unwrap();
        let reply = intent(room, "Final text");
        let progress = DeliveryIntent {
            id: crate::messaging::storage::key("progress", &reply.id).unwrap(),
            body: DeliveryBody::Progress {
                text: "Working".into(),
                reply_id: reply.id.clone(),
            },
            ..reply.clone()
        };
        ingress.storage().enqueue(&route, &progress).unwrap();
        let initial = fixture.sent().await;
        let initial_body: Value = serde_json::from_slice(&initial.bytes).unwrap();
        assert_eq!(initial_body["message_reference"]["message_id"], "747");
        assert_eq!(initial_body["allowed_mentions"]["replied_user"], false);
        status(&gateway, |row| {
            row["id"] == progress.id && row["part_state"] == "sent"
        })
        .await;

        fixture
            .wrong_message_owner
            .store(wrong_owner, Ordering::Relaxed);
        ingress.storage().enqueue(&route, &reply).unwrap();
        let marker = crate::messaging::MarkerRef {
            delivery: progress.id,
            part: 0,
        };
        if wrong_owner {
            status(&gateway, |row| {
                row["id"] == reply.id && row["part_state"] == "rejected"
            })
            .await;
            assert!(
                tokio::time::timeout(Duration::from_millis(150), fixture.sent.recv())
                    .await
                    .is_err()
            );
            fixture.wrong_message_owner.store(false, Ordering::Relaxed);
            assert!(ingress.retire_marker(marker).await.unwrap());
            let cleanup = fixture.sent().await;
            assert_eq!(cleanup.method, Method::DELETE);
            assert_eq!(cleanup.path, "/channels/301/messages/902");
        } else {
            let edit = fixture.sent().await;
            assert_eq!(edit.method, Method::PATCH);
            assert_eq!(edit.path, "/channels/301/messages/902");
            let body: Value = serde_json::from_slice(&edit.bytes).unwrap();
            assert_eq!(body["content"], "Final text");
            assert_eq!(body["allowed_mentions"]["parse"], json!([]));
            assert!(body["message_reference"].is_null());
            status(&gateway, |row| {
                row["id"] == reply.id && row["part_state"] == "sent"
            })
            .await;
            assert!(!ingress.retire_marker(marker).await.unwrap());
        }
        gateway.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn forum_starter_is_published_once_and_returns_the_same_destination_identity() {
    let root = TempDir::new().unwrap();
    let fixture = RestFixture::start(
        PlanOptions {
            surface: DeliverySurface::ForumPost,
            ..PlanOptions::default()
        },
        false,
    )
    .await;
    let room = Room {
        kind: RoomKind::Group,
        chat: "311".into(),
        thread: None,
    };
    let access = AccessPolicy {
        rooms: [("311".into(), Default::default())].into_iter().collect(),
        scope: ScopePolicy::default(),
        ..AccessPolicy::default()
    };
    let (gateway, ingress) = fixture.gateway(root.path(), access).await;
    let parent = ingress
        .verify_destination(room, chrono::Utc::now().timestamp_millis() + 60_000)
        .await
        .unwrap();
    let id = gateway
        .send_messaging_notification(&parent, "forum-test", "forum content".into())
        .unwrap();
    let post = fixture.sent().await;
    assert_eq!(post.path, "/channels/311/threads");
    assert_eq!(post.headers["content-type"], "application/json");
    let starter: Value = serde_json::from_slice(&post.bytes).unwrap();
    assert_eq!(starter["message"]["content"], "forum content");
    assert_eq!(starter["message"]["allowed_mentions"]["parse"], json!([]));
    status(&gateway, |row| {
        row["id"] == id && row["part"] == 1 && row["part_state"] == "sent"
    })
    .await;
    let route = gateway
        .bind_messaging_provisioned_destination(ingress.connection_name(), &id, 0, 1)
        .await
        .unwrap();
    assert_eq!(
        ingress
            .storage()
            .route(&route)
            .unwrap()
            .unwrap()
            .room
            .thread
            .as_deref(),
        Some("400")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), fixture.sent.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}
