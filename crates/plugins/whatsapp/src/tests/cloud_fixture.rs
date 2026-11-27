use super::*;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use ring::hmac;
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinHandle};

const APP: &str = "90001";
const ACCOUNT1: &str = "91001";
const ACCOUNT2: &str = "91002";
const PHONE1: &str = "92001";
const PHONE2: &str = "92002";
const ALICE: &str = "15550001111";
const BOB: &str = "15550002222";
const OPAQUE: &str = "US.8544355353245";

fn secret() -> String {
    format!("fixture-{}", "s".repeat(32))
}
fn verifier() -> String {
    format!("fixture-{}", "v".repeat(32))
}
fn token() -> String {
    format!("fixture-{}", "t".repeat(32))
}

#[derive(Clone)]
struct Sent {
    phone: String,
    json: Value,
    wamid: String,
}

#[derive(Default)]
struct Service {
    serial: u64,
    sends: Vec<Sent>,
    uploads: usize,
    downloads: usize,
    failures: VecDeque<(String, String, StatusCode, i64)>,
}

#[derive(Clone)]
struct Graph {
    endpoint: String,
    service: Arc<Mutex<Service>>,
}

struct Fixture {
    graph: Graph,
    worker: JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Fixture {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let graph = Graph {
            endpoint: format!("http://{}/", listener.local_addr().unwrap()),
            service: Arc::default(),
        };
        let api = Router::new()
            .route("/v27.0/debug_token", get(debug_token))
            .route("/v27.0/{waba}/phone_numbers", get(phones))
            .route("/v27.0/{waba}/subscribed_apps", get(subscription))
            .route("/v27.0/{phone}/messages", post(send))
            .route("/v27.0/{phone}/media", post(upload))
            .route("/v27.0/{id}", get(media))
            .route("/private/download", get(download))
            .with_state(graph.clone());
        let worker = tokio::spawn(async move { axum::serve(listener, api).await.unwrap() });
        Self { graph, worker }
    }

    fn settings(&self, account: &str, phone: &str, owner: bool) -> Value {
        json!({"app_id":APP,"waba_id":account,"phone_id":phone,"receiver_owner":owner,"graph_origin":self.graph.endpoint,
            "access":{"users":[ALICE,BOB,OPAQUE],"operators":[ALICE],"direct_tools":true}})
    }

    async fn add(&self, gateway: &RunningGateway, name: &str, settings: Value) {
        configure(gateway, "whatsapp_cloud", name, settings).await;
        enroll(
            gateway,
            name,
            json!({"access_token":token(),"app_secret":secret(),"verify_token":verifier()})
                .to_string(),
        )
        .await
        .unwrap();
        state(gateway, |s| {
            s["verification"][name]["registration"] == "ready"
        })
        .await;
    }

    async fn url(&self, gateway: &RunningGateway) -> String {
        let snapshot = state(gateway, |s| {
            s["verification"]["cloud-one"]["webhook_path"].is_string()
                && s["verification"]["cloud-one"]["registration"] == "ready"
        })
        .await;
        format!(
            "{}{}",
            gateway.http_url(),
            snapshot["verification"]["cloud-one"]["webhook_path"]
                .as_str()
                .unwrap()
        )
    }

    async fn webhook(&self, url: &str, body: Value) {
        let response = signed(url, body.to_string().into_bytes(), &secret()).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    fn sends(&self) -> Vec<Sent> {
        self.graph.service.lock().unwrap().sends.clone()
    }
    fn uploads(&self) -> usize {
        self.graph.service.lock().unwrap().uploads
    }
    fn fail(&self, phone: &str, kind: &str, status: StatusCode, code: i64) {
        self.graph.service.lock().unwrap().failures.push_back((
            phone.into(),
            kind.into(),
            status,
            code,
        ));
    }
}

#[tokio::test]
async fn cloud_retries_a_partial_signed_batch_after_a_failed_receipt_commit() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    graph
        .add(
            &gateway,
            "cloud-one",
            graph.settings(ACCOUNT1, PHONE1, true),
        )
        .await;
    let database = hermes_engine::db::open(root.path()).unwrap();
    database
        .execute_batch(
            "CREATE TRIGGER fixture_abort_receipt BEFORE INSERT ON messaging_whatsapp_sources
        WHEN NEW.anchor='wamid.second-part' BEGIN SELECT RAISE(ABORT,'fixture only'); END;",
        )
        .unwrap();
    let url = graph.url(&gateway).await;
    let notification = batch(vec![entry(
        ACCOUNT1,
        PHONE1,
        vec![
            event(ALICE, "first-part", "first committed row", now()),
            event(BOB, "second-part", "second committed row", now()),
        ],
        vec![],
    )]);
    assert_eq!(
        signed(&url, notification.to_string().into_bytes(), &secret())
            .await
            .status(),
        503
    );
    wait(|| graph.sends().len() == 1).await;
    database
        .execute_batch("DROP TRIGGER fixture_abort_receipt")
        .unwrap();
    graph.webhook(&url, notification).await;
    wait(|| graph.sends().len() == 2).await;
    assert_eq!(model.requests().len(), 2);
    assert_eq!(graph.sends()[0].json["to"], ALICE);
    assert_eq!(graph.sends()[1].json["to"], BOB);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn cloud_pairing_requires_private_owner_approval_before_user_turns() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    let mut settings = graph.settings(ACCOUNT1, PHONE1, true);
    settings["access"]["users"] = json!([]);
    settings["access"]["scope"]["pair_direct"] = json!(true);
    graph.add(&gateway, "cloud-one", settings).await;
    let url = graph.url(&gateway).await;
    one(&graph, &url, event(ALICE, "unpaired", "forbidden", now())).await;
    one(&graph, &url, event(ALICE, "pairing", "/pair", now())).await;
    let request = json!({"name":"cloud-one"});
    assert!(
        dispatch(
            &gateway,
            "messaging.profile.pending_pairings",
            &request,
            false
        )
        .await
        .is_err()
    );
    let pending = dispatch(
        &gateway,
        "messaging.profile.pending_pairings",
        &request,
        true,
    )
    .await
    .unwrap();
    assert_eq!(pending["pairings"][0]["actor"], ALICE);
    assert!(graph.sends().is_empty());
    let reviewed = dispatch(
        &gateway,
        "messaging.profile.pair",
        &json!({"name":"cloud-one","code":pending["pairings"][0]["code"]}),
        true,
    )
    .await
    .unwrap();
    assert_eq!(reviewed["paired"], true);
    one(
        &graph,
        &url,
        event(ALICE, "paired", "after verified pairing", now()),
    )
    .await;
    wait(|| graph.sends().len() == 1).await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(
        graph.sends()[0].json["text"]["body"],
        "answer after verified pairing"
    );
    gateway.shutdown().await.unwrap();
}

async fn debug_token(headers: HeaderMap) -> Json<Value> {
    assert_eq!(
        headers[header::AUTHORIZATION],
        format!("Bearer {APP}|{}", secret())
    );
    Json(json!({"data":{"app_id":APP,"is_valid":true,
        "scopes":["whatsapp_business_management","whatsapp_business_messaging"]}}))
}

fn assert_token(headers: &HeaderMap) {
    assert_eq!(
        headers[header::AUTHORIZATION],
        format!("Bearer {}", token())
    );
}

async fn phones(Path(waba): Path<String>, headers: HeaderMap) -> Json<Value> {
    assert_token(&headers);
    let phone = if waba == ACCOUNT1 { PHONE1 } else { PHONE2 };
    Json(json!({"data":[{"id":phone}]}))
}

async fn subscription(headers: HeaderMap) -> Json<Value> {
    assert_token(&headers);
    Json(json!({"data":[{"whatsapp_business_api_data":{"id":APP}}]}))
}

async fn send(
    State(graph): State<Graph>,
    Path(phone): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    assert_token(&headers);
    assert_eq!(body["messaging_product"], "whatsapp");
    if body["status"] == "read" {
        return Json(json!({"success":true})).into_response();
    }
    let recipient = body["to"]
        .as_str()
        .or_else(|| body["recipient"].as_str())
        .unwrap()
        .to_owned();
    let mut store = graph.service.lock().unwrap();
    store.serial += 1;
    let id = format!("wamid.message-{}", store.serial);
    let kind = body["type"].as_str().unwrap();
    let failure = store
        .failures
        .iter()
        .position(|entry| entry.0 == phone && entry.1 == kind)
        .and_then(|index| store.failures.remove(index));
    store.sends.push(Sent {
        phone,
        json: body,
        wamid: id.clone(),
    });
    if let Some((_, _, status, code)) = failure {
        return (
            status,
            [(header::RETRY_AFTER, "1")],
            Json(json!({"error":{"code":code}})),
        )
            .into_response();
    }
    Json(json!({"messaging_product":"whatsapp","contacts":[{"input":recipient}],"messages":[{"id":id}]})).into_response()
}

async fn upload(State(graph): State<Graph>, headers: HeaderMap, bytes: Bytes) -> Json<Value> {
    assert_token(&headers);
    assert!(
        bytes
            .windows(b"scoped artifact".len())
            .any(|window| window == b"scoped artifact")
    );
    graph.service.lock().unwrap().uploads += 1;
    Json(json!({"id":"93002"}))
}

async fn media(
    State(graph): State<Graph>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Json<Value> {
    assert_token(&headers);
    Json(
        json!({"id":id,"messaging_product":"whatsapp","file_size":10,"mime_type":"application/pdf",
        "url":format!("{}private/download",graph.endpoint)}),
    )
}

async fn download(State(graph): State<Graph>, headers: HeaderMap) -> &'static [u8] {
    assert_token(&headers);
    graph.service.lock().unwrap().downloads += 1;
    b"local file"
}

async fn signed(url: &str, body: Vec<u8>, key: &str) -> reqwest::Response {
    let mac = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key.as_bytes()), &body);
    let digest = mac
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    reqwest::Client::new()
        .post(url)
        .header("x-hub-signature-256", format!("sha256={digest}"))
        .body(body)
        .send()
        .await
        .unwrap()
}

fn event(from: &str, id: &str, text: &str, time: i64) -> Value {
    let field = if crate::common::bsuid(from) {
        "from_user_id"
    } else {
        "from"
    };
    let mut result = json!({"type":"text","id":format!("wamid.{id}"),"timestamp":(time/1000).to_string(),
        "text":{"body":text}});
    result[field] = json!(from);
    result
}

fn entry(account: &str, phone: &str, messages: Vec<Value>, statuses: Vec<Value>) -> Value {
    json!({"id":account,"changes":[{"field":"messages","value":{"messaging_product":"whatsapp",
        "metadata":{"phone_number_id":phone},"messages":messages,"statuses":statuses}}]})
}

fn batch(entries: Vec<Value>) -> Value {
    json!({"object":"whatsapp_business_account","entry":entries})
}

async fn one(f: &Fixture, url: &str, message: Value) {
    f.webhook(
        url,
        batch(vec![entry(ACCOUNT1, PHONE1, vec![message], vec![])]),
    )
    .await;
}

#[tokio::test]
async fn cloud_challenge_authentication_and_multibinding_signed_replay() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    graph
        .add(
            &gateway,
            "cloud-one",
            graph.settings(ACCOUNT1, PHONE1, true),
        )
        .await;
    graph
        .add(
            &gateway,
            "cloud-two",
            graph.settings(ACCOUNT2, PHONE2, false),
        )
        .await;
    let url = graph.url(&gateway).await;
    let client = reqwest::Client::new();
    let check = client
        .get(&url)
        .query(&[
            ("hub.mode", "subscribe"),
            ("hub.verify_token", &verifier()),
            ("hub.challenge", "012345"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(check.status(), 200);
    assert_eq!(check.text().await.unwrap(), "012345");
    assert_eq!(
        client
            .get(&url)
            .query(&[
                ("hub.mode", "subscribe"),
                ("hub.verify_token", "wrong"),
                ("hub.challenge", "123")
            ])
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        signed(&url, b"malformed".to_vec(), &secret())
            .await
            .status(),
        400
    );
    assert_eq!(
        signed(&url, b"malformed".to_vec(), "incorrect key")
            .await
            .status(),
        403
    );
    let mut paired = event(ALICE, "shared", "verified first installation", now());
    paired["from_user_id"] = json!(OPAQUE);
    let request = batch(vec![
        entry(
            ACCOUNT1,
            PHONE2,
            vec![event(ALICE, "bad-waba", "misrouted", now())],
            vec![],
        ),
        entry(
            ACCOUNT1,
            PHONE1,
            vec![paired, event("15550009999", "blocked", "denied", now())],
            vec![],
        ),
        entry(
            ACCOUNT2,
            PHONE2,
            vec![event(
                ALICE,
                "shared",
                "verified second installation",
                now(),
            )],
            vec![],
        ),
    ]);
    graph.webhook(&url, request.clone()).await;
    wait(|| graph.sends().len() == 2).await;
    one(
        &graph,
        &url,
        event(OPAQUE, "alias", "same recipient alias", now()),
    )
    .await;
    wait(|| graph.sends().len() == 3).await;
    assert!(graph.sends().iter().any(|send| send.phone == PHONE1
        && send.json["to"] == ALICE
        && send.json["text"]["body"] == "answer same recipient alias"));
    assert!(graph.sends().iter().any(|send| send.phone == PHONE2
        && send.json["text"]["body"] == "answer verified second installation"));
    gateway.shutdown().await.unwrap();
    let gateway = model.gateway(root.path(), vec![]).await;
    state(&gateway, |s| {
        s["verification"]["cloud-two"]["registration"] == "ready"
    })
    .await;
    let url = graph.url(&gateway).await;
    graph.webhook(&url, request).await;
    one(
        &graph,
        &url,
        event(BOB, "after-restart", "successful after restart", now()),
    )
    .await;
    wait(|| graph.sends().len() == 4).await;
    assert_eq!(model.requests().len(), 4);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn cloud_opaque_recipient_and_scoped_interaction_make_one_native_decision() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    graph
        .add(
            &gateway,
            "cloud-one",
            graph.settings(ACCOUNT1, PHONE1, true),
        )
        .await;
    let url = graph.url(&gateway).await;
    one(&graph, &url, event(OPAQUE, "question", "clarify", now())).await;
    wait(|| {
        graph
            .sends()
            .iter()
            .any(|send| send.json["type"] == "interactive")
    })
    .await;
    let card = graph
        .sends()
        .into_iter()
        .find(|send| send.json["type"] == "interactive")
        .unwrap();
    assert_eq!(card.json["recipient"], OPAQUE);
    assert!(card.json["to"].is_null());
    let option = card.json["interactive"]["action"]["buttons"][0]["reply"]["id"]
        .as_str()
        .unwrap();
    let choice = |actor: &str, id: &str| {
        let mut msg = event(actor, id, "", now());
        msg["type"] = json!("interactive");
        msg["context"] = json!({"id":card.wamid});
        msg["interactive"] = json!({"type":"button_reply","button_reply":{"id":option}});
        msg
    };
    one(&graph, &url, choice(ALICE, "wrong-actor")).await;
    one(&graph, &url, choice(OPAQUE, "correct-actor")).await;
    one(&graph, &url, choice(OPAQUE, "correct-actor")).await;
    wait(|| {
        graph
            .sends()
            .iter()
            .filter(|send| send.json["type"] == "text")
            .count()
            == 1
    })
    .await;
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

fn clock(path: &std::path::Path, time: i64) {
    std::fs::write(path, time.to_string()).unwrap();
}

#[tokio::test]
async fn cloud_service_window_rechecks_queued_and_rate_limited_results() {
    let root = tempfile::tempdir().unwrap();
    let path: PathBuf = root.path().join("fixture-clock");
    let base = now();
    clock(&path, base);
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    let mut settings = graph.settings(ACCOUNT1, PHONE1, true);
    settings["fixture_clock"] = json!(path);
    graph.add(&gateway, "cloud-one", settings).await;
    let url = graph.url(&gateway).await;
    let window = 24 * 60 * 60 * 1000;
    one(&graph, &url, event(ALICE, "slow", "steer wait", base)).await;
    wait(|| model.requests().len() == 1).await;
    clock(&path, base + window + 1000);
    model.release_waiting_turn();
    let closed = |snapshot: &Value| {
        snapshot["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["error"] == "reply_window_closed"))
    };
    state(&gateway, closed).await;
    assert!(graph.sends().is_empty());
    graph.fail(PHONE1, "text", StatusCode::TOO_MANY_REQUESTS, 131056);
    one(
        &graph,
        &url,
        event(ALICE, "reopened", "definite retry", base + window + 1000),
    )
    .await;
    wait(|| graph.sends().len() == 1).await;
    clock(&path, base + 2 * window + 3000);
    state(&gateway, |snapshot| {
        snapshot["deliveries"].as_array().is_some_and(|rows| {
            rows.iter()
                .filter(|row| row["error"] == "reply_window_closed")
                .count()
                >= 2
        })
    })
    .await;
    assert_eq!(graph.sends().len(), 1);
    one(
        &graph,
        &url,
        event(
            ALICE,
            "later",
            "new authorized send",
            base + 2 * window + 3000,
        ),
    )
    .await;
    wait(|| graph.sends().len() == 2).await;
    assert_eq!(
        graph.sends()[1].json["text"]["body"],
        "answer new authorized send"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn cloud_upload_receipt_survives_restart_before_visible_publication() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    let mut settings = graph.settings(ACCOUNT1, PHONE1, true);
    settings["access"]["scope"]["direct"]["turn"] = json!({"approval":"off"});
    graph.add(&gateway, "cloud-one", settings).await;
    let url = graph.url(&gateway).await;
    graph.fail(PHONE1, "document", StatusCode::TOO_MANY_REQUESTS, 131056);
    one(
        &graph,
        &url,
        event(ALICE, "artifact", "write artifact", now()),
    )
    .await;
    state(&gateway, |s| {
        s["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["error"] == "rate_limited"))
    })
    .await;
    assert_eq!(graph.uploads(), 1);
    gateway.shutdown().await.unwrap();
    let gateway = model.gateway(root.path(), vec![]).await;
    let _ = graph.url(&gateway).await;
    wait(|| {
        graph
            .sends()
            .iter()
            .filter(|send| send.json["type"] == "document")
            .count()
            == 2
    })
    .await;
    assert_eq!(graph.uploads(), 1);
    assert!(
        graph
            .sends()
            .iter()
            .filter(|send| send.json["type"] == "document")
            .all(|send| send.json["document"]["id"] == "93002" && send.json["to"] == ALICE)
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn cloud_private_media_preparation_and_uncertain_send_are_scoped() {
    let root = tempfile::tempdir().unwrap();
    let graph = Fixture::start().await;
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    graph
        .add(
            &gateway,
            "cloud-one",
            graph.settings(ACCOUNT1, PHONE1, true),
        )
        .await;
    let url = graph.url(&gateway).await;
    graph.fail(PHONE1, "text", StatusCode::INTERNAL_SERVER_ERROR, 2);
    let mut attachment = event(ALICE, "private", "", now());
    attachment["type"] = json!("document");
    attachment["document"] = json!({"id":"93001","filename":"scoped.pdf"});
    one(&graph, &url, attachment.clone()).await;
    state(&gateway, |s| {
        s["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["part_state"] == "uncertain"))
    })
    .await;
    assert_eq!(graph.graph.service.lock().unwrap().downloads, 1);
    gateway.shutdown().await.unwrap();
    let gateway = model.gateway(root.path(), vec![]).await;
    let url = graph.url(&gateway).await;
    one(&graph, &url, attachment).await;
    one(
        &graph,
        &url,
        event(BOB, "other-recipient", "independent room", now()),
    )
    .await;
    wait(|| graph.sends().len() == 2).await;
    assert_eq!(graph.sends()[1].json["to"], BOB);
    assert_eq!(graph.graph.service.lock().unwrap().downloads, 1);
    gateway.shutdown().await.unwrap();
}
