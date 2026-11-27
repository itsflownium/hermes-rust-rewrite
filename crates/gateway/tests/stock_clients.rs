mod support;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use hermes_engine::{Config, Entry, History, Status, Turn};
use hermes_gateway::{Gateway, GatewayConfig, RunningGateway};
use hermes_library::Library;
use reqwest::{Client, Response, StatusCode, header};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error, Message, client::IntoClientRequest},
};

const HOST: &str = "hermes.example.test";
const ORIGIN: &str = "https://hermes.example.test";
const TOKEN: &str = "integration-fixture-deployment-token-0123456789";
const PASSWORD: &str = "fixture password";
const PASSWORD_HASH: &str =
    "scrypt$16384$8$1$c3RvY2stYmFzaWMtc2FsdA==$OX5lsHXQjMib0uHwIBN5MaaBKis7daCfi9Y/tqZVQ9A=";

fn http() -> Client {
    Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn value(response: Response) -> Value {
    response.error_for_status().unwrap().json().await.unwrap()
}

async fn password_gateway(home: &Path) -> RunningGateway {
    fs::write(home.join("config.yaml"), format!(
        "webapp:\n  public_url: {ORIGIN}/hermes\ndashboard:\n  basic_auth:\n    username: fixture\n    password_hash: {PASSWORD_HASH}\n",
    )).unwrap();
    Gateway::start(GatewayConfig::new(home, Config::from_env()))
        .await
        .unwrap()
}

fn login_body() -> Value {
    json!({"provider":"basic","username":"fixture","password":PASSWORD,"next":"//foreign.example"})
}

fn cookie(response: &Response) -> String {
    response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

fn status(error: Error) -> u16 {
    match error {
        Error::Http(response) => response.status().as_u16(),
        error => panic!("Expected HTTP refusal, got {error}"),
    }
}

#[tokio::test]
async fn published_password_login_and_tickets_are_bound_to_profile_purpose_and_audience() {
    let home = TempDir::new().unwrap();
    let gateway = password_gateway(home.path()).await;
    let client = http();
    let base = gateway.http_url();
    let ws = gateway.websocket_url();

    let public_status = value(
        client
            .get(format!("{base}/api/status"))
            .header("host", HOST)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(public_status["auth_required"], true);
    assert!(public_status.get("install_id").is_none());

    for (host, token, expected) in [
        (HOST.to_string(), gateway.token(), StatusCode::UNAUTHORIZED),
        (
            gateway.address().to_string(),
            gateway.token(),
            StatusCode::OK,
        ),
    ] {
        let response = client
            .get(format!("{base}/api/profiles"))
            .header("host", host)
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
    let forged = client
        .get(format!("{base}/api/profiles"))
        .header("x-forwarded-host", HOST)
        .header("x-forwarded-proto", "https")
        .bearer_auth(gateway.token())
        .send()
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::FORBIDDEN);
    let strange_host = client
        .get(format!("{base}/api/status"))
        .header("host", "other.example")
        .header("x-forwarded-host", HOST)
        .send()
        .await
        .unwrap();
    assert_eq!(strange_host.status(), StatusCode::FORBIDDEN);

    let login = client
        .post(format!("{base}/auth/password-login"))
        .header("host", HOST)
        .header("origin", ORIGIN)
        .json(&login_body())
        .send()
        .await
        .unwrap();
    let cookie_header = login.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(cookie_header.contains("Path=/hermes"));
    assert!(cookie_header.contains("Secure; HttpOnly; SameSite=Lax"));
    let login_cookie = cookie(&login);
    assert_eq!(value(login).await["next"], "/hermes/");

    let identity = value(
        client
            .get(format!("{base}/api/auth/me"))
            .header("host", HOST)
            .header("cookie", &login_cookie)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(identity["user_id"], "fixture");
    let bad = client
        .get(format!("{base}/api/profiles"))
        .header("host", HOST)
        .bearer_auth(login_cookie.split_once('=').unwrap().1)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNAUTHORIZED);

    let foreign = client
        .post(format!("{base}/api/auth/ws-ticket?profile=foreign"))
        .header("host", HOST)
        .header("cookie", &login_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::FORBIDDEN);
    let mint = |path: &str| {
        client
            .post(format!("{base}{path}"))
            .header("host", HOST)
            .header("cookie", &login_cookie)
    };
    let chat = value(mint("/api/auth/ws-ticket").send().await.unwrap()).await;
    let ticket = chat["ticket"].as_str().unwrap();
    let local = format!("{ws}?ticket={ticket}");
    assert_eq!(status(connect_async(local).await.unwrap_err()), 401);

    let mut crossed = format!("{base}/api/host-terminal?ticket={ticket}")
        .replace("http:", "ws:")
        .into_client_request()
        .unwrap();
    crossed.headers_mut().insert("host", HOST.parse().unwrap());
    assert_eq!(status(connect_async(crossed).await.unwrap_err()), 401);

    let mut android = ws.as_str().into_client_request().unwrap();
    android.headers_mut().insert("host", HOST.parse().unwrap());
    android.headers_mut().insert(
        "sec-websocket-protocol",
        format!("hermes-gateway-v1, hermes-gateway-ticket.{ticket}")
            .parse()
            .unwrap(),
    );
    let (mut socket, handshake) = connect_async(android.clone()).await.unwrap();
    assert_eq!(
        handshake.headers()["sec-websocket-protocol"],
        "hermes-gateway-v1"
    );
    let ready: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(ready["params"]["type"], "gateway.ready");
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"ping","params":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let pong: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(pong["result"]["pong"], true);
    assert_eq!(status(connect_async(android).await.unwrap_err()), 401);

    let terminal = value(
        mint("/api/webapp/socket-ticket?socket=host-terminal")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let terminal_ticket = terminal["ticket"].as_str().unwrap();
    let mut wrong_purpose = format!("{ws}?ticket={terminal_ticket}")
        .into_client_request()
        .unwrap();
    wrong_purpose
        .headers_mut()
        .insert("host", HOST.parse().unwrap());
    assert_eq!(status(connect_async(wrong_purpose).await.unwrap_err()), 401);
    let mut wrong_origin = format!("{ws}?ticket={terminal_ticket}")
        .into_client_request()
        .unwrap();
    wrong_origin
        .headers_mut()
        .insert("host", HOST.parse().unwrap());
    wrong_origin
        .headers_mut()
        .insert("origin", "https://other.example".parse().unwrap());
    assert_eq!(status(connect_async(wrong_origin).await.unwrap_err()), 403);

    let multipart = "--test-boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"fixture.txt\"\r\n\r\nhello\r\n--test-boundary--\r\n";
    let upload = |profile: &str| {
        client
            .post(format!("{base}/api/chat/file-upload?profile={profile}"))
            .header("host", HOST)
            .header(
                "content-type",
                "multipart/form-data; boundary=test-boundary",
            )
            .body(multipart)
    };
    assert_eq!(
        upload("default").send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        upload("foreign")
            .header("cookie", &login_cookie)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let staged = value(
        upload("default")
            .header("cookie", &login_cookie)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(staged["size"], 5);
    assert_eq!(
        fs::read_to_string(staged["staged_upload"]["path"].as_str().unwrap()).unwrap(),
        "hello"
    );

    let unused = value(mint("/api/auth/ws-ticket").send().await.unwrap()).await;
    let sign_out = client
        .post(format!("{base}/auth/logout"))
        .header("host", HOST)
        .header("cookie", &login_cookie)
        .send()
        .await
        .unwrap();
    assert!(sign_out.status().is_redirection());
    let closed = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap();
    assert!(matches!(
        closed,
        Some(Ok(Message::Close(_))) | Some(Err(_)) | None
    ));
    let mut after_logout = format!("{ws}?ticket={}", unused["ticket"].as_str().unwrap())
        .into_client_request()
        .unwrap();
    after_logout
        .headers_mut()
        .insert("host", HOST.parse().unwrap());
    assert_eq!(status(connect_async(after_logout).await.unwrap_err()), 401);
    assert_eq!(
        mint("/api/auth/ws-ticket").send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn stock_history_pages_are_read_only_and_use_canonical_rows() {
    let home = TempDir::new().unwrap();
    let library = Library::open(home.path()).unwrap();
    let summary = library
        .create_session(None, Some("Fixture conversation"))
        .unwrap();
    let id = &summary.id;
    let mut history = History::default();
    history.turns.push(Turn {
        prompt: "First".into(),
        model: "fixture".into(),
        context: String::new(),
        attachments: Vec::new(),
        status: Status::Complete,
        entries: vec![
            Entry::Assistant {
                text: "Second".into(),
                calls: Vec::new(),
            },
            Entry::Tool {
                id: "tool-use".into(),
                name: "fixture_tool".into(),
                approved: true,
                result: "Third".into(),
            },
        ],
    });
    hermes_engine::restore_conversation(&summary.directory, serde_json::to_value(&history).unwrap()).unwrap();
    let original = hermes_engine::conversation_json(&summary.directory).unwrap();
    let gateway = Gateway::start(GatewayConfig::new(home.path(), Config::from_env()))
        .await
        .unwrap();
    let client = http();
    let base = gateway.http_url();
    let get = |path: &str| {
        client
            .get(format!("{base}{path}"))
            .bearer_auth(gateway.token())
    };

    let profiles = value(get("/api/profiles").send().await.unwrap()).await;
    assert_eq!(profiles["profiles"][0]["name"], "default");
    let list = value(
        get("/api/profiles/sessions?profile=default&order=recent&limit=1")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(list["sessions"][0]["id"], *id);
    assert_eq!(list["sessions"][0]["message_count"], 3);
    assert_eq!(list["profile_totals"]["default"], 1);
    let sidebar = value(
        get("/api/profiles/sessions/sidebar?recents_profile=default")
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(sidebar["recents"]["sessions"][0]["id"], *id);
    let messages = format!("/api/sessions/{id}/messages");
    let latest = value(
        get(&format!("{messages}?order=latest&limit=2"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let older = value(
        get(&format!("{messages}?order=latest&limit=2&offset=2"))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let oldest = value(get(&format!("{messages}?limit=1")).send().await.unwrap()).await;
    assert_eq!(
        latest["pagination"],
        json!({"limit":2,"offset":0,"order":"latest","returned":2})
    );
    assert_eq!(latest["messages"][0]["content"], "Second");
    assert_eq!(latest["messages"][1]["tool_name"], "fixture_tool");
    assert_eq!(latest["messages"][1]["id"], 3);
    assert_eq!(older["messages"], oldest["messages"]);
    assert_eq!(older["messages"][0]["id"], 1);
    assert_eq!(older["messages"][0]["content"], "First");
    assert_eq!(
        value(get(&messages).send().await.unwrap()).await["pagination"]["limit"],
        500
    );
    assert_eq!(
        value(
            get(&format!("{messages}?offset=99999"))
                .send()
                .await
                .unwrap()
        )
        .await["messages"],
        json!([])
    );
    assert_eq!(
        get(&format!("{messages}?profile=other"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        get("/api/sessions/missing/messages")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(hermes_engine::conversation_json(&summary.directory).unwrap(), original);

    let ticket = value(
        client
            .post(format!("{base}/api/auth/ws-ticket"))
            .bearer_auth(gateway.token())
            .send()
            .await
            .unwrap(),
    )
    .await;
    let dial = format!(
        "{}?ticket={}",
        gateway.websocket_url(),
        ticket["ticket"].as_str().unwrap()
    );
    let (mut socket, _) = connect_async(dial).await.unwrap();
    socket.next().await.unwrap().unwrap();
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"session.resume","params":{"session_id":id}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let snapshot: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(
        snapshot["result"]["messages"][0]["message_id"],
        oldest["messages"][0]["message_id"]
    );
    assert_eq!(
        snapshot["result"]["messages"][2]["message_id"],
        latest["messages"][1]["message_id"]
    );
    socket.close(None).await.unwrap();
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_password_pkce_has_one_time_codes_and_distinct_refresh_credentials() {
    let home = TempDir::new().unwrap();
    let gateway = password_gateway(home.path()).await;
    let client = http();
    let base = gateway.http_url();
    let verifier = "different-synthetic-characters-for-pkce-verification";
    let challenge = URL_SAFE_NO_PAD.encode(ring::digest::digest(
        &ring::digest::SHA256,
        verifier.as_bytes(),
    ));
    let query = [
        ("provider", "basic"),
        ("redirect_uri", "http://127.0.0.1:42424/callback"),
        ("state", "fixture-state"),
        ("code_challenge", challenge.as_str()),
        ("code_challenge_method", "S256"),
    ];
    let authorize = client
        .get(format!("{base}/auth/native/authorize"))
        .header("host", HOST)
        .query(&query)
        .send()
        .await
        .unwrap();
    assert!(authorize.status().is_redirection());
    let login = client
        .post(format!("{base}/auth/password-login"))
        .header("host", HOST)
        .header("origin", ORIGIN)
        .header("cookie", cookie(&authorize))
        .json(&login_body())
        .send()
        .await
        .unwrap();
    let result = value(login).await;
    let redirect = reqwest::Url::parse(result["next"].as_str().unwrap()).unwrap();
    assert_eq!(redirect.host_str(), Some("127.0.0.1"));
    let query: std::collections::HashMap<_, _> = redirect.query_pairs().collect();
    assert_eq!(query["state"], "fixture-state");
    let code = &query["code"];
    let redeem = || {
        client
            .post(format!("{base}/auth/native/token"))
            .header("host", HOST)
            .json(&json!({"code":code,"code_verifier":verifier}))
    };
    let tokens = value(redeem().send().await.unwrap()).await;
    assert_eq!(tokens["token_type"], "Bearer");
    assert_eq!(
        redeem().send().await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    let refresh = tokens["refresh_token"].as_str().unwrap();
    let bearer = tokens["access_token"].as_str().unwrap();
    let profile = |token: &str| {
        client
            .get(format!("{base}/api/profiles"))
            .header("host", HOST)
            .bearer_auth(token)
    };
    assert_eq!(
        profile(bearer).send().await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        profile(refresh).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let rotate = |token: &str| {
        client
            .post(format!("{base}/auth/native/refresh"))
            .header("host", HOST)
            .json(&json!({"refresh_token":token}))
    };
    assert_eq!(
        rotate(bearer).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let renewed = value(rotate(refresh).send().await.unwrap()).await;
    assert_ne!(renewed["refresh_token"], refresh);
    assert_eq!(
        rotate(refresh).send().await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    gateway.shutdown().await.unwrap();
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn provisioned_stock_token_authenticates_rest_and_requires_a_remote_ticket() {
    let home = TempDir::new().unwrap();
    let root = home.path();
    fs::write(root.join("token.fixture"), format!("\u{feff}{TOKEN}\n")).unwrap();
    let _process = Process(
        Command::new(env!("CARGO_BIN_EXE_hermes-gateway"))
            .args([
                "gateway",
                "run",
                "--ephemeral-token",
                "--port",
                "0",
                "--data-dir",
            ])
            .arg(root)
            .env("HERMES_DASHBOARD_PUBLIC_URL", ORIGIN)
            .env(
                "HERMES_DASHBOARD_SESSION_TOKEN",
                format!("file:{}", root.join("token.fixture").display()),
            )
            .env_remove("HERMES_DASHBOARD_BASIC_AUTH_USERNAME")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let local = support::wait_for_owner(root, Duration::from_secs(15)).await;
    let client = http();
    let base = &local.origin;
    let ws = format!("{}/api/ws", base.replacen("http:", "ws:", 1));
    let providers = value(
        client
            .get(format!("{base}/api/auth/providers"))
            .header("host", HOST)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(providers["providers"], json!([]));
    let identity = value(
        client
            .get(format!("{base}/api/auth/me"))
            .header("host", HOST)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(identity["provider"], "session-token");
    assert_eq!(
        client
            .get(format!("{base}/api/auth/me"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let mut direct = format!("{ws}?token={TOKEN}").into_client_request().unwrap();
    direct.headers_mut().insert("host", HOST.parse().unwrap());
    assert_eq!(status(connect_async(direct).await.unwrap_err()), 401);
    let ticket = value(
        client
            .post(format!("{base}/api/auth/ws-ticket"))
            .header("host", HOST)
            .header("x-hermes-session-token", TOKEN)
            .header("authorization", "Basic cHJveHk6Zml4dHVyZQ==")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let mut desktop = format!("{ws}?ticket={}", ticket["ticket"].as_str().unwrap())
        .into_client_request()
        .unwrap();
    desktop.headers_mut().insert("host", HOST.parse().unwrap());
    let (mut socket, _) = connect_async(desktop).await.unwrap();
    socket.close(None).await.unwrap();
}
