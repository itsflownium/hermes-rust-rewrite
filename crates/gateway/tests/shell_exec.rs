mod support;

use hermes_gateway::{
    client::{ClientError, ClientOptions, GatewayClient},
    owner::OwnerConnection,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

const PROVIDER_SECRET: &str = "synthetic-provider-value-for-shell-boundary";
const WORKSPACE: &str = "fixture-shell-working-directory";

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn launch(root: &Path) -> Process {
    Process(
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
            .env_remove("TERMINAL_ENV")
            .env_remove("HERMES_DASHBOARD_PUBLIC_URL")
            .env("HERMES_RUST_API_KEY", PROVIDER_SECRET)
            .env("OPENROUTER_API_KEY", PROVIDER_SECRET)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

async fn ready(root: &Path) -> OwnerConnection {
    support::wait_for_owner(root, Duration::from_secs(15)).await
}

async fn request(
    client: &GatewayClient,
    session: &Value,
    command: &str,
) -> Result<Value, ClientError> {
    client
        .request(
            "shell.exec",
            json!({"session_id":session,"command":command}),
        )
        .await
}

#[cfg(windows)]
#[tokio::test]
async fn deleted_live_workspace_is_reported_unavailable() {
    let home = tempfile::tempdir().unwrap();
    let cwd = home.path().join("deletable-workspace");
    fs::create_dir(&cwd).unwrap();
    let _process = launch(home.path());
    let owner = ready(home.path()).await;
    let client = GatewayClient::new(ClientOptions::default());
    owner.connect(&client).await.unwrap();
    let session = client.request("session.create", json!({"source":"tui","cwd":cwd}))
        .await.unwrap();
    fs::remove_dir(&cwd).unwrap();
    assert_eq!(request(&client, &session["session_id"], "cd").await.unwrap_err().reason(),
        Some("cwd_unavailable"));
    client.close();
    owner.stop().await.unwrap();
}

#[tokio::test]
async fn composer_capture_requires_private_local_provenance_and_a_frozen_local_workspace() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path();
    let cwd = root.join(WORKSPACE);
    fs::create_dir(&cwd).unwrap();
    let _process = launch(root);
    let owner = ready(root).await;

    let private = GatewayClient::new(ClientOptions::default());
    owner.connect(&private).await.unwrap();
    let http = GatewayClient::new(ClientOptions::default());
    http.connect_native(&owner.origin, &owner.local_token().await.unwrap())
        .await
        .unwrap();

    let project = hermes_library::Library::open(root)
        .unwrap()
        .add_project(&cwd)
        .unwrap();
    let local = private
        .request(
            "session.create",
            json!({"source":"tui","project_id":project.id,"cwd":root}),
        )
        .await
        .unwrap();
    let sid = &local["session_id"];
    let pwd = if cfg!(windows) { "cd" } else { "pwd" };
    let ran = request(&private, sid, pwd).await.unwrap();
    assert!(
        ran["stdout"]
            .as_str()
            .unwrap()
            .trim_end()
            .ends_with(WORKSPACE)
    );
    assert_eq!(ran["code"], 0);
    let status = if cfg!(windows) {
        "echo error 1>&2 & exit /b 3"
    } else {
        "echo error >&2; exit 3"
    };
    let ran = request(&private, sid, status).await.unwrap();
    assert_eq!(ran["stderr"].as_str().unwrap().trim(), "error");
    assert_eq!(ran["code"], 3);

    // Larger than the old per-stream cap, with a credential crossing the returned tail.
    fs::write(cwd.join("stdout.txt"), format!("{} https://fixture-user:fixture-pass@fixture.example?token=synthetic-query-credential {PROVIDER_SECRET} {}",
        "prefix".repeat(50_000), "x".repeat(3950))).unwrap();
    fs::write(cwd.join("stderr.txt"), "y".repeat(270_000)).unwrap();
    let read_files = if cfg!(windows) {
        "type stdout.txt & type stderr.txt 1>&2"
    } else {
        "cat stdout.txt; cat stderr.txt >&2"
    };
    let output = request(&private, sid, read_files).await.unwrap();
    let stdout = output["stdout"].as_str().unwrap();
    let stderr = output["stderr"].as_str().unwrap();
    assert_eq!(stdout.chars().count(), 4000);
    assert_eq!(stderr.chars().count(), 2000);
    assert!(stdout.contains("[REDACTED]"));
    for secret in [
        "synthetic-query-credential",
        "fixture-pass",
        PROVIDER_SECRET,
        "for-shell-boundary",
    ] {
        assert!(!stdout.contains(secret));
    }

    assert_eq!(
        request(&private, sid, "rm -rf example")
            .await
            .unwrap_err()
            .reason(),
        Some("dangerous_command")
    );
    let pipe = request(&private, sid, "echo first | echo second")
        .await
        .unwrap();
    assert_eq!(pipe["stdout"].as_str().unwrap().trim(), "second");
    let target = cwd.join("inside fixture.txt");
    let redirected = request(&private, sid, r#"echo saved>"inside fixture.txt""#)
        .await
        .unwrap();
    assert_eq!(redirected["code"], 0);
    assert_eq!(fs::read_to_string(target).unwrap().trim(), "saved");
    assert_eq!(
        private
            .request("shell.exec", json!({"command":"pwd"}))
            .await
            .unwrap_err()
            .reason(),
        Some("invalid_params")
    );

    http.request("session.resume", json!({"session_id":sid}))
        .await
        .unwrap();
    assert_eq!(
        request(&http, sid, "pwd").await.unwrap_err().reason(),
        Some("permission_denied")
    );
    let untrusted = http
        .request("session.create", json!({"source":"tui","cwd":cwd}))
        .await
        .unwrap();
    private
        .request(
            "session.resume",
            json!({"session_id":untrusted["session_id"]}),
        )
        .await
        .unwrap();
    assert_eq!(
        request(&private, &untrusted["session_id"], "pwd")
            .await
            .unwrap_err()
            .reason(),
        Some("permission_denied")
    );

    fs::write(root.join("config.yaml"), "terminal:\n  backend: docker\n").unwrap();
    let sandbox = private
        .request("session.create", json!({"source":"tui","cwd":cwd}))
        .await
        .unwrap();
    assert!(
        request(&private, sid, pwd).await.is_ok(),
        "later config cannot reinterpret an existing launch"
    );
    fs::write(root.join("config.yaml"), "terminal:\n  backend: local\n").unwrap();
    let denied = request(&private, &sandbox["session_id"], "pwd")
        .await
        .unwrap_err();
    match denied {
        ClientError::Rpc { data, .. } => assert_eq!(
            data,
            json!({
                "reason":"unsupported_terminal_backend","field":"terminal.backend","detail":"docker"
            })
        ),
        other => panic!("unexpected terminal backend refusal: {other}"),
    }

    let another = GatewayClient::new(ClientOptions::default());
    owner.connect(&another).await.unwrap();
    assert_eq!(request(&another, sid, pwd).await.unwrap()["code"], 0);
    for (params, reason, fields) in [
        (
            json!({"session_id":sid,"command":null}),
            "invalid_params",
            Some(json!(["command"])),
        ),
        (
            json!({"session_id":sid,"command":pwd,"unexpected":true}),
            "invalid_params",
            Some(json!(["unexpected"])),
        ),
        (
            json!({"session_id":"no-such-session","command":pwd}),
            "not_found",
            None,
        ),
        (
            json!({"session_id":sid,"profile":"other-profile","command":pwd}),
            "profile_mismatch",
            None,
        ),
    ] {
        match another.request("shell.exec", params).await.unwrap_err() {
            ClientError::Rpc {
                code,
                message,
                data,
            } => {
                assert_eq!(code, Some(4001));
                assert_eq!(message, reason);
                assert_eq!(data.get("fields"), fields.as_ref());
            }
            other => panic!("unexpected shell refusal: {other}"),
        }
    }
    another.close();

    let unchanged = private
        .request("session.resume", json!({"session_id":sid}))
        .await
        .unwrap();
    assert_eq!(unchanged["message_count"], 0);
    private.close();
    http.close();
    owner.stop().await.unwrap();
    let _restarted = launch(root);
    let owner = ready(root).await;
    let reopened = GatewayClient::new(ClientOptions::default());
    owner.connect(&reopened).await.unwrap();
    reopened
        .request("session.resume", json!({"session_id":sid}))
        .await
        .unwrap();
    assert!(request(&reopened, sid, pwd).await.is_ok());
    reopened.close();
    owner.stop().await.unwrap();
}
