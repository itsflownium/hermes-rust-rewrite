use hermes_gateway::{
    client::{ClientOptions, GatewayClient},
    owner,
};
use serde_json::json;
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::time::{Instant, sleep};

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
            .args(["gateway", "run", "--ephemeral-token", "--port", "0", "--data-dir"])
            .arg(root)
            .env("HERMES_RUST_BASE_URL","http://127.0.0.1:1/v1")
            .env("HERMES_RUST_MODEL","owner-test")
            .env_remove("HERMES_RUST_API_KEY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
async fn ready(home: &Path) -> owner::OwnerConnection {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(owner) = owner::discover(home).await.unwrap() {
            return owner;
        }
        assert!(
            Instant::now() < deadline,
            "gateway did not publish a profile endpoint"
        );
        sleep(Duration::from_millis(40)).await;
    }
}
#[tokio::test]
async fn bundled_gateway_registers_discord_for_the_profile_owner() {
    let dir = tempfile::tempdir().unwrap();
    let _gateway = launch(dir.path());
    let owner = ready(dir.path()).await;
    let client = GatewayClient::new(ClientOptions::default());
    owner.connect(&client).await.unwrap();

    let discovery = client.request("rpc.discover", json!({})).await.unwrap();
    let methods = discovery["methods"].as_array().unwrap();
    for method in ["list", "create", "update"] {
        assert!(methods.contains(&json!(format!("messaging.profile.discord.{method}"))));
    }
    let connection = client
        .request("messaging.profile.discord.create", json!({"name":"discord","settings":{}}))
        .await
        .unwrap();
    assert_eq!(connection["platform"], "discord");
    assert_eq!(connection["enabled"], false);
    let listed = client.request("messaging.profile.discord.list", json!({})).await.unwrap();
    assert_eq!(listed["connections"][0], connection);

    client.close();
    owner.stop().await.unwrap();
}

#[tokio::test]
async fn private_catalog_controls_coordinate_viewers_and_preserve_user_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let _gateway = launch(dir.path());
    let owner = ready(dir.path()).await;
    let client = GatewayClient::new(ClientOptions::default());
    owner.connect(&client).await.unwrap();
    let id = client.request("session.create",json!({})).await.unwrap()["session_id"].as_str().unwrap().to_owned();
    let workspace = dir.path().join("sessions").join(&id).join("workspace");
    std::fs::write(workspace.join("keep.txt"),"user data").unwrap();

    let web = GatewayClient::new(ClientOptions::default());
    web.connect_native(&owner.origin,&owner.local_token().await.unwrap()).await.unwrap();
    assert!(web.request("catalog.session.set",json!({"session_id":id,"archived":true})).await.is_err());
    let flags=client.request("catalog.session.set",json!({"session_id":id,"archived":true,"pinned":true})).await.unwrap();
    assert_eq!(flags["session"]["archived"],true);
    assert_eq!(flags["session"]["pinned"],true);
    assert!(client.request("catalog.session.delete",json!({"session_id":id})).await.is_err());
    client.request("catalog.session.set",json!({"session_id":id,"archived":false})).await.unwrap();
    client.request("session.close",json!({"session_id":id})).await.unwrap();
    assert!(client.request("catalog.session.delete",json!({"session_id":id})).await.is_err());
    client.request("catalog.session.set",json!({"session_id":id,"archived":true})).await.unwrap();
    for _ in 0..2 {
        assert_eq!(client.request("catalog.session.delete",json!({"session_id":id})).await.unwrap()["deleted"],true);
    }
    assert_eq!(std::fs::read(dir.path().join("retained-workspaces").join(&id).join("keep.txt")).unwrap(),b"user data");
    web.close(); client.close(); owner.stop().await.unwrap();
}

#[tokio::test]
async fn competing_processes_converge_and_secondary_profile_is_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut first = launch(root);
    let mut second = launch(root);
    let primary = ready(root).await;
    let client = GatewayClient::new(ClientOptions::default());
    primary.connect(&client).await.unwrap();
    let identity = client.request("runtime.describe", json!({})).await.unwrap();
    assert_eq!(identity["profile_id"], owner::profile_id(root).unwrap());
    client.close();
    let named = root.join("profiles").join("work");
    std::fs::create_dir_all(&named).unwrap();
    let mut third = launch(&named);
    let work = ready(&named).await;
    assert_ne!(primary.origin, work.origin);
    let client = GatewayClient::new(ClientOptions::default());
    work.connect(&client).await.unwrap();
    let response = client.request("runtime.describe", json!({})).await.unwrap();
    assert_eq!(response["profile_id"], owner::profile_id(&named).unwrap());
    assert_ne!(response["instance_id"], identity["instance_id"]);
    let created = client.request("session.create", json!({"request_id":"profile-bound-chat","source":"gui"})).await.unwrap();
    let sid = &created["session_id"];
    client.request("session.resume", json!({"session_id":sid})).await.unwrap();
    let receipt = client.request("prompt.submit",json!({"session_id":sid,"submission_id":"profile-bound-turn","text":"hello","queued":true})).await.unwrap();
    assert_eq!(receipt["ref"]["profile_id"],response["profile_id"]);
    let persisted = client.request("prompt.receipt",json!({"session_id":sid,"ref":receipt["ref"],"admission_id":receipt["admission_id"]})).await.unwrap();
    assert_eq!(persisted["ref"],receipt["ref"]);
    assert_eq!(receipt["authority_epoch"],response["authority_epoch"]);
    assert_eq!(persisted["authority_epoch"],response["authority_epoch"]);
    assert!(
        client
            .request(
                "session.list",
                json!({"ref":{"profile_id":identity["profile_id"]}})
            )
            .await
            .is_err()
    );
    assert!(
        client
            .request("session.list", json!({"profile_id":identity["profile_id"]}))
            .await
            .is_err()
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if first.0.try_wait().unwrap().is_some() || second.0.try_wait().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "duplicate owner still running");
        sleep(Duration::from_millis(50)).await;
    }
    assert!(first.0.try_wait().unwrap().is_none() ^ second.0.try_wait().unwrap().is_none());
    primary.stop().await.unwrap();
    assert!(owner::discover(root).await.unwrap().is_none());
    assert!(third.0.try_wait().unwrap().is_none());
    first.0.wait().unwrap();
    second.0.wait().unwrap();
    let _restarted = launch(root);
    let restarted = ready(root).await;
    let restored = GatewayClient::new(ClientOptions::default());
    restarted.connect(&restored).await.unwrap();
    let new_identity = restored.request("runtime.describe", json!({})).await.unwrap();
    assert_eq!(new_identity["profile_id"], identity["profile_id"]);
    assert_ne!(new_identity["instance_id"], identity["instance_id"]);
    assert!(new_identity["authority_epoch"].as_u64().unwrap() > identity["authority_epoch"].as_u64().unwrap());
    restored.close();
    restarted.stop().await.unwrap();

    let reconnect = GatewayClient::new(ClientOptions::default());
    ready(&named).await.connect(&reconnect).await.unwrap();
    assert_eq!(
        reconnect
            .request("runtime.describe", json!({}))
            .await
            .unwrap()["instance_id"],
        response["instance_id"]
    );
    client.close();
    reconnect.close();
    work.stop().await.unwrap();
}
