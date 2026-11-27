use super::*;
use crate::{
    GatewayConfig,
    messaging::{Room, RoomKind, Sender, test_support::dispatch},
};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{Method, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};
use hermes_engine::Config;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tempfile::tempdir;
use tokio::net::TcpListener;

fn group() -> Room {
    Room {
        kind: RoomKind::Group,
        chat: "21".into(),
        thread: None,
    }
}

fn member(id: &str, roles: &[&str]) -> Sender {
    Sender {
        id: id.into(),
        roles: roles.iter().map(|role| (*role).to_owned()).collect(),
    }
}

#[test]
fn discord_grants_keep_the_verified_guild_and_exact_thread() {
    let settings: DiscordSettings = serde_json::from_value(json!({
        "grants":{"users":["70"]},
        "dm_role_guild":"11",
        "direct":{"rules":{"grants":{"roles":["31"]}},"pairing":true},
        "guilds":{
            "11":{
                "rules":{"denied_users":["71"],"grants":{"roles":["31"]},"turn":{"model":"guild-model"}},
                "channels":{"21":{"rules":{"mention_required":false},"threads":{
                    "22":{"grants":{"open":true},"mention_required":true,"turn":{"prompt":"thread instruction"}},
                    "23":{"enabled":false}
                }}}
            },
            "12": {"channels":{"24":{}}}
        },
        "ignored_channels":["25"],
        "voice_enabled":true,
    })).unwrap();
    settings.validate().unwrap();
    let policy = settings.host_policy();
    assert!(policy.scope.require_actor_evidence);
    let participant = member("72", &[&scoped_role("11", "31")]);
    assert!(policy.grants_sender(&group(), &participant, false));
    let outsider = member("72", &[&scoped_role("12", "31")]);
    assert!(!policy.grants_sender(&group(), &outsider, false));
    let exact = Room {
        thread: Some("22".into()),
        ..group()
    };
    assert!(policy.grants_sender(&exact, &outsider, false));
    assert!(!policy.grants_sender(&exact, &member("71", &[]), false));
    assert_eq!(
        policy.turn_policy(&exact).model.as_deref(),
        Some("guild-model")
    );
    assert_eq!(
        policy.turn_policy(&exact).prompt.as_deref(),
        Some("thread instruction")
    );
    assert!(!settings.mention_required(Some("11"), "21", None));
    assert!(settings.mention_required(Some("11"), "21", Some("22")));
    assert!(settings.has_thread_mention_override("11", "21", "22"));
    assert!(!settings.allows_surface(Some("12"), "21", Some("22")));
    for id in ["23", "25"] {
        assert!(!settings.allows_surface(Some("11"), "21", Some(id)));
        assert!(!policy.grants_sender(
            &Room {
                thread: Some(id.into()),
                ..group()
            },
            &participant,
            false
        ));
    }
    let direct = Room {
        kind: RoomKind::Direct,
        chat: "77".into(),
        thread: None,
    };
    assert!(!settings.dm_open());
    assert!(policy.grants_sender(&direct, &participant, false));
    assert!(!policy.grants_sender(&direct, &outsider, false));
    assert!(policy.grants_sender(&direct, &outsider, true));
    assert_eq!(
        DiscordSettings::default().gateway_intents(),
        DIRECT_MESSAGES
    );
    assert_eq!(DiscordSettings::default().invite_permissions(), 0);
    assert_eq!(
        settings.gateway_intents(),
        DIRECT_MESSAGES
            | GUILDS
            | GUILD_MESSAGES
            | MESSAGE_CONTENT
            | GUILD_MEMBERS
            | GUILD_VOICE_STATES
    );
}

#[derive(Clone)]
struct DiscordFixture {
    origin: String,
    app_flags: Arc<AtomicU64>,
    wrong_thread: Arc<AtomicBool>,
    deny_send: Arc<AtomicBool>,
    deny_uploads: Arc<AtomicBool>,
    deny_voice: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<String>>>,
}

async fn discord_fixture(State(fixture): State<DiscordFixture>, request: Request) -> Response {
    assert_eq!(request.method(), Method::GET);
    if request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        != Some("Bot fixture-bot-secret")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let path = request.uri().path().strip_prefix("/api/v10/").unwrap();
    lock(&fixture.requests).push(path.to_owned());
    let document = match path {
        "users/@me" => json!({"id":"9","bot":true}),
        "applications/@me" => {
            json!({"id":"90","bot":{"id":"9"},"flags":fixture.app_flags.load(Ordering::SeqCst)})
        }
        "gateway/bot" => json!({
            "url":fixture.origin,"shards":1,
            "session_start_limit":{"total":1000,"remaining":0,"reset_after":1000,"max_concurrency":1}
        }),
        "channels/21" => {
            let mut deny = 0;
            if fixture.deny_send.load(Ordering::SeqCst) {
                deny |= SEND_IN_THREADS;
            }
            if fixture.deny_uploads.load(Ordering::SeqCst) {
                deny |= ATTACH_FILES;
            }
            if fixture.deny_voice.load(Ordering::SeqCst) {
                deny |= SEND_VOICE_MESSAGES;
            }
            json!({"id":"21","guild_id":"11","type":0,"permission_overwrites":[
                {"id":"11","type":0,"deny":ATTACH_FILES.to_string(),"allow":"0"},
                {"id":"31","type":0,"deny":"0","allow":ATTACH_FILES.to_string()},
                {"id":"9","type":1,"deny":deny.to_string(),"allow":"0"},
            ]})
        }
        "channels/22" => {
            let parent = if fixture.wrong_thread.load(Ordering::SeqCst) {
                "24"
            } else {
                "21"
            };
            json!({"id":"22","guild_id":"11","type":11,"parent_id":parent})
        }
        "guilds/11" => json!({"id":"11","owner_id":"80"}),
        "guilds/11/members/9" => json!({"user":{"id":"9","bot":true},"roles":["31"]}),
        "guilds/11/roles" => json!([
            {"id":"11","permissions":(SEE_CHANNEL | READ_MESSAGE_HISTORY | SEND_IN_THREADS | SEND_VOICE_MESSAGES).to_string()},
            {"id":"31","permissions":"0"}
        ]),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    Json(document).into_response()
}

#[tokio::test]
async fn discord_enrollment_reads_bot_application_and_home_permissions() {
    let dir = tempdir().unwrap();
    let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = tcp.local_addr().unwrap();
    let fixture = DiscordFixture {
        origin: format!("ws://{address}/"),
        app_flags: Arc::new(AtomicU64::new(0)),
        wrong_thread: Arc::default(),
        deny_send: Arc::default(),
        deny_uploads: Arc::default(),
        deny_voice: Arc::default(),
        requests: Arc::default(),
    };
    let router = Router::new()
        .fallback(discord_fixture)
        .with_state(fixture.clone());
    let server = tokio::spawn(async move {
        axum::serve(tcp, router).await.unwrap();
    });
    let settings = serde_json::from_value(json!({
        "guilds":{"11":{"rules":{"grants":{"roles":["31"]}},"channels":{"21":{}}}},
        "default_delivery_target":{"guild":"11","channel":"21","thread":"22"}
    }))
    .unwrap();
    let factory = DiscordFactory::for_fixture(
        dir.path().to_owned(),
        settings,
        Url::parse(&format!("http://{address}/api/v10/")).unwrap(),
    )
    .unwrap();
    let read = || {
        factory.connect_enrolled(
            EnrollmentCredential::new("fixture-bot-secret".into()).unwrap(),
            CancellationToken::new(),
        )
    };
    assert_eq!(read().await.err(), Some(ConnectionFailure::Configuration));
    assert_eq!(
        factory.verification_status().unwrap()["missing_intents"],
        json!(["guild_members", "message_content"])
    );
    assert_eq!(
        &*lock(&fixture.requests),
        &["users/@me", "applications/@me"]
    );
    fixture
        .app_flags
        .store(MEMBERS_FLAGS | CONTENT_FLAGS, Ordering::SeqCst);
    let verified = read().await.ok().unwrap();
    assert_eq!(verified.account, "9");
    assert_eq!(
        verified
            .adapter
            .verified_installation()
            .unwrap()
            .application
            .as_deref(),
        Some("90")
    );
    let status = factory.verification_status().unwrap();
    assert_eq!(status["rest_verified"], true);
    assert_eq!(status["home_verified"], true);
    assert_eq!(status["session_starts_remaining"], 0);
    assert_eq!(status["session_start_reset_ms"], 1000);
    assert!(status["invite"].as_str().unwrap().contains("client_id=90"));
    assert!(!status.to_string().contains("fixture-bot-secret"));
    assert!(
        lock(&fixture.requests)
            .iter()
            .any(|route| route == "gateway/bot")
    );

    fixture.deny_uploads.store(true, Ordering::SeqCst);
    assert!(read().await.is_ok());
    assert_eq!(
        factory.verification_status().unwrap()["missing_permissions"],
        json!(["attach_files"])
    );
    fixture.deny_uploads.store(false, Ordering::SeqCst);

    fixture.deny_voice.store(true, Ordering::SeqCst);
    assert!(read().await.is_ok());
    assert_eq!(
        factory.verification_status().unwrap()["missing_permissions"],
        json!(["send_voice_messages"])
    );
    fixture.deny_voice.store(false, Ordering::SeqCst);

    fixture.wrong_thread.store(true, Ordering::SeqCst);
    assert_eq!(read().await.err(), Some(ConnectionFailure::Configuration));
    assert_eq!(
        factory.verification_status().unwrap()["issue"],
        "default_thread_binding_mismatch"
    );
    fixture.wrong_thread.store(false, Ordering::SeqCst);
    fixture.deny_send.store(true, Ordering::SeqCst);
    assert_eq!(read().await.err(), Some(ConnectionFailure::Configuration));
    assert_eq!(
        factory.verification_status().unwrap()["missing_permissions"],
        json!(["send_messages_in_threads"])
    );
    let rejected = factory
        .connect_enrolled(
            EnrollmentCredential::new("fixture-wrong-secret".into()).unwrap(),
            CancellationToken::new(),
        )
        .await;
    assert_eq!(rejected.err(), Some(ConnectionFailure::Credentials));
    server.abort();
}

#[tokio::test]
async fn owner_can_create_update_and_restore_a_disabled_discord_factory() {
    let dir = tempdir().unwrap();
    let open = || {
        crate::start_test_gateway(GatewayConfig::new(
            dir.path(),
            Config {
                base_url: "http://127.0.0.1:1/v1".into(),
                model: "fixture".into(),
                api_key: String::new(),
            },
        ))
    };
    let gateway = open().await.unwrap();
    let request = json!({"name":"discord-fixture","settings":{"grants":{"users":["72"]}}});
    let denied = dispatch(&gateway, "messaging.profile.discord.create", &request, false)
        .await
        .unwrap_err();
    assert_eq!(denied, 4403);
    let created = dispatch(&gateway, "messaging.profile.discord.create", &request, true)
        .await
        .unwrap();
    assert_eq!(created["platform"], "discord");
    assert_eq!(created["enabled"], false);
    assert!(created["credential_ref"].is_null());
    assert!(
        dispatch(&gateway, "messaging.profile.discord.create", &request, true)
            .await
            .is_err()
    );
    assert!(
        dispatch(&gateway,
                "messaging.profile.discord.create",
                &json!({"name":"invalid","settings":{"token":"this-is-forbidden"}}),
                true
            )
            .await
            .is_err()
    );
    let altered = json!({"name":"discord-fixture","revision":0,
        "settings":{"grants":{"users":["73"]},"dispatch_silence_secs":60}});
    let updated = dispatch(&gateway, "messaging.profile.discord.update", &altered, true)
        .await
        .unwrap();
    assert_eq!(updated["revision"], 1);
    assert_eq!(
        updated["policy"]["scope"]["direct"]["grant"]["users"],
        json!(["73"])
    );
    assert!(
        dispatch(&gateway, "messaging.profile.discord.update", &altered, true)
            .await
            .is_err()
    );
    assert!(
        gateway
            .set_adapter_policy("discord-fixture", AccessPolicy::default())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();

    let resumed = open().await.unwrap();
    let profiles = dispatch(&resumed, "messaging.profile.discord.list", &json!({}), true)
        .await
        .unwrap();
    assert_eq!(
        profiles["connections"][0]["settings"]["dispatch_silence_secs"],
        60
    );
    assert_eq!(profiles["connections"][0]["enabled"], false);
    let enrollment = dispatch(&resumed,
            "messaging.profile.enrollment_begin",
            &json!({"name":"discord-fixture"}),
            true,
        )
        .await
        .unwrap();
    assert!(enrollment["id"].is_string());
    assert_eq!(
        resumed.messaging_status().await.unwrap()["verification"]["discord-fixture"]["rest_verified"],
        false
    );
    resumed.shutdown().await.unwrap();
}

#[test]
fn deployment_credentials_resolve_within_the_selected_profile() {
    let dir = tempdir().unwrap();
    std::fs::create_dir(dir.path().join("messaging-secrets")).unwrap();
    std::fs::write(
        dir.path().join("messaging-secrets").join("fixture.secret"),
        "fixture-secret\r\n",
    )
    .unwrap();
    assert_eq!(
        deployment_credential(dir.path(), "fixture")
            .unwrap()
            .secret(),
        "fixture-secret"
    );
    assert!(deployment_credential(dir.path(), "../fixture").is_err());
    assert!(deployment_credential(dir.path(), "another-owner").is_err());
}
