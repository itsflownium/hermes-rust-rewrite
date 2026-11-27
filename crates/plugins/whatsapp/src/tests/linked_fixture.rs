use super::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use hermes_gateway_core::messaging::{AdapterFuture, AudioService, GatewayError};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

const OWNER: &str = "15550000000@s.whatsapp.net";
const ALICE: &str = "15550001111@s.whatsapp.net";
const BOB: &str = "15550002222@s.whatsapp.net";
const LID: &str = "777888999000@lid";
const GROUP: &str = "15550001234-1467@g.us";

struct Fixture {
    root: tempfile::TempDir,
    sequence: AtomicU64,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("pending")).unwrap();
        std::fs::write(
            root.path().join("fixture.mjs"),
            include_str!("../../tests/bridge_fixture.mjs"),
        )
        .unwrap();
        Self {
            root,
            sequence: AtomicU64::new(1),
        }
    }

    fn path(&self) -> PathBuf {
        self.root.path().join("fixture.mjs")
    }

    fn push(&self, kind: &str, data: Value) {
        let id = self.sequence.fetch_add(1, Ordering::Relaxed);
        let target = self.root.path().join("pending");
        let temporary = target.join(format!("{id:06}.tmp"));
        let finish = target.join(format!("{id:06}.json"));
        std::fs::write(&temporary, json!({"type":kind,"data":data}).to_string()).unwrap();
        std::fs::rename(temporary, finish).unwrap();
    }

    fn handoff(&self, id: &str, items: Vec<Value>) {
        self.push("handoff", json!({"id":id,"items":items}));
    }

    fn trace(&self, kind: &str) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("trace.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|frame| frame["type"] == kind)
            .map(|frame| frame["data"].clone())
            .collect()
    }

    async fn add(&self, gateway: &RunningGateway, mode: &str, burst: u64) {
        let settings = json!({"account":OWNER,"mode":mode,"fixture_script":self.path(),"burst_ms":burst,
            "access":{"users":[ALICE],"operators":[ALICE],"direct_tools":true,"rooms":{(GROUP):{"mention_required":true}}}});
        configure(gateway, "whatsapp", "linked", settings).await;
        let lease = gateway
            .begin_adapter_enrollment("linked", Duration::from_secs(15))
            .await
            .unwrap();
        let challenge = serde_json::to_value(lease.prompt).unwrap();
        assert_eq!(challenge, json!({"kind":"qr","payload":"test-fixture-qr"}));
        assert!(self.trace("send").is_empty());
        self.push("pair", json!({}));
        gateway
            .complete_adapter_enrollment(
                "linked",
                &lease.id,
                EnrollmentCredential::new(lease.id.clone()).unwrap(),
            )
            .await
            .unwrap();
        state(gateway, |s| {
            s["verification"]["linked"]["bridge"] == "ready"
        })
        .await;
    }

    fn silence(&self) {
        std::fs::write(self.root.path().join("silence"), b"hold transport receipts").unwrap();
    }
    fn enable_ack(&self) {
        std::fs::remove_file(self.root.path().join("silence")).unwrap();
    }
}

fn message(id: &str, chat: &str, actor: &str, text: &str) -> Value {
    json!({"id":id,"chat":chat,"actor":actor,"text":text,"origin":"peer","live":true,"member":true})
}

fn owner(id: &str, text: &str) -> Value {
    let mut msg = message(id, OWNER, OWNER, text);
    msg["origin"] = json!("owner");
    msg
}

#[tokio::test]
async fn linked_private_pairing_self_chat_echo_suppression_and_restart() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    fixture.add(&gateway, "personal", 0).await;
    let original = owner("owner-input", "private linked message");
    fixture.handoff("first", vec![original.clone()]);
    wait(|| fixture.trace("send").len() == 1).await;
    let outbound = fixture.trace("send")[0]["id"].as_str().unwrap().to_owned();
    let mut ambiguous = owner("ambiguous-owner", "do not run on uncertainty");
    ambiguous["origin"] = json!("ambiguous");
    let mut historical = owner("older-owner", "history is context");
    historical["live"] = json!(false);
    fixture.handoff(
        "echo-and-replay",
        vec![
            owner(&outbound, "our own reply"),
            original.clone(),
            ambiguous,
            historical,
        ],
    );
    wait(|| fixture.trace("commit").len() == 2).await;
    assert_eq!(model.requests().len(), 1);
    gateway.shutdown().await.unwrap();

    let gateway = model.gateway(root.path(), vec![]).await;
    state(&gateway, |s| {
        s["verification"]["linked"]["bridge"] == "ready"
    })
    .await;
    fixture.handoff(
        "resume",
        vec![original, owner("new-owner-input", "resume private session")],
    );
    wait(|| fixture.trace("send").len() == 2).await;
    assert_eq!(
        fixture.trace("send")[1]["content"]["text"],
        "answer resume private session"
    );
    assert_eq!(
        fixture
            .trace("start")
            .iter()
            .filter(|frame| frame["resumed"] == false)
            .count(),
        1
    );
    assert_eq!(model.requests().len(), 2);
    let snapshot = gateway.messaging_status().await.unwrap();
    assert_eq!(
        snapshot["input"][0]["session"],
        snapshot["input"][1]["session"]
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn linked_bot_requires_group_addressing_and_scopes_verified_aliases_and_bursts() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    fixture.add(&gateway, "bot", 120).await;
    let mut alice = message("alice-original", ALICE, ALICE, "verified alias");
    alice["actor_alias"] = json!(LID);
    alice["chat_alias"] = json!(LID);
    fixture.handoff("direct", vec![alice]);
    wait(|| fixture.trace("send").len() == 1).await;
    let mut first = message("group-first", GROUP, LID, "first group message");
    first["actor_alias"] = json!(ALICE);
    first["mentions"] = json!([OWNER]);
    let mut second = message("group-second", GROUP, LID, "second group message");
    second["actor_alias"] = json!(ALICE);
    second["mentions"] = json!([OWNER]);
    fixture.handoff("group-burst", vec![first.clone(), second]);
    wait(|| fixture.trace("send").len() == 2).await;
    let direct = fixture.trace("send");
    assert_eq!(direct[1]["chat"], GROUP);
    assert!(
        direct[1]["content"]["text"]
            .as_str()
            .unwrap()
            .contains("second group message")
    );
    assert_eq!(model.requests().len(), 2);
    let mut bob = message("denied-group", GROUP, BOB, "wrong actor");
    bob["mentions"] = json!([OWNER]);
    let mut unverified = message("unverified-member", GROUP, ALICE, "missing membership");
    unverified["member"] = json!(false);
    unverified["mentions"] = json!([OWNER]);
    fixture.handoff(
        "group-forbidden",
        vec![
            bob,
            unverified,
            message("not-addressed", GROUP, ALICE, "nothing addressed"),
            first,
        ],
    );
    wait(|| fixture.trace("commit").len() == 3).await;
    assert_eq!(model.requests().len(), 2);
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn linked_media_burst_commits_all_constituents_and_recovers_one_turn() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    fixture.add(&gateway, "bot", 120).await;
    let mut text = message("burst-text", GROUP, ALICE, "include the linked document");
    text["mentions"] = json!([OWNER]);
    let mut file = message("burst-media", GROUP, ALICE, "");
    file["media"] = json!([{"id":"burst-media","kind":"document","voice":false,"name":"fixture.txt",
        "locator":{"size":12,"key":{"id":"burst-media"}}}]);
    text.as_object_mut().unwrap().remove("member");
    file.as_object_mut().unwrap().remove("member");
    fixture.handoff("mixed-burst", vec![text.clone(), file.clone()]);
    wait(|| fixture.trace("media").len() == 1 && fixture.trace("send").len() == 1).await;
    let snapshot = gateway.messaging_status().await.unwrap();
    assert_eq!(snapshot["input"].as_array().unwrap().len(), 2);
    assert_eq!(model.requests().len(), 1);
    assert!(
        model.requests()[0]
            .to_string()
            .contains("include the linked document")
    );
    gateway.shutdown().await.unwrap();
    let gateway = model.gateway(root.path(), vec![]).await;
    state(&gateway, |s| {
        s["verification"]["linked"]["bridge"] == "ready"
    })
    .await;
    fixture.handoff("replay-of-mixed-burst", vec![text, file]);
    wait(|| fixture.trace("commit").len() == 2).await;
    assert_eq!(model.requests().len(), 1);
    assert_eq!(fixture.trace("media").len(), 1);
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
            Ok(if wav[44] == 1 {
                "Mercury please answer this".into()
            } else {
                "Ordinary group audio".into()
            })
        })
    }
}

fn recording(value: u8) -> Vec<u8> {
    let samples = [value, 0, 0, 0, 0, 0, 0, 0];
    let mut output = b"RIFF".to_vec();
    output.extend_from_slice(&(36_u32 + samples.len() as u32).to_le_bytes());
    output.extend_from_slice(b"WAVEfmt ");
    output.extend_from_slice(&16_u32.to_le_bytes());
    output.extend_from_slice(&1_u16.to_le_bytes());
    output.extend_from_slice(&1_u16.to_le_bytes());
    output.extend_from_slice(&16000_u32.to_le_bytes());
    output.extend_from_slice(&32000_u32.to_le_bytes());
    output.extend_from_slice(&2_u16.to_le_bytes());
    output.extend_from_slice(&16_u16.to_le_bytes());
    output.extend_from_slice(b"data");
    output.extend_from_slice(&(samples.len() as u32).to_le_bytes());
    output.extend_from_slice(&samples);
    output
}

#[tokio::test]
async fn linked_group_voice_must_transcribe_to_a_configured_mention_before_claim() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model
        .gateway_with_audio(root.path(), vec![], Some(std::sync::Arc::new(Speech)))
        .await;
    let settings = json!({"account":OWNER,"mode":"bot","fixture_script":fixture.path(),
        "access":{"users":[ALICE],"rooms":{(GROUP):{"mention_required":true}}},
        "mention_patterns":{(GROUP):["Mercury"]}});
    configure(&gateway, "whatsapp", "linked", settings).await;
    let lease = gateway
        .begin_adapter_enrollment("linked", Duration::from_secs(15))
        .await
        .unwrap();
    fixture.push("pair", json!({}));
    gateway
        .complete_adapter_enrollment(
            "linked",
            &lease.id,
            EnrollmentCredential::new(lease.id.clone()).unwrap(),
        )
        .await
        .unwrap();
    state(&gateway, |s| {
        s["verification"]["linked"]["bridge"] == "ready"
    })
    .await;
    let input = |id: &str, value: u8| {
        let wav = recording(value);
        let mut source = message(id, GROUP, ALICE, "");
        source["media"] = json!([{"id":id,"kind":"audio","voice":true,"name":"voice.wav",
            "locator":{"size":wav.len(),"key":{"id":id},"fixture_bytes":STANDARD.encode(wav)}}]);
        source
    };
    fixture.handoff(
        "group-audio",
        vec![input("unmentioned-voice", 0), input("addressed-voice", 1)],
    );
    wait(|| fixture.trace("media").len() == 2 && fixture.trace("send").len() == 1).await;
    assert_eq!(model.requests().len(), 1);
    assert!(
        model.requests()[0]
            .to_string()
            .contains("Mercury please answer this")
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn linked_pipe_write_stays_uncertain_until_exact_platform_evidence_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    fixture.add(&gateway, "bot", 0).await;
    fixture.silence();
    fixture.handoff(
        "lost-ack",
        vec![message(
            "uncertain",
            ALICE,
            ALICE,
            "wait for provider receipt",
        )],
    );
    let uncertain = |s: &Value| {
        s["deliveries"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["part_state"] == "uncertain"))
    };
    state(&gateway, uncertain).await;
    let effect = fixture.trace("send")[0].clone();
    gateway.shutdown().await.unwrap();
    let gateway = model.gateway(root.path(), vec![]).await;
    state(&gateway, |s| {
        s["verification"]["linked"]["bridge"] == "ready"
    })
    .await;
    state(&gateway, uncertain).await;
    fixture.enable_ack();
    fixture.handoff(
        "next",
        vec![message("next-turn", ALICE, ALICE, "send after old settles")],
    );
    fixture.push(
        "receipt",
        json!({"id":effect["id"],"chat":BOB,"status":"accepted","timestamp":now()/1000}),
    );
    wait(|| fixture.trace("commit").len() == 2).await;
    assert_eq!(fixture.trace("send").len(), 1);
    fixture.push(
        "receipt",
        json!({"id":effect["id"],"chat":ALICE,"status":"accepted","timestamp":now()/1000}),
    );
    wait(|| fixture.trace("send").len() == 2).await;
    assert_eq!(
        fixture.trace("send")[1]["content"]["text"],
        "answer send after old settles"
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn linked_quoted_media_and_native_poll_answers_use_the_original_actor_and_prompt() {
    let root = tempfile::tempdir().unwrap();
    let fixture = Fixture::new();
    let model = model().await;
    let gateway = model.gateway(root.path(), vec![]).await;
    fixture.add(&gateway, "bot", 0).await;
    let mut image = message("attachment", ALICE, ALICE, "original caption");
    image["media"] = json!([{"id":"attachment","kind":"document","voice":false,"name":"fixture.txt",
        "locator":{"size":12,"key":{"id":"attachment"}}}]);
    fixture.handoff("file", vec![image]);
    wait(|| fixture.trace("send").len() == 1).await;
    let mut quoted = message("reply", ALICE, ALICE, "quote this thread");
    quoted["quote"] = json!("attachment");
    fixture.handoff("quoted", vec![quoted]);
    wait(|| fixture.trace("media").len() == 2 && fixture.trace("send").len() == 2).await;
    assert!(model.requests()[1].to_string().contains("original caption"));
    fixture.handoff("ask", vec![message("poll-intake", ALICE, ALICE, "clarify")]);
    wait(|| fixture.trace("send").len() == 3).await;
    let poll = fixture.trace("send")[2].clone();
    let choice = &poll["content"]["poll"]["choices"][0]["id"];
    assert!(choice.is_string());
    let mut answer = message("poll-answer", ALICE, ALICE, "");
    answer["quote"] = poll["id"].clone();
    answer["choice"] = choice.clone();
    fixture.handoff("vote", vec![answer.clone(), answer]);
    wait(|| fixture.trace("send").len() == 4).await;
    assert_eq!(model.requests().len(), 4);
    gateway.shutdown().await.unwrap();
}
