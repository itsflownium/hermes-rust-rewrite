use super::*;
use crate::messaging::{AdapterFuture, AudioService, GatewayError};
use std::sync::atomic::AtomicBool;

const REFUSED: u64 = 1;
const LOST: u64 = 2;
const DISCORD_EPOCH: u64 = 1_420_070_400_000;

pub(super) async fn record_voice(state: &Server, channel: &str, request: Request) -> Response {
    let mut form = Multipart::from_request(request, &()).await.unwrap();
    let mut payload = None;
    let mut audio = None;
    while let Some(field) = form.next_field().await.unwrap() {
        match field.name() {
            Some("payload_json") => {
                let bytes = field.bytes().await.unwrap();
                payload = Some(serde_json::from_slice::<Value>(&bytes).unwrap());
            }
            Some("files[0]") => {
                assert_eq!(field.file_name(), Some("voice-message.ogg"));
                assert_eq!(field.content_type(), Some("audio/ogg"));
                audio = Some(field.bytes().await.unwrap());
            }
            name => panic!("unexpected upload field {name:?}"),
        }
    }
    let payload = payload.unwrap();
    let audio = audio.unwrap();
    let id = (((now() as u64 - DISCORD_EPOCH) << 22)
        | state.counter.fetch_add(1, Ordering::SeqCst))
    .to_string();
    let message = json!({
        "id":id,"channel_id":channel,"author":{"id":"9","bot":true},
        "nonce":payload["nonce"],"content":"","flags":payload["flags"],
        "attachments":[{
            "id":"42","filename":payload["attachments"][0]["filename"],
            "content_type":"audio/ogg","size":audio.len(),
            "waveform":payload["attachments"][0]["waveform"],"duration_secs":payload["attachments"][0]["duration_secs"]
        }],
    });
    let outcome = state.voice_outcome.load(Ordering::SeqCst);
    if outcome != REFUSED {
        crate::lock(&state.messages).insert((channel.to_owned(), id), message.clone());
    }
    let _ = state
        .writes
        .send(Write::VoiceNote {
            channel: channel.to_owned(),
            payload,
            audio,
            message: message.clone(),
        })
        .await;
    match outcome {
        REFUSED => (StatusCode::FORBIDDEN, Json(json!({"code":50173}))).into_response(),
        LOST => StatusCode::BAD_GATEWAY.into_response(),
        _ => Json(message).into_response(),
    }
}

async fn voice_mode_command(
    wire: &Wire,
    fixture: &mut Fixtures,
    sequence: u64,
    action: &str,
    expected: &str,
) {
    let id = (((now() as u64 - DISCORD_EPOCH) << 22) | sequence).to_string();
    wire.dispatch(sequence, "INTERACTION_CREATE", json!({
        "id": id, "application_id":"90","channel_id":"20","type":2,"token":format!("voice-mode-{id}"),
        "user":{"id":"7"},"data":{"name":"voice","options":[{"name":"action","value":action}]}
    })).await;
    let Write::Callback(value) = fixture
        .next_write("a private voice acknowledgement", |write| {
            matches!(write, Write::Callback(_))
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!(value, json!({"type":5,"data":{"flags":64}}));
    let Write::Followup(value) = fixture
        .next_write("a private voice mode", |write| {
            matches!(write, Write::Followup(_))
        })
        .await
    else {
        unreachable!()
    };
    assert_eq!(value["content"], expected);
}

#[tokio::test]
async fn voice_reply_mode_is_private_and_survives_a_discord_owner_restart() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    fixture
        .transport
        .suppress_progress
        .store(true, Ordering::SeqCst);
    let speech = Arc::new(Speech::default());
    let configure = |fixture: &Fixtures| {
        fixture.configuration_with(
            root.path(),
            json!({
                "grants":{"users":["7"]},"operators":["7"],
                "media":{"spoken_replies":"always"},"commands":{"sync":false},
            }),
        )
    };
    let gateway = model
        .gateway_with_audio(root.path(), vec![configure(&fixture)], Some(speech.clone()))
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "voice-mode").await;
    voice_mode_command(&wire, &mut fixture, 2, "off", "Voice replies are off.").await;
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway_with_audio(root.path(), vec![configure(&fixture)], Some(speech.clone()))
        .await;
    let mut wire = fixture.wire().await;
    assert_eq!(wire.authentication(6).await["d"]["seq"], 2);
    wire.dispatch(3, "RESUMED", json!({})).await;
    voice_mode_command(&wire, &mut fixture, 4, "status", "Voice replies are off.").await;
    wire.dispatch(
        5,
        "MESSAGE_CREATE",
        message("229", "20", "remain text", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        unreachable!()
    };
    assert_eq!(payload["content"], "answer remain text");
    wait_reply(&gateway, "discord-message-229").await;
    assert!(crate::lock(&speech.spoken).is_empty());

    voice_mode_command(
        &wire,
        &mut fixture,
        6,
        "tts",
        "Mercury will speak channel replies.",
    )
    .await;
    wire.dispatch(
        7,
        "MESSAGE_CREATE",
        message("230", "20", "speak again", false),
    )
    .await;
    voice_write(&mut fixture).await;
    wait_reply(&gateway, "discord-message-230").await;
    assert_eq!(
        crate::lock(&speech.spoken).as_slice(),
        ["answer speak again"]
    );
    gateway.shutdown().await.unwrap();
}

#[derive(Default)]
struct Speech {
    heard: AtomicU64,
    unavailable: AtomicBool,
    spoken: Mutex<Vec<String>>,
}

fn waveform() -> Vec<u8> {
    super::super::media::pcm_wave(
        &(0..16_800)
            .map(|index| if index % 32 < 16 { 12_000 } else { -12_000 })
            .collect::<Vec<i16>>(),
    )
}

impl AudioService for Speech {
    fn transcribe<'a>(
        &'a self,
        _: &'a Path,
        wav: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            assert_eq!(&wav[..4], b"RIFF");
            self.heard.fetch_add(1, Ordering::SeqCst);
            Ok("recorded voice".into())
        })
    }

    fn synthesize<'a>(
        &'a self,
        _: &'a Path,
        text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async move {
            crate::lock(&self.spoken).push(text.to_owned());
            if self.unavailable.load(Ordering::SeqCst) {
                Err(GatewayError::Unsupported)
            } else {
                Ok(waveform())
            }
        })
    }
}

fn configuration(fixture: &Fixtures, root: &Path, mode: &str) -> AdapterConfig {
    // Exercise the voice-first plan without an editable progress message.
    fixture
        .transport
        .suppress_progress
        .store(true, Ordering::SeqCst);
    fixture.configuration_with(
        root,
        json!({
            "grants":{"users":["7"]},
            "media":{"spoken_replies":mode},
            "commands":{"enabled":false,"sync":false},
        }),
    )
}

async fn voice_write(fixture: &mut Fixtures) -> Value {
    let Write::VoiceNote {
        channel,
        payload,
        audio,
        message,
    } = fixture.write().await
    else {
        panic!("expected one voice-note upload");
    };
    assert_eq!(channel, "20");
    assert_eq!(payload["flags"].as_u64().unwrap() & (1 << 13), 1 << 13);
    assert!(payload.get("content").is_none());
    assert_eq!(payload["enforce_nonce"], true);
    assert_eq!(payload["nonce"].as_str().unwrap().len(), 25);
    assert_eq!(payload["attachments"].as_array().unwrap().len(), 1);
    let attachment = &payload["attachments"][0];
    assert_eq!(attachment["id"], 0);
    let waveform = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        attachment["waveform"].as_str().unwrap(),
    )
    .unwrap();
    assert!(waveform.len() <= 256 && waveform.iter().any(|sample| *sample != 0));
    assert_eq!(attachment["duration_secs"].as_f64(), Some(1.05));

    let mut ogg = ogg::PacketReader::new(std::io::Cursor::new(audio));
    let head = ogg.read_packet().unwrap().unwrap();
    assert_eq!(&head.data[..10], b"OpusHead\x01\x01");
    assert_eq!(
        u32::from_le_bytes(head.data[12..16].try_into().unwrap()),
        48_000
    );
    let skip = u64::from(u16::from_le_bytes(head.data[10..12].try_into().unwrap()));
    assert!(
        ogg.read_packet()
            .unwrap()
            .unwrap()
            .data
            .starts_with(b"OpusTags")
    );
    let first = ogg.read_packet().unwrap().unwrap();
    let mut decoder = opus2::Decoder::new(48_000, opus2::Channels::Mono).unwrap();
    assert_eq!(
        decoder
            .decode(&first.data, &mut [0i16; 960], false)
            .unwrap(),
        960
    );
    let last = std::iter::from_fn(|| ogg.read_packet().unwrap())
        .last()
        .unwrap();
    assert!(last.last_in_stream());
    assert_eq!(last.absgp_page(), 50_400 + skip);
    message
}

#[tokio::test]
async fn spoken_replies_follow_verified_discord_voice_notes_in_default_mode() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let speech = Arc::new(Speech::default());
    let gateway = model
        .gateway_with_audio(
            root.path(),
            vec![configuration(&fixture, root.path(), "voice_notes")],
            Some(speech.clone()),
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "voice-trigger").await;
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("201", "20", "text only", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.reply().await else {
        panic!("ordinary text should stay text")
    };
    assert_eq!(payload["content"], "answer text only");

    let mut recording = message("202", "20", "", false);
    let path = "/attachments/20/45/voice-message.ogg";
    let inbound = super::super::voice_note::encode(&waveform()).unwrap().bytes;
    recording["flags"] = json!(1 << 13);
    recording["attachments"] = json!([{
        "id":"45","filename":"voice-message.ogg","content_type":"audio/ogg",
        "size":inbound.len(),"url":fixture.api.join(path).unwrap().to_string(),
    }]);
    crate::lock(&fixture.transport.cdn).insert(path.into(), Bytes::from(inbound));
    crate::lock(&fixture.transport.messages).insert(("20".into(), "202".into()), recording.clone());
    wire.dispatch(3, "MESSAGE_CREATE", recording).await;
    voice_write(&mut fixture).await;
    wait_reply(&gateway, "discord-message-202").await;
    assert_eq!(speech.heard.load(Ordering::SeqCst), 1);
    assert_eq!(
        crate::lock(&speech.spoken).as_slice(),
        ["answer recorded voice"]
    );
    assert!(
        timeout(Duration::from_millis(250), fixture.writes.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_voice_refusal_and_unavailable_synthesis_release_ordered_text_fallback() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let speech = Arc::new(Speech::default());
    let gateway = model
        .gateway_with_audio(
            root.path(),
            vec![configuration(&fixture, root.path(), "always")],
            Some(speech.clone()),
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "voice-fallback").await;
    fixture
        .transport
        .voice_outcome
        .store(REFUSED, Ordering::SeqCst);
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("211", "20", "refused audio", false),
    )
    .await;
    let spoken = voice_write(&mut fixture).await;
    let Write::Message {
        channel, payload, ..
    } = fixture.write().await
    else {
        panic!("expected fallback text")
    };
    assert_eq!(channel, "20");
    assert_eq!(payload["content"], "answer refused audio");
    assert_eq!(payload["message_reference"]["message_id"], "211");
    assert_ne!(spoken["nonce"], payload["nonce"]);
    wait_reply(&gateway, "discord-message-211").await;

    speech.unavailable.store(true, Ordering::SeqCst);
    wire.dispatch(
        3,
        "MESSAGE_CREATE",
        message("212", "20", "no synthesizer", false),
    )
    .await;
    let Write::Message { payload, .. } = fixture.write().await else {
        panic!("a synthesis failure must not send an audio request");
    };
    assert_eq!(payload["content"], "answer no synthesizer");
    wait_reply(&gateway, "discord-message-212").await;
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_voice_response_waits_through_restart_for_exact_audio_create() {
    let root = tempfile::tempdir().unwrap();
    let model = Model::new().await.with_stock_adapter(crate::STOCK_ADAPTER);
    let mut fixture = Fixtures::start().await;
    let speech = Arc::new(Speech::default());
    let gateway = model
        .gateway_with_audio(
            root.path(),
            vec![configuration(&fixture, root.path(), "always")],
            Some(speech.clone()),
        )
        .await;
    fixture.activate(&gateway).await;
    let mut wire = fixture.wire().await;
    wire.authentication(2).await;
    fixture.ready(&wire, "voice-uncertain").await;
    fixture
        .transport
        .voice_outcome
        .store(LOST, Ordering::SeqCst);
    wire.dispatch(
        2,
        "MESSAGE_CREATE",
        message("221", "20", "possibly sent", false),
    )
    .await;
    let echo = voice_write(&mut fixture).await;
    wait_status(&gateway, |status| {
        status["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|part| part["part_state"] == "uncertain")
    })
    .await;
    let mut unrelated = echo.clone();
    unrelated["flags"] = json!(0);
    wire.dispatch(3, "MESSAGE_CREATE", unrelated).await;
    assert!(
        timeout(Duration::from_millis(300), fixture.writes.recv())
            .await
            .is_err()
    );
    gateway.shutdown().await.unwrap();

    let gateway = model
        .gateway_with_audio(
            root.path(),
            vec![configuration(&fixture, root.path(), "always")],
            Some(speech.clone()),
        )
        .await;
    let mut wire = fixture.wire().await;
    wire.authentication(6).await;
    wire.dispatch(4, "RESUMED", json!({})).await;
    fixture.transport.voice_outcome.store(0, Ordering::SeqCst);
    wire.dispatch(5, "MESSAGE_CREATE", message("222", "20", "later", false))
        .await;
    assert!(
        timeout(Duration::from_millis(300), fixture.writes.recv())
            .await
            .is_err()
    );

    wire.dispatch(6, "MESSAGE_CREATE", echo.clone()).await;
    let next = voice_write(&mut fixture).await;
    assert_ne!(echo["nonce"], next["nonce"]);
    wait_reply(&gateway, "discord-message-221").await;
    wait_reply(&gateway, "discord-message-222").await;
    assert_eq!(
        crate::lock(&speech.spoken).as_slice(),
        ["answer possibly sent", "answer later"]
    );
    gateway.shutdown().await.unwrap();
}
