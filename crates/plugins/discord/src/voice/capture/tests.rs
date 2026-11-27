use super::*;
use crate::media::fixtures;
use serde_json::json;

fn voice_states() -> serde_json::Value {
    json!({
        "id":"8","channels":[{"id":"12","type":2},{"id":"13","type":13}],
        "voice_states":[
            {"user_id":"10","channel_id":"12"},
            {"user_id":"20","channel_id":"12"},
            {"user_id":"30","channel_id":"12"}
        ]
    })
}

#[tokio::test]
async fn handshake_binds_bot_session_guild_channel_and_discord_endpoint() {
    let (_root, gateway, ingress, _) = fixtures::profile().await;
    let voice = Voice::new(900, Default::default());
    voice.observe("GUILD_CREATE", &voice_states(), &ingress);
    let room = fixtures::room();
    assert_eq!(
        voice
            .join(&ingress, &fixtures::actor("30"), &room, "8")
            .unwrap_err(),
        crate::messaging::GatewayError::Forbidden,
    );
    let join = voice
        .join(&ingress, &fixtures::actor("10"), &room, "8")
        .unwrap();
    assert_eq!(join["op"], 4);
    assert_eq!(join["d"]["channel_id"], "12");
    voice.observe(
        "VOICE_SERVER_UPDATE",
        &json!({
            "guild_id":"8","endpoint":"voice.discord.media.attacker.test:443","token":"synthetic"
        }),
        &ingress,
    );
    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({
            "guild_id":"8","channel_id":"12","user_id":"900","session_id":"fixture-session"
        }),
        &ingress,
    );
    {
        let state = crate::lock(&voice.state);
        let pending = state.calls.get(&8).unwrap();
        assert!(pending.connection_info(900).is_none());
        assert_eq!(pending.session.as_deref(), Some("fixture-session"));
    }
    // Pair a provider response at the handshake seam without opening UDP/TLS.
    let endpoint = super::super::authority("fixture.discord.media:443").unwrap();
    {
        let mut state = crate::lock(&voice.state);
        let pending = state.calls.get_mut(&8).unwrap();
        pending.server = Some(super::super::Server {
            endpoint,
            token: "fixture-token".into(),
        });
        let qualified = pending.connection_info(900).unwrap();
        assert_eq!(qualified.channel_id.0.get(), 12);
        assert_eq!(qualified.guild_id.0.get(), 8);
        assert_eq!(qualified.user_id.0.get(), 900);
        assert_eq!(qualified.session_id, "fixture-session");
        assert_eq!(qualified.endpoint, "fixture.discord.media");
    }
    assert_eq!(
        voice
            .leave_bound(&ingress, &fixtures::actor("20"), &room, "8")
            .unwrap_err(),
        crate::messaging::GatewayError::Forbidden,
    );
    assert!(
        voice
            .leave_bound(&ingress, &fixtures::actor("10"), &room, "8")
            .unwrap()
            .unwrap()["d"]["channel_id"]
            .is_null()
    );
    assert!(voice.leave("8").is_none());
    voice.reset();
    gateway.shutdown().await.unwrap();
}

async fn speech(sink: &mpsc::Sender<Signal>, link: &AudioLink, speaker: (u64, u32), sample: i16) {
    let (user, ssrc) = speaker;
    sink.send(Signal::Speaker {
        transport: link.transport_epoch.load(Ordering::Acquire),
        ssrc,
        user,
        microphone: true,
    })
    .await
    .unwrap();
    for _ in 0..14 {
        sink.send(Signal::Frame {
            epoch: link.epoch.load(Ordering::Acquire),
            ssrc,
            pcm: vec![sample; 320],
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn live_capture_admits_two_current_speakers_and_fences_old_or_muted_audio() {
    let (_root, gateway, ingress, spoken) = fixtures::profile().await;
    let voice = Voice::new(900, Default::default());
    voice.observe("GUILD_CREATE", &voice_states(), &ingress);
    let room = fixtures::room();
    voice
        .join(&ingress, &fixtures::actor("10"), &room, "8")
        .unwrap();
    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({
            "guild_id":"8","channel_id":"12","user_id":"900","session_id":"fixture-session"
        }),
        &ingress,
    );
    let (sink, source) = mpsc::channel(96);
    let link = Arc::new(AudioLink::new(Driver::new(Config::default())));
    link.refresh(true);
    let binding = {
        let mut state = crate::lock(&voice.state);
        let call = state.calls.get_mut(&8).unwrap();
        call.link = Some(link.clone());
        call.binding.clone()
    };
    let task = tokio::spawn(receive(
        voice.clone(),
        binding.clone(),
        ingress.clone(),
        link.clone(),
        source,
    ));
    speech(&sink, &link, (10, 1), 1_000).await;
    speech(&sink, &link, (20, 2), 2_000).await;
    speech(&sink, &link, (30, 3), 3_000).await;
    speech(&sink, &link, (900, 4), 4_000).await;
    let mut received = Vec::new();
    for _ in 0..2 {
        received.push(
            tokio::time::timeout(Duration::from_secs(5), spoken.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    received.sort_unstable();
    assert_eq!(received, vec![1_000, 2_000]);
    assert!(
        tokio::time::timeout(Duration::from_millis(150), spoken.recv())
            .await
            .is_err()
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let status = gateway.messaging_status().await.unwrap();
        let count = status["input"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| {
                entry["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with(&format!("{VOICE_ORIGIN}{}-", binding.id)))
            })
            .count();
        if count == 2 {
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(!voice.audience_allowed(&ingress, &binding));
    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({
            "guild_id":"8","channel_id":null,"user_id":"30"
        }),
        &ingress,
    );
    assert!(voice.audience_allowed(&ingress, &binding));
    crate::lock(&voice.state).complete_rosters.remove(&8);
    assert!(!voice.audience_allowed(&ingress, &binding));

    speech(&sink, &link, (20, 2), 5_000).await;
    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({
            "guild_id":"8","channel_id":"12","user_id":"20","mute":true
        }),
        &ingress,
    );
    speech(&sink, &link, (20, 2), 6_000).await;
    let old = link.epoch.load(Ordering::Acquire);
    link.refresh(false);
    sink.send(Signal::Frame {
        epoch: old,
        ssrc: 1,
        pcm: vec![7_000; 320],
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(900), spoken.recv())
            .await
            .is_err()
    );

    voice.reset();
    task.await.unwrap();
    assert!(link.stopped.is_cancelled());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn ambient_track_ducks_releases_and_stops_when_the_audience_changes() {
    let (_root, gateway, ingress, _) = fixtures::profile().await;
    let settings = serde_json::from_value(json!({
        "enabled":true,"ambient_gain":0.20,"duck_gain":0.03,"ack_enabled":false
    }))
    .unwrap();
    let voice = Voice::new(900, settings);
    let mut roster = voice_states();
    roster["voice_states"].as_array_mut().unwrap().pop();
    voice.observe("GUILD_CREATE", &roster, &ingress);
    voice
        .join(&ingress, &fixtures::actor("10"), &fixtures::room(), "8")
        .unwrap();
    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({"guild_id":"8","channel_id":"12","user_id":"900","session_id":"cue-test"}),
        &ingress,
    );
    let call = FixtureCall::attach(&voice, &ingress, 8).unwrap();
    let gain = || {
        crate::lock(&call.link.ambient)
            .as_ref()
            .map(AmbientTrack::gain)
    };
    tokio::time::timeout(Duration::from_secs(4), async {
        while gain() != Some(0.20) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let wav = crate::media::pcm_wave(&[512; 8000]);
    let input = super::super::cues::speech_input(&wav, Duration::from_secs(1), 0).unwrap();
    let (speech, _) = call.link.start_speech(input, &voice.cues);
    assert_eq!(gain(), Some(0.03));
    assert!(call.link.capture_blocked());
    drop(speech);
    assert!(call.link.capture_blocked());
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut rose_smoothly = false;
        loop {
            if let Some(level) = gain() {
                if level == 0.20 {
                    assert!(rose_smoothly);
                    break;
                }
                rose_smoothly |= (0.03..0.20).contains(&level);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    voice.observe(
        "VOICE_STATE_UPDATE",
        &json!({"guild_id":"8","channel_id":"12","user_id":"30"}),
        &ingress,
    );
    assert_eq!(gain(), None);
    tokio::time::sleep(Duration::from_millis(190)).await;
    assert_eq!(gain(), None);
    voice.leave("8").unwrap();
    assert!(call.link.stopped.is_cancelled());
    gateway.shutdown().await.unwrap();
}
