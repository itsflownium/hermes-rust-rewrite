use super::*;
use crate::messaging::EnrollmentCredential;
use axum::{Json, Router, http::HeaderMap, routing::get};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn metadata() -> Value {
    json!({"id":"11","channel_id":"42","author":{"id":"10"},"attachments":[{
        "id":"99","filename":"sample.txt","size":4,
        "url":"https://cdn.discordapp.com/attachments/42/99/sample.txt?ex=fixture&hm=fixture"
    }]})
}

#[test]
fn attachment_identity_excludes_signed_url_and_binds_exact_cdn_path() {
    let mut current = metadata();
    let expected = attachment_sources(&current).unwrap();
    let raw = current["attachments"][0]["url"].as_str().unwrap();
    assert!(verified_cdn_url(raw, "42", "99").is_ok());
    assert!(verified_cdn_url(raw, "43", "99").is_err());
    assert!(verified_cdn_url(raw, "42", "98").is_err());
    for fake in [
        "https://cdn.discordapp.com.attacker.test/attachments/42/99/sample.txt",
        "https://cdn.discordapp.com:8443/attachments/42/99/sample.txt",
        "https://cdn.discordapp.com@localhost/attachments/42/99/sample.txt",
        "http://cdn.discordapp.com/attachments/42/99/sample.txt",
        "https://cdn.discordapp.com/attachments/42/99/subdir/file.txt",
    ] {
        assert!(verified_cdn_url(fake, "42", "99").is_err(), "{fake}");
    }
    current["attachments"][0]["url"] =
        json!("https://cdn.discordapp.com/attachments/42/99/sample.txt?ex=refreshed");
    assert_eq!(attachment_sources(&current).unwrap(), expected);
    assert!(!serde_json::to_string(&expected).unwrap().contains("https"));
    assert!(!within_inbound_limit(&expected, 3));
    let default_limit =
        crate::settings::DiscordMediaSettings::default().max_inbound_bytes;
    current["attachments"][0]["size"] = json!(default_limit);
    let at_limit = attachment_sources(&current).unwrap();
    assert!(within_inbound_limit(&at_limit, default_limit));
    assert!(within_inbound_limit(
        &vec![at_limit[0].clone(); 4],
        default_limit
    ));
    assert!(!within_inbound_limit(
        &vec![at_limit[0].clone(); 5],
        default_limit
    ));
    current["attachments"][0]["size"] = json!(default_limit + 1);
    let over_default = attachment_sources(&current).unwrap();
    assert!(!within_inbound_limit(&over_default, default_limit));
    assert!(within_inbound_limit(&over_default, default_limit + 1));
    current["attachments"][0]["size"] = json!(MAX_ADAPTER_FILE + 1);
    assert!(!within_inbound_limit(
        &attachment_sources(&current).unwrap(),
        u64::MAX
    ));
}

#[tokio::test]
async fn only_authorized_event_opens_scoped_discord_attachment() {
    let (_root, gateway, ingress, _) = fixtures::profile().await;
    let server_calls = Arc::new(AtomicUsize::new(0));
    let calls = server_calls.clone();
    let server = Router::new().route(
        "/channels/42/messages/11",
        get(move |headers: HeaderMap| {
            let calls = calls.clone();
            async move {
                assert_eq!(headers["authorization"], "Bot bot");
                calls.fetch_add(1, Ordering::SeqCst);
                Json(metadata())
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let serving = tokio::spawn(async { axum::serve(listener, server).await.unwrap() });
    let discord =
        DiscordClient::for_fixture(EnrollmentCredential::new("bot".into()).unwrap(), url).unwrap();
    let captures = AtomicUsize::new(0);
    let open = |url: Url, size| {
        assert_eq!(url.host_str(), Some("cdn.discordapp.com"));
        assert_eq!(size, 4);
        captures.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(io::Cursor::new(b"test")))
    };
    let expected = attachment_sources(&metadata()).unwrap();
    let actor_source = |id| MediaSource {
        event_id: "discord-message-11".into(),
        room: fixtures::room(),
        sender: fixtures::actor(id),
        mentioned: true,
    };
    assert_eq!(
        hydrate_with(
            &discord,
            &ingress,
            &actor_source("33"),
            "42",
            "11",
            Message::text(""),
            &expected,
            32 * 1024 * 1024,
            open,
        )
        .await
        .unwrap_err(),
        GatewayError::Forbidden,
    );
    assert_eq!(server_calls.load(Ordering::SeqCst), 0);
    assert_eq!(captures.load(Ordering::SeqCst), 0);
    let message = hydrate_with(
        &discord,
        &ingress,
        &actor_source("10"),
        "42",
        "11",
        Message::text(""),
        &expected,
        32 * 1024 * 1024,
        open,
    )
    .await
    .unwrap();
    assert_eq!(server_calls.load(Ordering::SeqCst), 1);
    assert_eq!(captures.load(Ordering::SeqCst), 1);
    assert_eq!(message.attachments[0].name(), "sample.txt");
    serving.abort();
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn unsupported_audio_remains_an_authorized_opaque_file() {
    let (_root, gateway, ingress, _) = fixtures::profile().await;
    let source = MediaSource {
        event_id: "discord-message-audio".into(),
        room: fixtures::room(),
        sender: fixtures::actor("10"),
        mentioned: true,
    };
    let audio = b"fLaC\0synthetic";
    let grant = ingress
        .authorize_adapter_media(
            source,
            AttachmentManifest {
                name: "fixture.flac".into(),
                size: Some(audio.len()),
                sha256: None,
            },
            MediaPurpose::VoiceNote,
            Duration::from_secs(30),
            MAX_ADAPTER_FILE,
        )
        .await
        .unwrap();
    let prepared = ingress
        .prepare_adapter_audio(
            grant,
            None,
            || async { Ok(io::Cursor::new(audio)) },
            decode_voice_note,
        )
        .await
        .unwrap();
    let PreparedAudio::Attachment(file) = prepared else {
        panic!("expected opaque audio")
    };
    assert_eq!(file.name(), "fixture.flac");
    gateway.shutdown().await.unwrap();
}

#[test]
fn normalizes_ogg_opus_into_scoped_profile_wav() {
    use ogg::{PacketWriteEndInfo, PacketWriter};
    let mut writer = PacketWriter::new(io::Cursor::new(Vec::new()));
    let mut header = b"OpusHead\x01\x01".to_vec();
    header.extend_from_slice(&[0; 9]);
    writer
        .write_packet(header, 1, PacketWriteEndInfo::EndPage, 0)
        .unwrap();
    writer
        .write_packet(
            b"OpusTags\0\0\0\0\0\0\0\0".as_slice(),
            1,
            PacketWriteEndInfo::EndPage,
            0,
        )
        .unwrap();
    let mut codec =
        opus2::Encoder::new(16_000, opus2::Channels::Mono, opus2::Application::Voip).unwrap();
    let mut packet = vec![0; 1275];
    let bytes = codec.encode(&[1_500i16; 320], &mut packet).unwrap();
    packet.truncate(bytes);
    writer
        .write_packet(packet, 1, PacketWriteEndInfo::EndStream, 960)
        .unwrap();
    let original = writer.into_inner().into_inner();
    let wav = decode_voice_note(&original).unwrap();
    assert_eq!(&wav[..4], b"RIFF");
    assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16_000);
    assert_eq!(wav.len(), 44 + 640);
    assert!(decode_voice_note(b"not an Opus file").is_err());
}
