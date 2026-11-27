use super::*;
use std::{
    io::Cursor,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

const LEASE: Duration = Duration::from_secs(60);

fn manifest(name: &str, size: usize, sha256: Option<[u8; 32]>) -> AttachmentManifest {
    AttachmentManifest {
        name: name.into(),
        size: Some(size),
        sha256,
    }
}

fn wav(sample: i16) -> Vec<u8> {
    let mut wav = b"RIFF".to_vec();
    wav.extend(38u32.to_le_bytes());
    wav.extend(b"WAVEfmt ");
    wav.extend(16u32.to_le_bytes());
    wav.extend(1u16.to_le_bytes());
    wav.extend(1u16.to_le_bytes());
    wav.extend(16_000u32.to_le_bytes());
    wav.extend(32_000u32.to_le_bytes());
    wav.extend(2u16.to_le_bytes());
    wav.extend(16u16.to_le_bytes());
    wav.extend(b"data");
    wav.extend(2u32.to_le_bytes());
    wav.extend(sample.to_le_bytes());
    wav
}

#[derive(Default)]
struct SpeechFixture {
    synthesized: AtomicUsize,
}

impl AudioService for SpeechFixture {
    fn transcribe<'a>(
        &'a self,
        _profile: &'a Path,
        recording: &'a [u8],
    ) -> AdapterFuture<'a, Result<String, GatewayError>> {
        Box::pin(async move {
            Ok(if recording.ends_with(&1i16.to_le_bytes()) {
                "Hey Mercury, run the review".into()
            } else {
                "The microphone is on".into()
            })
        })
    }

    fn synthesize<'a>(
        &'a self,
        _profile: &'a Path,
        text: &'a str,
    ) -> AdapterFuture<'a, Result<Vec<u8>, GatewayError>> {
        Box::pin(async move {
            assert_eq!(text, "answer Hey Mercury, run the review");
            self.synthesized.fetch_add(1, Ordering::SeqCst);
            Ok(wav(5))
        })
    }
}

async fn audio_gateway(
    root: &Path,
    model: &Model,
    config: AdapterConfig,
    speech: Arc<SpeechFixture>,
) -> RunningGateway {
    model.gateway_with_audio(root, vec![config], Some(speech)).await
}

fn add_attachment(input: &mut InboundEvent, attachment: AttachmentRef) {
    let InboundBody::Message(message) = &mut input.body else {
        panic!("expected a message");
    };
    message.attachments.push(attachment);
}

#[tokio::test]
async fn media_refuses_revoked_reads_and_verifies_content_before_staging() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "source-authorization", true);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    let ingress = push.ready().await;
    let allowed = dm("protected-read", "destination", "");
    let mut denied = allowed.clone();
    denied.sender.id = "someone-else".into();

    assert!(matches!(
        ingress
            .authorize_media(
                MediaSource::from(&denied),
                manifest("summary.txt", 4, None),
                MediaPurpose::Attachment,
                LEASE,
            )
            .await,
        Err(GatewayError::Forbidden)
    ));
    let grant = ingress
        .authorize_media(
            MediaSource::from(&allowed),
            manifest("summary.txt", 4, Some([7; 32])),
            MediaPurpose::Attachment,
            LEASE,
        )
        .await
        .unwrap();
    assert_eq!(
        ingress
            .stage_attachment(grant, || async { Ok(&b"read"[..]) })
            .await
            .unwrap_err(),
        GatewayError::Invalid
    );

    let grant = ingress
        .authorize_media(
            MediaSource::from(&allowed),
            manifest("summary.txt", 4, None),
            MediaPurpose::Attachment,
            LEASE,
        )
        .await
        .unwrap();
    assert_eq!(
        ingress
            .stage_attachment(grant, || async { Ok(&b"oversized"[..]) })
            .await
            .unwrap_err(),
        GatewayError::Invalid
    );
    let grant = ingress
        .authorize_media(
            MediaSource::from(&allowed),
            manifest("summary.txt", 4, None),
            MediaPurpose::Attachment,
            LEASE,
        )
        .await
        .unwrap();
    gateway
        .set_adapter_policy("fixture", AccessPolicy::default())
        .await
        .unwrap();
    let opened = AtomicBool::new(false);
    assert_eq!(
        ingress
            .stage_attachment(grant, || async {
                opened.store(true, Ordering::SeqCst);
                Ok(&b"read"[..])
            })
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    assert!(!opened.load(Ordering::SeqCst));
    assert!(model.requests().is_empty());
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn staged_attachment_identity_and_scope_survive_restart() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let push = Driver::new(Transport::Push, "staged-media", true);
    let gateway = model
        .gateway(root.path(), vec![push.config(policy())])
        .await;
    let ingress = push.ready().await;
    let mut input = dm("attachment-only", "destination", "");
    let grant = ingress
        .authorize_media(
            MediaSource::from(&input),
            manifest("one.txt", 5, None),
            MediaPurpose::Attachment,
            LEASE,
        )
        .await
        .unwrap();
    let first = ingress
        .stage_attachment(grant, || async { Ok(&b"proof"[..]) })
        .await
        .unwrap();
    let grant = ingress
        .authorize_media(
            MediaSource::from(&input),
            manifest("two.txt", 5, None),
            MediaPurpose::Attachment,
            LEASE,
        )
        .await
        .unwrap();
    let second = ingress
        .stage_attachment(grant, || async { Ok(&b"proof"[..]) })
        .await
        .unwrap();
    assert_eq!(first.id(), second.id());
    assert_ne!(first.name(), second.name());
    gateway.shutdown().await.unwrap();

    let resumed = Driver::new(Transport::Push, "staged-media", true);
    let gateway = model
        .gateway(root.path(), vec![resumed.config(policy())])
        .await;
    resumed.ready().await;
    let mut wrong_route = dm("attachment-only", "somewhere-else", "");
    add_attachment(&mut wrong_route, first.clone());
    // A refused logical input still owns its receipt. Check the forged
    // association under a distinct ID so it cannot consume the real one.
    wrong_route.id = "wrong-association".into();
    assert_eq!(
        resumed.push(wrong_route).await.disposition,
        Disposition::Invalid
    );
    add_attachment(&mut input, first);
    assert_eq!(resumed.push(input).await.disposition, Disposition::Accepted);
    resumed
        .sent()
        .await
        .finish(confirmed("attachment-accepted"));
    assert!(model.requests()[0].to_string().contains("proof"));
    gateway.shutdown().await.unwrap();
}

#[tokio::test]
async fn spoken_addressing_and_voice_reply_use_persisted_artifact_capabilities() {
    let root = TempDir::new().unwrap();
    let model = Model::new().await;
    let speech = Arc::new(SpeechFixture::default());
    let push = Driver::new(Transport::Push, "voice-media", true);
    let mut access = policy();
    let group = access.rooms.get_mut("group").unwrap();
    group.spoken_addressing = vec!["hey mercury".into()];
    group.spoken_replies = true;
    let gateway = audio_gateway(
        root.path(),
        &model,
        push.config(access.clone()),
        speech.clone(),
    )
    .await;
    let ingress = push.ready().await;
    let mut input = dm("voice-note", "group", "");
    input.room.kind = RoomKind::Group;
    input.mentioned = false;
    let source = MediaSource::from(&input);
    let recording = wav(0);
    assert!(matches!(
        ingress
            .authorize_media(
                source.clone(),
                manifest("mixed.txt", 1, None),
                MediaPurpose::Attachment,
                LEASE
            )
            .await,
        Err(GatewayError::Forbidden)
    ));
    let grant = ingress
        .authorize_media(
            source.clone(),
            manifest("voice.wav", recording.len(), None),
            MediaPurpose::SpokenAddressing,
            LEASE,
        )
        .await
        .unwrap();
    assert_eq!(
        ingress
            .check_spoken_addressing(grant, || async { Ok(Cursor::new(recording)) })
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        ingress
            .authorize_media(
                source.clone(),
                manifest("mixed.txt", 1, None),
                MediaPurpose::Attachment,
                LEASE
            )
            .await,
        Err(GatewayError::Forbidden)
    ));
    assert!(model.requests().is_empty());

    let recording = wav(1);
    let grant = ingress
        .authorize_media(
            source.clone(),
            manifest("voice.wav", recording.len(), None),
            MediaPurpose::SpokenAddressing,
            LEASE,
        )
        .await
        .unwrap();
    let words = ingress
        .check_spoken_addressing(grant, || async { Ok(Cursor::new(recording)) })
        .await
        .unwrap()
        .unwrap();
    let recording = wav(1);
    let grant = ingress
        .authorize_media(
            source,
            manifest("voice.wav", recording.len(), None),
            MediaPurpose::VoiceNote,
            LEASE,
        )
        .await
        .unwrap();
    let transcript = ingress
        .transcribe_voice(grant, || async { Ok(Cursor::new(recording)) })
        .await
        .unwrap();
    assert_eq!(transcript, words);
    input.mentioned = true;
    input.body = InboundBody::Message(Message::text(transcript));
    assert_eq!(push.push(input).await.disposition, Disposition::Accepted);
    let attempt = push.sent().await;
    let file = ingress.synthesize_reply(&attempt.attempt).await.unwrap();
    assert_eq!(
        file,
        ingress.synthesize_reply(&attempt.attempt).await.unwrap()
    );
    assert_eq!(speech.synthesized.load(Ordering::SeqCst), 1);
    assert_eq!(
        ingress
            .reply_artifact(&attempt.attempt, &file)
            .await
            .unwrap(),
        wav(5)
    );

    let expires = ingress
        .authorize_artifact_fetch(&attempt.attempt, &file, Duration::from_millis(1))
        .await
        .unwrap();
    let revoke = ingress
        .authorize_artifact_fetch(&attempt.attempt, &file, LEASE)
        .await
        .unwrap();
    assert!(
        ingress
            .revoke_artifact_fetch(revoke.token().into())
            .await
            .unwrap()
    );
    assert_eq!(
        ingress
            .read_provider_artifact(revoke.token().into(), file.id().into())
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    let fetch = ingress
        .authorize_artifact_fetch(&attempt.attempt, &file, LEASE)
        .await
        .unwrap();
    let object = file.id().to_owned();
    assert_eq!(
        ingress
            .read_provider_artifact(fetch.token().into(), object.clone())
            .await
            .unwrap(),
        wav(5)
    );
    assert_eq!(
        ingress
            .read_provider_artifact(fetch.token().into(), "different-object".into())
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    assert_eq!(
        ingress
            .read_provider_artifact(expires.token().into(), object.clone())
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    let delivery_id = attempt.attempt.delivery_id.clone();
    attempt.finish(confirmed("voice-send"));
    status(&gateway, |value| {
        delivery(value, &delivery_id, 0).is_some_and(|row| row["part_state"] == "sent")
    })
    .await;
    gateway.shutdown().await.unwrap();

    let other_root = TempDir::new().unwrap();
    // The very same adapter/account key in another profile still cannot
    // use this capability because the reference belongs to the first store.
    let other = Driver::new(Transport::Push, "voice-media", true);
    let gateway = audio_gateway(
        other_root.path(),
        &model,
        other.config(access.clone()),
        speech.clone(),
    )
    .await;
    let ingress = other.ready().await;
    assert_eq!(
        ingress
            .read_provider_artifact(fetch.token().into(), object.clone())
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    gateway.shutdown().await.unwrap();

    let resumed = Driver::new(Transport::Push, "voice-media", true);
    let gateway = audio_gateway(root.path(), &model, resumed.config(access), speech).await;
    let ingress = resumed.ready().await;
    assert_eq!(
        ingress
            .read_provider_artifact(fetch.token().into(), object.clone())
            .await
            .unwrap(),
        wav(5)
    );
    gateway
        .set_adapter_policy("fixture", AccessPolicy::default())
        .await
        .unwrap();
    assert_eq!(
        ingress
            .read_provider_artifact(fetch.token().into(), object)
            .await
            .unwrap_err(),
        GatewayError::Forbidden
    );
    gateway.shutdown().await.unwrap();
}
