use super::*;
use crate::messaging::{Disposition, EnrollmentCredential, InboundBody, InboundEvent};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, header},
    routing::{get, post},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const FILE_BYTES: usize = 32 * 1024 * 1024;
const SENTINEL: &str = "opaque-channel-attachment-marker";
const MESSAGE_ID: &str = "111";
const EVENT_ID: &str = "discord-message-111";

fn metadata() -> Value {
    json!({"id":MESSAGE_ID,"channel_id":"42","author":{"id":"10"},"attachments":[{
        "id":"99","filename":"fixture.zip","size":FILE_BYTES,
        "url":"https://cdn.discordapp.com/attachments/42/99/fixture.zip?hm=fixture"
    }]})
}

async fn model(
    State(calls): State<async_channel::Sender<Value>>,
    Json(request): Json<Value>,
) -> impl axum::response::IntoResponse {
    calls.send(request.clone()).await.unwrap();
    let messages = request["messages"].as_array().unwrap();
    let last = messages.last().unwrap();
    let (delta, finish) = if last["role"] == "tool" {
        (json!({"content":"Inspected the attached bytes."}), "stop")
    } else {
        let input = last["content"].as_str().unwrap();
        let descriptor: Value = serde_json::from_str(
            input
                .lines()
                .find(|line| line.starts_with("[{\"id\":"))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(descriptor[0]["filename"], "fixture.zip");
        assert_eq!(descriptor[0]["bytes"], FILE_BYTES);
        (
            json!({"tool_calls":[{"index":0,"id":"artifact-read","type":"function","function":{
                "name":"read_channel_attachment",
                "arguments":json!({"id":descriptor[0]["id"],"offset":4,"limit":SENTINEL.len()}).to_string()
            }}]}),
            "tool_calls",
        )
    };
    (
        [(header::CONTENT_TYPE, "text/event-stream")],
        format!(
            "data: {}\n\n",
            json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]})
        ),
    )
}

#[tokio::test]
async fn stages_a_full_size_discord_file_and_only_discloses_pages_in_its_authorized_turn() {
    let mut bytes = vec![0; FILE_BYTES];
    bytes[..4].copy_from_slice(b"PK\x03\x04");
    bytes[4..4 + SENTINEL.len()].copy_from_slice(SENTINEL.as_bytes());
    let bytes = axum::body::Bytes::from(bytes);
    let (requests, provider) = async_channel::bounded(8);
    let cdn_reads = Arc::new(AtomicUsize::new(0));
    let cdn_counter = cdn_reads.clone();
    let metadata_reads = Arc::new(AtomicUsize::new(0));
    let metadata_counter = metadata_reads.clone();
    let server = Router::new()
        .route("/v1/chat/completions", post(model))
        .route(
            "/channels/42/messages/111",
            get(move |headers: HeaderMap| {
                let count = metadata_counter.clone();
                async move {
                    assert_eq!(headers[header::AUTHORIZATION], "Bot fixture");
                    count.fetch_add(1, Ordering::SeqCst);
                    Json(metadata())
                }
            }),
        )
        .route(
            "/object",
            get(move |headers: HeaderMap| {
                let count = cdn_counter.clone();
                let bytes = bytes.clone();
                async move {
                    assert!(!headers.contains_key(header::AUTHORIZATION));
                    count.fetch_add(1, Ordering::SeqCst);
                    bytes
                }
            }),
        )
        .route("/truncated", get(|| async { "short" }))
        .with_state(requests);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let base = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let task = tokio::spawn(async { axum::serve(listener, server).await.unwrap() });
    let (_root, gateway, ingress, _) = fixtures::profile_with_model(base.to_string(), true).await;
    let client = DiscordClient::for_fixture(
        EnrollmentCredential::new("fixture".into()).unwrap(),
        base.clone(),
    )
    .unwrap();
    let source = MediaSource {
        event_id: EVENT_ID.into(),
        room: fixtures::room(),
        sender: fixtures::actor("10"),
        mentioned: true,
    };
    let expected = attachment_sources(&metadata()).unwrap();
    let download = |url: Url, size| {
        assert_eq!(url.host_str(), Some("cdn.discordapp.com"));
        assert_eq!(url.path(), "/attachments/42/99/fixture.zip");
        open_cdn(&client, base.join("object").unwrap(), size)
    };
    let limited = hydrate_with(
        &client,
        &ingress,
        &source,
        "42",
        MESSAGE_ID,
        Message::text(""),
        &expected,
        FILE_BYTES as u64 - 1,
        download,
    )
    .await;
    assert_eq!(limited.unwrap_err(), GatewayError::Unsupported);
    assert_eq!(metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(cdn_reads.load(Ordering::SeqCst), 0);
    let forbidden = MediaSource {
        sender: fixtures::actor("30"),
        ..source.clone()
    };
    assert_eq!(
        hydrate_with(
            &client,
            &ingress,
            &forbidden,
            "42",
            MESSAGE_ID,
            Message::text(""),
            &expected,
            FILE_BYTES as u64,
            download
        )
        .await
        .unwrap_err(),
        GatewayError::Forbidden,
    );
    assert_eq!(metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(cdn_reads.load(Ordering::SeqCst), 0);
    assert!(matches!(
        open_cdn(&client, base.join("truncated").unwrap(), FILE_BYTES).await,
        Err(GatewayError::Invalid)
    ));

    let body = hydrate_with(
        &client,
        &ingress,
        &source,
        "42",
        MESSAGE_ID,
        Message::text(""),
        &expected,
        FILE_BYTES as u64,
        download,
    )
    .await
    .unwrap();
    assert_eq!(body.attachments.len(), 1);
    assert_eq!(metadata_reads.load(Ordering::SeqCst), 1);
    assert_eq!(cdn_reads.load(Ordering::SeqCst), 1);

    let event = InboundEvent {
        id: source.event_id,
        room: source.room,
        sender: source.sender,
        reply_anchor: None,
        mentioned: true,
        body: InboundBody::Message(body.clone()),
    };
    for substituted in [
        InboundEvent {
            id: "different-event".into(),
            ..event.clone()
        },
        InboundEvent {
            sender: fixtures::actor("20"),
            ..event.clone()
        },
        InboundEvent {
            room: crate::messaging::Room {
                chat: "50".into(),
                ..event.room.clone()
            },
            ..event.clone()
        },
    ] {
        let read = ingress
            .storage()
            .read(|db| {
                crate::messaging::media::read_input_artifact(
                    db,
                    ingress.connection_name(),
                    &substituted,
                    &body.attachments[0],
                    4,
                    SENTINEL.len(),
                )
            })
            .unwrap();
        assert!(read.is_none());
    }
    let result = ingress.submit(vec![event], None).await.unwrap();
    assert_eq!(result.receipts[0].disposition, Disposition::Accepted);
    let initial = tokio::time::timeout(Duration::from_secs(12), provider.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        initial["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["function"]["name"] == "read_channel_attachment")
    );
    assert!(!initial.to_string().contains(SENTINEL));
    let reading = tokio::time::timeout(Duration::from_secs(12), provider.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        reading["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| {
                message["role"] == "tool"
                    && message["content"]
                        .as_str()
                        .is_some_and(|content| content.contains(SENTINEL))
            })
    );
    let snapshot = tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let status = gateway.messaging_status().await.unwrap();
            if let Some(entry) = status["input"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| row["id"] == EVENT_ID && row["state"] == "done")
            {
                break entry["session"].as_str().unwrap().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let transcript = crate::messaging::test_support::transcript(&gateway, &snapshot).await.to_string();
    assert!(transcript.contains("read_channel_attachment"));
    assert!(transcript.contains("Attachment opened for the active request"));
    assert!(!transcript.contains(SENTINEL));

    gateway.shutdown().await.unwrap();
    task.abort();
}
