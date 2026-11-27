use super::{
    Connection,
    api::{Bucket, Failure},
};
use crate::{
    common::{self, MediaKind, lock, wamid},
    intake::Source,
    outbound,
};
use hermes_gateway_core::messaging::{
    DeliveryBlock, DeliveryBody, DeliveryResult, EffectOperation, EffectReceipt, GatewayError,
    Ingress, ReceiptKind, RoomKind, SendAttempt, UploadStep,
    runtime::{db, now},
    storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

pub(super) fn open_window(
    ingress: &Ingress,
    source: &Source,
    time: i64,
) -> Result<bool, GatewayError> {
    let proof = db(storage::key("whatsapp-cloud-window", &(source, time)))?;
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            let connection = ingress.connection_name();
            tx.execute("INSERT OR IGNORE INTO messaging_whatsapp_cloud_window_events VALUES(?1,?2,?3)",
                params![connection,source.event,proof])?;
            let saved = tx.query_row("SELECT proof FROM messaging_whatsapp_cloud_window_events WHERE connection=?1 AND event=?2",
                params![connection,source.event],|row|row.get::<_,String>(0))?;
            if saved != proof { return Ok(false); }
            tx.execute(
                "INSERT INTO messaging_whatsapp_cloud_windows(connection,recipient,newest)
                    VALUES(?1,?2,?3) ON CONFLICT(connection,recipient)
                    DO UPDATE SET newest=max(newest,excluded.newest)",
                params![connection, source.actor, time],
            )?;
            Ok(true)
        }))
    })
}

fn close_window(
    ingress: &Ingress,
    recipient: &str,
    attempted_window: i64,
) -> Result<(), GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_whatsapp_cloud_windows SET provider_closed=max(provider_closed,?3)
                    WHERE connection=?1 AND recipient=?2",
                params![ingress.connection_name(), recipient, attempted_window],
            )?;
            Ok(())
        }))
    })
}

fn eligible(ingress: &Ingress, recipient: &str, clock: i64) -> Result<Option<i64>, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|db| {
            Ok(db
                .query_row(
                    "SELECT newest FROM messaging_whatsapp_cloud_windows
            WHERE connection=?1 AND recipient=?2 AND newest>?3 AND newest>provider_closed",
                    params![
                        ingress.connection_name(),
                        recipient,
                        clock.saturating_sub(WINDOW_MS)
                    ],
                    |row| row.get(0),
                )
                .optional()?)
        }))
    })
}

fn window_closed() -> DeliveryResult {
    DeliveryResult::Blocked(DeliveryBlock::ReplyWindowClosed)
}

fn allowed(
    connection: &Connection,
    ingress: &Ingress,
    recipient: &str,
) -> Result<i64, DeliveryResult> {
    let window = match eligible(ingress, recipient, connection.settings.clock()) {
        Ok(Some(window)) => window,
        Ok(None) => return Err(window_closed()),
        Err(GatewayError::TemporarilyUnavailable) => {
            return Err(outbound::retry(Duration::from_secs(1)));
        }
        Err(_) => return Err(DeliveryResult::Rejected),
    };
    let until = ingress
        .checked(true, |_| {
            db(ingress.storage().read(|db| {
                Ok(db.query_row(
                    "SELECT max(until_ms) FROM messaging_whatsapp_cloud_cooldowns WHERE app=?1
                        AND (phone='' OR phone=?2) AND (recipient='' OR recipient=?3)",
                    params![
                        connection.settings.app_id,
                        connection.settings.phone_id,
                        recipient
                    ],
                    |row| row.get::<_, Option<i64>>(0),
                )?)
            }))
        })
        .map_err(|_| outbound::retry(Duration::from_secs(1)))?;
    if let Some(until) = until.filter(|until| *until > now()) {
        return Err(outbound::retry(Duration::from_millis(
            until.saturating_sub(now()).max(1) as u64,
        )));
    }
    Ok(window)
}

fn failure(
    connection: &Connection,
    ingress: &Ingress,
    recipient: &str,
    window: i64,
    error: Failure,
) -> DeliveryResult {
    match error {
        Failure::Window => {
            if close_window(ingress, recipient, window).is_ok() {
                window_closed()
            } else {
                DeliveryResult::Uncertain
            }
        }
        Failure::Retry(bucket, after) => {
            let phone = if bucket == Bucket::App {
                ""
            } else {
                &connection.settings.phone_id
            };
            let destination = if bucket == Bucket::Recipient {
                recipient
            } else {
                ""
            };
            let until = now().saturating_add(i64::try_from(after.as_millis()).unwrap_or(i64::MAX));
            let _ = ingress.checked(false, |_| db(ingress.storage().write(|tx| {
                tx.execute("INSERT INTO messaging_whatsapp_cloud_cooldowns VALUES(?1,?2,?3,?4)
                    ON CONFLICT(app,phone,recipient) DO UPDATE SET until_ms=max(until_ms,excluded.until_ms)",
                    params![connection.settings.app_id,phone,destination,until])?;
                Ok(())
            })));
            outbound::retry(after)
        }
        Failure::Credential => {
            connection
                .subscribed
                .store(false, std::sync::atomic::Ordering::Release);
            lock(&connection.status)["registration"] = json!("revoked");
            let _ = ingress.report_transport_ready(false);
            DeliveryResult::Rejected
        }
        Failure::Invalid | Failure::Permission | Failure::Unsupported => DeliveryResult::Rejected,
        Failure::Unknown | Failure::Cancelled => DeliveryResult::Uncertain,
    }
}

pub(super) async fn send(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    let recipient = room.chat.as_str();
    if room.kind != RoomKind::Direct
        || room.thread.is_some()
        || !(common::digits(recipient) || common::bsuid(recipient))
        || ingress.recheck_send(attempt).await.is_err()
    {
        return DeliveryResult::Rejected;
    }
    if let Err(result) = allowed(connection, ingress, recipient) {
        return result;
    }
    if let EffectOperation::Upload {
        artifact,
        step: UploadStep::Single,
    } = &attempt.payload.effect.operation
    {
        let Ok(bytes) = ingress.reply_artifact(attempt, artifact).await else {
            return DeliveryResult::Rejected;
        };
        let (kind, mime) = outbound::mime(artifact);
        if bytes.is_empty() || bytes.len() > media_limit(kind) {
            return DeliveryResult::Rejected;
        }
        if ingress.recheck_send(attempt).await.is_err() {
            return DeliveryResult::Rejected;
        }
        let window = match allowed(connection, ingress, recipient) {
            Ok(window) => window,
            Err(result) => return result,
        };
        return match connection
            .api
            .upload(
                &connection.settings.phone_id,
                artifact.name(),
                mime,
                bytes,
                cancel,
            )
            .await
        {
            Ok(id) => DeliveryResult::EffectConfirmed(EffectReceipt {
                kind: ReceiptKind::File,
                id,
            }),
            Err(error) => failure(connection, ingress, recipient, window, error),
        };
    }
    let mut fallback = None;
    let (mut payload, text) = match &attempt.payload.body {
        DeliveryBody::Reply(message) if outbound::is_voice(attempt) => {
            let id = match speech(connection, ingress, attempt, cancel).await {
                Ok(id) => id,
                Err(result) => return result,
            };
            let mut body = json!({"type":"audio","audio":{"id":id}});
            if !connection.settings.audio_attachment {
                body["audio"]["voice"] = json!(true);
            }
            (body, message.text.as_str().to_owned())
        }
        DeliveryBody::Reply(message) => {
            let body = if let EffectOperation::Publish { .. } = &attempt.payload.effect.operation {
                let Some(artifact) = message.attachments.first() else {
                    return DeliveryResult::Rejected;
                };
                let [(_, proof)] = &attempt.effect.dependencies[..] else {
                    return DeliveryResult::Rejected;
                };
                if proof.kind != ReceiptKind::File || !common::digits(&proof.id) {
                    return DeliveryResult::Rejected;
                }
                let (kind, _) = outbound::mime(artifact);
                let kind = match kind {
                    MediaKind::Audio => "audio",
                    MediaKind::Image => "image",
                    MediaKind::Video => "video",
                    MediaKind::Document => "document",
                };
                let mut media = json!({"id":proof.id});
                if kind == "document" {
                    media["filename"] = json!(artifact.name());
                }
                let mut result = json!({"type":kind});
                result[kind] = media;
                result
            } else {
                text_message(&message.text)
            };
            (body, message.text.clone())
        }
        DeliveryBody::Prompt { action_id, prompt } => {
            let Ok(prompt) = outbound::prompt(ingress, attempt, action_id, prompt) else {
                return DeliveryResult::Rejected;
            };
            fallback = Some(text_message(&prompt.text_fallback()));
            (interactive(&prompt), prompt.text)
        }
        _ => return DeliveryResult::Rejected,
    };
    address(&mut payload, recipient, attempt);
    if let Some(fallback) = &mut fallback {
        address(fallback, recipient, attempt);
    }
    if ingress.recheck_send(attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }
    let mut window = match allowed(connection, ingress, recipient) {
        Ok(window) => window,
        Err(result) => return result,
    };
    let mut result = connection
        .api
        .send(&connection.settings.phone_id, payload, cancel)
        .await;
    if result == Err(Failure::Unsupported)
        && let Some(fallback) = fallback
    {
        if ingress.recheck_send(attempt).await.is_err() {
            return DeliveryResult::Rejected;
        }
        window = match allowed(connection, ingress, recipient) {
            Ok(window) => window,
            Err(result) => return result,
        };
        result = connection
            .api
            .send(&connection.settings.phone_id, fallback, cancel)
            .await;
    }
    let response = match result {
        Ok(response) => response,
        Err(error) => return failure(connection, ingress, recipient, window, error),
    };
    if response["messaging_product"] != "whatsapp"
        || response["contacts"].as_array().is_none_or(|contacts| {
            contacts.len() != 1
                || contacts[0]["input"]
                    .as_str()
                    .is_none_or(|input| input.trim_start_matches('+') != recipient)
        })
    {
        return DeliveryResult::Uncertain;
    }
    let Some(id) = response["messages"]
        .as_array()
        .filter(|rows| rows.len() == 1)
        .and_then(|rows| rows[0]["id"].as_str())
        .filter(|id| wamid(id))
    else {
        return DeliveryResult::Uncertain;
    };
    if outbound::record(ingress, attempt, id, &text).is_err() {
        return DeliveryResult::Uncertain;
    }
    DeliveryResult::Confirmed {
        receipt: id.into(),
        editable_message: None,
    }
}

fn address(payload: &mut Value, to: &str, attempt: &SendAttempt) {
    payload["messaging_product"] = json!("whatsapp");
    payload["recipient_type"] = json!("individual");
    if common::bsuid(to) {
        payload["recipient"] = json!(to);
    } else {
        payload["to"] = json!(to);
    }
    if let Some(id) = &attempt.payload.effect.operation_key {
        payload["biz_opaque_callback_data"] = json!(id);
    }
    if let Some(reply) = attempt.reply_anchor.as_deref().filter(|id| wamid(id)) {
        payload["context"] = json!({"message_id":reply});
    }
}

fn text_message(text: &str) -> Value {
    json!({"type":"text","text":{"body":text,"preview_url":false}})
}

fn interactive(prompt: &outbound::PromptPlan) -> Value {
    if prompt.choices.len() <= 3 {
        let buttons: Vec<_> = prompt
            .choices
            .iter()
            .map(|choice| {
                json!({"type":"reply",
            "reply":{"id":choice.id,"title":choice.label.chars().take(20).collect::<String>()}})
            })
            .collect();
        return json!({"type":"interactive","interactive":{"type":"button",
            "body":{"text":prompt.text},"action":{"buttons":buttons}}});
    }
    let rows: Vec<_> = prompt
        .choices
        .iter()
        .map(|choice| {
            json!({
                "id":choice.id,"title":choice.label.chars().take(24).collect::<String>()
            })
        })
        .collect();
    json!({"type":"interactive","interactive":{"type":"list","body":{"text":prompt.text},
        "action":{"button":"Choose an answer","sections":[{"title":"Mercury","rows":rows}]}}})
}

fn media_limit(kind: MediaKind) -> usize {
    match kind {
        MediaKind::Image => 5 * 1024 * 1024,
        MediaKind::Video | MediaKind::Audio => 16 * 1024 * 1024,
        MediaKind::Document => 100 * 1024 * 1024,
    }
}

async fn speech(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> Result<String, DeliveryResult> {
    let recipient = &attempt.payload.room.chat;
    let stored = ingress.checked(true, |_| db(ingress.storage().read(|db| {
        Ok(db.query_row("SELECT media FROM messaging_whatsapp_cloud_speech WHERE connection=?1 AND delivery=?2 AND part=?3",
            params![ingress.connection_name(),attempt.delivery_id,attempt.part], |row| row.get::<_,String>(0)).optional()?)
    }))).map_err(|_| outbound::retry(Duration::from_secs(1)))?;
    if let Some(id) = stored {
        return Ok(id);
    }
    let bytes = outbound::encode_voice(ingress, attempt)
        .await
        .map_err(|_| DeliveryResult::Rejected)?;
    ingress
        .recheck_send(attempt)
        .await
        .map_err(|_| DeliveryResult::Rejected)?;
    let window = allowed(connection, ingress, recipient)?;
    let id = connection
        .api
        .upload(
            &connection.settings.phone_id,
            "voice.ogg",
            "audio/ogg",
            bytes,
            cancel,
        )
        .await
        .map_err(|error| failure(connection, ingress, recipient, window, error))?;
    ingress
        .checked(false, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_whatsapp_cloud_speech VALUES(?1,?2,?3,?4)",
                    params![
                        ingress.connection_name(),
                        attempt.delivery_id,
                        attempt.part,
                        id
                    ],
                )?;
                Ok(())
            }))
        })
        .map_err(|_| DeliveryResult::Uncertain)?;
    Ok(id)
}
