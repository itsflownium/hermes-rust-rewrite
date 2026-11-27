use super::{Connection, DeliveryResult, failure, part, retry};
use crate::identity::id;
use hermes_gateway_core::messaging::{
    AttachmentRef, DeliveryBody, DeliveryIntent, DeliveryPart, Effect, EffectDependency,
    EffectOperation, EffectReceipt, GatewayError, Ingress, Message, PreparationFailure,
    ReceiptKind, SendAttempt, SpokenReplyMode, UploadStep,
    media::MAX_ADAPTER_FILE,
    runtime::{blocking, db, now},
    storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::json;
use std::{collections::BTreeSet, time::Duration};
use tokio_util::sync::CancellationToken;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_uploads (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, ordinal INTEGER NOT NULL,
    file TEXT NOT NULL, endpoint TEXT NOT NULL, expires INTEGER NOT NULL,
    transferred INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,delivery,ordinal)
);
";

const UPLOAD_MS: i64 = 30 * 60 * 1_000;
const SPOKEN: &str = "slack-spoken-";

pub(super) fn append_plan(
    connection: &Connection,
    intent: &DeliveryIntent,
    attachments: &[AttachmentRef],
    plan: &mut Vec<DeliveryPart>,
) -> Result<(), PreparationFailure> {
    if attachments.is_empty() {
        return Ok(());
    }
    if !connection.settings.files {
        return Err(PreparationFailure::Unsupported);
    }
    let mut files = Vec::new();
    for artifact in attachments {
        let begin = plan.len() as u32;
        let body = DeliveryBody::Reply(Message {
            text: String::new(),
            attachments: vec![artifact.clone()],
        });
        for step in [
            UploadStep::Begin,
            UploadStep::Chunk { begin, index: 0 },
            UploadStep::Finish {
                begin,
                chunks: vec![begin + 1],
            },
        ] {
            plan.push(part(
                intent,
                body.clone(),
                Effect {
                    operation: EffectOperation::Upload {
                        artifact: artifact.clone(),
                        step,
                    },
                    ..Effect::default()
                },
            ));
        }
        files.push(EffectDependency {
            part: begin + 2,
            kind: ReceiptKind::File,
        });
    }
    let share = DeliveryBody::Reply(Message {
        text: String::new(),
        attachments: attachments.to_vec(),
    });
    plan.push(part(
        intent,
        share,
        Effect {
            operation: EffectOperation::Publish { using: files },
            ..Effect::default()
        },
    ));
    Ok(())
}

pub(super) async fn upload(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    artifact: &AttachmentRef,
    step: &UploadStep,
    cancel: &CancellationToken,
) -> DeliveryResult {
    match step {
        UploadStep::Single => DeliveryResult::Rejected,
        UploadStep::Begin => {
            let bytes = match ingress.reply_artifact(attempt, artifact).await {
                Ok(bytes) if (1..=MAX_ADAPTER_FILE).contains(&bytes.len()) => bytes.len(),
                _ => return DeliveryResult::Rejected,
            };
            match allocate(connection, ingress, attempt, artifact.name(), bytes, cancel).await {
                Ok(file) => proof(ReceiptKind::UploadSession, file.id),
                Err(result) => result,
            }
        }
        UploadStep::Chunk { begin, index: 0 } => {
            let Some(expected) = dependency(attempt, *begin, ReceiptKind::UploadSession) else {
                return DeliveryResult::Rejected;
            };
            let file = match load(ingress, attempt, *begin) {
                Ok(Some(file)) if file.id == expected => file,
                Ok(_) => return DeliveryResult::Rejected,
                Err(_) => return retry(Duration::from_secs(1)),
            };
            let Ok(bytes) = ingress.reply_artifact(attempt, artifact).await else {
                return DeliveryResult::Rejected;
            };
            match transfer(connection, ingress, attempt, *begin, &file, bytes, cancel).await {
                Ok(()) => proof(ReceiptKind::UploadChunk, format!("bytes:{}", file.id)),
                Err(result) => result,
            }
        }
        UploadStep::Finish { begin, chunks } => {
            let Some(file) = dependency(attempt, *begin, ReceiptKind::UploadSession) else {
                return DeliveryResult::Rejected;
            };
            let [chunk] = chunks.as_slice() else {
                return DeliveryResult::Rejected;
            };
            if dependency(attempt, *chunk, ReceiptKind::UploadChunk)
                != Some(format!("bytes:{file}").as_str())
            {
                return DeliveryResult::Rejected;
            }
            proof(ReceiptKind::File, file.to_owned())
        }
        _ => DeliveryResult::Rejected,
    }
}

fn proof(kind: ReceiptKind, id: String) -> DeliveryResult {
    DeliveryResult::EffectConfirmed(EffectReceipt { kind, id })
}

fn dependency(attempt: &SendAttempt, ordinal: u32, kind: ReceiptKind) -> Option<&str> {
    attempt
        .effect
        .dependencies
        .iter()
        .find_map(|(part, receipt)| {
            (*part == ordinal && receipt.kind == kind).then_some(receipt.id.as_str())
        })
}

struct Allocation {
    id: String,
    endpoint: String,
    transferred: bool,
}

fn load(
    ingress: &Ingress,
    attempt: &SendAttempt,
    ordinal: u32,
) -> Result<Option<Allocation>, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT file,endpoint,transferred FROM messaging_slack_uploads
            WHERE connection=?1 AND delivery=?2 AND ordinal=?3 AND expires>?4",
                    params![
                        ingress.connection_name(),
                        attempt.delivery_id,
                        ordinal,
                        now()
                    ],
                    |row| {
                        Ok(Allocation {
                            id: row.get(0)?,
                            endpoint: row.get(1)?,
                            transferred: row.get(2)?,
                        })
                    },
                )
                .optional()?)
        }))
    })
}

async fn allocate(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    name: &str,
    bytes: usize,
    cancel: &CancellationToken,
) -> Result<Allocation, DeliveryResult> {
    if ingress.recheck_send(attempt).await.is_err() {
        return Err(DeliveryResult::Rejected);
    }
    let allocated = connection
        .api
        .scoped(
            ingress,
            "files.getUploadURLExternal",
            json!({
                "filename":name,"length":bytes,
            }),
            cancel,
        )
        .await
        .map_err(failure)?;
    let file = allocated["file_id"]
        .as_str()
        .filter(|file| id(file, b"F"))
        .ok_or(DeliveryResult::Uncertain)?
        .to_owned();
    let endpoint = allocated["upload_url"]
        .as_str()
        .filter(|url| !url.is_empty() && url.len() <= 8192)
        .ok_or(DeliveryResult::Uncertain)?
        .to_owned();
    connection
        .api
        .validate_upload(&endpoint)
        .map_err(|_| DeliveryResult::Uncertain)?;
    let root = ingress.root().to_owned();
    let protected = blocking(move || db(hermes_engine::secret::seal(&root, &endpoint)))
        .await
        .map_err(|_| DeliveryResult::Uncertain)?;
    ingress.checked(false, |_| db(ingress.storage().write(|tx| {
        let scope = ingress.connection_name();
        tx.execute("DELETE FROM messaging_slack_uploads WHERE connection=?1 AND expires<=?2", params![scope,now()])?;
        tx.execute("INSERT INTO messaging_slack_uploads (connection,delivery,ordinal,file,endpoint,expires)
                    VALUES(?1,?2,?3,?4,?5,?6)",
            params![scope,attempt.delivery_id,attempt.part,file,protected,now().saturating_add(UPLOAD_MS)])?;
        Ok(())
    }))).map_err(|_| DeliveryResult::Uncertain)?;
    Ok(Allocation {
        id: file,
        endpoint: protected,
        transferred: false,
    })
}

async fn transfer(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    ordinal: u32,
    file: &Allocation,
    bytes: Vec<u8>,
    cancel: &CancellationToken,
) -> Result<(), DeliveryResult> {
    if file.transferred {
        return Ok(());
    }
    let sealed = file.endpoint.clone();
    let endpoint = blocking(move || db(hermes_engine::secret::unseal(&sealed)))
        .await
        .map_err(|_| retry(Duration::from_secs(5)))?;
    if ingress.recheck_send(attempt).await.is_err() {
        return Err(DeliveryResult::Rejected);
    }
    connection
        .api
        .transfer(&endpoint, bytes, cancel)
        .await
        .map_err(failure)?;
    ingress.checked(false, |_| db(ingress.storage().write(|tx| {
        tx.execute("UPDATE messaging_slack_uploads SET transferred=1 WHERE connection=?1 AND delivery=?2 AND ordinal=?3 AND file=?4",
            params![ingress.connection_name(),attempt.delivery_id,ordinal,file.id])?;
        Ok(())
    }))).map_err(|_| DeliveryResult::Uncertain)
}

pub(super) async fn publish(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let DeliveryBody::Reply(message) = &attempt.payload.body else {
        return DeliveryResult::Rejected;
    };
    let files = attempt
        .effect
        .dependencies
        .iter()
        .filter(|(_, receipt)| receipt.kind == ReceiptKind::File)
        .zip(&message.attachments)
        .map(|((_, receipt), artifact)| (receipt.id.as_str(), artifact.name()))
        .collect::<Vec<_>>();
    if files.len() != message.attachments.len() {
        return DeliveryResult::Rejected;
    }
    share(connection, ingress, attempt, &files, cancel).await
}

async fn share(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    files: &[(&str, &str)],
    cancel: &CancellationToken,
) -> DeliveryResult {
    let expected: BTreeSet<_> = files.iter().map(|(id, _)| *id).collect();
    if expected.is_empty()
        || expected.len() != files.len()
        || !expected.iter().all(|value| id(value, b"F"))
    {
        return DeliveryResult::Rejected;
    }
    let room = &attempt.payload.room;
    let mut body = json!({"channel_id":room.chat,"files":files.iter().map(|(id, title)| {
        json!({"id":id,"title":title})
    }).collect::<Vec<_>>()});
    if let Some(thread) = &room.thread {
        body["thread_ts"] = json!(thread);
    }
    if ingress.recheck_send(attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }
    let result = match connection
        .api
        .scoped(ingress, "files.completeUploadExternal", body, cancel)
        .await
    {
        Ok(result) => result,
        Err(error) => return failure(error),
    };
    let returned = result["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|file| file["id"].as_str())
        .collect::<BTreeSet<_>>();
    if expected != returned {
        return DeliveryResult::Uncertain;
    }
    let Ok(receipt) = storage::key("slack-share", &(room, expected)) else {
        return DeliveryResult::Uncertain;
    };
    let cleaned = ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "DELETE FROM messaging_slack_uploads WHERE connection=?1 AND delivery=?2",
                params![ingress.connection_name(), attempt.delivery_id],
            )?;
            Ok(())
        }))
    });
    if cleaned.is_err() || super::mark_thread(ingress, room).is_err() {
        return DeliveryResult::Uncertain;
    }
    DeliveryResult::Confirmed {
        receipt,
        editable_message: None,
    }
}

pub(super) fn attach_spoken_reply(
    connection: &Connection,
    ingress: &Ingress,
    intent: &DeliveryIntent,
    parts: &mut Vec<DeliveryPart>,
) -> Result<(), PreparationFailure> {
    let DeliveryBody::Reply(message) = &intent.body else {
        return Ok(());
    };
    if !connection.settings.files
        || message.text.trim().is_empty()
        || parts.len() >= super::MAX_PARTS
    {
        return Ok(());
    }
    let mode = ingress
        .spoken_reply_mode(&intent.room)
        .unwrap_or(SpokenReplyMode::Off);
    if mode == SpokenReplyMode::Off {
        return Ok(());
    }
    let voice_only = mode == SpokenReplyMode::VoiceNotes;
    let selected = ingress.checked(true, |_| db(ingress.storage().read(|tx| {
        Ok(tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM messaging_deliveries d
             JOIN messaging_effect_sources s ON s.delivery=d.id
             LEFT JOIN messaging_slack_voice_sources v ON v.connection=d.connection AND v.event=s.event
             WHERE d.connection=?1 AND d.id=?2 AND d.sender IS NOT NULL
                AND json_extract(d.intent,'$.room')=json(?3)
                AND (?4=0 OR (v.room=?3 AND v.actor=json_extract(d.sender,'$.id'))))",
            params![ingress.connection_name(),intent.id,serde_json::to_string(&intent.room)?,voice_only],
            |row| row.get::<_,bool>(0))?)
    }))).unwrap_or(false);
    if selected {
        parts.push(part(
            intent,
            DeliveryBody::Reply(Message::text(&message.text)),
            Effect {
                operation_key: Some(
                    storage::key("slack-spoken", &intent.id)
                        .map_err(|_| PreparationFailure::Rejected)?,
                ),
                ..Effect::default()
            },
        ));
    }
    Ok(())
}

pub(super) async fn spoken(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> Option<DeliveryResult> {
    if !attempt
        .payload
        .effect
        .operation_key
        .as_deref()
        .is_some_and(|key| key.starts_with(SPOKEN))
    {
        return None;
    }
    let result = async {
        let artifact = ingress
            .synthesize_reply(attempt)
            .await
            .map_err(|_| DeliveryResult::AttachmentNotPublished)?;
        let bytes = ingress
            .reply_artifact(attempt, &artifact)
            .await
            .map_err(|_| DeliveryResult::AttachmentNotPublished)?;
        let file = match load(ingress, attempt, attempt.part) {
            Ok(Some(file)) => file,
            Ok(None) => {
                allocate(
                    connection,
                    ingress,
                    attempt,
                    "mercury-reply.wav",
                    bytes.len(),
                    cancel,
                )
                .await?
            }
            Err(_) => return Err(retry(Duration::from_secs(1))),
        };
        transfer(
            connection,
            ingress,
            attempt,
            attempt.part,
            &file,
            bytes,
            cancel,
        )
        .await?;
        Ok(share(
            connection,
            ingress,
            attempt,
            &[(&file.id, "mercury-reply.wav")],
            cancel,
        )
        .await)
    }
    .await;
    Some(result.unwrap_or_else(|result| result))
}
