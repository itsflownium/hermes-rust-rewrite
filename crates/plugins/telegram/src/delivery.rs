use super::{
    Connection,
    address::{Destination, Topic},
    intake,
    updates::Tap,
};
use hermes_gateway_core::messaging::{
    ActorKind, Advance, Control, DeliveryBody, DeliveryIntent, DeliveryPart, DeliveryResult,
    Disposition, Effect, EffectOperation, GatewayError, InboundBody, InboundEvent, Ingress,
    InvocationOutcome, Message, PreparationFailure, Prompt, Response, RetryScope, Room, RoomKind,
    SendAttempt, Sender, SpokenReplyMode,
    runtime::{blocking, db, now},
    storage, voice_codec,
};
use hermes_telegram::{Error, ErrorKind, Method, telegram::Upload};
use rusqlite::{OptionalExtension, params};
use serde_json::{Map, Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use unicode_segmentation::UnicodeSegmentation;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_telegram_prompts (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, ordinal INTEGER NOT NULL,
    room TEXT NOT NULL, action TEXT NOT NULL, actor TEXT NOT NULL, kind TEXT NOT NULL,
    expires INTEGER NOT NULL, message INTEGER,
    PRIMARY KEY(connection,delivery,ordinal)
);
CREATE TABLE IF NOT EXISTS messaging_telegram_callbacks (
    connection TEXT NOT NULL, token TEXT NOT NULL, response TEXT NOT NULL,
    delivery TEXT NOT NULL, ordinal INTEGER NOT NULL,
    PRIMARY KEY(connection,token),
    FOREIGN KEY(connection,delivery,ordinal) REFERENCES messaging_telegram_prompts(connection,delivery,ordinal)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS messaging_telegram_prompt_message
    ON messaging_telegram_prompts(connection,room,message,actor);
";

const MAX_TEXT: usize = 4_000;
const MAX_PARTS: usize = 128;
const RETIRED_PROMPT_MS: i64 = 24 * 60 * 60 * 1_000;
const VOICE_PART: &str = "telegram-voice-";

pub(super) fn prepare(intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    Destination::from_room(&intent.room).ok_or(PreparationFailure::Rejected)?;
    let make = |body, effect, edit_target| DeliveryPart {
        room: intent.room.clone(),
        body,
        effect,
        edit_target,
    };
    match &intent.body {
        DeliveryBody::Retire { marker } => Ok(vec![make(
            intent.body.clone(),
            Effect {
                operation: EffectOperation::Retire {
                    marker: marker.clone(),
                },
                ..Effect::default()
            },
            None,
        )]),
        DeliveryBody::Reply(message) => {
            let mut parts = Vec::new();
            for text in split(&message.text) {
                let first = parts.is_empty();
                let edit = if first {
                    intent.edit_target.clone()
                } else {
                    None
                };
                parts.push(make(
                    DeliveryBody::Reply(Message::text(text)),
                    Effect {
                        publish_after_definite_edit_refusal: edit.is_some(),
                        ..Effect::default()
                    },
                    edit,
                ));
            }
            for file in &message.attachments {
                parts.push(make(
                    DeliveryBody::Reply(Message {
                        text: String::new(),
                        attachments: vec![file.clone()],
                    }),
                    Effect::default(),
                    None,
                ));
            }
            if parts.is_empty() || parts.len() > MAX_PARTS {
                return Err(PreparationFailure::Rejected);
            }
            Ok(parts)
        }
        _ => Ok(vec![make(intent.body.clone(), Effect::default(), None)]),
    }
}

pub(super) fn prepare_with_ingress(
    intent: &DeliveryIntent,
    ingress: &Ingress,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    let mut plan = prepare(intent)?;
    let DeliveryBody::Reply(message) = &intent.body else {
        return Ok(plan);
    };
    if !message.text.trim().is_empty()
        && plan.len() < MAX_PARTS
        && voice_for(ingress, intent).unwrap_or(false)
    {
        let voice = DeliveryPart {
            room: intent.room.clone(),
            body: DeliveryBody::Reply(Message::text(&message.text)),
            effect: Effect {
                operation_key: Some(
                    storage::key("telegram-voice", &intent.id)
                        .map_err(|_| PreparationFailure::Rejected)?,
                ),
                ..Effect::default()
            },
            edit_target: None,
        };
        if plan.len() == 1 && plan[0].edit_target.is_none() && message.attachments.is_empty() {
            plan[0].effect.attachment_fallback_for = Some(0);
            plan.insert(0, voice);
        } else {
            plan.push(voice);
        }
    } else if plan[0].edit_target.is_none() {
        let notifications = ingress
            .checked(true, |state| {
                Ok(state.policy.delivery_policy(&intent.room).notifications)
            })
            .unwrap_or(false);
        plan[0].effect.visibility.notify = notifications;
    }
    Ok(plan)
}

fn voice_for(ingress: &Ingress, intent: &DeliveryIntent) -> Result<bool, GatewayError> {
    let mode = ingress.spoken_reply_mode(&intent.room)?;
    if mode == SpokenReplyMode::Off {
        return Ok(false);
    }
    let voice_note = mode == SpokenReplyMode::VoiceNotes;
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_deliveries d
              JOIN messaging_effect_sources s ON s.delivery=d.id
              LEFT JOIN messaging_telegram_voice v ON v.connection=d.connection AND v.event=s.event
              WHERE d.connection=?1 AND d.id=?2 AND d.sender IS NOT NULL
                AND json_extract(d.intent,'$.room')=json(?3)
                AND (?4=0 OR (v.room=?3 AND v.actor=json_extract(d.sender,'$.id'))))",
                params![
                    ingress.connection_name(),
                    intent.id,
                    serde_json::to_string(&intent.room)?,
                    voice_note
                ],
                |row| row.get(0),
            )?)
        }))
    })
}

fn split(text: &str) -> Vec<String> {
    fn add(out: &mut Vec<String>, part: &mut String, size: &mut usize, piece: &str, units: usize) {
        if *size + units > MAX_TEXT {
            out.push(std::mem::take(part));
            *size = 0;
        }
        part.push_str(piece);
        *size += units;
    }
    let mut out = Vec::new();
    let mut part = String::new();
    let mut size = 0;
    for grapheme in text.graphemes(true) {
        let units = grapheme.encode_utf16().count();
        if units <= MAX_TEXT {
            add(&mut out, &mut part, &mut size, grapheme, units);
        } else {
            for character in grapheme.chars() {
                add(
                    &mut out,
                    &mut part,
                    &mut size,
                    character.encode_utf8(&mut [0; 4]),
                    character.len_utf16(),
                );
            }
        }
    }
    if !part.is_empty() {
        out.push(part);
    }
    out
}

fn bounded(text: &str, units: usize) -> &str {
    let mut length = 0;
    for (index, character) in text.char_indices() {
        length += character.len_utf16();
        if length > units {
            return &text[..index];
        }
    }
    text
}

fn positive_id(value: &str) -> Option<i64> {
    let id = value.parse::<i64>().ok()?;
    (id > 0 && id.to_string() == value).then_some(id)
}

fn failure(error: Error, editing: bool, uploading: bool) -> DeliveryResult {
    match error.kind {
        ErrorKind::RateLimited => DeliveryResult::Retryable {
            after: Duration::from_secs(error.retry_after.unwrap_or(3)),
            scope: RetryScope::Account,
        },
        ErrorKind::Rejected if editing && error.message.contains("message is not modified") => {
            DeliveryResult::Uncertain
        }
        ErrorKind::Rejected if editing => DeliveryResult::EditNotPublished,
        ErrorKind::Rejected if uploading => DeliveryResult::AttachmentNotPublished,
        ErrorKind::Rejected | ErrorKind::Credentials | ErrorKind::Conflict => {
            DeliveryResult::Rejected
        }
        ErrorKind::Invalid | ErrorKind::Retryable | ErrorKind::Cancelled => {
            DeliveryResult::Uncertain
        }
    }
}

fn confirmed(destination: Destination, result: &Value) -> Option<i64> {
    destination
        .matches_message(result)
        .then(|| result["message_id"].as_i64().filter(|id| *id > 0))
        .flatten()
}

fn publication(id: i64, editable: bool) -> DeliveryResult {
    let receipt = id.to_string();
    DeliveryResult::Confirmed {
        editable_message: editable.then(|| receipt.clone()),
        receipt,
    }
}

fn healthy(connection: &Connection) {
    super::lock(&connection.verification)["send_health"] = json!("ready");
}

fn reply_fields(fields: &mut Map<String, Value>, text: &str, rich: bool) -> Option<Value> {
    let formatted = super::format::html(text, rich);
    fields.insert("text".into(), json!(text));
    fields.insert("link_preview_options".into(), json!({"is_disabled":true}));
    let formatted = formatted.filter(|html| html != text)?;
    let plain = Value::Object(fields.clone());
    if rich {
        fields.remove("text");
        fields.remove("link_preview_options");
        fields.insert(
            "rich_message".into(),
            json!({"html":formatted,"skip_entity_detection":true}),
        );
    } else {
        fields.insert("text".into(), json!(formatted));
        fields.insert("parse_mode".into(), json!("HTML"));
    }
    Some(plain)
}

pub(super) async fn send(
    connection: &Connection,
    attempt: SendAttempt,
    ingress: &Ingress,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    let Some(destination) = Destination::from_room(room) else {
        return DeliveryResult::Rejected;
    };
    let mut fields = destination.send_fields();
    let mut method = Method::SendMessage;
    let mut upload = None;
    let mut retired = None;
    let mut presentation = None;
    let mut prompt_delivery = false;
    let mut plain_fallback = None;
    let editing = attempt.payload.edit_target.is_some();
    let initial = attempt.part == 0 || attempt.payload.effect.attachment_fallback_for == Some(0);
    let anchored = initial
        && !editing
        && matches!(
            attempt.payload.body,
            DeliveryBody::Reply(_) | DeliveryBody::Prompt { .. }
        );
    if anchored && let Some(anchor) = &attempt.reply_anchor {
        let Some(message) = positive_id(anchor) else {
            return DeliveryResult::Rejected;
        };
        fields.insert(
            "reply_parameters".into(),
            json!({"message_id":message,"allow_sending_without_reply":false}),
        );
    }
    fields.insert(
        "disable_notification".into(),
        json!(!attempt.payload.effect.visibility.notify),
    );
    match &attempt.payload.body {
        DeliveryBody::Reply(message) => {
            if attempt
                .payload
                .effect
                .operation_key
                .as_deref()
                .is_some_and(|key| key.starts_with(VOICE_PART))
            {
                let Ok(file) = voice_upload(ingress, &attempt).await else {
                    return DeliveryResult::AttachmentNotPublished;
                };
                method = Method::SendVoice;
                upload = Some(file);
            } else if let Some(target) = &attempt.payload.edit_target {
                let Some(id) = positive_id(target) else {
                    return DeliveryResult::Rejected;
                };
                method = Method::EditMessageText;
                fields = Map::from_iter([
                    ("chat_id".into(), json!(destination.chat)),
                    ("message_id".into(), json!(id)),
                ]);
                plain_fallback = reply_fields(&mut fields, &message.text, false);
            } else if let Some(file) = message.attachments.first() {
                let Ok(bytes) = ingress.reply_artifact(&attempt, file).await else {
                    return DeliveryResult::AttachmentNotPublished;
                };
                method = Method::SendDocument;
                upload = Some(Upload {
                    field: "document",
                    filename: file.name().into(),
                    content_type: None,
                    bytes,
                });
            } else {
                plain_fallback = reply_fields(
                    &mut fields,
                    &message.text,
                    connection.settings.rich_messages,
                );
                if fields.contains_key("rich_message") {
                    method = Method::SendRichMessage;
                }
            }
        }
        DeliveryBody::Progress { text, reply_id } => {
            if connection.settings.drafts
                && connection.bot.topic_mode
                && room.kind == RoomKind::Direct
            {
                method = Method::SendMessageDraft;
                fields.remove("disable_notification");
                let digest = ring::digest::digest(&ring::digest::SHA256, reply_id.as_bytes());
                let id =
                    i32::from_be_bytes(digest.as_ref()[..4].try_into().expect("four digest bytes"))
                        & i32::MAX;
                let id = id.max(1);
                fields.insert("draft_id".into(), json!(id));
                presentation = Some(format!("draft:{id}"));
                fields.insert("text".into(), json!(bounded(text, MAX_TEXT)));
            } else if connection.settings.progress_messages {
                fields.insert("text".into(), json!(bounded(text, MAX_TEXT)));
            } else {
                if matches!(destination.topic, Some(Topic::ChannelDirect(_))) {
                    return DeliveryResult::Rejected;
                }
                method = Method::SendChatAction;
                fields.remove("disable_notification");
                fields.insert("action".into(), json!("typing"));
                presentation = Some("typing".into());
            }
        }
        DeliveryBody::Prompt { action_id, prompt } => {
            let (text, choices) = prompt_options(prompt);
            let keyboard = match stage_buttons(ingress, &attempt, action_id, &choices) {
                Ok(keyboard) => keyboard,
                Err(GatewayError::TemporarilyUnavailable) => {
                    return DeliveryResult::Retryable {
                        after: Duration::from_secs(1),
                        scope: RetryScope::Part,
                    };
                }
                Err(_) => return DeliveryResult::Rejected,
            };
            fields.insert("text".into(), json!(bounded(text, MAX_TEXT)));
            if !keyboard.is_empty() {
                fields.insert("reply_markup".into(), json!({"inline_keyboard":keyboard}));
            }
            prompt_delivery = true;
        }
        DeliveryBody::Retire { .. } => {
            let Some(id) = attempt
                .effect
                .marker
                .as_ref()
                .and_then(|marker| positive_id(&marker.id))
            else {
                return DeliveryResult::Rejected;
            };
            method = Method::EditMessageReplyMarkup;
            fields = Map::from_iter([
                ("chat_id".into(), json!(destination.chat)),
                ("message_id".into(), json!(id)),
                ("reply_markup".into(), json!({"inline_keyboard":[]})),
            ]);
            retired = Some(id);
        }
    }
    if ingress.recheck_send(&attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }
    let uploading = upload.is_some();
    let mut result = if let Some(upload) = upload {
        connection
            .client
            .upload(method, Value::Object(fields), upload, cancel)
            .await
    } else {
        connection
            .client
            .request(method, Value::Object(fields), cancel)
            .await
    };
    if let Some(plain) = plain_fallback
        && result.as_ref().is_err_and(|error| {
            error.kind == ErrorKind::Rejected
                && (matches!(method, Method::SendRichMessage)
                    || error.message.contains("parse entities"))
        })
        && ingress.recheck_send(&attempt).await.is_ok()
    {
        let plain_method = if editing {
            Method::EditMessageText
        } else {
            Method::SendMessage
        };
        result = connection.client.request(plain_method, plain, cancel).await;
    }
    let outcome = match result {
        Err(error) => failure(error, editing, uploading),
        Ok(result) => {
            if let Some(receipt) = presentation {
                if result.as_bool() == Some(true) {
                    healthy(connection);
                    return DeliveryResult::Confirmed {
                        receipt,
                        editable_message: None,
                    };
                }
            } else if let Some(id) = retired {
                if confirmed(destination, &result) == Some(id) {
                    healthy(connection);
                    return publication(id, false);
                }
            } else if let Some(id) = confirmed(destination, &result)
                && attempt
                    .payload
                    .edit_target
                    .as_deref()
                    .is_none_or(|expected| positive_id(expected) == Some(id))
            {
                if prompt_delivery && confirm_prompt(ingress, &attempt, id).is_err() {
                    return DeliveryResult::Uncertain;
                }
                healthy(connection);
                return publication(id, !uploading);
            }
            DeliveryResult::Uncertain
        }
    };
    super::lock(&connection.verification)["send_health"] = json!(match outcome {
        DeliveryResult::Retryable { .. } => "cooldown",
        DeliveryResult::Uncertain => "uncertain",
        _ => "rejected",
    });
    outcome
}

async fn voice_upload(ingress: &Ingress, attempt: &SendAttempt) -> Result<Upload, GatewayError> {
    let artifact = ingress.synthesize_reply(attempt).await?;
    let wav = ingress.reply_artifact(attempt, &artifact).await?;
    let voice = blocking(move || voice_codec::encode_voice_note(&wav)).await?;
    Ok(Upload {
        field: "voice",
        filename: "reply.ogg".into(),
        content_type: Some("audio/ogg"),
        bytes: voice.bytes,
    })
}

fn prompt_options(prompt: &Prompt) -> (&str, Vec<(&str, Response)>) {
    match prompt {
        Prompt::Approval {
            text,
            allow_session,
            allow_persistent,
        } => {
            let mut choices = vec![("Approve once", Response::Approve)];
            if *allow_session {
                choices.push(("Approve for this session", Response::ApproveForSession));
            }
            if *allow_persistent {
                choices.push(("Always approve", Response::ApprovePersistently));
            }
            choices.push(("Deny", Response::Deny));
            (text, choices)
        }
        Prompt::Clarification { question, choices } => (
            question,
            choices
                .iter()
                .take(50)
                .map(|choice| (choice.as_str(), Response::Clarify(choice.clone())))
                .collect(),
        ),
    }
}

fn stage_buttons(
    ingress: &Ingress,
    attempt: &SendAttempt,
    action: &str,
    choices: &[(&str, Response)],
) -> Result<Vec<Vec<Value>>, GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            let observed = now();
            let connection = ingress.connection_name();
            tx.execute(
                "DELETE FROM messaging_telegram_prompts WHERE connection=?1 AND expires<=?2",
                params![connection, observed.saturating_sub(RETIRED_PROMPT_MS)],
            )?;
            let Some((actor, kind, expires)) = tx
                .query_row(
                    "SELECT a.recipient,a.type,p.expires FROM messaging_actions a
             JOIN messaging_action_authority p USING(id)
             JOIN messaging_deliveries d ON d.route=a.route AND d.connection=a.connection
             WHERE a.connection=?1 AND d.id=?2 AND a.id=?3 AND a.resolved IS NULL AND p.expires>?4",
                    params![connection, attempt.delivery_id, action, observed],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?
            else {
                return Ok(Err(GatewayError::Forbidden));
            };
            let room = serde_json::to_string(&attempt.payload.room)?;
            tx.execute(
                "INSERT OR IGNORE INTO messaging_telegram_prompts
            (connection,delivery,ordinal,room,action,actor,kind,expires)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    connection,
                    attempt.delivery_id,
                    attempt.part,
                    room,
                    action,
                    actor,
                    kind,
                    expires
                ],
            )?;
            let mut keyboard = Vec::new();
            for (index, (label, response)) in choices.iter().enumerate() {
                let key = storage::key(
                    "telegram-control",
                    &(
                        ingress.connection_name(),
                        &attempt.delivery_id,
                        attempt.part,
                        action,
                        index,
                    ),
                )?;
                let token = key
                    .strip_prefix("telegram-control-")
                    .expect("stable callback namespace");
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_telegram_callbacks VALUES(?1,?2,?3,?4,?5)",
                    params![
                        connection,
                        token,
                        serde_json::to_string(response)?,
                        attempt.delivery_id,
                        attempt.part
                    ],
                )?;
                keyboard.push(vec![
                    json!({"text":bounded(label, 64),"callback_data":token}),
                ]);
            }
            Ok(Ok(keyboard))
        }))?
    })
}

fn confirm_prompt(
    ingress: &Ingress,
    attempt: &SendAttempt,
    message: i64,
) -> Result<(), GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_telegram_prompts SET message=?4
            WHERE connection=?1 AND delivery=?2 AND ordinal=?3",
                params![
                    ingress.connection_name(),
                    attempt.delivery_id,
                    attempt.part,
                    message
                ],
            )?;
            Ok(())
        }))
    })
}

pub(super) fn clarification(
    ingress: &Ingress,
    room: &Room,
    sender: &Sender,
    replying_to: i64,
) -> Result<Option<String>, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT action FROM messaging_telegram_prompts
            WHERE connection=?1 AND room=?2 AND message=?3 AND actor=?4 AND kind='clarification'",
                    params![
                        ingress.connection_name(),
                        serde_json::to_string(room)?,
                        replying_to,
                        sender.id
                    ],
                    |row| row.get(0),
                )
                .optional()?)
        }))
    })
}

pub(super) async fn callback(
    connection: &Connection,
    ingress: &Ingress,
    tap: Tap<'_>,
    id: String,
    advance: Advance,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let room = tap.room;
    let user = tap.user.to_string();
    let prompt = ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT p.action,b.response,p.expires FROM messaging_telegram_callbacks b
            JOIN messaging_telegram_prompts p USING(connection,delivery,ordinal)
            WHERE b.connection=?1 AND b.token=?2 AND p.message=?3 AND p.room=?4 AND p.actor=?5",
                    params![
                        ingress.connection_name(),
                        tap.data,
                        tap.message,
                        serde_json::to_string(&room)?,
                        user
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?)
        }))
    })?;
    let mut outcome = None;
    if let Some((action_id, response, expires)) = prompt {
        let roles =
            intake::observe_actor(connection, ingress, &room, &user, ActorKind::Person, cancel)
                .await?;
        let response = serde_json::from_str(&response).map_err(|_| GatewayError::Invalid)?;
        let sender = Sender { id: user, roles };
        let event = InboundEvent {
            id: id.clone(),
            room: room.clone(),
            sender: sender.clone(),
            reply_anchor: Some(tap.message.to_string()),
            mentioned: true,
            body: InboundBody::Control(Control::Respond {
                action_id,
                response,
            }),
        };
        let result = ingress.submit(vec![event], Some(advance)).await?;
        if result
            .receipts
            .first()
            .is_some_and(|receipt| receipt.disposition == Disposition::Accepted)
        {
            outcome = Some((id, room, sender, expires));
        }
    } else {
        intake::ignore(
            ingress,
            id,
            &json!({"callback":tap.id,"room":room,"user":user}),
            advance,
        )
        .await?;
    }
    let ingress = ingress.clone();
    let client = connection.client.clone();
    let cancel = cancel.clone();
    let callback_id = tap.id.to_owned();
    tokio::spawn(async move {
        let answer = if let Some((id, room, sender, expires)) = outcome {
            let invoked = tokio::time::timeout(Duration::from_millis(400), async {
                loop {
                    let outcome = ingress
                        .callback_outcome(id.clone(), room.clone(), sender.clone(), expires)
                        .await;
                    if outcome != Ok(None) {
                        break outcome;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await;
            match invoked {
                Ok(Ok(Some(InvocationOutcome::Applied))) => "Response recorded",
                Ok(Ok(Some(_)) | Err(_)) => "This action is unavailable",
                Err(_) => "Response received",
                Ok(Ok(None)) => "Response received",
            }
        } else {
            "This action is unavailable"
        };
        let fields = json!({"callback_query_id":callback_id,"text":answer});
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            client.request(Method::AnswerCallbackQuery, fields, &cancel),
        )
        .await;
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_at_unicode_boundaries_and_preserves_content() {
        for unit in ["x", "🧑🏿‍🚀", "e\u{301}"] {
            let text = unit.repeat(4_353);
            let pieces = split(&text);
            assert!(pieces.len() > 1);
            assert!(
                pieces
                    .iter()
                    .all(|part| part.encode_utf16().count() <= MAX_TEXT)
            );
            assert!(pieces.iter().all(|part| part.len() % unit.len() == 0));
            assert_eq!(pieces.concat(), text);
        }
        let unusual = format!("a{}", "\u{301}".repeat(MAX_TEXT * 2));
        let pieces = split(&unusual);
        assert_eq!(pieces.concat(), unusual);
        assert!(
            pieces
                .iter()
                .all(|part| part.encode_utf16().count() <= MAX_TEXT)
        );
    }
}
