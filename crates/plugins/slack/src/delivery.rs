mod controls;
mod files;
mod streams;

pub(crate) use controls::{interaction, text_response};
pub(crate) use streams::stop;
pub(crate) const CONTROLS: &str = controls::TABLES;
pub(crate) const STREAMS: &str = streams::TABLES;
pub(crate) const FILES: &str = files::TABLES;

use crate::{
    Connection,
    api::Failure,
    identity::{room as valid_room, ts},
    intake,
};
use hermes_gateway_core::messaging::{
    DeliveryBody, DeliveryIntent, DeliveryPart, DeliveryResult, Effect, EffectOperation,
    EffectVisibility, GatewayError, Ingress, Message, PreparationFailure, RetryScope, Room,
    SendAttempt, Sender, runtime::db, storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Map, Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use unicode_segmentation::UnicodeSegmentation;

const MAX_TEXT: usize = 2_800;
const MAX_PARTS: usize = 128;

pub(crate) fn prepare(
    connection: &Connection,
    intent: &DeliveryIntent,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    prepare_plan(connection, intent, None)
}

pub(crate) fn prepare_scoped(
    connection: &Connection,
    intent: &DeliveryIntent,
    ingress: &Ingress,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    let mut parts = prepare_plan(connection, intent, Some(ingress))?;
    files::attach_spoken_reply(connection, ingress, intent, &mut parts)?;
    Ok(parts)
}

fn prepare_plan(
    connection: &Connection,
    intent: &DeliveryIntent,
    ingress: Option<&Ingress>,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    if !valid_room(&intent.room) {
        return Err(PreparationFailure::Rejected);
    }
    match &intent.body {
        DeliveryBody::Reply(message) => {
            let mut plan = Vec::new();
            let stream = if let Some(ingress) = ingress {
                streams::for_reply(ingress, intent).map_err(|_| PreparationFailure::Rejected)?
            } else {
                None
            };
            let split = chunks(&message.text);
            if stream.is_some() {
                streams::finalize_plan(intent, split, &mut plan)?;
            } else {
                let broadcast = if let Some(ingress) = ingress {
                    broadcast_granted(connection, ingress, &intent.room, &intent.id)
                        .map_err(|_| PreparationFailure::Rejected)?
                } else {
                    false
                };
                for (index, text) in split.into_iter().enumerate() {
                    plan.push(part(
                        intent,
                        DeliveryBody::Reply(Message::text(text)),
                        Effect {
                            visibility: EffectVisibility {
                                notify: broadcast && index == 0,
                                mention: false,
                            },
                            ..Effect::default()
                        },
                    ));
                }
            }
            files::append_plan(connection, intent, &message.attachments, &mut plan)?;
            if stream.is_none()
                && connection.settings.agent
                && !plan.is_empty()
                && intent.room.thread.is_some()
            {
                streams::ready_plan(intent, &mut plan)?;
            }
            if plan.is_empty() || plan.len() > MAX_PARTS {
                return Err(PreparationFailure::Rejected);
            }
            Ok(plan)
        }
        DeliveryBody::Retire { marker } => Ok(vec![part(
            intent,
            intent.body.clone(),
            Effect {
                operation: EffectOperation::Retire {
                    marker: marker.clone(),
                },
                ..Effect::default()
            },
        )]),
        _ => Ok(vec![part(intent, intent.body.clone(), Effect::default())]),
    }
}

fn part(intent: &DeliveryIntent, body: DeliveryBody, effect: Effect) -> DeliveryPart {
    DeliveryPart {
        room: intent.room.clone(),
        body,
        effect,
        edit_target: None,
    }
}

fn chunks(text: &str) -> Vec<String> {
    fn add(
        parts: &mut Vec<String>,
        current: &mut String,
        size: &mut usize,
        piece: &str,
        units: usize,
    ) {
        if *size + units > MAX_TEXT {
            parts.push(std::mem::take(current));
            *size = 0;
        }
        current.push_str(piece);
        *size += units;
    }
    let mut pieces = Vec::new();
    let mut current = String::new();
    let mut size = 0;
    for grapheme in text.graphemes(true) {
        let units = grapheme.chars().map(markup_units).sum();
        if units <= MAX_TEXT {
            add(&mut pieces, &mut current, &mut size, grapheme, units);
        } else {
            for character in grapheme.chars() {
                add(
                    &mut pieces,
                    &mut current,
                    &mut size,
                    character.encode_utf8(&mut [0; 4]),
                    markup_units(character),
                );
            }
        }
    }
    if !current.is_empty() {
        pieces.push(current);
    }
    pieces
}

fn markup_units(character: char) -> usize {
    match character {
        '&' => 5,
        '<' | '>' => 4,
        _ => character.len_utf16(),
    }
}

fn literal(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn fields(room: &Room) -> Map<String, Value> {
    let mut fields = Map::from_iter([("channel".into(), json!(room.chat))]);
    if let Some(thread) = &room.thread {
        fields.insert("thread_ts".into(), json!(thread));
    }
    fields
}

fn sender(ingress: &Ingress, delivery: &str) -> Result<Option<Sender>, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT sender FROM messaging_deliveries WHERE connection=?1 AND id=?2",
                    params![ingress.connection_name(), delivery],
                    |row| storage::optional_decode(row, 0),
                )
                .optional()?
                .flatten())
        }))
    })
}

fn broadcast_granted(
    connection: &Connection,
    ingress: &Ingress,
    room: &Room,
    delivery: &str,
) -> Result<bool, GatewayError> {
    if room.thread.is_none() || !connection.settings.broadcast_channels.contains(&room.chat) {
        return Ok(false);
    }
    let Some(sender) = sender(ingress, delivery)? else {
        return Ok(false);
    };
    let mut parent = room.clone();
    parent.thread = None;
    ingress.checked(true, |state| {
        let policy = &state.policy;
        if !policy.delivery_policy(&parent).notifications
            || !policy.delivery_policy(room).notifications
        {
            return Ok(false);
        }
        Ok(ingress
            .authorized_identity(policy, room, &sender)?
            .is_some_and(|(actor, kind)| {
                kind == hermes_gateway_core::messaging::ActorKind::Person
                    && policy.grants_sender(&parent, &actor, false)
            }))
    })
}

pub(crate) async fn send(
    connection: &Connection,
    attempt: SendAttempt,
    ingress: &Ingress,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    if !valid_room(room) || ingress.recheck_send(&attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }
    let actor = match sender(ingress, &attempt.delivery_id) {
        Ok(actor) => actor,
        Err(_) => return retry(Duration::from_secs(1)),
    };
    match intake::conversation(
        connection,
        ingress,
        room,
        actor.as_ref().map(|sender| sender.id.as_str()),
        cancel,
    )
    .await
    {
        Ok(()) => {}
        Err(GatewayError::TemporarilyUnavailable) => return retry(Duration::from_secs(1)),
        Err(_) => return DeliveryResult::Rejected,
    }
    if ingress.recheck_send(&attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }

    let result = match &attempt.payload.effect.operation {
        EffectOperation::Upload { artifact, step } => {
            files::upload(connection, ingress, &attempt, artifact, step, cancel).await
        }
        EffectOperation::Publish { .. } => {
            files::publish(connection, ingress, &attempt, cancel).await
        }
        EffectOperation::Retire { marker } => {
            if let Some(result) =
                streams::retire(connection, ingress, &attempt, marker, cancel).await
            {
                result
            } else {
                controls::retire(connection, ingress, &attempt, cancel).await
            }
        }
        EffectOperation::Message => match &attempt.payload.body {
            DeliveryBody::Progress { reply_id, .. } => {
                match broadcast_granted(connection, ingress, room, &attempt.delivery_id) {
                    Ok(false) => {
                        streams::start(
                            connection,
                            ingress,
                            &attempt,
                            reply_id,
                            actor.as_ref(),
                            cancel,
                        )
                        .await
                    }
                    Ok(true) => DeliveryResult::Rejected,
                    Err(_) => retry(Duration::from_secs(1)),
                }
            }
            DeliveryBody::Reply(message) => {
                if let Some(result) =
                    streams::finish(connection, ingress, &attempt, &message.text, cancel).await
                {
                    result
                } else if let Some(result) =
                    files::spoken(connection, ingress, &attempt, cancel).await
                {
                    result
                } else {
                    post(connection, ingress, &attempt, &message.text, None, cancel).await
                }
            }
            DeliveryBody::Prompt { action_id, prompt } => {
                match controls::buttons(ingress, &attempt, action_id, prompt) {
                    Ok((text, blocks)) => {
                        let result =
                            post(connection, ingress, &attempt, &text, blocks, cancel).await;
                        if let DeliveryResult::Confirmed { receipt, .. } = &result
                            && controls::confirm(ingress, &attempt, receipt).is_err()
                        {
                            return DeliveryResult::Uncertain;
                        }
                        result
                    }
                    Err(GatewayError::TemporarilyUnavailable) => retry(Duration::from_secs(1)),
                    Err(_) => DeliveryResult::Rejected,
                }
            }
            _ => DeliveryResult::Rejected,
        },
        _ => DeliveryResult::Rejected,
    };
    crate::lock(&connection.verification)["web_api"] = json!(match result {
        DeliveryResult::Confirmed { .. } | DeliveryResult::EffectConfirmed(_) => "ready",
        DeliveryResult::Retryable { .. } => "backoff",
        DeliveryResult::Uncertain => "uncertain",
        _ => "refused",
    });
    result
}

async fn post(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    text: &str,
    blocks: Option<Value>,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    let mut request = fields(room);
    if attempt.payload.effect.visibility.notify {
        match broadcast_granted(connection, ingress, room, &attempt.delivery_id) {
            Ok(true) => {}
            Ok(false) => return DeliveryResult::Rejected,
            Err(_) => return retry(Duration::from_secs(1)),
        }
        request.insert("reply_broadcast".into(), json!(true));
    }
    let escaped = literal(text);
    request.insert("text".into(), json!(escaped));
    request.insert(
        "unfurl_links".into(),
        json!(connection.settings.link_unfurls),
    );
    request.insert(
        "unfurl_media".into(),
        json!(connection.settings.media_unfurls),
    );
    request.insert("parse".into(), json!("none"));
    let feedback = if blocks.is_none() && attempt.part == 0 {
        match controls::feedback_actor(connection, ingress, attempt) {
            Ok(actor) => actor,
            Err(_) => return retry(Duration::from_secs(1)),
        }
    } else {
        None
    };
    let blocks = blocks.or_else(|| {
        (connection.settings.block_kit || feedback.is_some()).then(|| {
            let mut items = vec![json!({
                "type":"section","text":{"type":"mrkdwn","text":escaped,"verbatim":true}
            })];
            if feedback.is_some() {
                items.push(controls::feedback_block());
            }
            json!(items)
        })
    });
    if let Some(blocks) = blocks {
        request.insert("blocks".into(), blocks);
    }
    let response = match connection
        .api
        .scoped(ingress, "chat.postMessage", Value::Object(request), cancel)
        .await
    {
        Ok(response) => response,
        Err(error) => return failure(error),
    };
    let Some(receipt) = returned_message(room, &response) else {
        return DeliveryResult::Uncertain;
    };
    if mark_thread(ingress, room).is_err() {
        return DeliveryResult::Uncertain;
    }
    if let Some(actor) = feedback
        && controls::remember_feedback(ingress, room, receipt, &actor).is_err()
    {
        return DeliveryResult::Uncertain;
    }
    DeliveryResult::Confirmed {
        receipt: receipt.into(),
        editable_message: Some(receipt.into()),
    }
}

fn returned_message<'a>(room: &Room, response: &'a Value) -> Option<&'a str> {
    if response["channel"] != room.chat
        || response
            .pointer("/message/thread_ts")
            .and_then(Value::as_str)
            != room.thread.as_deref()
    {
        return None;
    }
    ts(response, "ts")
}

fn mark_thread(ingress: &Ingress, room: &Room) -> Result<(), GatewayError> {
    if let Some(thread) = &room.thread {
        ingress.checked(false, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_slack_threads VALUES(?1,?2,?3)",
                    params![ingress.connection_name(), room.chat, thread],
                )?;
                Ok(())
            }))
        })?;
    }
    Ok(())
}

fn retry(after: Duration) -> DeliveryResult {
    DeliveryResult::Retryable {
        after,
        scope: RetryScope::Part,
    }
}

fn failure(error: Failure) -> DeliveryResult {
    match error {
        Failure::RateLimited(after) => retry(after),
        Failure::Unknown | Failure::Cancelled => DeliveryResult::Uncertain,
        _ => DeliveryResult::Rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_splits_count_escaped_markup_and_keep_graphemes_together() {
        let unit = "🧑‍🚀<&>";
        let text = unit.repeat(2_000);
        let pieces = chunks(&text);
        assert!(pieces.len() > 1);
        assert_eq!(pieces.concat(), text);
        assert!(
            pieces
                .iter()
                .all(|piece| literal(piece).encode_utf16().count() <= MAX_TEXT)
        );
        assert_eq!(
            literal("<@UACTOR> <!channel>"),
            "&lt;@UACTOR&gt; &lt;!channel&gt;"
        );
    }
}
