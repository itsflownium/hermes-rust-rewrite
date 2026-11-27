use super::{
    Connection, DeliveryResult, controls, failure, fields, literal, mark_thread, part, post,
};
use crate::{
    identity::{channel, string, ts},
    intake,
};
use hermes_gateway_core::messaging::{
    Control, DeliveryBody, DeliveryIntent, DeliveryPart, Effect, GatewayError, InboundBody,
    InboundEvent, Ingress, MarkerRef, Message, PreparationFailure, Room, RoomKind, SendAttempt,
    Sender,
    runtime::{db, now},
    storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_presentations (
    connection TEXT NOT NULL, reply TEXT NOT NULL, progress TEXT NOT NULL, room TEXT NOT NULL,
    actor TEXT NOT NULL, execution INTEGER, message TEXT, expires INTEGER NOT NULL,
    closed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,reply), UNIQUE(connection,progress)
);
";

const STREAM_MS: i64 = 10 * 60 * 1_000;
const APPEND: &str = "slack-stream-append";
const FINISH: &str = "slack-stream-finish";
const READY: &str = "slack-session-ready";

#[derive(Clone)]
pub(super) struct Presentation {
    progress: String,
    actor: String,
    execution: Option<i64>,
    message: Option<String>,
    expires: i64,
    closed: bool,
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<Presentation> {
    Ok(Presentation {
        progress: row.get(0)?,
        actor: row.get(1)?,
        execution: row.get(2)?,
        message: row.get(3)?,
        expires: row.get(4)?,
        closed: row.get(5)?,
    })
}

fn by_reply(
    ingress: &Ingress,
    room: &Room,
    id: &str,
) -> Result<Option<Presentation>, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row("SELECT progress,actor,execution,message,expires,closed FROM messaging_slack_presentations
            WHERE connection=?1 AND room=?2 AND reply=?3",
            params![ingress.connection_name(),serde_json::to_string(room)?,id], decode).optional()?)
    })))
}

pub(super) fn for_reply(
    ingress: &Ingress,
    intent: &DeliveryIntent,
) -> Result<Option<Presentation>, GatewayError> {
    Ok(
        by_reply(ingress, &intent.room, &intent.id)?.filter(|presentation| {
            presentation.message.is_some() && presentation.message == intent.edit_target
        }),
    )
}

pub(super) fn finalize_plan(
    intent: &DeliveryIntent,
    text: Vec<String>,
    plan: &mut Vec<DeliveryPart>,
) -> Result<(), PreparationFailure> {
    let mut budget = 12_000;
    let mut text = text.into_iter().peekable();
    while let Some(size) = text
        .peek()
        .map(|piece| piece.chars().map(super::markup_units).sum::<usize>())
        && size <= budget
    {
        budget -= size;
        let piece = text.next().expect("stream chunk");
        let body = DeliveryBody::Reply(Message::text(piece));
        plan.push(operation(intent, body, APPEND, plan.len())?);
    }
    plan.push(operation(intent, intent.body.clone(), FINISH, plan.len())?);
    plan.extend(text.map(|line| {
        part(
            intent,
            DeliveryBody::Reply(Message::text(line)),
            Effect::default(),
        )
    }));
    Ok(())
}

pub(super) fn ready_plan(
    intent: &DeliveryIntent,
    plan: &mut Vec<DeliveryPart>,
) -> Result<(), PreparationFailure> {
    plan.push(operation(intent, intent.body.clone(), READY, plan.len())?);
    Ok(())
}

fn operation(
    intent: &DeliveryIntent,
    body: DeliveryBody,
    name: &str,
    revision: usize,
) -> Result<DeliveryPart, PreparationFailure> {
    let key =
        storage::key(name, &(&intent.id, revision)).map_err(|_| PreparationFailure::Rejected)?;
    Ok(part(
        intent,
        body,
        Effect {
            operation_key: Some(key),
            ..Effect::default()
        },
    ))
}

pub(super) async fn start(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    reply: &str,
    actor: Option<&Sender>,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    let (Some(thread), Some(actor)) = (room.thread.as_deref(), actor) else {
        return DeliveryResult::Rejected;
    };
    if !connection.settings.agent {
        return DeliveryResult::Rejected;
    }
    let execution = ingress
        .active_execution(room, actor)
        .await
        .ok()
        .flatten()
        .and_then(|value| i64::try_from(value).ok());
    if ingress.recheck_send(attempt).await.is_err() {
        return DeliveryResult::Rejected;
    }
    let started = now();
    let message = if connection.settings.native_streams {
        let mut payload = fields(room);
        if room.kind == RoomKind::Group {
            let user = match connection
                .api
                .scoped(
                    ingress,
                    "users.info",
                    json!({"user":actor.id,"team_id":connection.id.team}),
                    cancel,
                )
                .await
            {
                Ok(user) => user,
                Err(crate::api::Failure::RateLimited(after)) => return super::retry(after),
                Err(crate::api::Failure::Unknown) => {
                    return super::retry(std::time::Duration::from_secs(1));
                }
                Err(_) => return DeliveryResult::Rejected,
            };
            let recipient = &user["user"];
            let Some(team) = string(recipient, "team_id", b"T")
                .filter(|_| recipient["id"] == actor.id && recipient["deleted"] != true)
            else {
                return DeliveryResult::Rejected;
            };
            payload.insert("recipient_user_id".into(), json!(actor.id));
            payload.insert("recipient_team_id".into(), json!(team));
        }
        if ingress.recheck_send(attempt).await.is_err() {
            return DeliveryResult::Rejected;
        }
        match connection
            .api
            .scoped(ingress, "chat.startStream", Value::Object(payload), cancel)
            .await
        {
            Ok(response) if response["channel"] == room.chat => {
                let Some(message) = ts(&response, "ts") else {
                    return DeliveryResult::Uncertain;
                };
                Some(message.to_owned())
            }
            Ok(_) => return DeliveryResult::Uncertain,
            Err(error) => return failure(error),
        }
    } else {
        if let Err(error) = status(connection, ingress, room, thread, "processing", cancel).await {
            return error;
        }
        None
    };
    let recorded = ingress.checked(false, |_| db(ingress.storage().write(|tx| {
        tx.execute("INSERT INTO messaging_slack_presentations
            (connection,reply,progress,room,actor,execution,message,expires) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![ingress.connection_name(),reply,attempt.delivery_id,serde_json::to_string(room)?,
                actor.id,execution,message,started.saturating_add(STREAM_MS)])?;
        Ok(())
    })));
    if recorded.is_err() || mark_thread(ingress, room).is_err() {
        return DeliveryResult::Uncertain;
    }
    let marker = message.unwrap_or_else(|| attempt.delivery_id.clone());
    DeliveryResult::Confirmed {
        receipt: marker.clone(),
        editable_message: Some(marker),
    }
}

async fn status(
    connection: &Connection,
    ingress: &Ingress,
    room: &Room,
    thread: &str,
    status: &str,
    cancel: &CancellationToken,
) -> Result<(), DeliveryResult> {
    let result = connection
        .api
        .scoped(
            ingress,
            "agents.sessions.setStatus",
            json!({
                "channel_id":room.chat,"thread_ts":thread,"status":status,
            }),
            cancel,
        )
        .await
        .map_err(failure)?;
    if result["agent_status"] != status {
        return Err(DeliveryResult::Uncertain);
    }
    Ok(())
}

pub(super) async fn finish(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    text: &str,
    cancel: &CancellationToken,
) -> Option<DeliveryResult> {
    let key = attempt.payload.effect.operation_key.as_deref()?;
    let room = &attempt.payload.room;
    let thread = room.thread.as_deref()?;
    if key.starts_with(READY) {
        return Some(
            match status(connection, ingress, room, thread, "active", cancel).await {
                Ok(()) => {
                    if close(ingress, &attempt.delivery_id).is_err() {
                        DeliveryResult::Uncertain
                    } else {
                        confirmed(attempt.delivery_id.clone())
                    }
                }
                Err(error) => error,
            },
        );
    }
    let append = key.starts_with(APPEND);
    if !append && !key.starts_with(FINISH) {
        return None;
    }
    let presentation = match by_reply(ingress, room, &attempt.delivery_id) {
        Ok(Some(presentation)) => presentation,
        _ => return Some(DeliveryResult::Rejected),
    };
    let Some(message) = presentation.message else {
        return Some(DeliveryResult::Rejected);
    };
    if append && (presentation.closed || now() >= presentation.expires) {
        return Some(post(connection, ingress, attempt, text, None, cancel).await);
    }
    if !append && presentation.closed {
        return Some(confirmed(message));
    }
    let mut feedback = None;
    let result = if append {
        connection
            .api
            .scoped(
                ingress,
                "chat.appendStream",
                json!({
                    "channel":room.chat,"ts":message,"markdown_text":literal(text),
                }),
                cancel,
            )
            .await
    } else {
        feedback = match controls::feedback_actor(connection, ingress, attempt) {
            Ok(actor) => actor,
            Err(_) => return Some(super::retry(std::time::Duration::from_secs(1))),
        };
        let mut body = json!({"channel":room.chat,"ts":message,"session_status":"active"});
        if feedback.is_some() {
            body["blocks"] = json!([controls::feedback_block()]);
        }
        connection
            .api
            .scoped(ingress, "chat.stopStream", body, cancel)
            .await
    };
    Some(match result {
        Ok(value)
            if value["channel"] == room.chat
                && ts(&value, "ts") == Some(&message)
                && (append || close(ingress, &attempt.delivery_id).is_ok()) =>
        {
            if feedback.as_ref().is_some_and(|actor| {
                controls::remember_feedback(ingress, room, &message, actor).is_err()
            }) {
                DeliveryResult::Uncertain
            } else {
                confirmed(message)
            }
        }
        Ok(_) => DeliveryResult::Uncertain,
        Err(error) => failure(error),
    })
}

fn confirmed(receipt: String) -> DeliveryResult {
    DeliveryResult::Confirmed {
        receipt,
        editable_message: None,
    }
}

fn close(ingress: &Ingress, reply: &str) -> Result<(), GatewayError> {
    ingress.checked(false, |_| db(ingress.storage().write(|tx| {
        tx.execute("UPDATE messaging_slack_presentations SET closed=1 WHERE connection=?1 AND reply=?2",
            params![ingress.connection_name(),reply])?;
        Ok(())
    })))
}

pub(super) async fn retire(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    marker: &MarkerRef,
    cancel: &CancellationToken,
) -> Option<DeliveryResult> {
    let room = &attempt.payload.room;
    let row = ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT reply,message,expires,closed FROM messaging_slack_presentations
            WHERE connection=?1 AND room=?2 AND progress=?3",
                    params![
                        ingress.connection_name(),
                        serde_json::to_string(room)?,
                        marker.delivery
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<String>>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, bool>(3)?,
                        ))
                    },
                )
                .optional()?)
        }))
    });
    let (reply, message, expires, closed) = match row {
        Ok(Some(found)) => found,
        Ok(None) => return None,
        Err(_) => return Some(DeliveryResult::Rejected),
    };
    if closed {
        return Some(confirmed(marker.delivery.clone()));
    }
    if let Some(ts) = &message {
        if now() < expires {
            let value = connection
                .api
                .scoped(
                    ingress,
                    "chat.stopStream",
                    json!({
                        "channel":room.chat,"ts":ts,"session_status":"active",
                    }),
                    cancel,
                )
                .await;
            match value {
                Ok(value) if value["channel"] == room.chat && value["ts"] == *ts => {}
                Ok(_) => return Some(DeliveryResult::Uncertain),
                Err(error) => return Some(failure(error)),
            }
        }
    } else if let Some(thread) = &room.thread
        && let Err(error) = status(connection, ingress, room, thread, "active", cancel).await
    {
        return Some(error);
    }
    Some(match close(ingress, &reply) {
        Ok(()) => confirmed(message.unwrap_or(marker.delivery.clone())),
        Err(_) => DeliveryResult::Uncertain,
    })
}

pub(crate) async fn stop(
    connection: &Connection,
    ingress: &Ingress,
    event: &Value,
    wire: &str,
) -> Result<(), GatewayError> {
    if !connection.settings.agent {
        return Ok(());
    }
    let (Some(chat), Some(thread), Some(actor)) = (
        string(event, "channel", b"CDG"),
        ts(event, "thread_ts"),
        string(event, "user", b"UW"),
    ) else {
        return Ok(());
    };
    let Some(room) = channel(chat, Some(thread)) else {
        return Ok(());
    };
    let Some(streams) = event["streaming_message_ts"].as_array() else {
        return Ok(());
    };
    let expected = match streams.as_slice() {
        [] => None,
        [message] => {
            let Some(message) = message
                .as_str()
                .filter(|message| crate::identity::timestamp(message))
            else {
                return Ok(());
            };
            Some(message.to_owned())
        }
        _ => return Ok(()),
    };
    let presentation = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row("SELECT progress,actor,execution,message,expires,closed FROM messaging_slack_presentations
            WHERE connection=?1 AND room=?2 AND actor=?3 AND message IS ?4 AND closed=0 AND expires>?5
            ORDER BY expires DESC LIMIT 1",
            params![ingress.connection_name(),serde_json::to_string(&room)?,actor,expected,now()], decode).optional()?)
    })))?;
    let Some(presentation) = presentation else {
        return Ok(());
    };
    intake::observe(ingress, &room, &presentation.actor).await?;
    let sender = Sender {
        id: actor.to_owned(),
        roles: Default::default(),
    };
    let execution = match ingress.active_execution(&room, &sender).await {
        Ok(Some(execution))
            if presentation.execution.is_some()
                && presentation.execution == i64::try_from(execution).ok() =>
        {
            execution
        }
        _ => return Ok(()),
    };
    let input = InboundEvent {
        id: wire.to_owned(),
        room,
        sender,
        mentioned: true,
        reply_anchor: None,
        body: InboundBody::Control(Control::Interrupt {
            execution_generation: execution,
        }),
    };
    let result = ingress.submit(vec![input], None).await?;
    if result.receipts.first().is_some_and(|receipt| {
        receipt.disposition == hermes_gateway_core::messaging::Disposition::Accepted
    }) {
        ingress
            .retire_marker(MarkerRef {
                delivery: presentation.progress,
                part: 0,
            })
            .await?;
    }
    Ok(())
}
