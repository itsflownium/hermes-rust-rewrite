use super::{Connection, DeliveryResult, failure, literal, sender};
use crate::{
    identity::{channel, string, ts},
    intake,
};
use hermes_gateway_core::messaging::{
    ActorKind, Control, Disposition, GatewayError, InboundBody, InboundEvent, Ingress, Prompt,
    Response, Room, SendAttempt, Sender,
    runtime::{db, now},
    storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_prompts (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, room TEXT NOT NULL,
    action TEXT NOT NULL, actor TEXT NOT NULL, kind TEXT NOT NULL, expires INTEGER NOT NULL,
    message TEXT, PRIMARY KEY(connection,delivery)
);
CREATE TABLE IF NOT EXISTS messaging_slack_buttons (
    connection TEXT NOT NULL, token TEXT NOT NULL, delivery TEXT NOT NULL, response TEXT NOT NULL,
    PRIMARY KEY(connection,token),
    FOREIGN KEY(connection,delivery) REFERENCES messaging_slack_prompts(connection,delivery) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS messaging_slack_feedback (
    connection TEXT NOT NULL, room TEXT NOT NULL, message TEXT NOT NULL,
    actor TEXT NOT NULL, value TEXT, PRIMARY KEY(connection,room,message)
);
";

pub(super) fn buttons(
    ingress: &Ingress,
    attempt: &SendAttempt,
    action: &str,
    prompt: &Prompt,
) -> Result<(String, Option<Value>), GatewayError> {
    let (text, options) = match prompt {
        Prompt::Approval {
            text,
            allow_session,
            allow_persistent,
        } => {
            let mut options = vec![("Approve once", Response::Approve)];
            if *allow_session {
                options.push(("Approve for this session", Response::ApproveForSession));
            }
            if *allow_persistent {
                options.push(("Always approve", Response::ApprovePersistently));
            }
            options.push(("Deny", Response::Deny));
            (text, options)
        }
        Prompt::Clarification { question, choices } => (
            question,
            choices
                .iter()
                .take(20)
                .map(|label| (label.as_str(), Response::Clarify(label.clone())))
                .collect(),
        ),
    };
    let text = super::chunks(text)
        .into_iter()
        .next()
        .unwrap_or_else(|| "Mercury needs a response.".into());
    let entries = ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            let scope = ingress.connection_name();
            let outstanding = tx
                .query_row(
                    "SELECT a.recipient,a.type,p.expires FROM messaging_actions a
             JOIN messaging_action_authority p USING(id)
             JOIN messaging_deliveries d ON d.connection=a.connection AND d.route=a.route
             WHERE d.id=?1 AND a.connection=?2 AND a.id=?3 AND a.resolved IS NULL AND p.expires>?4",
                    params![attempt.delivery_id, scope, action, now()],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?;
            let Some((actor, kind, expires)) = outstanding else {
                return Ok(Err(GatewayError::Forbidden));
            };
            tx.execute(
                "INSERT OR IGNORE INTO messaging_slack_prompts
            (connection,delivery,room,action,actor,kind,expires) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    scope,
                    attempt.delivery_id,
                    serde_json::to_string(&attempt.payload.room)?,
                    action,
                    actor,
                    kind,
                    expires
                ],
            )?;
            tx.execute(
                "DELETE FROM messaging_slack_prompts WHERE connection=?1 AND expires<?2",
                params![scope, now().saturating_sub(24 * 60 * 60 * 1_000)],
            )?;
            let mut entries = Vec::new();
            for (index, (label, response)) in options.iter().enumerate() {
                let token = storage::key(
                    "slack-button",
                    &(scope, &attempt.delivery_id, action, index),
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_slack_buttons VALUES(?1,?2,?3,?4)",
                    params![
                        scope,
                        token,
                        attempt.delivery_id,
                        serde_json::to_string(response)?
                    ],
                )?;
                entries.push(json!({
                    "type":"button","action_id":format!("mercury-respond-{index}"),
                    "text":{"type":"plain_text","text":label.chars().take(70).collect::<String>()},
                    "value":token,
                }));
            }
            Ok(Ok(entries))
        }))?
    })?;
    let blocks = (!entries.is_empty()).then(|| {
        json!([
            {"type":"section","text":{"type":"mrkdwn","text":literal(&text),"verbatim":true}},
            {"type":"actions","elements":entries}
        ])
    });
    Ok((text, blocks))
}

pub(super) fn confirm(
    ingress: &Ingress,
    attempt: &SendAttempt,
    ts: &str,
) -> Result<(), GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_slack_prompts SET message=?3 WHERE connection=?1 AND delivery=?2",
                params![ingress.connection_name(), attempt.delivery_id, ts],
            )?;
            Ok(())
        }))
    })
}

const FEEDBACK: &str = "mercury-feedback";

pub(super) fn feedback_actor(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
) -> Result<Option<String>, GatewayError> {
    if !connection.settings.feedback_buttons {
        return Ok(None);
    }
    let Some(sender) = sender(ingress, &attempt.delivery_id)? else {
        return Ok(None);
    };
    ingress.checked(true, |state| {
        Ok(ingress
            .authorized_identity(&state.policy, &attempt.payload.room, &sender)?
            .filter(|(_, kind)| *kind == ActorKind::Person)
            .map(|(sender, _)| sender.id))
    })
}

pub(super) fn feedback_block() -> Value {
    json!({"type":"context_actions","elements":[{
        "type":"feedback_buttons","action_id":FEEDBACK,
        "positive_button":{"text":{"type":"plain_text","text":"Helpful"},"value":"positive"},
        "negative_button":{"text":{"type":"plain_text","text":"Unhelpful"},"value":"negative"},
    }]})
}

pub(super) fn remember_feedback(
    ingress: &Ingress,
    room: &Room,
    message: &str,
    actor: &str,
) -> Result<(), GatewayError> {
    ingress.checked(false, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_slack_feedback(connection,room,message,actor)
                    VALUES(?1,?2,?3,?4)",
                params![
                    ingress.connection_name(),
                    serde_json::to_string(room)?,
                    message,
                    actor
                ],
            )?;
            Ok(())
        }))
    })
}

async fn record_feedback(
    connection: &Connection,
    ingress: &Ingress,
    room: &Room,
    message: &str,
    actor: &str,
    action: &Value,
) -> Result<Option<Value>, GatewayError> {
    let Some(value) = action["value"]
        .as_str()
        .filter(|value| matches!(*value, "positive" | "negative"))
    else {
        return Ok(None);
    };
    if !connection.settings.feedback_buttons {
        return Ok(None);
    }
    let key = db(serde_json::to_string(room).map_err(anyhow::Error::from))?;
    let matches_card = ingress.checked(true, |_| {
        db(ingress.storage().read(|db| {
            Ok(db.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_slack_feedback
                        WHERE connection=?1 AND room=?2 AND message=?3 AND actor=?4)",
                params![ingress.connection_name(), key, message, actor],
                |row| row.get::<_, bool>(0),
            )?)
        }))
    })?;
    if !matches_card {
        return Ok(None);
    }
    intake::observe(ingress, room, actor).await?;
    let sender = Sender {
        id: actor.into(),
        roles: Default::default(),
    };
    ingress.checked(true, |state| {
        if !ingress.authorize_sender(&state.policy, room, &sender)? {
            return Ok(None);
        }
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_slack_feedback SET value=coalesce(value,?5)
                        WHERE connection=?1 AND room=?2 AND message=?3 AND actor=?4",
                params![ingress.connection_name(), key, message, actor, value],
            )?;
            Ok(Some(
                json!({"response_type":"ephemeral","text":"Mercury recorded your feedback."}),
            ))
        }))
    })
}

pub(crate) async fn interaction(
    connection: &Connection,
    ingress: &Ingress,
    payload: &Value,
    id: &str,
) -> Result<Option<Value>, GatewayError> {
    if payload["type"] != "block_actions" {
        return Ok(None);
    }
    let actor = string(&payload["user"], "id", b"UW");
    let chat = string(&payload["channel"], "id", b"CDG");
    let message = &payload["message"];
    let receipt = ts(message, "ts");
    let [button] = payload["actions"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
    else {
        return Ok(None);
    };
    let (Some(actor), Some(chat), Some(receipt)) = (actor, chat, receipt) else {
        return Ok(None);
    };
    if string(message, "bot_id", b"B") != Some(connection.id.bot.as_str())
        || payload
            .pointer("/container/message_ts")
            .and_then(Value::as_str)
            != Some(receipt)
        || payload
            .pointer("/container/channel_id")
            .and_then(Value::as_str)
            != Some(chat)
    {
        return Ok(None);
    }
    let Some(room) = channel(chat, message["thread_ts"].as_str()) else {
        return Ok(None);
    };
    if button["action_id"] == FEEDBACK {
        return record_feedback(connection, ingress, &room, receipt, actor, button).await;
    }
    let Some(token) = button["value"].as_str().filter(|token| {
        button["action_id"]
            .as_str()
            .is_some_and(|label| label.starts_with("mercury-respond-"))
            && token.starts_with("slack-button-")
            && token.len() == "slack-button-".len() + 64
    }) else {
        return Ok(None);
    };
    let stored = ingress.checked(true, |_| {
        db(ingress.storage().read(|db| {
            Ok(db
                .query_row(
                    "SELECT p.action,b.response,p.expires FROM messaging_slack_buttons b
             JOIN messaging_slack_prompts p USING(connection,delivery)
             WHERE p.connection=?1 AND p.room=?2 AND p.message=?3 AND p.actor=?4 AND b.token=?5",
                    params![
                        ingress.connection_name(),
                        serde_json::to_string(&room)?,
                        receipt,
                        actor,
                        token
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            storage::decode::<Response>(row, 1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .optional()?)
        }))
    })?;
    let Some((action_id, response, expires)) = stored else {
        return Ok(None);
    };
    if expires <= now() {
        return Ok(None);
    }
    intake::observe(ingress, &room, actor).await?;
    let event = InboundEvent {
        id: id.to_owned(),
        room,
        sender: Sender {
            id: actor.to_owned(),
            roles: Default::default(),
        },
        mentioned: true,
        reply_anchor: None,
        body: InboundBody::Control(Control::Respond {
            action_id,
            response,
        }),
    };
    let result = ingress.submit(vec![event], None).await?;
    Ok(result
        .receipts
        .first()
        .filter(|receipt| receipt.disposition == Disposition::Accepted)
        .map(|_| json!({"response_type":"ephemeral","text":"Mercury received your response."})))
}

pub(crate) fn text_response(
    connection: &Connection,
    ingress: &Ingress,
    event: &InboundEvent,
    text: &str,
) -> Result<Option<Control>, GatewayError> {
    if !connection.settings.typed_clarification {
        return Ok(None);
    }
    ingress.checked(true, |_| {
        db(ingress.storage().read(|tx| {
            let mut query = tx.prepare(
                "SELECT p.action FROM messaging_slack_prompts p
            JOIN messaging_actions a ON a.id=p.action AND a.connection=p.connection
            WHERE p.connection=?1 AND p.room=?2 AND p.actor=?3 AND p.kind='clarification'
                AND p.message IS NOT NULL AND p.expires>?4 AND a.resolved IS NULL LIMIT 2",
            )?;
            let mut matching = query.query_map(
                params![
                    ingress.connection_name(),
                    serde_json::to_string(&event.room)?,
                    event.sender.id,
                    now()
                ],
                |row| row.get::<_, String>(0),
            )?;
            let first = matching.next().transpose()?;
            let single = matching.next().transpose()?.is_none();
            Ok(first.filter(|_| single).map(|action_id| Control::Respond {
                action_id,
                response: Response::Clarify(text.to_owned()),
            }))
        }))
    })
}

pub(super) async fn retire(
    connection: &Connection,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let Some(receipt) = attempt
        .effect
        .marker
        .as_ref()
        .map(|receipt| receipt.id.as_str())
    else {
        return DeliveryResult::Rejected;
    };
    let request = json!({"channel":attempt.payload.room.chat,"ts":receipt,"blocks":[]});
    match connection
        .api
        .scoped(ingress, "chat.update", request, cancel)
        .await
    {
        Ok(value)
            if value["channel"] == attempt.payload.room.chat
                && ts(&value, "ts") == Some(receipt) =>
        {
            DeliveryResult::Confirmed {
                receipt: receipt.to_owned(),
                editable_message: None,
            }
        }
        Ok(_) => DeliveryResult::Uncertain,
        Err(error) => failure(error),
    }
}
