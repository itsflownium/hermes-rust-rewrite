use crate::{
    Connection,
    api::Failure,
    identity::{string, ts},
    intake,
};
use hermes_gateway_core::messaging::{
    GatewayError, InboundBody, InboundEvent, Ingress, Message, ResourceSource, Sender,
    runtime::{MAX_TEXT, blocking, db, now},
    storage,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_reactions (
    connection TEXT NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL,
    expires INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(connection,event)
);
";

const OPERATION: &str = "slack-reaction";
const RETENTION_MS: i64 = 10 * 60 * 1000;

#[derive(Deserialize, Serialize)]
struct Reaction {
    name: String,
    actor: String,
    channel: String,
    message: String,
}

pub(crate) async fn stage(
    connection: &Connection,
    ingress: &Ingress,
    event: &Value,
    wire: &str,
) -> Result<(), GatewayError> {
    let Some(name) = event["reaction"].as_str() else {
        return Ok(());
    };
    let Some(trigger) = connection.settings.reaction_triggers.get(name) else {
        return Ok(());
    };
    let item = &event["item"];
    let (Some(actor), Some(channel), Some(message)) = (
        string(event, "user", b"UW"),
        string(item, "channel", b"CDG"),
        ts(item, "ts"),
    ) else {
        return Ok(());
    };
    if item["type"] != "message" || trigger.source.chat != channel || actor == connection.id.user {
        return Ok(());
    }
    let reaction = Reaction {
        name: name.into(),
        actor: actor.into(),
        channel: channel.into(),
        message: message.into(),
    };
    let scope = ingress.clone();
    let wire = wire.to_owned();
    blocking(move || scope.checked(true, |_| db(scope.storage().write(|tx| {
        tx.execute(
            "INSERT OR IGNORE INTO messaging_slack_reactions(connection,event,source,expires)
             VALUES(?1,?2,?3,?4)",
            params![scope.connection_name(), wire, serde_json::to_string(&reaction)?, now() + RETENTION_MS],
        )?;
        Ok(())
    })))).await
}

pub(crate) async fn resume(connection: &Connection, ingress: &Ingress, cancel: &CancellationToken) {
    if connection.settings.reaction_triggers.is_empty() {
        return;
    }
    let Ok(pending) = ingress.checked(true, |_| {
        db(ingress.storage().read(|db| {
            let mut query = db.prepare(
                "SELECT event,source FROM messaging_slack_reactions
             WHERE connection=?1 AND done=0 AND expires>?2 ORDER BY rowid LIMIT 16",
            )?;
            let rows = query.query_map(params![ingress.connection_name(), now()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    storage::decode::<Reaction>(row, 1)?,
                ))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }))
    }) else {
        return;
    };
    for (id, reaction) in pending {
        match admit(connection, ingress, &id, reaction, cancel).await {
            Err(GatewayError::TemporarilyUnavailable) => continue,
            Err(GatewayError::StaleConnection) => return,
            _ => {}
        }
        let _ = ingress.checked(true, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "UPDATE messaging_slack_reactions SET done=1 WHERE connection=?1 AND event=?2",
                    params![ingress.connection_name(), id],
                )?;
                Ok(())
            }))
        });
    }
}

async fn admit(
    connection: &Connection,
    ingress: &Ingress,
    id: &str,
    reaction: Reaction,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let trigger = connection
        .settings
        .reaction_triggers
        .get(&reaction.name)
        .filter(|trigger| trigger.source.chat == reaction.channel)
        .ok_or(GatewayError::Forbidden)?;
    let resource = format!("{}/{}", reaction.channel, reaction.name);
    let actor = &reaction.actor;
    let sender = Sender {
        id: actor.clone(),
        roles: Default::default(),
    };
    let identity = connection
        .api
        .scoped(
            ingress,
            "users.info",
            json!({"user":actor,"team_id":connection.id.team}),
            cancel,
        )
        .await
        .map_err(Failure::read)?;
    let user = &identity["user"];
    if user["id"] != *actor
        || user["is_bot"] != false
        || user["is_app_user"] == true
        || user["deleted"] == true
    {
        return Err(GatewayError::Forbidden);
    }
    for room in [&trigger.source, &trigger.destination] {
        intake::conversation(connection, ingress, room, Some(actor), cancel).await?;
        intake::observe(ingress, room, actor).await?;
        if !ingress.checked(true, |state| {
            ingress.authorize_sender(&state.policy, room, &sender)
        })? {
            return Err(GatewayError::Forbidden);
        }
    }
    ingress
        .authorize_resource(OPERATION, &resource, &trigger.destination, &sender)
        .await?;
    let item = connection.api.scoped(
        ingress, "reactions.get",
        json!({"channel":reaction.channel,"timestamp":reaction.message,"team_id":connection.id.team}),
        cancel,
    ).await.map_err(Failure::read)?;
    let post = &item["message"];
    let thread = ts(post, "thread_ts").or_else(|| ts(post, "ts"));
    if item["type"] != "message"
        || item["channel"] != reaction.channel
        || ts(post, "ts") != Some(&reaction.message)
        || !trigger.source.thread.as_deref().map_or_else(
            || post["thread_ts"].is_null() || thread == Some(&reaction.message),
            |root| thread == Some(root),
        )
    {
        return Err(GatewayError::Forbidden);
    }
    let mut text = trigger.prompt.clone();
    if trigger.destination == trigger.source
        && let Some(quoted) = post["text"].as_str().filter(|text| !text.contains('\0'))
    {
        let budget = MAX_TEXT - text.len() - 32;
        text.push_str("\n\nSlack message\n");
        text.push_str(&quoted[..quoted.floor_char_boundary(budget.min(quoted.len()))]);
    }
    ingress
        .submit_resource_event(
            ResourceSource {
                operation: OPERATION.into(),
                resource,
            },
            InboundEvent {
                id: id.into(),
                room: trigger.destination.clone(),
                sender,
                reply_anchor: trigger.destination.thread.clone(),
                mentioned: true,
                body: InboundBody::Message(Message::text(text)),
            },
            None,
        )
        .await?;
    Ok(())
}
