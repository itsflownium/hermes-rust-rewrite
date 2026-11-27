//! Bounded Discord feedback for a leased native progress operation.

use super::{Call, Rest, read_message, snowflake};
use crate::messaging::{
    DeliveryResult,
    runtime::{blocking, db, now},
};
use reqwest::{Method, RequestBuilder, StatusCode};
use rusqlite::params;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_millis(500);
const ACKNOWLEDGED: &str = "👀";

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_discord_feedback (
    connection TEXT NOT NULL, delivery TEXT NOT NULL, ordinal INTEGER NOT NULL, kind TEXT NOT NULL,
    attempt INTEGER NOT NULL, started_ms INTEGER NOT NULL, state TEXT NOT NULL,
    PRIMARY KEY(connection,delivery,ordinal,kind)
);
";

pub(super) async fn publish(rest: &Rest<'_>, channel: &str, typing: bool, reactions: bool) {
    if typing {
        send_once(
            rest,
            "typing",
            rest.bot(Method::POST, &["channels", channel, "typing"]),
        )
        .await;
    }
    if reactions
        && let Some(anchor) = rest
            .attempt
            .reply_anchor
            .as_deref()
            .filter(|id| snowflake(id))
        && let Ok(Ok(message)) =
            tokio::time::timeout(TIMEOUT, read_message(rest, channel, anchor)).await
        && message["id"].as_str() == Some(anchor)
        && message["channel_id"].as_str() == Some(channel)
    {
        send_once(
            rest,
            "reaction",
            rest.bot(
                Method::PUT,
                &[
                    "channels",
                    channel,
                    "messages",
                    anchor,
                    "reactions",
                    ACKNOWLEDGED,
                    "@me",
                ],
            ),
        )
        .await;
    }
}

async fn send_once(rest: &Rest<'_>, kind: &'static str, request: RequestBuilder) {
    if rest
        .ingress
        .recheck_send_destination(rest.attempt, rest.destination)
        .await
        .is_err()
    {
        return;
    }
    let scope = rest.ingress.clone();
    let effect = rest.attempt.clone();
    let (reserved_scope, delivery) = (scope.clone(), effect.delivery_id.clone());
    let reservation = blocking(move || {
        reserved_scope.checked(true, |_| {
            db(reserved_scope.storage().write(|tx| {
                Ok(tx.execute(
                    "INSERT OR IGNORE INTO messaging_discord_feedback
                 (connection,delivery,ordinal,kind,attempt,started_ms,state)
                 VALUES (?1,?2,?3,?4,?5,?6,'attempting')",
                    params![
                        reserved_scope.connection_name(),
                        delivery,
                        effect.part,
                        kind,
                        effect.attempt,
                        now()
                    ],
                )? == 1)
            }))
        })
    })
    .await;
    if reservation != Ok(true) {
        return;
    }
    let state = match tokio::time::timeout(TIMEOUT, rest.execute(request, Call::Mutation)).await {
        Ok(Ok(reply)) if reply.status == StatusCode::NO_CONTENT => "confirmed",
        Ok(Ok(reply)) if reply.failure(Call::Mutation) == DeliveryResult::Rejected => "refused",
        Ok(Err(DeliveryResult::Rejected)) => "refused",
        Ok(Err(DeliveryResult::Retryable { .. })) => "skipped",
        _ => "uncertain",
    };
    // An interrupted or ambiguous feedback attempt never fences the conversation.
    let _ = blocking(move || {
        scope.checked(false, |_| {
            db(scope.storage().write(|tx| {
                tx.execute(
                    "UPDATE messaging_discord_feedback SET state=?6
                 WHERE connection=?1 AND delivery=?2 AND ordinal=?3 AND kind=?4
                   AND attempt=?5 AND state='attempting'",
                    params![
                        scope.connection_name(),
                        effect.delivery_id,
                        effect.part,
                        kind,
                        effect.attempt,
                        state
                    ],
                )?;
                Ok(())
            }))
        })
    })
    .await;
}
