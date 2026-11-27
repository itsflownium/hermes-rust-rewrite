use crate::{Connection, api::Failure, intake};
use hermes_gateway_core::messaging::{
    GatewayError, Ingress, RequesterInvocation,
    runtime::{blocking, db, now},
};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_callbacks (
    connection TEXT NOT NULL, event TEXT NOT NULL, payload TEXT,
    expires INTEGER NOT NULL, eligible INTEGER NOT NULL, stage TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
";

const RESPONSE_MS: i64 = 29 * 60 * 1_000;

#[derive(Deserialize, Serialize)]
struct Callback {
    request: RequesterInvocation,
    endpoint: String,
    text: String,
}

pub(crate) async fn stage(
    connection: &Connection,
    ingress: &Ingress,
    request: RequesterInvocation,
    endpoint: &str,
    text: &str,
) -> Result<(), GatewayError> {
    if connection.api.validate_response(endpoint).is_err() {
        return Ok(());
    }
    let scope = ingress.clone();
    let callback = Callback {
        request,
        endpoint: endpoint.into(),
        text: text.into(),
    };
    blocking(move || {
        let json = serde_json::to_string(&callback).map_err(|_| GatewayError::Invalid)?;
        let opaque = db(hermes_engine::secret::seal(scope.root(), &json))?;
        scope.checked(true, |_| db(scope.storage().write(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO messaging_slack_callbacks VALUES(?1,?2,?3,?4,?5,'pending')",
                params![scope.connection_name(), callback.request.id, opaque, now() + RESPONSE_MS, now()],
            )?;
            Ok(())
        })))
    }).await
}

pub(crate) fn recover(ingress: &Ingress) -> Result<(), GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_slack_callbacks SET stage='uncertain',payload=NULL
                    WHERE connection=?1 AND stage='sending'",
                [ingress.connection_name()],
            )?;
            Ok(())
        }))
    })
}

pub(crate) async fn resume(connection: &Connection, ingress: &Ingress, cancel: &CancellationToken) {
    let Ok(rows) = ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            tx.execute(
                "UPDATE messaging_slack_callbacks SET stage='expired',payload=NULL
                    WHERE connection=?1 AND stage='pending' AND expires<=?2",
                params![ingress.connection_name(), now()],
            )?;
            let mut query = tx.prepare(
                "SELECT event,payload FROM messaging_slack_callbacks
            WHERE connection=?1 AND stage='pending' AND eligible<=?2 AND expires>?2
            ORDER BY rowid LIMIT 8",
            )?;
            let rows = query.query_map(params![ingress.connection_name(), now()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }))
    }) else {
        return;
    };
    for (event, sealed) in rows {
        let parsed = hermes_engine::secret::unseal(&sealed)
            .and_then(|plain| Ok(serde_json::from_str::<Callback>(&plain)?));
        let outcome = match parsed {
            Ok(callback) if callback.request.id == event => {
                deliver(connection, ingress, callback, cancel).await
            }
            _ => Err(Failure::Rejected),
        };
        let (stage, delay) = match outcome {
            Ok(()) => ("sent", Duration::ZERO),
            Err(Failure::RateLimited(delay)) => ("pending", delay),
            Err(Failure::Unknown | Failure::Cancelled) => ("uncertain", Duration::ZERO),
            Err(_) => ("refused", Duration::ZERO),
        };
        let eligible = now().saturating_add(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
        let _ = ingress.checked(false, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "UPDATE messaging_slack_callbacks SET stage=?3,eligible=?4,
                        payload=CASE WHEN ?3='pending' THEN payload END
                        WHERE connection=?1 AND event=?2 AND stage IN ('pending','sending')",
                    params![ingress.connection_name(), event, stage, eligible],
                )?;
                Ok(())
            }))
        });
    }
}

async fn deliver(
    connection: &Connection,
    ingress: &Ingress,
    callback: Callback,
    cancel: &CancellationToken,
) -> Result<(), Failure> {
    let request = &callback.request;
    ingress
        .requester_command_response(request)
        .map_err(|_| Failure::Permission)?;
    if let Err(error) = intake::conversation(
        connection,
        ingress,
        &request.room,
        Some(&request.sender.id),
        cancel,
    )
    .await
    {
        return Err(match error {
            GatewayError::TemporarilyUnavailable => Failure::RateLimited(Duration::from_secs(1)),
            GatewayError::StaleConnection => Failure::Cancelled,
            _ => Failure::Permission,
        });
    }
    ingress
        .requester_command_response(request)
        .map_err(|_| Failure::Permission)?;
    ingress
        .checked(true, |_| {
            db(ingress.storage().write(|tx| {
                Ok(tx.execute(
                    "UPDATE messaging_slack_callbacks SET stage='sending'
            WHERE connection=?1 AND event=?2 AND stage='pending' AND expires>?3",
                    params![ingress.connection_name(), request.id, now()],
                )? == 1)
            }))
        })
        .map_err(|_| Failure::Unknown)?
        .then_some(())
        .ok_or(Failure::Rejected)?;
    connection
        .api
        .private_response(
            &callback.endpoint,
            json!({"response_type":"ephemeral","replace_original":false,"text":callback.text}),
            cancel,
        )
        .await
}
