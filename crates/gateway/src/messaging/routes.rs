use super::{
    runtime::{Ingress, blocking, db, valid_id},
    store::{Route, Store, key, read_route},
    types::*,
};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_route_bindings (
    destination TEXT PRIMARY KEY REFERENCES messaging_routes(id),
    binding TEXT NOT NULL REFERENCES messaging_routes(id)
);
CREATE INDEX IF NOT EXISTS messaging_binding_members ON messaging_route_bindings(binding);
CREATE TABLE IF NOT EXISTS messaging_verified_destinations (
    route TEXT PRIMARY KEY REFERENCES messaging_routes(id),
    expires INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_destination_scope (
    route TEXT PRIMARY KEY REFERENCES messaging_verified_destinations(route), scope TEXT NOT NULL
);
";

impl Ingress {
    /// Record scoped, provider-verified write access. This allows the owner to
    /// configure a home destination before a conversation exists.
    pub async fn verify_destination(
        &self,
        room: Room,
        expires_ms: i64,
    ) -> Result<String, GatewayError> {
        if !self.capabilities().text_delivery {
            return Err(GatewayError::Unsupported);
        }
        if !valid_id(&room.chat)
            || room.thread.as_deref().is_some_and(|id| !valid_id(id))
            || expires_ms <= chrono::Utc::now().timestamp_millis()
            || room.thread.is_some() && !self.capabilities().threads
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                if state.policy.room_policy(&room).is_none() {
                    return Err(GatewayError::Forbidden);
                }
                let route = db(Route::new(&scope.connection, room, None))?;
                db(scope.store.verify_destination(&route, expires_ms, &scope.transport_scope()?))?;
                Ok(route.id)
            })
        })
        .await
    }
}

impl Store {
    pub(super) fn conversation(&self, route: &Route) -> Result<Route> {
        self.read(|db| conversation(db, route))
    }

    pub(super) fn share_conversation(&self, destination: &Route, binding: &Route) -> Result<()> {
        self.write(|tx| share_conversation(tx, destination, binding))
    }

    pub(super) fn allows_notification(&self, route: &str, now: i64, scope: &str) -> Result<bool> {
        self.read(|db| Ok(destination_permission(db, route, now, scope)?.unwrap_or(true)))
    }

    fn verify_destination(&self, route: &Route, expires_ms: i64, scope: &str) -> Result<()> {
        self.write(|tx| {
            insert_route(tx, route)?;
            tx.execute(
                "INSERT INTO messaging_verified_destinations(route,expires) VALUES (?1,?2)
                 ON CONFLICT(route) DO UPDATE SET expires = excluded.expires",
                params![route.id, expires_ms],
            )?;
            tx.execute(
                "INSERT INTO messaging_destination_scope(route,scope) VALUES (?1,?2)
                 ON CONFLICT(route) DO UPDATE SET scope=excluded.scope",
                params![route.id,scope],
            )?;
            Ok(())
        })
    }

    pub(super) fn notification(
        &self,
        route: &Route,
        producer: &str,
        text: String,
    ) -> Result<String> {
        ensure!(
            !text.trim().is_empty() && text.len() <= super::runtime::MAX_TEXT,
            "Invalid messaging notification"
        );
        let id = key("notification", &producer)?;
        self.enqueue(
            route,
            &DeliveryIntent {
                id: id.clone(),
                room: route.room.clone(),
                reply_anchor: None,
                edit_target: None,
                body: DeliveryBody::Reply(Message::text(text)),
            },
        )?;
        Ok(id)
    }
}

pub fn insert_route(tx: &Transaction<'_>, route: &Route) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_routes(id,connection,room) VALUES (?1,?2,?3)",
        params![
            route.id,
            route.connection,
            serde_json::to_string(&route.room)?
        ],
    )?;
    Ok(())
}

pub(super) fn destination_permission(db: &Connection, route: &str, now: i64, scope: &str) -> Result<Option<bool>> {
    Ok(db.query_row(
        "SELECT d.expires > ?2 AND COALESCE(s.scope, '') = ?3 FROM messaging_verified_destinations d
         LEFT JOIN messaging_destination_scope s USING(route) WHERE d.route = ?1",
        params![route,now,scope], |row| row.get(0),
    ).optional()?)
}

pub(super) fn conversation(db: &Connection, route: &Route) -> Result<Route> {
    let binding = db
        .query_row(
            "SELECT binding FROM messaging_route_bindings WHERE destination = ?1",
            [&route.id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match binding {
        Some(id) => read_route(db, &id)?
            .ok_or_else(|| anyhow::anyhow!("Messaging session binding was removed")),
        None => Ok(route.clone()),
    }
}

/// Only destinations without an established session can join another
/// binding. A previously admitted turn remains with its original owner.
pub(super) fn share_conversation(
    tx: &Transaction<'_>,
    destination: &Route,
    binding: &Route,
) -> Result<()> {
    ensure!(
        destination.connection == binding.connection && destination.id != binding.id,
        "Messaging session binding must stay within one verified account"
    );
    ensure!(
        !destination.is_background() && !binding.is_background(),
        "A background job has its own session"
    );
    let current = tx
        .query_row(
            "SELECT binding FROM messaging_route_bindings WHERE destination = ?1",
            [&destination.id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if current.as_deref() == Some(binding.id.as_str()) {
        return Ok(());
    }
    ensure!(
        current.is_none(),
        "The destination already shares a conversation"
    );
    let valid = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_routes WHERE id = ?1 AND session IS NULL AND blocked = 0)
                  AND NOT EXISTS(SELECT 1 FROM messaging_route_bindings WHERE destination = ?2 OR binding = ?1)
                  AND NOT EXISTS(SELECT 1 FROM messaging_quoted_context
                    WHERE route IN (?1, ?2) AND context IS NOT NULL)
                  AND NOT EXISTS(SELECT 1 FROM messaging_ingress WHERE route = ?1 AND stage NOT IN ('refused','done'))
                  AND EXISTS(SELECT 1 FROM messaging_routes WHERE id = ?2 AND blocked = 0)",
                params![destination.id,binding.id], |row| row.get::<_,bool>(0),
            )?;
    ensure!(
        valid,
        "Both bindings must be available and the destination must be idle and unbound"
    );
    tx.execute(
        "INSERT INTO messaging_route_bindings(destination,binding) VALUES (?1,?2)",
        params![destination.id, binding.id],
    )?;
    Ok(())
}
