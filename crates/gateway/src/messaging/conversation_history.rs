use super::{
    super::{
        GatewayError, Ingress,
        runtime::db,
        store::{self, Route},
    },
    InvocationOutcome,
};
use rusqlite::params;
use serde::{Deserialize, Serialize};

const MAX_TURNS: i64 = 4;
const PREVIEW_CHARS: usize = 100;

/// A durable private result references admitted inputs and the exact active
/// conversation binding. History text remains in the original stores.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistoryCursor {
    route_revision: i64,
    binding: String,
    binding_revision: i64,
    session: String,
    events: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HistoryTurn {
    pub prompt: String,
    pub reply: String,
}

pub(super) fn snapshot(scope: &Ingress, route: &Route) -> Result<InvocationOutcome, GatewayError> {
    let route = db(scope.store.route(&route.id))?.ok_or(GatewayError::Forbidden)?;
    let binding = db(scope.store.conversation(&route))?;
    if route.blocked || binding.blocked {
        return Err(GatewayError::Forbidden);
    }
    let Some(session) = binding.session else {
        return Ok(InvocationOutcome::History { turns: Vec::new() });
    };
    let events = db(scope.store.read(|tx| {
        let mut query = tx.prepare(
            "SELECT i.event FROM messaging_ingress i WHERE i.connection=?1 AND i.route=?2
               AND i.stage='done' AND EXISTS(SELECT 1 FROM messaging_effect_sources s
                 JOIN messaging_deliveries d ON d.id=s.delivery
                 WHERE s.event=i.event AND d.connection=i.connection AND d.route=i.route
                   AND d.stage='sent' AND json_type(d.intent,'$.body.reply')='object')
             ORDER BY i.position DESC LIMIT ?3",
        )?;
        Ok(query
            .query_map(params![scope.connection, route.id, MAX_TURNS], |row| {
                row.get(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }))?;
    Ok(InvocationOutcome::HistoryCursor(HistoryCursor {
        route_revision: route.revision,
        binding: binding.id,
        binding_revision: binding.revision,
        session,
        events,
    }))
}

pub(super) fn resolve(
    scope: &Ingress,
    route: &Route,
    cursor: &HistoryCursor,
) -> Result<InvocationOutcome, GatewayError> {
    let route = db(scope.store.route(&route.id))?.ok_or(GatewayError::Forbidden)?;
    let binding = db(scope.store.conversation(&route))?;
    if route.blocked
        || binding.blocked
        || route.revision != cursor.route_revision
        || binding.id != cursor.binding
        || binding.revision != cursor.binding_revision
        || binding.session.as_deref() != Some(cursor.session.as_str())
    {
        return Ok(InvocationOutcome::Stale);
    }
    if cursor.events.is_empty() {
        return Ok(InvocationOutcome::History { turns: Vec::new() });
    }
    let library = db(hermes_library::Library::open(scope.root()))?;
    let record = db(crate::read_record(&db(
        library.session_dir(&cursor.session)
    )?))?;
    let Some(authority) = &record.authority else {
        return Ok(InvocationOutcome::Stale);
    };
    let mut turns = Vec::new();
    for event in cursor.events.iter().rev() {
        let native = db(store::key("channel-input", &(&scope.connection, event)))?;
        let Some(turn) = authority
            .receipts
            .iter()
            .find(|receipt| {
                receipt.input_id.as_deref() == Some(native.as_str())
                    && receipt.status == "terminal"
                    && receipt.outcome.as_deref() == Some("complete")
                    && receipt.channel.is_some()
            })
            .and_then(|receipt| receipt.turn_index)
            .and_then(|index| record.history.turns.get(index))
        else {
            continue;
        };
        let reply = db(store::key("reply", &native))?;
        let text = db(scope.store.notification_text(&route.id, &reply))?.unwrap_or_default();
        turns.push(HistoryTurn {
            prompt: preview(&turn.prompt),
            reply: preview(&text),
        });
    }
    Ok(InvocationOutcome::History { turns })
}

fn preview(text: &str) -> String {
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut characters = text.chars();
    let mut excerpt = characters.by_ref().take(PREVIEW_CHARS).collect::<String>();
    if characters.next().is_some() {
        excerpt.push('…');
    }
    excerpt
}
