use super::InvocationOutcome;
use crate::messaging::{
    GatewayError, InboundEvent, Ingress, Room,
    runtime::{State, db},
    store::{self, PendingDelivery, Route},
};
use rusqlite::{OptionalExtension, params};

const BACKGROUND_ROUTE: &str = "channel-background";

impl Route {
    pub(in crate::messaging) fn is_background(&self) -> bool {
        Self::is_background_id(&self.id)
    }

    pub(in crate::messaging) fn is_background_id(id: &str) -> bool {
        id.strip_prefix(BACKGROUND_ROUTE)
            .is_some_and(|suffix| suffix.starts_with('-'))
    }
}

impl Ingress {
    pub(in crate::messaging) fn background_route(
        &self,
        room: &Room,
        actor: &str,
        event: &str,
    ) -> Result<Route, GatewayError> {
        Ok(Route {
            id: db(store::key(
                BACKGROUND_ROUTE,
                &(&self.connection, self.installation(), room, actor, event),
            ))?,
            connection: self.connection.clone(),
            room: room.clone(),
            session: None,
            blocked: false,
            revision: 0,
        })
    }

    pub(super) fn background_outcome(
        &self,
        state: &State,
        event: &InboundEvent,
    ) -> Result<Option<InvocationOutcome>, GatewayError> {
        let route = self.background_route(&event.room, &event.sender.id, &event.id)?;
        let outcome = db(self.store.invocation(&route, &event.id, &event.sender.id))?;
        if outcome.is_some()
            && !self.authorize_scoped_command(&state.policy, &event.room, &event.sender)?
        {
            return Ok(Some(InvocationOutcome::Forbidden));
        }
        Ok(outcome)
    }

    pub(in crate::messaging) fn background_delivery_allowed(
        &self,
        state: &State,
        route: &Route,
        delivery: &PendingDelivery,
    ) -> Result<bool, GatewayError> {
        let Some(sender) = &delivery.sender else {
            return Ok(false);
        };
        if !self.authorize_scoped_command(&state.policy, &route.room, sender)? {
            return Ok(false);
        }
        // The ingress payload is removed at completion. Keep the original
        // interaction and requester bound through the outbox source and the
        // native queued receipt, including across send retries.
        let event: Option<String> = db(self.store.read(|tx| {
            Ok(tx
                .query_row(
                    "SELECT n.event FROM messaging_effect_sources s
                 JOIN messaging_native_invocations n ON n.event = s.event
                 WHERE s.delivery = ?1 AND n.connection = ?2 AND n.route = ?3 AND n.actor = ?4",
                    params![delivery.intent.id, route.connection, route.id, sender.id],
                    |row| row.get(0),
                )
                .optional()?)
        }))?;
        let Some(event) = event else { return Ok(false) };
        Ok(self.background_route(&route.room, &sender.id, &event)?.id == route.id)
    }
}
