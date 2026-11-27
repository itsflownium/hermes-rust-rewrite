use super::{Command, InvocationOutcome, record_invocation};
use crate::{
    Shared,
    messaging::{
        GatewayError, InboundBody, InboundEvent, Ingress, Room, Sender,
        runtime::{State, blocking, db, now, valid_id},
        store::{self, PendingInput, Route, Store},
    },
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

pub(super) const MAX_CHOICES: usize = 25;
const PICKER_LIFETIME_MS: i64 = 5 * 60 * 1000;
const CATALOG_TIMEOUT: Duration = Duration::from_secs(3);

/// A private list of model choices. The native receipt pins the original
/// requester and route, and this state fences the published list.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ModelPicker {
    pub choices: Vec<String>,
    provider: String,
    policy_revision: i64,
    route_revision: i64,
    expires_ms: i64,
    message: Option<String>,
    consumed: bool,
}

pub(super) fn valid_model(model: &str) -> bool {
    valid_id(model) && !model.chars().any(char::is_whitespace)
}

struct ModelAccess {
    provider: String,
    revision: i64,
    models: Vec<String>,
}

impl ModelPicker {
    fn refusal(
        &self,
        provider: &str,
        policy_revision: i64,
        route: &Route,
    ) -> Option<InvocationOutcome> {
        if self.expires_ms <= now() {
            return Some(InvocationOutcome::Expired);
        }
        if self.provider != provider
            || self.policy_revision != policy_revision
            || self.route_revision != route.revision
            || route.blocked
        {
            return Some(InvocationOutcome::Stale);
        }
        self.consumed.then_some(InvocationOutcome::AlreadyClaimed)
    }
}

pub(in crate::messaging) async fn apply_profile_model(
    owner: &Shared,
    scope: &Ingress,
    work: &PendingInput,
    command: &Command,
) -> Result<(), GatewayError> {
    let discover = scope.checked(true, |state| {
        Ok(model_operator(scope, state, work)?
            && state.policy.turn_policy(&work.event.room).model.is_none())
    })?;
    let mut catalog = if discover {
        tokio::time::timeout(
            CATALOG_TIMEOUT,
            crate::methods::model_choices(owner.provider.clone()),
        )
        .await
        .map_or_else(|_| Vec::new(), |(models, _)| models)
    } else {
        Vec::new()
    };
    // The owner-configured default is an explicit selection even if this
    // provider cannot list it through its discovery endpoint.
    if !catalog.contains(&owner.provider.model) {
        catalog.insert(0, owner.provider.model.clone());
    }
    let mut seen = BTreeSet::new();
    catalog.retain(|model| valid_model(model) && seen.insert(model.clone()));
    scope.checked(true, move |state| {
        let access = if model_operator(scope, state, work)? {
            let models = match state.policy.turn_policy(&work.event.room).model {
                Some(model) if valid_model(&model) => vec![model],
                Some(_) => Vec::new(),
                None => catalog,
            };
            Some(ModelAccess {
                provider: scope.model_provider_scope()?,
                revision: state.revision,
                models,
            })
        } else {
            None
        };
        db(scope
            .store
            .execute_profile_model(work, command, access.as_ref()))
    })
}

fn model_operator(
    scope: &Ingress,
    state: &State,
    work: &PendingInput,
) -> Result<bool, GatewayError> {
    Ok(scope
        .route_event(&state.policy, &work.event)?
        .is_ok_and(|route| route.id == work.route.id)
        && scope.authorize_operator(&state.policy, &work.event.room, &work.event.sender)?)
}

impl Ingress {
    pub(super) fn current_model_picker(
        &self,
        state: &State,
        route: &Route,
        actor: &Sender,
        picker: &ModelPicker,
    ) -> Result<Option<InvocationOutcome>, GatewayError> {
        if !self.authorize_operator(&state.policy, &route.room, actor)? {
            return Err(GatewayError::Forbidden);
        }
        let Some(route) = db(self.store.route(&route.id))? else {
            return Ok(Some(InvocationOutcome::Stale));
        };
        Ok(Some(
            picker
                .refusal(&self.model_provider_scope()?, state.revision, &route)
                .unwrap_or_else(|| InvocationOutcome::ModelPicker(picker.clone())),
        ))
    }

    /// Bind the provider-confirmed private message before its component can
    /// select a model. A failed or uncertain edit leaves a picker unusable.
    pub async fn bind_model_picker_message(
        &self,
        event: String,
        room: Room,
        actor: Sender,
        message: String,
    ) -> Result<(), GatewayError> {
        if !valid_id(&event) || !valid_id(&message) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let mut input = InboundEvent {
                    id: event,
                    room,
                    sender: actor,
                    reply_anchor: None,
                    mentioned: true,
                    body: InboundBody::Command(Command::ModelPicker),
                };
                let Ok(route) = scope.route_event(&state.policy, &input)? else {
                    return Err(GatewayError::Forbidden);
                };
                if !scope.authorize_operator(&state.policy, &input.room, &input.sender)? {
                    return Err(GatewayError::Forbidden);
                }
                scope.normalize_event(&mut input)?;
                let provider = scope.model_provider_scope()?;
                db(scope.store.write(|tx| {
                    let Some(route) = store::read_route(tx, &route.id)? else {
                        return Ok(false);
                    };
                    let Some(mut picker) = read_picker(tx, &route, &input.id, &input.sender.id)?
                    else {
                        return Ok(false);
                    };
                    if picker.refusal(&provider, state.revision, &route).is_some()
                        || picker
                            .message
                            .as_ref()
                            .is_some_and(|original| original != &message)
                    {
                        return Ok(false);
                    }
                    picker.message = Some(message);
                    save_picker(tx, &route, &input.id, &input.sender.id, picker)?;
                    Ok(true)
                }))?
                .then_some(())
                .ok_or(GatewayError::Forbidden)
            })
        })
        .await
    }
}

impl Store {
    fn execute_profile_model(
        &self,
        work: &PendingInput,
        command: &Command,
        access: Option<&ModelAccess>,
    ) -> Result<()> {
        self.write(|tx| {
            let committed = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_native_invocations
                  WHERE connection = ?1 AND event = ?2)",
                params![work.route.connection, work.event.id],
                |row| row.get::<_, bool>(0),
            )?;
            if committed {
                return Ok(());
            }
            let route = store::read_route(tx, &work.route.id)?;
            let outcome = match (access, route) {
                (Some(access), Some(route))
                    if !route.blocked && route.revision == work.route.revision =>
                {
                    execute(tx, &route, &work.event.sender.id, command, access)?
                }
                (Some(_), _) => InvocationOutcome::Stale,
                (None, _) => InvocationOutcome::Forbidden,
            };
            record_invocation(
                tx,
                &work.route,
                &work.event.id,
                &work.event.sender.id,
                outcome,
            )?;
            store::set_input(
                tx,
                &work.route,
                &work.event.id,
                "done",
                Some("command_complete"),
            )
        })
    }
}

fn execute(
    tx: &Connection,
    route: &Route,
    actor: &str,
    action: &Command,
    access: &ModelAccess,
) -> Result<InvocationOutcome> {
    match action {
        Command::SelectProfileModel { model } if access.models.contains(model) => {
            apply_model(tx, route, model, &access.provider)?;
            Ok(InvocationOutcome::Applied)
        }
        Command::SelectProfileModel { .. } => Ok(InvocationOutcome::Forbidden),
        Command::ModelPicker if !access.models.is_empty() => {
            Ok(InvocationOutcome::ModelPicker(ModelPicker {
                choices: access.models.iter().take(MAX_CHOICES).cloned().collect(),
                provider: access.provider.clone(),
                policy_revision: access.revision,
                route_revision: route.revision,
                expires_ms: now() + PICKER_LIFETIME_MS,
                message: None,
                consumed: false,
            }))
        }
        Command::ChooseProfileModel {
            picker,
            message,
            option,
        } => {
            let Some(mut choices) = read_picker(tx, route, picker, actor)? else {
                return Ok(InvocationOutcome::Forbidden);
            };
            if let Some(reason) = choices.refusal(&access.provider, access.revision, route) {
                return Ok(reason);
            }
            if choices.message.as_deref() != Some(message.as_str()) {
                return Ok(InvocationOutcome::Forbidden);
            }
            let Some(model) = choices
                .choices
                .get(*option)
                .filter(|model| access.models.contains(model))
            else {
                return Ok(InvocationOutcome::Stale);
            };
            apply_model(tx, route, model, &access.provider)?;
            choices.consumed = true;
            save_picker(tx, route, picker, actor, choices)?;
            Ok(InvocationOutcome::Applied)
        }
        _ => Ok(InvocationOutcome::Unsupported),
    }
}

fn apply_model(tx: &Connection, route: &Route, model: &str, provider: &str) -> Result<()> {
    tx.execute(
        "INSERT INTO messaging_route_models(route, model) VALUES (?1, ?2)
         ON CONFLICT(route) DO UPDATE SET model = excluded.model",
        params![route.id, model],
    )?;
    tx.execute(
        "INSERT INTO messaging_route_model_scopes(route, provider) VALUES (?1, ?2)
         ON CONFLICT(route) DO UPDATE SET provider = excluded.provider",
        params![route.id, provider],
    )?;
    Ok(())
}

fn read_picker(
    tx: &Connection,
    route: &Route,
    event: &str,
    actor: &str,
) -> Result<Option<ModelPicker>> {
    let outcome = tx
        .query_row(
            "SELECT outcome FROM messaging_native_invocations
         WHERE connection = ?1 AND event = ?2 AND route = ?3 AND actor = ?4",
            params![route.connection, event, route.id, actor],
            |row| store::decode(row, 0),
        )
        .optional()?;
    Ok(match outcome {
        Some(InvocationOutcome::ModelPicker(picker)) => Some(picker),
        _ => None,
    })
}

fn save_picker(
    tx: &Connection,
    route: &Route,
    event: &str,
    actor: &str,
    picker: ModelPicker,
) -> Result<()> {
    tx.execute(
        "UPDATE messaging_native_invocations SET outcome = ?5
         WHERE connection = ?1 AND event = ?2 AND route = ?3 AND actor = ?4",
        params![
            route.connection,
            event,
            route.id,
            actor,
            serde_json::to_string(&InvocationOutcome::ModelPicker(picker))?
        ],
    )?;
    Ok(())
}
