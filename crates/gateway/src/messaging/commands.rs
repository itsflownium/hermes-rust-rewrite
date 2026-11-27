use super::{
    runtime::{Ingress, blocking, db, valid_id},
    store::{self, PendingInput, Route, Store},
    types::*,
};
use crate::{LiveSession, lock};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[path = "conversation_history.rs"]
mod history;
pub use history::{HistoryCursor, HistoryTurn};

#[path = "profile_models.rs"]
mod profile_models;
pub use profile_models::ModelPicker;
pub(super) use profile_models::apply_profile_model;

#[path = "requester_commands.rs"]
mod requester;
pub use requester::{RequesterAuthority, RequesterCommand, RequesterInvocation};

#[path = "background.rs"]
mod background;

pub(super) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_native_invocations (
    connection TEXT NOT NULL, event TEXT NOT NULL, route TEXT NOT NULL, actor TEXT NOT NULL,
    outcome TEXT NOT NULL, PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_route_models (
    route TEXT PRIMARY KEY REFERENCES messaging_routes(id), model TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_route_model_scopes (
    route TEXT PRIMARY KEY REFERENCES messaging_route_models(route), provider TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messaging_home_routes (
    connection TEXT PRIMARY KEY, route TEXT NOT NULL REFERENCES messaging_routes(id)
);
CREATE TABLE IF NOT EXISTS messaging_spoken_reply_modes (
    connection TEXT NOT NULL, room TEXT NOT NULL, mode TEXT NOT NULL,
    PRIMARY KEY(connection,room)
);
CREATE TABLE IF NOT EXISTS messaging_fetch_actions (
    id TEXT PRIMARY KEY, connection TEXT NOT NULL, route TEXT NOT NULL, actor TEXT NOT NULL,
    input TEXT NOT NULL, delivery TEXT NOT NULL UNIQUE, expires INTEGER NOT NULL,
    claimed_by TEXT, released INTEGER NOT NULL DEFAULT 0
);
";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Command {
    Status,
    Queue,
    History,
    SpokenReplies { change: SpokenReplyChange },
    Reset,
    SelectModel { model: String },
    SelectProfileModel { model: String },
    ModelPicker,
    ChooseProfileModel { picker: String, message: String, option: usize },
    UseSkill { skill: String, input: String },
    Background { text: String },
    SetHome,
    FetchResult { action: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpokenReplyMode {
    Off,
    VoiceNotes,
    Always,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpokenReplyChange {
    Status,
    Toggle,
    Set(SpokenReplyMode),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentKind {
    Identifier,
    Text,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CommandArgument {
    pub name: &'static str,
    pub kind: ArgumentKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationAudience {
    Requester,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CommandDescriptor {
    pub name: &'static str,
    pub arguments: &'static [CommandArgument],
    pub operator: bool,
    pub audience: InvocationAudience,
}

pub const COMMANDS: &[CommandDescriptor] = &[
    CommandDescriptor {
        name: "status",
        arguments: &[],
        operator: false,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "queue",
        arguments: &[],
        operator: false,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "history",
        arguments: &[],
        operator: false,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "reset",
        arguments: &[],
        operator: true,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "model",
        arguments: &[CommandArgument {
            name: "model",
            kind: ArgumentKind::Identifier,
        }],
        operator: true,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "skill",
        arguments: &[
            CommandArgument {
                name: "skill",
                kind: ArgumentKind::Identifier,
            },
            CommandArgument {
                name: "input",
                kind: ArgumentKind::Text,
            },
        ],
        operator: false,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "bg",
        arguments: &[CommandArgument {
            name: "text",
            kind: ArgumentKind::Text,
        }],
        operator: false,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "set-home",
        arguments: &[],
        operator: true,
        audience: InvocationAudience::Requester,
    },
    CommandDescriptor {
        name: "fetch-result",
        arguments: &[CommandArgument {
            name: "action",
            kind: ArgumentKind::Identifier,
        }],
        operator: false,
        audience: InvocationAudience::Requester,
    },
];

impl Command {
    pub(super) fn uses_scoped_grant(&self) -> bool {
        matches!(self, Self::Background { .. } | Self::History | Self::SpokenReplies { .. })
    }

    pub(super) fn operator(&self) -> bool {
        matches!(self, Self::SelectModel { .. } | Self::SelectProfileModel { .. }
            | Self::ModelPicker | Self::ChooseProfileModel { .. } | Self::SetHome | Self::Reset
            | Self::SpokenReplies { change: SpokenReplyChange::Toggle | SpokenReplyChange::Set(_) })
    }

    pub(super) fn uses_profile_model(&self) -> bool {
        matches!(self, Self::SelectProfileModel { .. } | Self::ModelPicker | Self::ChooseProfileModel { .. })
    }

    pub(super) fn valid(&self) -> bool {
        match self {
            Self::SelectModel { model } | Self::SelectProfileModel { model } => profile_models::valid_model(model),
            Self::ChooseProfileModel { picker, message, option } => {
                valid_id(picker) && valid_id(message) && *option < profile_models::MAX_CHOICES
            }
            Self::UseSkill { skill, input } => {
                valid_id(skill)
                    && input.len() <= super::runtime::MAX_TEXT
                    && !input.trim().is_empty()
            }
            Self::Background { text } => {
                !text.trim().is_empty() && text.len() <= super::runtime::MAX_TEXT
            }
            Self::FetchResult { action } => valid_id(action),
            _ => true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationOutcome {
    Ready,
    ModelPicker(ModelPicker),
    Queue { pending: usize, running: bool },
    HistoryCursor(HistoryCursor),
    History { turns: Vec<HistoryTurn> },
    SpokenReplies { mode: SpokenReplyMode },
    Applied,
    PendingResult,
    Busy,
    BackgroundQueued,
    ResultReady { delivery: String },
    AlreadyClaimed,
    Expired,
    Stale,
    Forbidden,
    Unsupported,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FetchAction {
    pub id: String,
    pub expires_ms: i64,
}

impl Ingress {
    pub fn commands(
        &self,
        room: Room,
        sender: Sender,
    ) -> Result<Vec<CommandDescriptor>, GatewayError> {
        self.checked(true, |state| {
            let event = InboundEvent {
                id: "command-discovery".into(),
                room,
                sender,
                reply_anchor: None,
                mentioned: true,
                body: InboundBody::Command(Command::Status),
            };
            if self.route_event(&state.policy, &event)?.is_err() {
                return Err(GatewayError::Forbidden);
            }
            let operator = self.authorize_operator(&state.policy, &event.room, &event.sender)?;
            let turn = state.policy.turn_policy(&event.room);
            let individual = self
                .authorized_identity(&state.policy, &event.room, &event.sender)?
                .is_some_and(|(_, kind)| kind == super::ActorKind::Person);
            let scoped_command =
                self.authorize_scoped_command(&state.policy, &event.room, &event.sender)?;
            let tools = state
                .policy
                .room_policy(&event.room)
                .is_some_and(|policy| policy.tools);
            Ok(COMMANDS
                .iter()
                .copied()
                .filter(|item| {
                    (individual || item.name == "status")
                        && (!item.operator || operator)
                        && (item.name != "skill"
                            || tools
                                && turn
                                    .skills
                                    .as_ref()
                                    .is_some_and(|skills| !skills.is_empty()))
                        && (!matches!(item.name, "bg" | "history") || scoped_command)
                        && (item.name != "bg" || self.capabilities().text_delivery)
                        && (item.name != "fetch-result" || self.capabilities().interactive_prompts)
                })
                .collect())
        })
    }

    pub fn authorize_scoped_command(
        &self,
        policy: &AccessPolicy,
        room: &Room,
        sender: &Sender,
    ) -> Result<bool, GatewayError> {
        Ok(self
            .authorized_identity(policy, room, sender)?
            .is_some_and(|(actor, kind)| {
                kind == super::ActorKind::Person && policy.allows_scoped_command(room, &actor)
            }))
    }

    /// Read the durable command or control receipt for the original actor and room.
    pub async fn invocation_outcome(
        &self,
        event_id: String,
        room: Room,
        sender: Sender,
    ) -> Result<Option<InvocationOutcome>, GatewayError> {
        self.read_invocation(event_id, room, sender, None).await
    }

    /// Read an outcome for an authenticated provider callback before its deadline.
    pub async fn callback_outcome(
        &self,
        event_id: String,
        room: Room,
        sender: Sender,
        expires_ms: i64,
    ) -> Result<Option<InvocationOutcome>, GatewayError> {
        self.read_invocation(event_id, room, sender, Some(expires_ms))
            .await
    }

    async fn read_invocation(
        &self,
        event_id: String,
        room: Room,
        sender: Sender,
        expires_ms: Option<i64>,
    ) -> Result<Option<InvocationOutcome>, GatewayError> {
        if !valid_id(&event_id) {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let mut event = InboundEvent {
                    id: event_id,
                    room,
                    sender,
                    reply_anchor: None,
                    mentioned: true,
                    body: InboundBody::Command(Command::Status),
                };
                let Ok(route) = scope.route_event(&state.policy, &event)? else {
                    return Err(GatewayError::Forbidden);
                };
                scope.normalize_event(&mut event)?;
                if expires_ms.is_some_and(|end| end <= super::runtime::now()) {
                    return Ok(Some(InvocationOutcome::Expired));
                }
                let outcome = db(scope.store.invocation(&route, &event.id, &event.sender.id))?;
                if matches!(
                    &outcome,
                    Some(InvocationOutcome::HistoryCursor(_) | InvocationOutcome::SpokenReplies { .. })
                ) && !scope.authorize_scoped_command(&state.policy, &event.room, &event.sender)?
                {
                    return Ok(Some(InvocationOutcome::Forbidden));
                }
                if let Some(InvocationOutcome::ModelPicker(picker)) = &outcome {
                    return scope.current_model_picker(state, &route, &event.sender, picker);
                }
                if let Some(InvocationOutcome::HistoryCursor(cursor)) = &outcome {
                    return history::resolve(&scope, &route, cursor).map(Some);
                }
                if outcome.is_none() {
                    return scope.background_outcome(state, &event);
                }
                Ok(outcome)
            })
        })
        .await
    }

    /// Reserve a pending-result action before publishing its button or other
    /// callback. It uses the original logical input and existing outbox reply.
    pub async fn defer_result(
        &self,
        input: String,
        room: Room,
        sender: Sender,
        expires_ms: i64,
    ) -> Result<FetchAction, GatewayError> {
        if !valid_id(&input)
            || expires_ms <= chrono::Utc::now().timestamp_millis()
            || !self.capabilities().interactive_prompts
        {
            return Err(GatewayError::Invalid);
        }
        let scope = self.clone();
        blocking(move || {
            scope.checked(true, |state| {
                let mut event = InboundEvent {
                    id: input,
                    room,
                    sender,
                    reply_anchor: None,
                    mentioned: true,
                    body: InboundBody::Command(Command::Status),
                };
                let Ok(route) = scope.route_event(&state.policy, &event)? else {
                    return Err(GatewayError::Forbidden);
                };
                scope.normalize_event(&mut event)?;
                db(scope
                    .store
                    .defer_result(&route, &event.id, &event.sender.id, expires_ms))?
                .ok_or(GatewayError::Forbidden)
            })
        })
        .await
    }

    /// Release exactly the final claimed by this invocation, after the
    /// adapter has bound any provider reply grant to that effect. The current
    /// actor and room still have to be eligible.
    pub async fn release_result(
        &self,
        action: String,
        invocation: String,
        room: Room,
        sender: Sender,
    ) -> Result<bool, GatewayError> {
        let scope = self.clone();
        let changed = blocking(move || {
            scope.checked(true, |state| {
                let mut event = InboundEvent {
                    id: invocation,
                    room,
                    sender,
                    reply_anchor: None,
                    mentioned: true,
                    body: InboundBody::Command(Command::FetchResult {
                        action: action.clone(),
                    }),
                };
                let Ok(route) = scope.route_event(&state.policy, &event)? else {
                    return Err(GatewayError::Forbidden);
                };
                scope.normalize_event(&mut event)?;
                db(scope.store.release_result(
                    &route,
                    &action,
                    &event.id,
                    &event.sender.id,
                    chrono::Utc::now().timestamp_millis(),
                ))
            })
        })
        .await?;
        if changed {
            self.notify();
        }
        Ok(changed)
    }

    pub fn spoken_reply_mode(&self, room: &Room) -> Result<SpokenReplyMode, GatewayError> {
        self.checked(true, |state| {
            let Some(default) = state.policy.spoken_reply_default(room) else {
                return Ok(SpokenReplyMode::Off);
            };
            db(self.store.read(|tx| read_spoken_mode(tx, &self.connection, room, default)))
        })
    }

    pub fn home_destination(&self) -> Result<Option<String>, GatewayError> {
        self.checked(true, |_| db(self.store.home(&self.connection)))
    }
}

pub(super) fn execute(
    scope: &Ingress,
    work: &PendingInput,
    command: &Command,
    session: Option<&LiveSession>,
    allowed: bool,
    operator: bool,
    spoken_default: Option<SpokenReplyMode>,
) -> Result<(), GatewayError> {
    let operation = if !allowed || command.operator() && !operator {
        Operation::Output(InvocationOutcome::Forbidden)
    } else {
        match command {
            Command::Status => Operation::Output(InvocationOutcome::Ready),
            Command::Queue => {
                let queue = session
                    .map(|session| {
                        let view = lock(&session.view);
                        InvocationOutcome::Queue {
                            pending: view
                                .authority
                                .receipts
                                .iter()
                                .filter(|r| r.status == "queued")
                                .count(),
                            running: view.running,
                        }
                    })
                    .unwrap_or(InvocationOutcome::Queue {
                        pending: 0,
                        running: false,
                    });
                Operation::Output(queue)
            }
            Command::History => Operation::Output(history::snapshot(scope, &work.route)?),
            Command::SpokenReplies { change } => Operation::SpokenReplies(*change, spoken_default),
            Command::Reset => Operation::Reset,
            Command::SelectModel { model } => Operation::SelectModel(model),
            Command::SetHome => Operation::Home(scope.transport_scope()?),
            Command::FetchResult { action } => Operation::Fetch(action),
            Command::UseSkill { .. } | Command::Background { .. } | Command::SelectProfileModel { .. }
            | Command::ModelPicker | Command::ChooseProfileModel { .. } =>
                Operation::Output(InvocationOutcome::Unsupported),
        }
    };
    db(scope
        .store
        .complete_command(&work.route, &work.event, operation))
}

enum Operation<'a> {
    Output(InvocationOutcome),
    Reset,
    SelectModel(&'a str),
    Home(String),
    SpokenReplies(SpokenReplyChange, Option<SpokenReplyMode>),
    Fetch(&'a str),
}

pub(super) fn record_invocation(
    tx: &rusqlite::Connection,
    route: &Route,
    event: &str,
    actor: &str,
    outcome: InvocationOutcome,
) -> Result<()> {
    record_requester_invocation(tx, &route.connection, event, &route.id, actor, outcome)
}

fn record_requester_invocation(
    tx: &rusqlite::Connection,
    connection: &str,
    event: &str,
    scope: &str,
    actor: &str,
    outcome: InvocationOutcome,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_native_invocations(connection,event,route,actor,outcome)
         VALUES (?1,?2,?3,?4,?5)",
        params![
            connection,
            event,
            scope,
            actor,
            serde_json::to_string(&outcome)?
        ],
    )?;
    Ok(())
}

impl Store {
    pub(super) fn route_model(&self, route: &str, provider: &str) -> Result<Option<String>> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT m.model FROM messaging_route_models m
                 LEFT JOIN messaging_route_model_scopes s USING(route)
                 WHERE m.route = ?1 AND (s.provider IS NULL OR s.provider = ?2)",
                params![route, provider], |r| r.get(0),
            ).optional()?)
        })
    }

    fn home(&self, connection: &str) -> Result<Option<String>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT route FROM messaging_home_routes WHERE connection = ?1",
                    [connection],
                    |r| r.get(0),
                )
                .optional()?)
        })
    }

    fn invocation(
        &self,
        route: &Route,
        event: &str,
        actor: &str,
    ) -> Result<Option<InvocationOutcome>> {
        self.read(|db| {
            Ok(db.query_row(
            "SELECT outcome FROM messaging_native_invocations WHERE connection = ?1 AND event = ?2
              AND route = ?3 AND actor = ?4",
            params![route.connection,event,route.id,actor], |r| store::decode(r,0),
        ).optional()?)
        })
    }

    fn complete_command(
        &self,
        route: &Route,
        event: &InboundEvent,
        operation: Operation<'_>,
    ) -> Result<()> {
        self.write(|tx| {
            let existing = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_native_invocations WHERE connection = ?1 AND event = ?2)",
                params![route.connection,event.id], |row| row.get::<_,bool>(0),
            )?;
            if existing { return Ok(()) }
            let outcome = match operation {
                Operation::Output(outcome) => outcome,
                Operation::Reset => reset_route(tx, route, &event.id)?,
                Operation::SelectModel(model) => {
                    tx.execute("INSERT INTO messaging_route_models(route,model) VALUES (?1,?2)
                                ON CONFLICT(route) DO UPDATE SET model = excluded.model", params![route.id,model])?;
                    tx.execute("DELETE FROM messaging_route_model_scopes WHERE route = ?1", [&route.id])?;
                    InvocationOutcome::Applied
                }
                Operation::Home(scope) => {
                    if super::routes::destination_permission(tx, &route.id, super::runtime::now(), &scope)? == Some(true) {
                        tx.execute("INSERT INTO messaging_home_routes(connection,route) VALUES (?1,?2)
                                  ON CONFLICT(connection) DO UPDATE SET route = excluded.route", params![route.connection,route.id])?;
                        InvocationOutcome::Applied
                    } else { InvocationOutcome::Forbidden }
                }
                Operation::SpokenReplies(change, default) => change_spoken_mode(tx, route, change, default)?,
                Operation::Fetch(action) => claim_result(tx, route, event, action)?,
            };
            record_invocation(tx, route, &event.id, &event.sender.id, outcome)?;
            store::set_input(tx, route, &event.id, "done", Some("command_complete"))
        })
    }

    fn defer_result(
        &self,
        route: &Route,
        input: &str,
        actor: &str,
        expires: i64,
    ) -> Result<Option<FetchAction>> {
        let id = store::key(
            "fetch-action",
            &(&route.connection, input, &route.id, actor),
        )?;
        let delivery = store::key(
            "reply",
            &store::key("channel-input", &(&route.connection, input))?,
        )?;
        self.write(|tx| {
            if let Some(expires_ms) = tx.query_row(
                "SELECT expires FROM messaging_fetch_actions WHERE id = ?1", [&id], |r| r.get::<_, i64>(0),
            ).optional()? {
                return Ok(Some(FetchAction { id, expires_ms }));
            }
            let source = tx.query_row(
                "SELECT input FROM messaging_ingress WHERE connection = ?1 AND event = ?2 AND route = ?3
                   AND stage IN ('pending','admitted') AND control = 0",
                params![route.connection,input,route.id], |r| store::decode::<InboundEvent>(r,0),
            ).optional()?;
            if source.is_none_or(|source| source.sender.id != actor) {
                return Ok(None);
            }
            ensure!(tx.query_row("SELECT NOT EXISTS(SELECT 1 FROM messaging_deliveries WHERE id = ?1)",
                [&delivery], |r| r.get::<_,bool>(0))?, "The final was already published to the outbox");
            tx.execute(
                "INSERT INTO messaging_fetch_actions(id,connection,route,actor,input,delivery,expires)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)", params![id,route.connection,route.id,actor,input,delivery,expires],
            )?;
            Ok(Some(FetchAction { id, expires_ms: expires }))
        })
    }

    fn release_result(
        &self,
        route: &Route,
        action: &str,
        invocation: &str,
        actor: &str,
        now: i64,
    ) -> Result<bool> {
        self.write(|tx| Ok(tx.execute(
            "UPDATE messaging_fetch_actions SET released = 1 WHERE id = ?1 AND connection = ?2 AND route = ?3
                AND actor = ?4 AND claimed_by = ?5 AND expires > ?6 AND released = 0",
            params![action,route.connection,route.id,actor,invocation,now],
        )? == 1))
    }

    pub(super) fn result_released(&self, connection: &str, delivery: &str) -> Result<bool> {
        self.read(|db| Ok(db.query_row(
            "SELECT COALESCE((SELECT released FROM messaging_fetch_actions WHERE connection = ?1 AND delivery = ?2),1)",
            params![connection,delivery], |r| r.get(0),
        )?))
    }
}

fn read_spoken_mode(
    tx: &rusqlite::Connection,
    connection: &str,
    room: &Room,
    default: SpokenReplyMode,
) -> Result<SpokenReplyMode> {
    Ok(tx.query_row(
        "SELECT mode FROM messaging_spoken_reply_modes WHERE connection=?1 AND room=?2",
        params![connection,serde_json::to_string(room)?], |row| store::decode(row,0),
    ).optional()?.unwrap_or(default))
}

fn change_spoken_mode(
    tx: &rusqlite::Connection,
    route: &Route,
    change: SpokenReplyChange,
    default: Option<SpokenReplyMode>,
) -> Result<InvocationOutcome> {
    let Some(default) = default else {
        return Ok(match change {
            SpokenReplyChange::Status => InvocationOutcome::SpokenReplies { mode: SpokenReplyMode::Off },
            _ => InvocationOutcome::Unsupported,
        });
    };
    let current = read_spoken_mode(tx, &route.connection, &route.room, default)?;
    let mode = match change {
        SpokenReplyChange::Status => return Ok(InvocationOutcome::SpokenReplies { mode: current }),
        SpokenReplyChange::Set(mode) => mode,
        SpokenReplyChange::Toggle if current == SpokenReplyMode::Off => SpokenReplyMode::VoiceNotes,
        SpokenReplyChange::Toggle => SpokenReplyMode::Off,
    };
    tx.execute(
        "INSERT INTO messaging_spoken_reply_modes(connection,room,mode) VALUES (?1,?2,?3)
         ON CONFLICT(connection,room) DO UPDATE SET mode=excluded.mode",
        params![route.connection,serde_json::to_string(&route.room)?,serde_json::to_string(&mode)?],
    )?;
    Ok(InvocationOutcome::SpokenReplies { mode })
}

fn reset_route(tx: &rusqlite::Connection, route: &Route, request: &str) -> Result<InvocationOutcome> {
    // A linked destination can share a session with a different audience.
    // Scope this reset to the exact unlinked conversation.
    let linked = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_route_bindings
           WHERE destination = ?1 OR binding = ?1)",
        [&route.id], |row| row.get::<_, bool>(0),
    )?;
    if linked {
        return Ok(InvocationOutcome::Unsupported);
    }
    let busy = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_ingress
           WHERE route = ?1 AND event != ?2 AND stage NOT IN ('done','refused'))
          OR EXISTS(SELECT 1 FROM messaging_deliveries
           WHERE route = ?1 AND stage IN ('pending','planned'))",
        params![route.id, request], |row| row.get::<_, bool>(0),
    )?;
    if busy {
        return Ok(InvocationOutcome::Busy);
    }
    // The existing native allocator will create the new revision on demand.
    // Keep the original conversation and its committed receipts in the library.
    let reset = tx.execute(
        "UPDATE messaging_routes SET session = NULL, revision = revision + 1
         WHERE id = ?1 AND revision = ?2 AND blocked = 0",
        params![route.id, route.revision],
    )?;
    Ok(if reset == 1 { InvocationOutcome::Applied } else { InvocationOutcome::Stale })
}

fn claim_result(
    db: &rusqlite::Connection,
    route: &Route,
    event: &InboundEvent,
    id: &str,
) -> Result<InvocationOutcome> {
    let item = db.query_row(
        "SELECT f.delivery,f.expires,f.claimed_by,
                EXISTS(SELECT 1 FROM messaging_deliveries WHERE id = f.delivery)
         FROM messaging_fetch_actions f WHERE f.id = ?1 AND f.connection = ?2 AND f.route = ?3 AND f.actor = ?4",
        params![id,route.connection,route.id,event.sender.id],
        |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,bool>(3)?)),
    ).optional()?;
    let Some((delivery, expires, claimed, ready)) = item else {
        return Ok(InvocationOutcome::Forbidden);
    };
    if expires <= chrono::Utc::now().timestamp_millis() {
        return Ok(InvocationOutcome::Expired);
    }
    if claimed.is_some() {
        return Ok(InvocationOutcome::AlreadyClaimed);
    }
    if !ready {
        return Ok(InvocationOutcome::PendingResult);
    }
    db.execute(
        "UPDATE messaging_fetch_actions SET claimed_by = ?2 WHERE id = ?1",
        params![id, event.id],
    )?;
    Ok(InvocationOutcome::ResultReady { delivery })
}
