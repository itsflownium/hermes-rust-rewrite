use crate::messaging::{
    Advance, Command, ConnectionFailure, Control, DeliveryBody, DeliveryIntent, Disposition,
    GatewayError, InboundBody, InboundEvent, InvocationOutcome, Prompt, Response, Room, Sender,
    SpokenReplyChange, SpokenReplyMode,
    commands::{COMMANDS, RequesterCommand, RequesterInvocation},
    runtime::{db, now, valid_id},
    storage as store,
};
use super::{
    DiscordAdapter, Ingress,
    channels::{Member, User, read_id, snowflake, snowflake_timestamp},
    client::{DiscordClient, RestFailure, json_response},
    dispatch::{refuse, skip},
    manual_thread::{ManualThread, ManualThreadIngress, ThreadOutcome, ThreadSpec, has_permissions},
    socket::{Dispatch, intake_failure},
};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

const ACK_MS: i64 = 3_000;
const CALLBACK_MS: i64 = 15 * 60 * 1_000;
const EPHEMERAL: u32 = 64;

#[derive(Deserialize)]
struct Invoker {
    user: User,
    permissions: Option<String>,
    #[serde(flatten)]
    membership: Member,
}

#[derive(Deserialize)]
struct Interaction {
    id: String,
    application_id: String,
    #[serde(rename = "type")]
    kind: u8,
    guild_id: Option<String>,
    channel_id: String,
    token: String,
    app_permissions: Option<String>,
    user: Option<User>,
    member: Option<Invoker>,
    data: Value,
    message: Option<Value>,
}

enum Action {
    Native(InboundBody),
    Help,
    Identity,
    Modal { id: String, question: String },
    Voice { guild: String, leave: bool },
    Thread(ThreadSpec),
}

impl Action {
    fn requester_command(&self) -> Option<RequesterCommand> {
        match self {
            Self::Native(InboundBody::Command(Command::Status)) => Some(RequesterCommand::Status),
            Self::Help => Some(RequesterCommand::Help),
            Self::Identity => Some(RequesterCommand::Identity),
            _ => None,
        }
    }

    fn input(&self) -> InboundBody {
        match self {
            Self::Native(body) => body.clone(),
            _ => InboundBody::Command(Command::Status),
        }
    }
}

enum Presentation {
    Text(String),
    Requester(RequesterInvocation),
    ManualThread(ManualThread),
    Deferred {
        input: String,
        room: Room,
        sender: Sender,
    },
    Modal {
        id: String,
        question: String,
    },
}

fn deadline(id: &str, duration: i64) -> Option<i64> {
    Some(snowflake_timestamp(id)? + duration)
}

impl DiscordAdapter {
    pub(super) async fn interaction(
        &self,
        ingress: &Ingress,
        frame: Dispatch,
        advance: Advance,
        outgoing: &mpsc::Sender<Value>,
        cancel: &CancellationToken,
    ) -> Result<(), ConnectionFailure> {
        let Ok(mut input) = serde_json::from_value::<Interaction>(frame.data) else {
            return skip(ingress, advance).await;
        };
        if input.application_id != self.bot.application_id
            || !snowflake(&input.id)
            || !snowflake(&input.channel_id)
            || input.token.is_empty()
            || input.token.len() > 512
            || input.token.bytes().any(|byte| byte.is_ascii_control())
            || !matches!(input.kind, 2 | 3 | 5)
        {
            return skip(ingress, advance).await;
        }

        let Some(route) = self
            .channel_route(&input.channel_id, input.guild_id.as_deref())
            .await?
            .filter(|route| route.guild == input.guild_id)
        else {
            skip(ingress, advance).await?;
            self.respond(
                ingress,
                input,
                Presentation::Text("This channel is unavailable.".into()),
                frame.received,
                cancel,
            );
            return Ok(());
        };
        let actor_permissions = input.member.as_ref().and_then(|member| member.permissions.clone());
        let (user, membership) = match input.member.take() {
            Some(member) => (Some(member.user), Some(member.membership)),
            None => (input.user.take(), None),
        };
        let Some(user) = user.filter(|user| !user.bot && !user.system && snowflake(&user.id))
        else {
            return skip(ingress, advance).await;
        };
        let event_id = format!("discord-interaction-{}", input.id);
        let name = input.data.get("name").and_then(Value::as_str);
        let background = input.kind == 2 && name == Some("bg");
        let recorded = db(
            ingress.storage().read(|tx| store::intake_receipt(tx, ingress.connection_name(), &event_id))
        ).map_err(intake_failure)?;
        let (sender, allowed_command) = if recorded.is_some() {
            // A replay's role snapshot may predate a later membership change.
            let sender = Sender { id: user.id.clone(), roles: BTreeSet::new() };
            let allowed = !self.settings.commands.has_explicit_grants() || ingress.checked(true, |state| {
                Ok(ingress.effective_sender(&state.policy, &route.room, &sender)?
                    .is_some_and(|(actor, _)| state.policy.requester_command_grant(&route.room, &actor).is_some()))
            }).map_err(intake_failure)?;
            (sender, allowed)
        } else {
            let Some(roles) = self.roles(&route, &user.id, membership).await? else {
                return skip(ingress, advance).await;
            };
            let sender = self.record_sender(ingress, &route, &user, &roles)
                .await.map_err(intake_failure)?;
            let allowed = self.settings.allows_command(&user.id, route.guild.as_deref(), &roles);
            (sender, allowed)
        };
        let mut event = InboundEvent {
            id: event_id.clone(),
            room: route.room.clone(),
            sender: sender.clone(),
            reply_anchor: None,
            mentioned: true,
            body: InboundBody::Command(Command::Status),
        };
        if !self.settings.commands.enabled || !background && !allowed_command {
            refuse(ingress, event, Disposition::Forbidden, advance).await?;
            self.respond(
                ingress,
                input,
                Presentation::Text("You aren't allowed to use this command here.".into()),
                frame.received,
                cancel,
            );
            return Ok(());
        }
        let expired = deadline(&input.id, ACK_MS).is_none_or(|until| until <= now());
        let can_finish_as_message =
            input.kind == 2 && matches!(name, Some("skill" | "bg"));
        if expired && !can_finish_as_message {
            if recorded.is_some() {
                skip(ingress, advance).await?;
            } else {
                refuse(ingress, event, Disposition::Expired, advance).await?;
            }
            return Ok(());
        }
        let action = self
            .native_action(ingress, &input, &route.room, &sender)
            .await
            .map_err(intake_failure)?;
        let Some(action) = action else {
            refuse(ingress, event, Disposition::Unsupported, advance).await?;
            self.respond(
                ingress,
                input,
                Presentation::Text("That action is unavailable or has expired.".into()),
                frame.received,
                cancel,
            );
            return Ok(());
        };
        if let Action::Thread(spec) = &action {
            let parent = crate::lock(&self.channels)
                .get(&route.room.chat).is_some_and(|channel| matches!(channel.kind, 0 | 5));
            let valid_surface = route.room.thread.is_none() && parent && route.guild.is_some();
            let allowed = has_permissions(input.app_permissions.as_deref(), spec.bot_permissions())
                && has_permissions(actor_permissions.as_deref(), spec.actor_permissions());
            if !valid_surface || !allowed {
                let refusal = if valid_surface { Disposition::Forbidden } else { Disposition::Unsupported };
                refuse(ingress, event, refusal, advance).await?;
                let text = if valid_surface {
                    "Discord thread permissions are missing."
                } else {
                    "Run /thread in a guild text or announcement channel."
                };
                self.respond(ingress, input, Presentation::Text(text.into()), frame.received, cancel);
                return Ok(());
            }
            let request = ManualThread {
                event: event_id,
                guild: route.guild.unwrap(),
                room: route.room,
                sender,
                spec: spec.clone(),
                created_ms: snowflake_timestamp(&input.id).ok_or(ConnectionFailure::Retryable)?,
            };
            let accepted = ingress
                .commit_manual_thread(
                    request.clone(),
                    advance,
                    self.settings.delivery.pin_first_thread_message,
                )
                .await
                .map_err(intake_failure)?;
            let presentation = if accepted == Disposition::Accepted {
                Presentation::ManualThread(request)
            } else {
                Presentation::Text("The gateway refused this thread request.".into())
            };
            self.respond(ingress, input, presentation, frame.received, cancel);
            return Ok(());
        }
        if self.settings.commands.has_explicit_grants()
            && let Some(command) = action.requester_command()
            && ingress
                .checked(true, |state| {
                    Ok(!ingress.authorize_sender(&state.policy, &route.room, &sender)?)
                })
                .map_err(intake_failure)?
        {
            let request = RequesterInvocation {
                id: event.id,
                room: route.room,
                sender,
                command,
            };
            let disposition = ingress
                .commit_requester_command(request.clone(), advance)
                .await
                .map_err(intake_failure)?;
            let presentation = if disposition == Disposition::Accepted {
                Presentation::Requester(request)
            } else {
                Presentation::Text("The gateway refused this interaction.".into())
            };
            self.respond(ingress, input, presentation, frame.received, cancel);
            return Ok(());
        }
        event.body = action.input();
        let result = ingress
            .submit(vec![event], Some(advance))
            .await
            .map_err(intake_failure)?;
        let disposition = result
            .receipts
            .first()
            .ok_or(ConnectionFailure::Retryable)?;
        let presentation = if disposition.disposition == Disposition::Accepted {
            match action {
                Action::Help => {
                    let commands = ingress
                        .commands(route.room.clone(), sender.clone())
                        .unwrap_or_default();
                    let names = commands
                        .iter()
                        .map(|command| format!("/{}", command.name))
                        .collect::<Vec<_>>()
                        .join(", ");
                    Presentation::Text(format!("Available in this conversation: {names}"))
                }
                Action::Identity => Presentation::Text(format!("Discord account {}", sender.id)),
                Action::Modal { id, question } => Presentation::Modal { id, question },
                Action::Voice { guild, leave } => {
                    let queued = if disposition.repeated {
                        true
                    } else if leave {
                        self.leave_voice(ingress, &sender, &route.room, &guild)
                            .is_some_and(|value| outgoing.try_send(value).is_ok())
                    } else {
                        match self.join_voice(ingress, &sender, &route.room, &guild).await {
                            Ok(operation) => outgoing.try_send(operation).is_ok(),
                            Err(_) => false,
                        }
                    };
                    Presentation::Text(
                        if queued {
                            "Voice request queued."
                        } else {
                            "Voice is unavailable for this conversation."
                        }
                        .into(),
                    )
                }
                Action::Native(InboundBody::Command(Command::UseSkill { .. })) => {
                    Presentation::Text(
                        "Accepted. The result will arrive in this conversation.".into(),
                    )
                }
                Action::Thread(_) => unreachable!("thread command uses its dedicated receipt"),
                Action::Native(_) => Presentation::Deferred {
                    input: event_id,
                    room: route.room,
                    sender,
                },
            }
        } else {
            Presentation::Text("The gateway refused this interaction.".into())
        };
        self.respond(ingress, input, presentation, frame.received, cancel);
        Ok(())
    }

    async fn native_action(
        &self,
        ingress: &Ingress,
        interaction: &Interaction,
        room: &Room,
        sender: &Sender,
    ) -> Result<Option<Action>, GatewayError> {
        if interaction.kind != 2 {
            if let Some(picker) = interaction
                .data
                .get("custom_id")
                .and_then(Value::as_str)
                .and_then(|id| id.strip_prefix("mrc:model:"))
            {
                return Ok(self.model_choice(interaction, picker));
            }
            return self.component_action(ingress, interaction, room, sender);
        }
        let name = interaction
            .data
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("");
        let option = |wanted: &str| {
            interaction
                .data
                .get("options")
                .and_then(Value::as_array)
                .and_then(|options| {
                    options
                        .iter()
                        .find(|option| option.get("name").and_then(Value::as_str) == Some(wanted))
                })
                .and_then(|option| option.get("value"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        let command = |command| Some(Action::Native(InboundBody::Command(command)));
        let body = match name {
            "status" => command(Command::Status),
            "queue" => command(Command::Queue),
            "history" => command(Command::History),
            "reset" => command(Command::Reset),
            "model" => command(match option("model") {
                Some(model) => Command::SelectProfileModel { model },
                None => Command::ModelPicker,
            }),
            "bg" if room.thread.is_some()
                || !crate::lock(&self.channels)
                    .get(&room.chat)
                    .is_some_and(|channel| matches!(channel.kind, 15 | 16)) =>
            {
                option("text").and_then(|text| command(Command::Background { text }))
            }
            "skill" => option("skill")
                .zip(option("input"))
                .and_then(|(skill, input)| command(Command::UseSkill { skill, input })),
            "thread" => ThreadSpec::parse(&interaction.data).map(Action::Thread),
            "sethome" => {
                let required = if room.thread.is_some() {
                    1 << 38
                } else {
                    1 << 11
                };
                let may_write = interaction
                    .app_permissions
                    .as_deref()
                    .and_then(|value| value.parse::<u64>().ok())
                    .map_or(room.kind == crate::messaging::RoomKind::Direct, |bits| {
                        bits & required != 0 || bits & 8 != 0
                    });
                let operator = ingress.checked(true, |state| {
                    ingress.authorize_operator(&state.policy, room, sender)
                })?;
                if may_write && operator {
                    ingress
                        .verify_destination(room.clone(), now() + 300_000)
                        .await?;
                    command(Command::SetHome)
                } else {
                    None
                }
            }
            "fetch-result" => {
                option("action").and_then(|action| command(Command::FetchResult { action }))
            }
            "help" => Some(Action::Help),
            "whoami" => Some(Action::Identity),
            "stop" | "steer" | "redirect" => {
                let generation = match ingress.active_execution(room, sender).await {
                    Err(GatewayError::Forbidden) => None,
                    other => other?,
                };
                generation.and_then(|execution_generation| {
                    let control = match name {
                        "stop" => Control::Interrupt {
                            execution_generation,
                        },
                        "steer" => Control::Steer {
                            execution_generation,
                            text: option("text")?,
                        },
                        _ => Control::Redirect {
                            execution_generation,
                            text: option("text")?,
                        },
                    };
                    Some(Action::Native(InboundBody::Control(control)))
                })
            }
            "edit" => option("message")
                .filter(|id| snowflake(id))
                .zip(option("text"))
                .map(|(id, text)| {
                    Action::Native(InboundBody::Control(Control::ReplaceQueued {
                        event_id: format!("discord-message-{id}"),
                        text,
                    }))
                }),
            "voice" if self.settings.voice_enabled || self.settings.sends_voice_notes() => {
                let action = option("action");
                match action.as_deref() {
                    Some("channel" | "join" | "leave") if self.settings.voice_enabled => {
                        let operator = ingress.checked(true, |state| {
                            ingress.authorize_operator(&state.policy, room, sender)
                        })?;
                        interaction.guild_id.clone().filter(|_| operator).map(|guild| Action::Voice {
                            guild,
                            leave: action.as_deref() == Some("leave"),
                        })
                    }
                    Some("status") => command(Command::SpokenReplies { change: SpokenReplyChange::Status }),
                    change if self.settings.sends_voice_notes() => {
                        let change = match change {
                            None => SpokenReplyChange::Toggle,
                            Some("on") => SpokenReplyChange::Set(SpokenReplyMode::VoiceNotes),
                            Some("off") => SpokenReplyChange::Set(SpokenReplyMode::Off),
                            Some("tts") => SpokenReplyChange::Set(SpokenReplyMode::Always),
                            _ => return Ok(None),
                        };
                        command(Command::SpokenReplies { change })
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        Ok(body)
    }

    fn model_choice(&self, input: &Interaction, picker: &str) -> Option<Action> {
        if input.kind != 3
            || input.data["component_type"] != 3
            || !picker.starts_with("discord-interaction-")
            || !valid_id(picker)
        {
            return None;
        }
        let message = input.message.as_ref()?;
        if message.pointer("/author/id").and_then(Value::as_str) != Some(self.bot.bot_id.as_str()) {
            return None;
        }
        let id = read_id(message, "id")?;
        let [value] = input.data["values"].as_array()?.as_slice() else {
            return None;
        };
        let option = value.as_str()?.parse::<usize>().ok()?;
        Some(Action::Native(InboundBody::Command(
            Command::ChooseProfileModel {
                picker: picker.into(),
                message: id.to_owned(),
                option,
            },
        )))
    }

    fn component_action(
        &self,
        ingress: &Ingress,
        interaction: &Interaction,
        room: &Room,
        sender: &Sender,
    ) -> Result<Option<Action>, GatewayError> {
        let Some(custom_id) = interaction
            .data
            .get("custom_id")
            .and_then(Value::as_str)
            .filter(|value| value.len() <= 100 && value.starts_with("mrc:"))
        else {
            return Ok(None);
        };
        let mut parts = custom_id.split(':').skip(1);
        let (Some(id), Some(choice)) = (parts.next(), parts.next()) else {
            return Ok(None);
        };
        if !id.starts_with("action-") || !valid_id(id) {
            return Ok(None);
        }
        let parameter = parts.next();
        if parts.next().is_some() {
            return Ok(None);
        }
        let message_id = interaction.message.as_ref().and_then(|message| {
            let author = message.pointer("/author/id").and_then(Value::as_str);
            (author == Some(self.bot.bot_id.as_str()))
                .then(|| read_id(message, "id"))
                .flatten()
        });
        let marker = match (interaction.kind, message_id, choice, parameter) {
            (3, Some(id), _, _) => id,
            (5, marker, "t", Some(id))
                if snowflake(id) && marker.is_none_or(|marker| marker == id) =>
            {
                id
            }
            _ => return Ok(None),
        };
        let prompt = ingress.checked(true, |state| {
            let discovery = InboundEvent { id: "discord-component".into(), room: room.clone(),
                sender: sender.clone(), reply_anchor: None, mentioned: true, body: InboundBody::Command(Command::Status) };
            let Ok(route) = ingress.route_event(&state.policy, &discovery)? else { return Ok(None) };
            let Some(action) = db(ingress.storage().action(&route, id))? else { return Ok(None) };
            if action.recipient != sender.id { return Ok(None) }
            db(ingress.storage().read(|tx| {
                use rusqlite::OptionalExtension;
                Ok(tx.query_row(
                    "SELECT d.intent FROM messaging_deliveries d JOIN messaging_parts p ON p.delivery=d.id
                     WHERE d.connection=?1 AND d.route=?2 AND d.id=?3 AND p.ordinal=0 AND p.stage='sent'
                       AND (p.editable_message=?4 OR p.receipt=?4)",
                    rusqlite::params![ingress.connection_name(),route.id,id,marker],
                    |row| store::decode::<DeliveryIntent>(row, 0),
                ).optional()?.and_then(|intent| {
                    match intent.body {
                        DeliveryBody::Prompt { prompt, .. } => Some(prompt),
                        _ => None,
                    }
                }))
            }))
        })?;
        let response = match (prompt, interaction.kind, choice, parameter) {
            (Some(Prompt::Approval { .. }), 3, "once", None) => Response::Approve,
            (Some(Prompt::Approval { .. }), 3, "session", None) => Response::ApproveForSession,
            (Some(Prompt::Approval { .. }), 3, "always", None) => Response::ApprovePersistently,
            (Some(Prompt::Approval { .. }), 3, "deny", None) => Response::Deny,
            (Some(Prompt::Clarification { choices, .. }), 3, "c", Some(index)) => {
                let Some(answer) = index
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| choices.get(index))
                else {
                    return Ok(None);
                };
                Response::Clarify(answer.clone())
            }
            (Some(Prompt::Clarification { question, .. }), 3, "text", None) => {
                return Ok(Some(Action::Modal {
                    id: format!("mrc:{id}:t:{marker}"),
                    question,
                }));
            }
            (Some(Prompt::Clarification { .. }), 5, "t", Some(_)) => {
                let answer = interaction
                    .data
                    .get("components")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|row| row.get("components").and_then(Value::as_array))
                    .flatten()
                    .find(|component| {
                        component.get("custom_id").and_then(Value::as_str) == Some("answer")
                    })
                    .and_then(|component| component.get("value"))
                    .and_then(Value::as_str);
                let Some(answer) =
                    answer.filter(|text| !text.trim().is_empty() && text.len() <= 4_000)
                else {
                    return Ok(None);
                };
                Response::Clarify(answer.into())
            }
            _ => return Ok(None),
        };
        Ok(Some(Action::Native(InboundBody::Control(
            Control::Respond {
                action_id: id.into(),
                response,
            },
        ))))
    }

    fn respond(
        &self,
        ingress: &Ingress,
        input: Interaction,
        presentation: Presentation,
        received: Instant,
        cancel: &CancellationToken,
    ) {
        let Some(deadline_ms) = deadline(&input.id, ACK_MS) else {
            return;
        };
        if deadline_ms <= now() {
            return;
        }
        let local_deadline = (received + Duration::from_millis(ACK_MS as u64))
            .min(Instant::now() + Duration::from_millis((deadline_ms - now()).max(0) as u64));
        let Ok(permit) = self.callbacks.clone().try_acquire_owned() else {
            return;
        };
        let client = self.client.clone();
        let bot = self.bot.bot_id.clone();
        let cancel = cancel.clone();
        let ingress = ingress.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let initial = match &presentation {
                Presentation::Text(text) => json!({"type":4,"data": private_text(text)}),
                Presentation::Requester(request) => {
                    let text = match ingress.requester_command_response(request) {
                        Ok(access) => match request.command {
                            RequesterCommand::Status if access.operator => {
                                "Mercury is ready. Operator role verified.".into()
                            }
                            RequesterCommand::Status => outcome_text(InvocationOutcome::Ready),
                            RequesterCommand::Identity => {
                                format!("Discord account {}", access.actor)
                            }
                            RequesterCommand::Help => {
                                "Available with this grant: /status, /help, /whoami.".into()
                            }
                        },
                        Err(_) => "The gateway refused this interaction.".into(),
                    };
                    json!({"type":4,"data":private_text(&text)})
                }
                Presentation::Deferred { .. } | Presentation::ManualThread(_) =>
                    json!({"type":5,"data":{"flags":EPHEMERAL}}),
                Presentation::Modal { id, question } => json!({"type":9,"data":{
                    "custom_id":id,"title":question.chars().take(45).collect::<String>(),
                    "components":[{"type":1,"components":[{"type":4,"custom_id":"answer",
                        "label":"Your answer","style":2,"required":true,"max_length":4000}]}]
                }}),
            };
            let request = client
                .open(
                    Method::POST,
                    &["interactions", &input.id, &input.token, "callback"],
                )
                .json(&initial);
            let result = tokio::select! {
                _ = cancel.cancelled() => return,
                result = timeout_at(local_deadline, request.send()) => result,
            };
            let Ok(Ok(result)) = result else { return };
            // Discord returns 40060 for a replay of an already acknowledged interaction.
            if !result.status().is_success() {
                let status = result.status();
                if status != reqwest::StatusCode::BAD_REQUEST
                    || ack_error(result).await != Some(40060)
                {
                    return;
                }
            }
            let (event, room, sender, thread) = match presentation {
                Presentation::Deferred { input, room, sender } => (input, room, sender, None),
                Presentation::ManualThread(request) => (
                    request.event.clone(), request.room.clone(), request.sender.clone(), Some(request),
                ),
                _ => return,
            };
            let Some(expires_ms) = deadline(&input.id, CALLBACK_MS) else {
                return;
            };
            let until = Instant::now() + Duration::from_secs(12);
            loop {
                if now() >= expires_ms || cancel.is_cancelled() {
                    return;
                }
                let outcome = if let Some(request) = &thread {
                    ingress.manual_thread_outcome(request, expires_ms).await.map(|outcome| match outcome {
                        ThreadOutcome::Pending => None,
                        ThreadOutcome::Refused => Some((response_content("The thread was not created."), false)),
                        ThreadOutcome::Created { id, starter_failed } => {
                            let text = if starter_failed {
                                format!("Created <#{id}>. Discord refused the starter message.")
                            } else {
                                format!("Created <#{id}>.")
                            };
                            Some((response_content(&text), false))
                        }
                    })
                } else {
                    ingress.callback_outcome(event.clone(), room.clone(), sender.clone(), expires_ms).await
                        .map(|outcome| outcome.map(|outcome| match outcome {
                            InvocationOutcome::ModelPicker(picker) => (model_picker_payload(&event, &picker.choices), true),
                            outcome => (response_content(&outcome_text(outcome)), false),
                        }))
                };
                let (payload, is_picker) = match outcome {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) if Instant::now() < until => {
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(Duration::from_millis(40)) => {},
                        }
                        continue;
                    }
                    Ok(None) => (response_content("The outcome is still pending."), false),
                    Err(_) => (response_content("The outcome is unavailable."), false),
                };
                let request = client
                    .open(
                        Method::PATCH,
                        &[
                            "webhooks",
                            &input.application_id,
                            &input.token,
                            "messages",
                            "@original",
                        ],
                    )
                    .json(&payload);
                let result = tokio::time::timeout(Duration::from_secs(4), async {
                    let response = request.send().await.map_err(|_| RestFailure::Unavailable)?;
                    if is_picker {
                        json_response::<Value>(response).await.map(Some)
                    } else {
                        Ok(None)
                    }
                })
                .await;
                if let Ok(Ok(Some(message))) = result
                    && message.pointer("/author/id").and_then(Value::as_str) == Some(bot.as_str())
                    && message.get("channel_id").and_then(Value::as_str)
                        == Some(room.thread.as_deref().unwrap_or(&room.chat))
                    && let Some(id) = read_id(&message, "id")
                {
                    let _ = ingress
                        .bind_model_picker_message(event, room, sender, id.to_owned())
                        .await;
                }
                return;
            }
        });
    }
}

async fn ack_error(mut response: reqwest::Response) -> Option<u64> {
    let chunk = response.chunk().await.ok()??;
    if chunk.len() > 8 * 1024 {
        return None;
    }
    serde_json::from_slice::<Value>(&chunk)
        .ok()?
        .get("code")?
        .as_u64()
}

fn response_content(text: &str) -> Value {
    json!({"content":text,"allowed_mentions":{"parse":[],"replied_user":false}})
}

fn private_text(text: &str) -> Value {
    let mut payload = response_content(text);
    payload["flags"] = json!(EPHEMERAL);
    payload
}

fn model_picker_payload(event: &str, choices: &[String]) -> Value {
    let mut payload = response_content("Choose a model from the configured provider.");
    let options: Vec<_> = choices
        .iter()
        .enumerate()
        .map(|(index, model)| {
            let label = &model[..model.floor_char_boundary(model.len().min(100))];
            json!({"label":label,"value":index.to_string()})
        })
        .collect();
    payload["components"] = json!([{"type":1,"components":[{
        "type":3,"custom_id":format!("mrc:model:{event}"),
        "options":options,"min_values":1,"max_values":1,
    }]}]);
    payload
}

fn outcome_text(outcome: InvocationOutcome) -> String {
    match outcome {
        InvocationOutcome::Ready => "Mercury is ready.".into(),
        InvocationOutcome::ModelPicker(_) => "Model choices are ready.".into(),
        InvocationOutcome::Queue { pending, running } => format!(
            "{pending} queued. {}",
            if running {
                "A turn is running."
            } else {
                "The conversation is idle."
            }
        ),
        InvocationOutcome::History { turns } if turns.is_empty() => {
            "This conversation has no delivered turns yet.".into()
        }
        InvocationOutcome::History { turns } => turns.into_iter().map(|turn| {
            format!("**You**\n{}\n**Mercury**\n{}", turn.prompt, turn.reply)
        }).collect::<Vec<_>>().join("\n\n"),
        InvocationOutcome::HistoryCursor(_) => "The history is unavailable.".into(),
        InvocationOutcome::SpokenReplies { mode } => match mode {
            SpokenReplyMode::Off => "Voice replies are off.",
            SpokenReplyMode::VoiceNotes => "Voice replies follow incoming voice notes.",
            SpokenReplyMode::Always => "Mercury will speak channel replies.",
        }.into(),
        InvocationOutcome::Applied => "Applied.".into(),
        InvocationOutcome::PendingResult => "The result is still pending.".into(),
        InvocationOutcome::Busy => {
            "The conversation still has work. Retry after it settles.".into()
        }
        InvocationOutcome::BackgroundQueued => {
            "Background work queued. The result will arrive in this conversation.".into()
        }
        InvocationOutcome::ResultReady { .. } => "The result is ready.".into(),
        InvocationOutcome::AlreadyClaimed => "The result was already claimed.".into(),
        InvocationOutcome::Expired => "The action expired.".into(),
        InvocationOutcome::Stale => "The request is no longer current.".into(),
        InvocationOutcome::Forbidden => "You aren't allowed to apply this action.".into(),
        InvocationOutcome::Unsupported => "This operation is unavailable.".into(),
    }
}

pub(super) async fn synchronize(
    client: &DiscordClient,
    application: &str,
    guild: Option<&str>,
    voice: bool,
    voice_notes: bool,
) -> Result<(), RestFailure> {
    let mut collection = vec!["applications", application];
    if let Some(guild) = guild {
        collection.extend(["guilds", guild]);
    }
    collection.push("commands");
    let old: Vec<Value> = client.get(&collection).await?;
    let mut names = BTreeSet::new();
    let mut catalogue = COMMANDS
        .iter()
        .map(|command| {
            let name = if command.name == "set-home" {
                "sethome"
            } else {
                command.name
            };
            let options = command
                .arguments
                .iter()
                .map(|argument| {
                    let mut field = option(argument.name);
                    if name == "model" {
                        field["required"] = json!(false);
                    }
                    field
                })
                .collect::<Vec<_>>();
            (name, options, command.operator)
        })
        .collect::<Vec<_>>();
    catalogue.extend([
        ("help", vec![], false),
        ("whoami", vec![], false),
        ("stop", vec![], true),
        ("steer", vec![option("text")], true),
        ("redirect", vec![option("text")], true),
        ("edit", vec![option("message"), option("text")], false),
        ("thread", vec![
            json!({"name":"name","description":"Thread title","type":3,"required":true,"min_length":1,"max_length":100}),
            json!({"name":"message","description":"Optional message posted inside the thread","type":3,"required":false,"max_length":2000}),
            json!({"name":"auto_archive_duration","description":"Minutes of inactivity before archive","type":4,"required":false,
                "choices":[{"name":"1 hour","value":60},{"name":"1 day","value":1440},
                    {"name":"3 days","value":4320},{"name":"7 days","value":10080}]}),
        ], false),
    ]);
    if voice || voice_notes {
        let mut choices = vec!["status"];
        if voice_notes { choices.extend(["on", "off", "tts"]); }
        if voice { choices.extend(["join", "leave"]); }
        let mut action = option("action");
        action["required"] = json!(false);
        action["choices"] = json!(choices.into_iter().map(|name| json!({"name":name,"value":name})).collect::<Vec<_>>());
        catalogue.push(("voice", vec![action], true));
    }
    for (name, options, operator) in catalogue {
        if !names.insert(name) {
            continue;
        }
        let mut desired = json!({"name":name,"type":1,"description":format!("Mercury {name}"),
            "options":options,"default_member_permissions":if operator { Some("0") } else { None }});
        if guild.is_none() {
            desired["contexts"] = if name == "thread" { json!([0]) } else { json!([0, 1]) };
            desired["integration_types"] = json!([0]);
        }
        if name == "thread" {
            desired["default_member_permissions"] = json!(super::settings::CREATE_PUBLIC_THREADS.to_string());
        }
        let mut path = collection.clone();
        let method = if let Some(existing) = old.iter().find(|item| {
            item.get("name").and_then(Value::as_str) == Some(name)
                && item.get("type").and_then(Value::as_u64) == Some(1)
        }) {
            if desired.as_object().is_some_and(|fields| {
                fields.iter().all(|(key, value)| {
                    existing.get(key) == Some(value)
                        || value.is_null() && existing.get(key).is_none()
                        || key == "options"
                            && value.as_array().is_some_and(Vec::is_empty)
                            && existing.get(key).is_none()
                })
            }) {
                continue;
            }
            path.push(read_id(existing, "id").ok_or(RestFailure::Invalid)?);
            Method::PATCH
        } else {
            Method::POST
        };
        let response = client
            .bot(method, &path)
            .json(&desired)
            .send()
            .await
            .map_err(|_| RestFailure::Unavailable)?;
        let _: Value = json_response(response).await?;
    }
    Ok(())
}

fn option(name: &str) -> Value {
    json!({"type":3,"name":name,"description":format!("Mercury {name}"),"required":true})
}
