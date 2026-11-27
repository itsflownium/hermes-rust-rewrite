use super::{
    commands::{self, Command},
    runtime::{Ingress, db},
    store::{self, Action, PendingInput, Route},
    types::*,
};
use crate::{LiveSession, Settings, Shared, authority::ChannelExecution, lock};
use hermes_engine::{Attachment, CancellationToken};
use serde_json::Value;
use std::{io::Read, path::Path, sync::Arc};

pub(super) async fn advance(
    owner: &Arc<Shared>,
    scope: &Ingress,
    adapter: &dyn Adapter,
    stop: &CancellationToken,
) -> Result<(), GatewayError> {
    db(scope.store.expire_preparations(&scope.connection, chrono::Utc::now().timestamp_millis()))?;
    for input in db(scope.store.inputs(&scope.connection))? {
        if stop.is_cancelled() {
            return Ok(());
        }
        match &input.event.body {
            InboundBody::Control(control) => apply_control(owner, scope, &input, control).await?,
            InboundBody::Command(Command::UseSkill { .. } | Command::Background { .. })
            | InboundBody::Message(_) | InboundBody::Wake => advance_message(owner, scope, adapter, input).await?,
            InboundBody::Command(command) => apply_command(owner, scope, &input, command).await?,
        }
    }
    Ok(())
}

async fn session_for(
    owner: &Arc<Shared>,
    scope: &Ingress,
    route: &Route,
    create: bool,
) -> Result<Option<Arc<LiveSession>>, GatewayError> {
    let binding = db(scope.store.conversation(route))?;
    if binding.blocked {
        return Ok(None);
    }
    let route = &binding;
    let creation_key = db(store::key("channel-session", &(&route.id, route.revision)))?;
    let session_id = match &route.session {
        Some(id) => id.clone(),
        None if !create => return Ok(None),
        None => {
            let owner = owner.clone();
            let key = creation_key.clone();
            let route_id = route.id.clone();
            let title = if route.is_background() {
                format!("{} background job", scope.platform())
            } else {
                format!("{} conversation", scope.platform())
            };
            let created = tokio::task::spawn_blocking(move || {
                let mut collision = false;
                let result = owner
                    .library
                    .create_background_session(&title, &key, |summary| {
                        let settings = Settings::read(&summary.directory)?;
                        if settings.channel_route.as_ref() == Some(&route_id)
                            && settings.creation_key.as_ref() == Some(&key)
                        {
                            return Ok(());
                        }
                        if !settings.source.is_empty()
                            || settings.creation_key.is_some()
                            || hermes_engine::has_conversation(&summary.directory)?
                        {
                            collision = true;
                            anyhow::bail!("Channel route collides with an existing session");
                        }
                        Settings {
                            source: "channel".into(),
                            channel_route: Some(route_id),
                            creation_key: Some(key.clone()),
                            ..Settings::default()
                        }
                        .save(&summary.directory)
                    });
                if collision {
                    Ok(None)
                } else {
                    result.map(|summary| Some(summary.id))
                }
            })
            .await
            .map_err(|_| GatewayError::TemporarilyUnavailable)?;
            let Some(id) = db(created)? else {
                db(scope.store.block_route(&route.id))?;
                return Ok(None);
            };
            db(scope.store.bind(route, &id))?;
            id
        }
    };
    match owner.session(&session_id).await {
        Ok(session) => {
            let matches = {
                let settings = lock(&session.settings);
                settings.channel_route.as_ref() == Some(&route.id)
                    && settings.creation_key.as_ref() == Some(&creation_key)
            };
            if matches {
                Ok(Some(session))
            } else {
                db(scope.store.block_route(&route.id))?;
                Ok(None)
            }
        }
        Err(error) if error.code == 4004 => {
            db(scope.store.block_route(&route.id))?;
            Ok(None)
        }
        Err(error)
            if error.data["reason"] == "session_busy"
                || error.data["reason"] == "session_limit" =>
        {
            Ok(None)
        }
        Err(_) => Err(GatewayError::TemporarilyUnavailable),
    }
}

fn submission(route: &Route, event: &str) -> Result<String, GatewayError> {
    db(store::key("channel-input", &(&route.connection, event)))
}

fn authorized(
    scope: &Ingress,
    state: &super::runtime::State,
    work: &PendingInput,
) -> Result<bool, GatewayError> {
    if matches!(work.event.body, InboundBody::Wake) {
        let Some(target) = db(scope.store.wake_target(&scope.connection))? else { return Ok(false) };
        return Ok(scope.capabilities().wake && db(target.route(&scope.connection))?.id == work.route.id);
    }
    Ok(scope
        .route_event(&state.policy, &work.event)?
        .is_ok_and(|route| route.id == work.route.id)
        && scope.thread_source_allowed(state, &work.event.room, &work.event.sender, &work.event.id)?
        && scope.recheck_quote(state, &work.event, &work.route)?)
}

async fn advance_message(
    owner: &Arc<Shared>,
    scope: &Ingress,
    adapter: &dyn Adapter,
    work: PendingInput,
) -> Result<(), GatewayError> {
    let wake = matches!(work.event.body, InboundBody::Wake);
    let background = work.route.is_background();
    let unadmitted = matches!(work.stage.as_str(), "pending" | "admitting");
    let allowed = scope.checked(true, |state| authorized(scope, state, &work))?;
    let quote_revoked = !allowed
        && !scope.checked(true, |state| scope.recheck_quote(state, &work.event, &work.route))?;
    let session = session_for(owner, scope, &work.route, allowed && unadmitted).await?;
    let native_id = submission(&work.route, &work.event.id)?;
    if quote_revoked {
        let settled = match &session {
            Some(session) => db(owner.retire_channel_submission(session, &native_id))?,
            None => work.route.session.is_none() && unadmitted,
        };
        if settled {
            db(scope.store.refuse_quoted_input(&work.route, &work.event.id))?;
        }
        return Ok(());
    }
    let Some(session) = session else {
        return Ok(());
    };
    let receipt = match owner.submission_receipt(&session, &native_id) {
        Some(receipt) => receipt,
        None if !unadmitted => {
            db(scope.store.mark(
                &work.route,
                &work.event.id,
                "blocked",
                Some("native_receipt_missing"),
            ))?;
            return db(scope.store.block_route(&work.route.id));
        }
        None if !allowed => return Ok(()),
        None => {
            let synthetic_message;
            let message = match &work.event.body {
                InboundBody::Message(message) => message,
                InboundBody::Wake => {
                    synthetic_message = scope.checked(true, |state| {
                        if !authorized(scope, state, &work)? {
                            return Err(GatewayError::Forbidden);
                        }
                        let target = db(scope.store.wake_target(&scope.connection))?
                            .ok_or(GatewayError::Invalid)?;
                        db(scope.store.reserve_wake(&work.route, &work.event.id))?;
                        Ok(Message::text(target.instructions))
                    })?;
                    &synthetic_message
                }
                InboundBody::Command(Command::UseSkill { input, .. } | Command::Background { text: input }) => {
                    synthetic_message = Message::text(input);
                    &synthetic_message
                }
                _ => return Err(GatewayError::Invalid),
            };
            let attachments = match scope.checked(true, |state| {
                if !authorized(scope, state, &work)? {
                    return Ok(None);
                }
                capture(scope, &session.dir, &work.event, &message.attachments)
                    .map(Some)
                    .map_err(|_| GatewayError::Invalid)
            }) {
                Ok(Some(files)) => files,
                Ok(None) => return Ok(()),
                Err(GatewayError::Invalid) => {
                    return db(scope.store.mark(
                        &work.route,
                        &work.event.id,
                        "done",
                        Some("invalid_attachment"),
                    ));
                }
                Err(error) => return Err(error),
            };
            let (attachments, opaque) = attachments;
            let text = if opaque && message.text.trim().is_empty() && attachments.is_empty() {
                "A file was attached.".to_owned()
            } else {
                message.text.clone()
            };
            let result = owner.submit_channel(session, text, attachments, native_id).await;
            if let Err(error) = result {
                return match error.data["reason"].as_str() {
                    Some("admission_conflict") => {
                        db(scope.store.mark(
                            &work.route,
                            &work.event.id,
                            "blocked",
                            Some("native_admission_conflict"),
                        ))?;
                        db(scope.store.block_route(&work.route.id))
                    }
                    Some("provider_configuration") => db(scope.store.mark(
                        &work.route,
                        &work.event.id,
                        "pending",
                        Some("provider_configuration"),
                    )),
                    Some("queue_full") => db(scope.store.mark(
                        &work.route,
                        &work.event.id,
                        "pending",
                        Some("native_queue_full"),
                    )),
                    Some("invalid_params") => db(scope.store.mark(
                        &work.route,
                        &work.event.id,
                        "done",
                        Some("invalid_input"),
                    )),
                    _ => Err(GatewayError::TemporarilyUnavailable),
                };
            }
            return db(scope
                .store
                .mark(&work.route, &work.event.id, "admitted", None));
        }
    };

    if unadmitted {
        db(scope
            .store
            .mark(&work.route, &work.event.id, "admitted", None))?;
    }
    // Admit the next input while an earlier turn is active. The route head
    // alone may claim an execution or publish its committed reply.
    if !work.head {
        return Ok(());
    }
    match receipt["status"].as_str() {
        Some("queued") if allowed => {
            let feedback = (!background && !wake)
                .then(|| adapter.first_tool_feedback(scope.clone(), &work.event))
                .flatten();
            let claim = scope.checked(true, |state| {
                if !authorized(scope, state, &work)? {
                    return Ok(None);
                }
                let context = scope.prepared_context_source(state, &work.event, &work.route)?;
                let attachments = attachment_context(scope, &work)?;
                let individual = wake || scope.authorized_identity(&state.policy, &work.event.room, &work.event.sender)?
                    .is_some_and(|(_, kind)| kind == super::ActorKind::Person);
                let resource = super::profile::resource_event_id(&work.event.id);
                let tools = individual && !resource && state.policy.room_policy(&work.event.room).is_some_and(|room| room.tools);
                let mut policy = state.policy.turn_policy(&work.event.room);
                if !individual || resource {
                    policy.prompt = None;
                    policy.skills = None;
                }
                let model_route = if background {
                    let participant = state.policy.room_policy(&work.event.room)
                        .filter(|room| room.per_participant)
                        .map(|_| work.event.sender.id.as_str());
                    db(Route::new(&scope.connection, work.event.room.clone(), participant))?.id
                } else {
                    work.route.id.clone()
                };
                let model = policy.model.or(db(scope.store.route_model(
                    &model_route, &scope.model_provider_scope()?,
                ))?);
                let selected_skill = match &work.event.body {
                    InboundBody::Command(Command::UseSkill { skill, .. }) => Some(skill.clone()),
                    _ => None,
                };
                let approval_mode = policy.approval.map(|mode| match mode {
                    super::ApprovalPolicy::Manual => "manual",
                    super::ApprovalPolicy::Smart => "smart",
                    super::ApprovalPolicy::Off => "off",
                }.to_owned());
                db(owner.claim_channel(
                    &session,
                    &native_id,
                    ChannelExecution {
                        tools,
                        interactive: individual && !background && scope.capabilities().interactive_prompts,
                        model,
                        first_tool_feedback: if tools { feedback } else { None },
                        approval_context: (individual && !background && !wake && tools).then(|| super::ToolApprovalContext {
                            ingress: scope.clone(),
                            route: work.route.id.clone(),
                            room: work.event.room.clone(),
                            actor: work.event.sender.clone(),
                            session: lock(&session.summary).id.clone(),
                        }),
                        policy: hermes_engine::ChannelTurnPolicy {
                            prompt: policy.prompt,
                            approval_mode,
                            skills: policy.skills,
                            selected_skill,
                            context,
                            attachments,
                        },
                    },
                ))
            })?;
            if let Some(claim) = claim {
                owner.spawn_worker(session, claim).await;
            }
        }
        Some("started") if allowed && !background => {
            scope.checked(true, |state| {
                if !authorized(scope, state, &work)? {
                    return Ok(());
                }
                let caps = scope.capabilities();
                if wake {
                    let session_id = lock(&session.summary).id.clone();
                    let _ = scope.store.wake_activity(
                        &scope.connection, &work.event.id, &session_id, super::WakeStatus::Thinking,
                    );
                    return Ok(());
                }
                if caps.progress {
                    let reply = db(store::key("reply", &native_id))?;
                    db(scope
                        .store
                        .publish_progress(&work.route, &work.event, reply))?;
                }
                if caps.interactive_prompts && scope.authorized_identity(&state.policy, &work.event.room, &work.event.sender)?
                    .is_some_and(|(_, kind)| kind == super::ActorKind::Person)
                {
                    publish_prompts(scope, &session, &work, &receipt,
                        state.policy.turn_policy(&work.event.room).approval_reuse)?;
                }
                Ok(())
            })?;
        }
        Some("unknown") if work.stage != "uncertain" => {
            db(scope.store.mark(
                &work.route,
                &work.event.id,
                "uncertain",
                Some("execution_outcome_unknown"),
            ))?;
        }
        Some("terminal") => {
            if wake {
                let session_id = lock(&session.summary).id.clone();
                let status = if receipt["outcome"] == "complete" {
                    super::WakeStatus::Complete
                } else {
                    super::WakeStatus::Error
                };
                let _ = scope.store.wake_activity(&scope.connection, &work.event.id, &session_id, status);
                return db(scope.store.mark(&work.route, &work.event.id, "done", None));
            }
            if receipt["outcome"] == "complete" {
                let text = receipt["result"]["text"].as_str().unwrap_or_default();
                let tools = scope.checked(true, |state| {
                    Ok(scope.authorized_identity(&state.policy, &work.event.room, &work.event.sender)?
                        .is_some_and(|(_, kind)| kind == super::ActorKind::Person)
                        && state.policy.room_policy(&work.event.room).is_some_and(|policy| policy.tools))
                })?;
                let message = outbound_message(scope, &session.dir, text, tools);
                if !message.text.trim().is_empty() || !message.attachments.is_empty() {
                    let intent = DeliveryIntent {
                        id: db(store::key("reply", &native_id))?,
                        room: work.route.room.clone(),
                        reply_anchor: work.event.reply_anchor.clone(),
                        edit_target: None,
                        body: DeliveryBody::Reply(message),
                    };
                    return scope.checked(true, |state| {
                        if !scope.recheck_quote(state, &work.event, &work.route)? {
                            return Ok(());
                        }
                        db(scope.store.publish_reply(&work.route, &work.event, &intent))
                    });
                }
            }
            db(scope.store.mark(&work.route, &work.event.id, "done", None))?;
        }
        _ => {}
    }
    Ok(())
}

fn publish_prompts(
    scope: &Ingress,
    session: &LiveSession,
    work: &PendingInput,
    receipt: &Value,
    reuse: Option<super::ApprovalReuse>,
) -> Result<(), GatewayError> {
    let generation = receipt["execution_generation"]
        .as_u64()
        .ok_or(GatewayError::Invalid)?;
    let prompt_list = {
        let view = lock(&session.view);
        if !view.running || view.generation != generation {
            return Ok(());
        }
        let approvals = view
            .approvals
            .iter()
            .filter(|(_, pending)| !pending.answer.is_closed())
            .map(|(id, pending)| {
                let text = pending.payload["command"]
                    .as_str()
                    .unwrap_or("Approve this tool call?")
                    .to_owned();
                (id.clone(), "approval", pending.expires_ms, pending.tool.clone(), Prompt::Approval {
                    text, allow_session: reuse.is_some(),
                    allow_persistent: reuse == Some(super::ApprovalReuse::Persistent),
                })
            });
        let clarifications = view
            .clarifications
            .iter()
            .filter(|(_, pending)| !pending.answer.is_closed())
            .map(|(id, pending)| {
                let question = pending.payload["question"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let choices =
                    serde_json::from_value(pending.payload["choices"].clone()).unwrap_or_default();
                (
                    id.clone(),
                    "clarification",
                    pending.expires_ms,
                    None,
                    Prompt::Clarification { question, choices },
                )
            });
        approvals.chain(clarifications).collect::<Vec<_>>()
    };
    let session_id = lock(&session.summary).id.clone();
    for (request, kind, expires, tool, prompt) in prompt_list {
        let action = Action {
            id: db(store::key(
                "action",
                &(&work.route.id, &session_id, generation, &request),
            ))?,
            route: work.route.id.clone(),
            session: session_id.clone(),
            recipient: work.event.sender.id.clone(),
            generation,
            request,
            kind: kind.into(),
            expires,
            tool,
            resolved: None,
        };
        db(scope
            .store
            .prompt(&work.route, &work.event, &action, prompt))?;
    }
    Ok(())
}

async fn apply_command(
    owner: &Arc<Shared>,
    scope: &Ingress,
    work: &PendingInput,
    command: &Command,
) -> Result<(), GatewayError> {
    if command.uses_profile_model() {
        return commands::apply_profile_model(owner, scope, work, command).await;
    }
    let session = if matches!(command, Command::Queue) {
        session_for(owner, scope, &work.route, false).await?
    } else { None };
    scope.checked(true, |state| commands::execute(
        scope, work, command, session.as_deref(),
        authorized(scope, state, work)? && match command {
            Command::SelectModel { model } => state.policy.turn_policy(&work.event.room).model
                .is_none_or(|owner_model| owner_model == *model),
            _ => true,
        },
        scope.authorize_operator(&state.policy, &work.event.room, &work.event.sender)?,
        state.policy.spoken_reply_default(&work.event.room),
    ))
}

async fn apply_control(
    owner: &Arc<Shared>,
    scope: &Ingress,
    work: &PendingInput,
    control: &Control,
) -> Result<(), GatewayError> {
    let session = session_for(owner, scope, &work.route, false).await?;
    scope.checked(true, |state| {
        if !authorized(scope, state, work)? {
            return finish_control(scope, work, "forbidden");
        }
        let operator = scope.authorize_operator(&state.policy, &work.event.room, &work.event.sender)?;
        if let Control::ReplaceQueued { event_id, text } = control {
            let Some((stage, target)) = db(scope.store.input(&work.route, event_id))? else {
                return finish_control(scope, work, "stale");
            };
            if !operator && target.sender.id != work.event.sender.id {
                return finish_control(scope, work, "forbidden");
            }
            if let Some(session) = &session
                && let Some(receipt) =
                    owner.submission_receipt(session, &submission(&work.route, event_id)?)
                && let Some(id) = receipt["admission_id"].as_str()
            {
                return finish_control(
                    scope,
                    work,
                    outcome(owner.replace_queued(session, id, text))?,
                );
            }
            if stage == "pending" && scope.route_event(&state.policy, &target)?.is_ok() {
                // The durable input will get its native admission before a
                // queued edit is applied on the next pass.
                return Ok(());
            }
            return finish_control(scope, work, "stale");
        }
        let Some(session) = &session else {
            return finish_control(scope, work, "stale");
        };
        if let Control::Respond {
            action_id,
            response,
        } = control
        {
            let Some(action) = db(scope.store.action(&work.route, action_id))? else {
                return finish_control(scope, work, "stale");
            };
            let clarification = matches!(response, Response::Clarify(_));
            if action.session != lock(&session.summary).id
                || action.route != work.route.id
                || action.recipient != work.event.sender.id
                || clarification != (action.kind == "clarification")
                || (!clarification && !operator)
            {
                return finish_control(scope, work, "forbidden");
            }
            if let Some(old) = action.resolved.as_ref() {
                return finish_control(
                    scope,
                    work,
                    if old == response { "applied" } else { "stale" },
                );
            }
            if action.expires <= chrono::Utc::now().timestamp_millis() {
                return finish_control(scope, work, "expired");
            }
            if !super::approvals::permits(response, state.policy.turn_policy(&work.event.room).approval_reuse) {
                return finish_control(scope, work, "forbidden");
            }
            let policy = db(store::key("approval-policy", &(&state.policy, state.revision, scope.installation())))?;
            let result = outcome(owner.answer_prompt_checked(
                session,
                &action.request,
                action.generation,
                response,
                || {
                    scope.store.settle_action(
                        &work.route,
                        &work.event.id,
                        &action,
                        response,
                        &policy,
                        scope.capabilities().effects.durable_presentation,
                    ).map_err(crate::RpcError::internal)
                },
            ))?;
            return if result == "applied" { Ok(()) } else { finish_control(scope, work, result) };
        }
        if !operator {
            return finish_control(scope, work, "forbidden");
        }
        let result = match control {
            Control::Interrupt {
                execution_generation,
            } => owner.interrupt_generation(session, *execution_generation),
            Control::Steer {
                execution_generation,
                text,
            }
            | Control::Redirect {
                execution_generation,
                text,
            } => {
                let id = db(store::key(
                    "channel-control",
                    &(&work.route.connection, &work.event.id),
                ))?;
                owner.correct_turn(
                    session,
                    *execution_generation,
                    text,
                    matches!(control, Control::Redirect { .. }),
                    Some(&id),
                )
            }
            Control::ReplaceQueued { .. } | Control::Respond { .. } => {
                unreachable!("handled above")
            }
        };
        finish_control(scope, work, outcome(result)?)
    })
}

fn outcome(result: crate::RpcResult) -> Result<&'static str, GatewayError> {
    match result {
        Ok(_) => Ok("applied"),
        Err(error) if error.code == -32603 => Err(GatewayError::TemporarilyUnavailable),
        Err(_) => Ok("stale"),
    }
}

fn finish_control(scope: &Ingress, work: &PendingInput, result: &str) -> Result<(), GatewayError> {
    let outcome = match result {
        "applied" => super::InvocationOutcome::Applied,
        "expired" => super::InvocationOutcome::Expired,
        "forbidden" => super::InvocationOutcome::Forbidden,
        _ => super::InvocationOutcome::Stale,
    };
    db(scope.store.write(|tx| {
        super::commands::record_invocation(tx, &work.route, &work.event.id, &work.event.sender.id, outcome)?;
        store::set_input(tx, &work.route, &work.event.id, "done", Some(result))
    }))
}

fn capture(
    scope: &Ingress,
    session: &Path,
    event: &InboundEvent,
    refs: &[AttachmentRef],
) -> anyhow::Result<(Vec<Attachment>, bool)> {
    let media = scope.store.read(|db| super::media::input_media(db, &scope.connection, event, refs))?
        .ok_or_else(|| anyhow::anyhow!("Attachment source is not available"))?;
    let mut files = Vec::new();
    let mut opaque = false;
    for entry in media {
        if entry.preview {
            let bytes = scope.store.attachment(&scope.connection, &entry.file)?;
            files.push(Attachment::capture_bytes(session, entry.file.name, &bytes)?);
        } else {
            opaque = true;
        }
    }
    Ok((files, opaque))
}

fn attachment_context(
    scope: &Ingress,
    work: &PendingInput,
) -> Result<Option<hermes_engine::ChannelAttachments>, GatewayError> {
    let InboundBody::Message(message) = &work.event.body else { return Ok(None) };
    if message.attachments.is_empty() {
        return Ok(None);
    }
    let media = db(scope.store.read(|tx| {
        super::media::input_media(tx, &scope.connection, &work.event, &message.attachments)
    }))?.ok_or(GatewayError::Forbidden)?;
    let mut files = Vec::new();
    let mut refs = std::collections::BTreeMap::new();
    for (index, entry) in media.into_iter().enumerate() {
        if !entry.preview {
            let id = db(store::key("input-attachment", &(&scope.connection, &work.event.id, index)))?;
            files.push(hermes_engine::ChannelAttachment {
                id: id.clone(),
                filename: entry.file.name.clone(),
                bytes: entry.size,
            });
            refs.insert(id, entry.file);
        }
    }
    if files.is_empty() {
        return Ok(None);
    }

    let work = Arc::new(work.clone());
    let (check_scope, check_work) = (scope.clone(), work.clone());
    let recheck = Arc::new(move || check_scope.checked(true, |state| {
        if !authorized(&check_scope, state, &check_work)? {
            return Err(GatewayError::Forbidden);
        }
        Ok(())
    }).map_err(Into::into));
    let reader = scope.clone();
    let read = Arc::new(move |id: &str, offset, limit| -> anyhow::Result<Vec<u8>> {
        let reference = refs.get(id).ok_or(GatewayError::Forbidden)?;
        anyhow::ensure!(limit > 0 && limit <= hermes_engine::MAX_CHANNEL_ATTACHMENT_READ,
            "Invalid attachment range");
        Ok(reader.checked(true, |state| {
            if !authorized(&reader, state, &work)?
                || !state.policy.room_policy(&work.event.room).is_some_and(|room| room.tools)
                || !reader.authorized_identity(&state.policy, &work.event.room, &work.event.sender)?
                    .is_some_and(|(_, kind)| kind == super::ActorKind::Person)
            {
                return Err(GatewayError::Forbidden);
            }
            db(reader.store.read(|tx| {
                super::media::read_input_artifact(tx, &reader.connection, &work.event, reference, offset, limit)
            }))?.ok_or(GatewayError::Forbidden)
        })?)
    });
    Ok(Some(hermes_engine::ChannelAttachments { files, recheck, read }))
}

fn outbound_message(scope: &Ingress, session: &Path, text: &str, tools: bool) -> Message {
    let mut message = Message::text("");
    for line in text.lines() {
        if let Some(path) = line.trim().strip_prefix("MEDIA:") {
            if tools
                && scope.capabilities().attachments
                && message.attachments.len() < 8
                && let Some(file) = reply_file(scope, &session.join("workspace"), path.trim())
            {
                message.attachments.push(file);
            }
        } else {
            if !message.text.is_empty() {
                message.text.push('\n');
            }
            message.text.push_str(line);
        }
    }
    message
}

fn reply_file(scope: &Ingress, workspace: &Path, path: &str) -> Option<AttachmentRef> {
    const LIMIT: u64 = 4 * 1024 * 1024;
    let workspace = workspace.canonicalize().ok()?;
    let file = workspace.join(path).canonicalize().ok()?;
    if file == workspace
        || !file.starts_with(&workspace)
        || !std::fs::metadata(&file).ok()?.is_file()
    {
        return None;
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&file)
        .ok()?
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > LIMIT {
        return None;
    }
    let name = file.file_name()?.to_str()?.to_owned();
    scope
        .store
        .put_attachment(&scope.connection, name, &bytes)
        .ok()
}
