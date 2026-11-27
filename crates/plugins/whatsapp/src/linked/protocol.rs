use super::{
    Linked, Mode, Settings,
    ipc::{Bridge, CallFailure, MAX_MEDIA},
    persist_active, record_revocation,
};
use crate::{
    common::{self, Media, MediaKind, jid, lock, text_id},
    intake::{self, MediaDescription, MediaReader, MediaStream, Source},
    outbound,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hermes_gateway_core::messaging::{
    AdapterFuture, ConnectionFailure, DeliveryBody, DeliveryResult, GatewayError, Ingress,
    ProviderDeliveryState, Room, RoomKind, SendAttempt,
    runtime::{db, now},
    storage,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::{io::Cursor, time::Duration};
use tokio_util::sync::CancellationToken;

pub(super) struct Reader<'a> {
    pub bridge: &'a Bridge,
    pub settings: &'a Settings,
}

impl MediaReader for Reader<'_> {
    fn signal<'a>(
        &'a self,
        _: &'a Ingress,
        source: &'a Source,
        _: &'a CancellationToken,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            if self.settings.read_signals || self.settings.typing {
                let _ = self.bridge.notify("reading",json!({"chat":source.provider_chat,"id":source.anchor,
                    "participant":source.provider_actor,"read":self.settings.read_signals,"typing":self.settings.typing}));
            }
        })
    }

    fn describe<'a>(
        &'a self,
        _: &'a Ingress,
        media: &'a Media,
        _: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaDescription, GatewayError>> {
        Box::pin(async move {
            let origin = media.locator.as_ref().ok_or(GatewayError::Invalid)?;
            let size = origin["size"]
                .as_u64()
                .and_then(|size| usize::try_from(size).ok())
                .filter(|size| *size <= MAX_MEDIA)
                .ok_or(GatewayError::Invalid)?;
            let name = media.name.clone().unwrap_or_else(|| {
                match media.kind {
                    MediaKind::Image => "whatsapp.jpg",
                    MediaKind::Video => "whatsapp.mp4",
                    MediaKind::Audio => "whatsapp.ogg",
                    MediaKind::Document => "whatsapp.bin",
                }
                .into()
            });
            Ok(MediaDescription {
                name,
                size,
                endpoint: media.id.clone(),
            })
        })
    }

    fn open<'a>(
        &'a self,
        media: &'a Media,
        address: &'a MediaDescription,
        limit: usize,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaStream, GatewayError>> {
        Box::pin(async move {
            let proof = media.locator.as_ref().ok_or(GatewayError::Invalid)?;
            let response = self
                .bridge
                .request(
                    "media",
                    json!({"locator":proof,"limit":limit.min(MAX_MEDIA)}),
                    cancel,
                )
                .await
                .map_err(|error| match error {
                    CallFailure::Rejected => GatewayError::Forbidden,
                    _ => GatewayError::TemporarilyUnavailable,
                })?;
            let encoded = response["bytes"]
                .as_str()
                .filter(|s| s.len() <= MAX_MEDIA.div_ceil(3) * 4)
                .ok_or(GatewayError::Invalid)?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| GatewayError::Invalid)?;
            if bytes.len() != address.size || bytes.len() > limit {
                return Err(GatewayError::Invalid);
            }
            Ok(Box::pin(Cursor::new(bytes)) as MediaStream)
        })
    }
}

pub(super) async fn events(
    linked: &Linked,
    bridge: &Bridge,
    ingress: &Ingress,
    cancel: &CancellationToken,
) -> ConnectionFailure {
    let saved = match pending(ingress) {
        Ok(saved) => saved,
        Err(_) => return ConnectionFailure::Retryable,
    };
    for data in saved {
        if handoff(
            &linked.settings,
            bridge,
            ingress,
            &linked.status,
            &data,
            cancel,
        )
        .await
        .is_err()
        {
            return ConnectionFailure::Retryable;
        }
    }
    while let Some(event) = bridge.next().await {
        if cancel.is_cancelled() {
            return ConnectionFailure::Stopped;
        }
        let data = event.data;
        match event.kind.as_str() {
            "credentials" => {
                let Some(auth) = data.get("auth") else {
                    return ConnectionFailure::Retryable;
                };
                if persist_active(ingress, &linked.auth_file, auth.clone())
                    .await
                    .is_err()
                {
                    return ConnectionFailure::Retryable;
                }
                if let Some(revision) = data["revision"].as_u64() {
                    if bridge
                        .notify("persisted", json!({"revision":revision}))
                        .is_err()
                    {
                        return ConnectionFailure::Retryable;
                    }
                }
            }
            "handoff" => {
                if handoff(
                    &linked.settings,
                    bridge,
                    ingress,
                    &linked.status,
                    &data,
                    cancel,
                )
                .await
                .is_err()
                {
                    return ConnectionFailure::Retryable;
                }
            }
            "receipt" => {
                if status(ingress, &data).await.is_err() {
                    return ConnectionFailure::Retryable;
                }
            }
            "gap" => {
                let _ = ingress.report_gap();
            }
            "member_left" => {
                if let (Some(chat), Some(wire)) = (
                    data["chat"].as_str().filter(|id| jid(id)),
                    data["actor"].as_str().filter(|id| jid(id)),
                ) && let Ok(Some(actor)) = common::lookup_alias(ingress, wire)
                {
                    let chat = chat.to_owned();
                    if ingress
                        .withdraw_observed_memberships(actor, move |member| {
                            member.room.chat != chat
                        })
                        .await
                        .is_err()
                    {
                        return ConnectionFailure::Retryable;
                    }
                }
            }
            "ready" => {
                if data["account"] != linked.settings.account || data["device"] != linked.device {
                    lock(&linked.status)["bridge"] = json!("account_mismatch");
                    return ConnectionFailure::Credentials;
                }
            }
            "closed" if data["revoked"] == true => {
                let durable = ingress.checked(true, |_| db(record_revocation(&linked.auth_file)));
                if durable.is_err() {
                    return ConnectionFailure::Retryable;
                }
                lock(&linked.status)["bridge"] = json!("revoked");
                return ConnectionFailure::Credentials;
            }
            "closed" => {
                lock(&linked.status)["bridge"] = json!("reconnecting");
                return ConnectionFailure::Retryable;
            }
            _ => {}
        }
    }
    if bridge.state().revoked {
        if ingress
            .checked(true, |_| db(record_revocation(&linked.auth_file)))
            .is_err()
        {
            return ConnectionFailure::Retryable;
        }
        lock(&linked.status)["bridge"] = json!("revoked");
        return ConnectionFailure::Credentials;
    }
    ConnectionFailure::Retryable
}

fn pending(ingress: &Ingress) -> Result<Vec<Value>, GatewayError> {
    let rows = ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            let mut query = conn.prepare(
                "SELECT body FROM messaging_whatsapp_linked_handoffs
            WHERE connection=?1 AND complete=0 ORDER BY rowid LIMIT 256",
            )?;
            let rows =
                query.query_map([ingress.connection_name()], |row| row.get::<_, String>(0))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }))
    })?;
    rows.into_iter()
        .map(|sealed| {
            let plain = db(hermes_engine::secret::unseal(&sealed))?;
            serde_json::from_str(&plain).map_err(|_| GatewayError::Invalid)
        })
        .collect()
}

async fn handoff(
    settings: &Settings,
    bridge: &Bridge,
    ingress: &Ingress,
    diagnostics: &std::sync::Mutex<Value>,
    data: &Value,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let id = data["id"]
        .as_str()
        .filter(|id| text_id(id))
        .ok_or(GatewayError::Invalid)?;
    let updates = data["items"]
        .as_array()
        .filter(|items| (1..=64).contains(&items.len()))
        .ok_or(GatewayError::Invalid)?;
    let digest = db(storage::key("linked-handoff", updates))?;
    let json = db(serde_json::to_string(data).map_err(anyhow::Error::from))?;
    let sealed = db(hermes_engine::secret::seal(ingress.root(), &json))?;
    let (saved,complete) = ingress.checked(true, |_| db(ingress.storage().write(|tx| {
        tx.execute("INSERT OR IGNORE INTO messaging_whatsapp_linked_handoffs(connection,envelope,digest,body)
                    VALUES(?1,?2,?3,?4)",params![ingress.connection_name(),id,digest,sealed])?;
        Ok(tx.query_row("SELECT digest,complete FROM messaging_whatsapp_linked_handoffs WHERE connection=?1 AND envelope=?2",
            params![ingress.connection_name(),id],|row| Ok((row.get::<_,String>(0)?,row.get::<_,bool>(1)?)))?)
    })))?;
    if saved != digest {
        let key = db(storage::key("linked-conflict", &(id, digest)))?;
        intake::refuse(ingress, &key, "handoff_conflict")?;
    } else if !complete {
        let mut sources = Vec::new();
        for raw in updates {
            if let Some(source) = normalize(settings, bridge, ingress, raw, cancel).await? {
                sources.push(source);
            }
        }
        let burst = settings.burst_ms > 0 && intake::stage_burst(ingress, &sources).await?;
        for source in sources {
            let home = settings
                .home
                .as_deref()
                .is_some_and(|target| target == source.room.chat || target == source.provider_chat);
            let room = source.room.clone();
            let admitted = if burst {
                true
            } else {
                intake::stage(ingress, source).await?
            };
            if admitted && home {
                let route = ingress
                    .verify_destination(room, now().saturating_add(common::EVIDENCE_MS))
                    .await?;
                lock(diagnostics)["home_route"] = json!(route);
            }
        }
        ingress.checked(true, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "UPDATE messaging_whatsapp_linked_handoffs SET complete=1,body=NULL
                WHERE connection=?1 AND envelope=?2",
                    params![ingress.connection_name(), id],
                )?;
                Ok(())
            }))
        })?;
    }
    bridge
        .notify("commit", json!({"id":id}))
        .map_err(|_| GatewayError::TemporarilyUnavailable)?;
    Ok(())
}

async fn normalize(
    settings: &Settings,
    bridge: &Bridge,
    ingress: &Ingress,
    raw: &Value,
    cancel: &CancellationToken,
) -> Result<Option<Source>, GatewayError> {
    let occurrence = db(storage::key("linked-wire", raw))?;
    let (Some(chat), Some(actor), Some(native)) = (
        raw["chat"].as_str().filter(|id| jid(id)),
        raw["actor"]
            .as_str()
            .filter(|id| jid(id) && !id.ends_with("@g.us")),
        raw["id"].as_str().filter(|id| text_id(id)),
    ) else {
        intake::refuse(ingress, &occurrence, "invalid_message_key")?;
        return Ok(None);
    };
    let group = chat.ends_with("@g.us");
    let from_owner = raw["origin"] == "owner" && actor == settings.account;
    if raw["live"] != true
        || raw["origin"] == "ambiguous"
        || !from_owner && raw["origin"] != "peer"
        || raw["origin"] == "peer" && actor == settings.account
        || group && raw["member"] == false
        || (settings.mode == Mode::Personal && (group || !from_owner || chat != settings.account))
    {
        intake::refuse(ingress, &occurrence, "out_of_scope_event")?;
        return Ok(None);
    }
    if group && raw["member"] != true {
        match bridge
            .request("membership", json!({"chat":chat,"actor":actor}), cancel)
            .await
        {
            Ok(proof)
                if proof["chat"] == chat && proof["actor"] == actor && proof["member"] == true => {}
            Err(CallFailure::Unknown | CallFailure::Unavailable) => {
                return Err(GatewayError::TemporarilyUnavailable);
            }
            _ => {
                intake::refuse(ingress, &occurrence, "group_membership_unverified")?;
                return Ok(None);
            }
        }
    }
    let opposite = |base: &str, field: &str| {
        raw[field].as_str().filter(|alias| {
            jid(alias)
                && !alias.ends_with("@g.us")
                && alias.ends_with("@lid") != base.ends_with("@lid")
        })
    };
    let canonical_actor = match common::bind_aliases(ingress, actor, opposite(actor, "actor_alias"))
    {
        Ok(actor) => actor,
        Err(GatewayError::Conflict) => {
            intake::refuse(ingress, &occurrence, "identity_conflict")?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let canonical_chat = if group {
        chat.to_owned()
    } else {
        match common::bind_aliases(ingress, chat, opposite(chat, "chat_alias")) {
            Ok(chat) => chat,
            Err(GatewayError::Conflict) => {
                intake::refuse(ingress, &occurrence, "identity_conflict")?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
    };
    let room = common::room(&canonical_chat, group);
    if from_owner && own_effect(ingress, &room, native)? {
        intake::refuse(ingress, &occurrence, "own_effect")?;
        return Ok(None);
    }
    if from_owner && settings.mode == Mode::Bot {
        if settings.handover_ms > 0 {
            common::observe(ingress, &room, &canonical_actor).await?;
            let granted = ingress.checked(true, |state| {
                ingress.authorize_operator(&state.policy, &room, &common::sender(&canonical_actor))
            })?;
            if granted {
                let until = now().saturating_add(i64::from(settings.handover_ms));
                ingress.checked(true, |_| db(ingress.storage().write(|tx| {
                    tx.execute("INSERT INTO messaging_whatsapp_handover VALUES(?1,?2,?3)
                        ON CONFLICT(connection,room) DO UPDATE SET until_ms=max(until_ms,excluded.until_ms)",
                        params![ingress.connection_name(),serde_json::to_string(&room)?,until])?;
                    Ok(())
                })))?;
            }
        }
        intake::refuse(ingress, &occurrence, "owner_outbound")?;
        return Ok(None);
    }
    if held(ingress, &room)? {
        intake::refuse(ingress, &occurrence, "human_handover")?;
        return Ok(None);
    }
    let quote = raw["quote"]
        .as_str()
        .filter(|id| text_id(id))
        .map(str::to_owned);
    let mut text = raw["text"].as_str().unwrap_or_default().to_owned();
    if let (Some(lat), Some(lon)) = (
        raw["location"]["latitude"].as_f64(),
        raw["location"]["longitude"].as_f64(),
    ) && (-90.0..=90.0).contains(&lat)
        && (-180.0..=180.0).contains(&lon)
    {
        let style = if raw["location"]["live"] == true {
            "Live location"
        } else {
            "Shared location"
        };
        text = format!("{style} {lat:.6}, {lon:.6}");
    }
    let addressed = !group
        || raw["mentions"]
            .as_array()
            .is_some_and(|mentions| mentions.iter().any(|id| id == &settings.account))
        || settings
            .mention_patterns
            .get(&room.chat)
            .is_some_and(|patterns| {
                let input = text.to_lowercase();
                patterns
                    .iter()
                    .any(|pattern| input.contains(&pattern.to_lowercase()))
            })
        || quote
            .as_deref()
            .is_some_and(|anchor| own_effect(ingress, &room, anchor).unwrap_or(false));
    let media = raw["media"]
        .as_array()
        .into_iter()
        .flatten()
        .take(intake::MAX_FILES + 1)
        .filter_map(|file| serde_json::from_value::<Media>(file.clone()).ok())
        .collect::<Vec<_>>();
    let choice = raw["choice"]
        .as_str()
        .filter(|id| text_id(id))
        .map(str::to_owned);
    let speech_mentions = if group && !addressed && media.iter().any(|item| item.voice) {
        settings
            .mention_patterns
            .get(&room.chat)
            .cloned()
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    if text.is_empty() && media.is_empty() && choice.is_none() {
        intake::refuse(ingress, &occurrence, "unsupported_message")?;
        return Ok(None);
    }
    Ok(Some(Source {
        event: db(storage::key(
            "whatsapp-linked-inbound",
            &(&room, &canonical_actor, native),
        ))?,
        room,
        actor: canonical_actor,
        anchor: native.into(),
        provider_chat: chat.into(),
        provider_actor: group.then(|| actor.into()),
        text,
        media,
        quote,
        choice,
        mentioned: addressed || !speech_mentions.is_empty(),
        owner: from_owner,
        speech_mentions,
    }))
}

fn held(ingress: &Ingress, room: &Room) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_whatsapp_handover WHERE connection=?1 AND room=?2 AND until_ms>?3)",
        params![ingress.connection_name(),serde_json::to_string(room)?,now()],|row|row.get(0),
    )?))))
}

fn own_effect(ingress: &Ingress, room: &Room, native: &str) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| db(ingress.storage().read(|conn| Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM messaging_whatsapp_linked_effects WHERE connection=?1 AND room=?2 AND native=?3)",
        params![ingress.connection_name(),serde_json::to_string(room)?,native],|row|row.get(0),
    )?))))
}

async fn status(ingress: &Ingress, receipt: &Value) -> Result<(), GatewayError> {
    let (Some(chat), Some(id)) = (
        receipt["chat"].as_str().filter(|id| jid(id)),
        receipt["id"].as_str().filter(|id| text_id(id)),
    ) else {
        return Ok(());
    };
    let group = chat.ends_with("@g.us");
    let actor = if group {
        chat.into()
    } else {
        common::lookup_alias(ingress, chat)?.unwrap_or_else(|| chat.into())
    };
    let room = common::room(&actor, group);
    let Some(time) =
        common::provider_millis(&receipt["timestamp"]).filter(|t| *t <= now() + 60_000)
    else {
        return Ok(());
    };
    let state = match receipt["status"].as_str() {
        Some("accepted") => ProviderDeliveryState::Accepted,
        Some("delivered") => ProviderDeliveryState::Delivered,
        Some("read") => ProviderDeliveryState::Read,
        Some("played") => ProviderDeliveryState::Played,
        Some("failed") => ProviderDeliveryState::Failed,
        _ => return Ok(()),
    };
    let recipient = receipt["participant"]
        .as_str()
        .filter(|id| jid(id))
        .map(str::to_owned);
    if group && state != ProviderDeliveryState::Accepted && recipient.is_none() {
        return Ok(());
    }
    if !own_effect(ingress, &room, id)? {
        return Ok(());
    }
    let operation = ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            let key = serde_json::to_string(&room)?;
            let (operation, delivery, text) = tx.query_row(
                "SELECT operation,delivery,text FROM messaging_whatsapp_linked_effects
             WHERE connection=?1 AND room=?2 AND native=?3",
                params![ingress.connection_name(), key, id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )?;
            if recipient.is_none()
                && matches!(
                    state,
                    ProviderDeliveryState::Accepted
                        | ProviderDeliveryState::Delivered
                        | ProviderDeliveryState::Read
                        | ProviderDeliveryState::Played
                )
            {
                tx.execute(
                    "UPDATE messaging_whatsapp_linked_effects SET state='accepted'
                WHERE connection=?1 AND room=?2 AND native=?3",
                    params![ingress.connection_name(), key, id],
                )?;
                outbound::record_in(tx, ingress.connection_name(), &key, id, &delivery, &text)?;
            }
            Ok(operation)
        }))
    })?;
    outbound::observe_status(
        ingress,
        room,
        id.into(),
        Some(operation),
        recipient,
        time,
        state,
    )
    .await
}

pub(super) async fn send(
    settings: &Settings,
    bridge: &Bridge,
    ingress: &Ingress,
    attempt: &SendAttempt,
    cancel: &CancellationToken,
) -> DeliveryResult {
    let room = &attempt.payload.room;
    if !jid(&room.chat)
        || room.thread.is_some()
        || ingress.recheck_send(attempt).await.is_err()
        || held(ingress, room).unwrap_or(true)
    {
        return DeliveryResult::Rejected;
    }
    if room.kind == RoomKind::Group {
        let actor = match outbound::delivery_actor(ingress, attempt) {
            Ok(actor) => actor,
            Err(_) => return outbound::retry(Duration::from_secs(1)),
        };
        let membership = bridge
            .request(
                "membership",
                json!({"chat":room.chat,"actor":actor}),
                cancel,
            )
            .await;
        match membership {
            Ok(answer)
                if answer["chat"] == room.chat
                    && answer["member"] == true
                    && answer["actor"] == json!(actor) =>
            {
                ()
            }
            Err(CallFailure::Unavailable) => return outbound::retry(Duration::from_secs(1)),
            _ => return DeliveryResult::Rejected,
        }
    }
    let mut fallback = None;
    let (mut content, text) = match &attempt.payload.body {
        DeliveryBody::Progress { text, .. } => (json!({"text":text}), text.clone()),
        DeliveryBody::Reply(message) if outbound::is_voice(attempt) => {
            let Ok(bytes) = outbound::encode_voice(ingress, attempt).await else {
                return DeliveryResult::Rejected;
            };
            (
                json!({"media":{"kind":"audio","mime":"audio/ogg; codecs=opus","voice":true,"bytes":STANDARD.encode(bytes)}}),
                message.text.clone(),
            )
        }
        DeliveryBody::Reply(message) if message.attachments.is_empty() => {
            (json!({"text":message.text}), message.text.clone())
        }
        DeliveryBody::Reply(message) => {
            let Some(artifact) = message.attachments.first() else {
                return DeliveryResult::Rejected;
            };
            let Ok(bytes) = ingress.reply_artifact(attempt, artifact).await else {
                return DeliveryResult::Rejected;
            };
            if bytes.len() > MAX_MEDIA || bytes.is_empty() {
                return DeliveryResult::Rejected;
            }
            let (kind, mime) = outbound::mime(artifact);
            (
                json!({"media":{"kind":kind,"mime":mime,"name":artifact.name(),"bytes":STANDARD.encode(bytes)}}),
                message.text.clone(),
            )
        }
        DeliveryBody::Prompt { action_id, prompt } => {
            let Ok(plan) = outbound::prompt(ingress, attempt, action_id, prompt) else {
                return DeliveryResult::Rejected;
            };
            let text = plan.text_fallback();
            if plan.clarification && settings.mode == Mode::Bot && plan.choices.len() >= 2 {
                fallback = Some(json!({"text":text}));
                (
                    json!({"poll":{"name":plan.text,"choices":plan.choices}}),
                    text,
                )
            } else {
                (json!({"text":text}), text)
            }
        }
        _ => return DeliveryResult::Rejected,
    };
    let Some(operation) = &attempt.payload.effect.operation_key else {
        return DeliveryResult::Rejected;
    };
    let Some(hash) = operation.rsplit('-').next().filter(|hash| hash.len() >= 28) else {
        return DeliveryResult::Rejected;
    };
    let native = format!("3EB0{}", hash[..28].to_ascii_uppercase());
    if let Some(anchor) = &attempt.reply_anchor
        && let Ok(Some(source)) = intake::source_at(ingress, room, anchor)
    {
        let quote = json!({"id":anchor,"chat":source.provider_chat,"participant":source.provider_actor,
            "from_me":source.owner,"text":source.text.chars().take(800).collect::<String>()});
        content["quote"] = quote.clone();
        if let Some(fallback) = &mut fallback {
            fallback["quote"] = quote;
        }
    }
    if ingress.recheck_send(attempt).await.is_err() || held(ingress, room).unwrap_or(true) {
        return DeliveryResult::Rejected;
    }
    let reserved = ingress.checked(true, |_| db(ingress.storage().write(|tx| {
        let key = serde_json::to_string(room)?;
        tx.execute("INSERT OR IGNORE INTO messaging_whatsapp_linked_effects(connection,room,native,operation,delivery,text,issued)
                    VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![ingress.connection_name(),key,native,operation,attempt.delivery_id,text,now()])?;
        Ok(tx.query_row("SELECT operation FROM messaging_whatsapp_linked_effects WHERE connection=?1 AND room=?2 AND native=?3",
            params![ingress.connection_name(),key,native],|row|row.get::<_,String>(0))? == *operation)
    })));
    if !matches!(reserved, Ok(true)) {
        return outbound::retry(Duration::from_secs(1));
    }
    let wire = |content: Value| json!({"chat":room.chat,"id":native,"content":content});
    let mut answer = bridge.request("send", wire(content), cancel).await;
    if answer == Err(CallFailure::Rejected)
        && let Some(fallback) = fallback
    {
        if ingress.recheck_send(attempt).await.is_err() || held(ingress, room).unwrap_or(true) {
            return DeliveryResult::Rejected;
        }
        answer = bridge.request("send", wire(fallback), cancel).await;
    }
    match answer {
        Ok(result) if result["id"] == native && result["chat"] == room.chat => {}
        Err(CallFailure::Rejected) => return DeliveryResult::Rejected,
        Err(CallFailure::Unavailable) => return outbound::retry(Duration::from_secs(1)),
        _ => return DeliveryResult::Uncertain,
    }
    let wait = async {
        loop {
            let accepted = ingress.checked(true, |_| {
                db(ingress.storage().read(|conn| {
                    Ok(conn
                        .query_row(
                            "SELECT state='accepted' FROM messaging_whatsapp_linked_effects
                     WHERE connection=?1 AND room=?2 AND native=?3",
                            params![
                                ingress.connection_name(),
                                serde_json::to_string(room)?,
                                native
                            ],
                            |row| row.get::<_, Option<bool>>(0),
                        )
                        .optional()?
                        .flatten()
                        .unwrap_or(false))
                }))
            })?;
            if accepted {
                return Ok::<(), GatewayError>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::select! {
        _ = cancel.cancelled() => DeliveryResult::Uncertain,
        result = tokio::time::timeout(Duration::from_secs(3),wait) => {
            match result {
                Ok(Ok(())) => DeliveryResult::Confirmed { receipt:native,editable_message:None },
                _ => DeliveryResult::Uncertain,
            }
        }
    }
}
