use crate::{
    Connection,
    api::Failure,
    callbacks, context, delivery,
    hub::{Hub, Member},
    identity::{channel, string, ts},
    reactions,
    settings::PeerBotMode,
};
use hermes_gateway_core::messaging::{
    ActorKind, AttachmentManifest, Command, Control, DeferredInput, Disposition, GatewayError,
    Handoff, HandoffItem, InboundBody, InboundEvent, Ingress, MediaPurpose, MediaSource,
    MemberEvidence, Message, Preparation, PreparationPurpose, RequesterCommand,
    RequesterInvocation, Room, RoomKind, ScrollbackReference, Sender, SpokenReplyChange,
    SpokenReplyMode,
    media::{MAX_ADAPTER_FILE, MAX_ADAPTER_INPUT_BYTES, PreparedAudio, valid_name},
    runtime::{MAX_TEXT, blocking, db, now, valid_id},
    storage, voice_codec,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};
use tokio_util::sync::CancellationToken;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_wire (
    app TEXT NOT NULL, wire TEXT NOT NULL, fingerprint TEXT NOT NULL,
    target TEXT, committed INTEGER NOT NULL DEFAULT 0, response TEXT,
    PRIMARY KEY(app,wire)
);
CREATE TABLE IF NOT EXISTS messaging_slack_wire_conflicts (
    app TEXT NOT NULL, wire TEXT NOT NULL, fingerprint TEXT NOT NULL,
    PRIMARY KEY(app,wire,fingerprint)
);
CREATE TABLE IF NOT EXISTS messaging_slack_sources (
    connection TEXT NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL,
    expires INTEGER NOT NULL, PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_slack_peer_posts (
    connection TEXT NOT NULL, event TEXT NOT NULL, source TEXT NOT NULL,
    expires INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(connection,event)
);
CREATE TABLE IF NOT EXISTS messaging_slack_threads (
    connection TEXT NOT NULL, channel TEXT NOT NULL, thread TEXT NOT NULL,
    PRIMARY KEY(connection,channel,thread)
);
CREATE TABLE IF NOT EXISTS messaging_slack_inactive (
    connection TEXT NOT NULL, credential TEXT NOT NULL, PRIMARY KEY(connection,credential)
);
CREATE TABLE IF NOT EXISTS messaging_slack_member_targets (
    connection TEXT NOT NULL, channel TEXT NOT NULL, actor TEXT NOT NULL,
    PRIMARY KEY(connection,channel)
);
CREATE TABLE IF NOT EXISTS messaging_slack_voice_sources (
    connection TEXT NOT NULL,event TEXT NOT NULL,room TEXT NOT NULL,actor TEXT NOT NULL,
    PRIMARY KEY(connection,event)
);
";

const EVIDENCE_MS: i64 = 30 * 60 * 1_000;
const INPUT_MS: i64 = 10 * 60 * 1_000;
const MEDIA_LIFETIME: Duration = Duration::from_secs(90);
const MAX_FILES: usize = 8;

enum WireState {
    Handle(Option<String>),
    Done(Option<Acknowledgement>),
}

#[derive(Clone, Deserialize, Serialize)]
struct Acknowledgement {
    payload: Value,
    expires: Option<i64>,
}

impl Acknowledgement {
    fn ephemeral(text: String) -> Self {
        Self {
            payload: json!({"response_type":"ephemeral","text":text}),
            expires: None,
        }
    }

    fn active(self) -> Option<Value> {
        self.expires
            .is_none_or(|expiry| expiry > now())
            .then_some(self.payload)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct FileSource {
    id: String,
    voice: bool,
}

pub(crate) fn attached_files(message: &Value) -> impl Iterator<Item = FileSource> + '_ {
    message["files"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|file| {
            string(file, "id", b"F").map(|id| FileSource {
                id: id.to_owned(),
                voice: file["subtype"] == "slack_audio"
                    || file["mimetype"]
                        .as_str()
                        .is_some_and(|kind| kind.starts_with("audio/")),
            })
        })
        .take(MAX_FILES + 1)
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct PeerSource {
    bot: String,
    mentioned: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct Source {
    event: InboundEvent,
    text: String,
    files: Vec<FileSource>,
    history: Option<ScrollbackReference>,
    #[serde(default)]
    peer: Option<PeerSource>,
}

pub(crate) async fn receive(
    receiver: &Connection,
    ingress: &Ingress,
    hub: &Hub,
    frame: &Value,
) -> Result<Option<Value>, GatewayError> {
    let kind = frame["type"].as_str().unwrap_or_default();
    let payload = &frame["payload"];
    let identity = match kind {
        "events_api" => payload["event_id"].as_str(),
        "slash_commands" | "interactive" => payload["trigger_id"].as_str(),
        _ => None,
    }
    .filter(|id| valid_id(id));
    let wire = match identity {
        Some(id) => db(storage::key("slack-wire", &(kind, id)))?,
        None => db(storage::key("slack-wire", &(kind, payload)))?,
    };
    let fingerprint = db(storage::key("slack-wire-body", &(kind, payload)))?;
    let target = (payload["api_app_id"].as_str() == Some(receiver.id.app.as_str()))
        .then(|| installation(hub, kind, payload))
        .flatten();
    let proposed = target
        .as_ref()
        .map(|target| target.ingress.connection_name().to_owned());
    let scope = ingress.clone();
    let app = receiver.id.app.clone();
    let persist_wire = wire.clone();
    let stage = blocking(move || scope.checked(true, |_| db(scope.storage().write(|tx| {
        tx.execute("INSERT OR IGNORE INTO messaging_slack_wire(app,wire,fingerprint,target) VALUES(?1,?2,?3,?4)",
            params![app,persist_wire,fingerprint,proposed])?;
        let (saved, target, committed, response): (String, Option<String>, bool, Option<String>) = tx.query_row(
            "SELECT fingerprint,target,committed,response FROM messaging_slack_wire WHERE app=?1 AND wire=?2",
            params![app,persist_wire], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        if saved != fingerprint {
            tx.execute("INSERT OR IGNORE INTO messaging_slack_wire_conflicts VALUES(?1,?2,?3)",
                params![app,persist_wire,fingerprint])?;
            return Ok(WireState::Done(None));
        }
        if committed {
            let response = response.map(|body| {
                let plain = hermes_engine::secret::unseal(&body)?;
                Ok::<_, anyhow::Error>(serde_json::from_str(&plain)?)
            }).transpose()?;
            return Ok(WireState::Done(response));
        }
        Ok(WireState::Handle(target))
    })))).await?;
    let expected = match stage {
        WireState::Done(response) => return Ok(response.and_then(Acknowledgement::active)),
        WireState::Handle(expected) => expected,
    };
    let response = if let Some(expected) = expected {
        let target = target
            .filter(|target| target.ingress.connection_name() == expected)
            .ok_or(GatewayError::StaleConnection)?;
        let scope = &target.ingress;
        let connection = &target.connection;
        if is_inactive(connection, scope)? {
            None
        } else {
            match kind {
                "events_api" => {
                    event(connection, scope, payload, &wire).await?;
                    None
                }
                "slash_commands" => {
                    slash(
                        connection,
                        scope,
                        payload,
                        &wire,
                        frame["accepts_response_payload"] == true,
                    )
                    .await?
                }
                "interactive" => delivery::interaction(connection, scope, payload, &wire)
                    .await?
                    .map(|payload| Acknowledgement {
                        payload,
                        expires: None,
                    }),
                _ => None,
            }
        }
    } else {
        None
    };
    let scope = ingress.clone();
    let app = receiver.id.app.clone();
    let body = response.clone();
    blocking(move || scope.checked(true, |_| db(scope.storage().write(|tx| {
        let sealed = body.map(|body| {
            let json = serde_json::to_string(&body)?;
            hermes_engine::secret::seal(scope.root(), &json)
        }).transpose()?;
        tx.execute("UPDATE messaging_slack_wire SET committed=1,response=?3 WHERE app=?1 AND wire=?2",
            params![app,wire,sealed])?;
        Ok(())
    })))).await?;
    Ok(response.and_then(Acknowledgement::active))
}

fn installation(hub: &Hub, kind: &str, payload: &Value) -> Option<Member> {
    if kind != "events_api" {
        let team =
            string(payload, "team_id", b"T").or_else(|| string(&payload["team"], "id", b"T"))?;
        return hub.member(team);
    }
    if payload["type"] == "app_rate_limited" || payload["event"]["type"] == "app_uninstalled" {
        return hub.member(string(payload, "team_id", b"T")?);
    }
    let authorized = payload["authorizations"].as_array()?;
    let mut members = BTreeSet::new();
    for proof in authorized {
        if proof["is_bot"] != true {
            continue;
        }
        let team = string(proof, "team_id", b"T").or_else(|| {
            (proof["is_enterprise_install"] == true)
                .then(|| string(payload, "context_team_id", b"T"))
                .flatten()
        });
        let Some(member) = team.and_then(|team| hub.member(team)) else {
            continue;
        };
        if string(proof, "user_id", b"UW") != Some(member.connection.id.user.as_str()) {
            continue;
        }
        let enterprise = string(proof, "enterprise_id", b"E");
        if enterprise.is_some() && enterprise != member.connection.id.enterprise.as_deref() {
            continue;
        }
        if proof["is_enterprise_install"] == true && enterprise.is_none() {
            continue;
        }
        members.insert(member.connection.id.team.clone());
    }
    if members.len() != 1 {
        return None;
    }
    hub.member(members.first()?)
}

pub(crate) fn is_inactive(
    connection: &Connection,
    ingress: &Ingress,
) -> Result<bool, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM messaging_slack_inactive WHERE connection=?1 AND credential=?2)",
                params![ingress.connection_name(),connection.api.credential_binding()],
                |row| row.get(0),
            )?)
        }))
    })
}

async fn event(
    connection: &Connection,
    ingress: &Ingress,
    payload: &Value,
    wire: &str,
) -> Result<(), GatewayError> {
    if payload["type"] == "app_rate_limited" {
        return ingress.report_gap();
    }
    let event = &payload["event"];
    match event["type"].as_str() {
        Some("app_uninstalled") => {
            ingress.invalidate_actor_evidence(None).await?;
            ingress.checked(true, |_| {
                db(ingress.storage().write(|tx| {
                    tx.execute(
                        "INSERT OR IGNORE INTO messaging_slack_inactive VALUES(?1,?2)",
                        params![
                            ingress.connection_name(),
                            connection.api.credential_binding()
                        ],
                    )?;
                    Ok(())
                }))
            })?;
            ingress.report_transport_ready(false)?;
            crate::lock(&connection.verification)["socket"] = json!("uninstalled");
        }
        Some("member_left_channel") => {
            if let (Some(user), Some(chat)) = (
                string(event, "user", b"UW"),
                string(event, "channel", b"CDG"),
            ) {
                let chat = chat.to_owned();
                ingress
                    .withdraw_observed_memberships(user.to_owned(), move |member| {
                        member.room.chat != chat
                    })
                    .await?;
            }
        }
        Some("agent_session_stopped") => delivery::stop(connection, ingress, event, wire).await?,
        Some("app_context_changed") => context::observe_view(connection, event, "context"),
        Some("reaction_added") => reactions::stage(connection, ingress, event, wire).await?,
        Some("message" | "app_mention") => {
            if event["subtype"] == "message_changed" {
                if let Some(mut source) = message(connection, event, &event["message"], ingress)?
                    && source.peer.is_none()
                {
                    let previous = source.event.id;
                    source.event.id = wire.to_owned();
                    source.event.body = InboundBody::Control(Control::ReplaceQueued {
                        event_id: previous,
                        text: source.text,
                    });
                    observe(ingress, &source.event.room, &source.event.sender.id).await?;
                    ingress.submit(vec![source.event], None).await?;
                }
            } else if let Some(mut source) = message(connection, event, event, ingress)? {
                if source.peer.is_some() {
                    return stage_peer(ingress, source).await;
                }
                if source.event.room.kind == RoomKind::Direct {
                    context::observe_view(connection, event, "app_context");
                }
                observe(ingress, &source.event.room, &source.event.sender.id).await?;
                if connection.settings.shared_dm_history
                    && source.event.room.kind == RoomKind::Direct
                    && source.event.room.thread.is_some()
                {
                    let mut parent = source.event.room.clone();
                    parent.thread = None;
                    observe(ingress, &parent, &source.event.sender.id).await?;
                    match ingress.share_direct_thread(source.event.clone()).await {
                        Ok(()) | Err(GatewayError::Forbidden) => {}
                        Err(error) => return Err(error),
                    }
                }
                if let Some(command) = parse_command(&source.text) {
                    source.event.body = InboundBody::Command(command);
                    if matches!(
                        source.event.body,
                        InboundBody::Command(Command::Background { .. })
                    ) {
                        source.event.reply_anchor = None;
                    }
                    ingress.submit(vec![source.event], None).await?;
                } else if let Some(response) =
                    delivery::text_response(connection, ingress, &source.event, &source.text)?
                {
                    source.event.body = InboundBody::Control(response);
                    ingress.submit(vec![source.event], None).await?;
                } else {
                    stage(ingress, source).await?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn message(
    connection: &Connection,
    envelope: &Value,
    message: &Value,
    ingress: &Ingress,
) -> Result<Option<Source>, GatewayError> {
    let Some(actor) = string(message, "user", b"UW") else {
        return Ok(None);
    };
    if actor == connection.id.user || message["hidden"] == true {
        return Ok(None);
    }
    let peer = match string(message, "bot_id", b"B") {
        Some(bot)
            if bot != connection.id.bot && connection.settings.peer_bots.contains_key(bot) =>
        {
            Some(bot)
        }
        Some(_) => return Ok(None),
        None if message["bot_id"].is_string()
            || message["bot_profile"].is_object()
            || message["app_id"].is_string() =>
        {
            return Ok(None);
        }
        None => None,
    };
    let supported = matches!(
        message["subtype"].as_str(),
        None | Some("file_share" | "thread_broadcast")
    ) || peer.is_some() && message["subtype"] == "bot_message";
    if !supported {
        return Ok(None);
    }
    let Some(chat) = string(envelope, "channel", b"CDG") else {
        return Ok(None);
    };
    let Some(anchor) = ts(message, "ts") else {
        return Ok(None);
    };
    let thread = match message.get("thread_ts") {
        Some(value) => {
            let Some(root) = value
                .as_str()
                .filter(|value| crate::identity::timestamp(value))
            else {
                return Ok(None);
            };
            Some(root)
        }
        None if !chat.starts_with('D') && !connection.settings.flat_channels.contains(chat) => {
            Some(anchor)
        }
        None if chat.starts_with('D') && connection.settings.agent => Some(anchor),
        None => None,
    };
    let Some(room) = channel(chat, thread) else {
        return Ok(None);
    };
    let raw = message["text"].as_str().unwrap_or_default();
    let mention = format!("<@{}>", connection.id.user);
    let addressed = raw.contains(&mention);
    let peer_addressed = match peer.and_then(|bot| connection.settings.peer_bots.get(bot)) {
        Some(PeerBotMode::Mention) if !addressed => return Ok(None),
        Some(PeerBotMode::Always) => true,
        _ => false,
    };
    let text = raw.replace(&mention, "").trim().to_owned();
    let files = if connection.settings.files && peer.is_none() {
        attached_files(message).collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if (text.is_empty() && files.is_empty())
        || text.len() > MAX_TEXT
        || files.len() > MAX_FILES
        || text.contains('\0')
    {
        return Ok(None);
    }
    let continued = if let Some(root) = thread.filter(|root| *root != anchor)
        && !addressed
        && connection.settings.continue_threads
        && !connection.settings.strict_threads.contains(chat)
    {
        ingress.checked(true, |_| db(ingress.storage().read(|conn| {
            Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM messaging_slack_threads WHERE connection=?1 AND channel=?2 AND thread=?3)",
                params![ingress.connection_name(),chat,root], |r| r.get(0))?)
        })))?
    } else {
        false
    };
    let event = InboundEvent {
        id: db(storage::key("slack-post", &(chat, anchor)))?,
        room,
        sender: Sender {
            id: actor.to_owned(),
            roles: Default::default(),
        },
        reply_anchor: Some(anchor.to_owned()),
        mentioned: addressed || peer_addressed || continued,
        body: InboundBody::Message(Message::text("")),
    };
    let history = peer
        .is_none()
        .then(|| context::reference(connection, &event))
        .flatten();
    Ok(Some(Source {
        event,
        text,
        history,
        files,
        peer: peer.map(|bot| PeerSource {
            bot: bot.into(),
            mentioned: addressed,
        }),
    }))
}

pub(crate) async fn observe(
    ingress: &Ingress,
    room: &Room,
    actor: &str,
) -> Result<(), GatewayError> {
    observe_actor(ingress, room, actor, ActorKind::Person).await
}

async fn observe_actor(
    ingress: &Ingress,
    room: &Room,
    actor: &str,
    kind: ActorKind,
) -> Result<(), GatewayError> {
    ingress
        .observe_membership(
            actor.to_owned(),
            kind,
            MemberEvidence {
                room: room.clone(),
                roles: BTreeSet::new(),
            },
            now().saturating_add(EVIDENCE_MS),
        )
        .await
}

async fn stage(ingress: &Ingress, source: Source) -> Result<(), GatewayError> {
    let scope = ingress.clone();
    let (event, preparation) = blocking(move || scope.checked(true, |state| {
        let decision = scope.route_candidate(&state.policy, &source.event, true)?;
        if decision.is_err() || state.policy.delivery_policy(&source.event.room).protection.is_some() {
            let denial = decision.err().unwrap_or(Disposition::ProtectionUnavailable);
            let mut event = source.event;
            event.body = InboundBody::Message(Message::text(db(storage::key("slack-refused", &source.text))?));
            db(scope.storage().intake(state.revision(), vec![storage::Candidate {
                event: event.clone(), decision: Err(denial), protection: None,
            }], None))?;
            return Ok((event, None));
        }
        db(scope.storage().write(|tx| {
            let expires = now().saturating_add(INPUT_MS);
            let serialized = serde_json::to_string(&source)?;
            let connection = scope.connection_name();
            tx.execute("INSERT OR IGNORE INTO messaging_slack_sources VALUES(?1,?2,?3,?4)",
                params![connection,source.event.id,serialized,expires])?;
            let (previous, expires): (String, i64) = tx.query_row(
                "SELECT source,expires FROM messaging_slack_sources WHERE connection=?1 AND event=?2",
                params![connection,source.event.id], |row| Ok((row.get(0)?,row.get(1)?)))?;
            let mut before: Source = serde_json::from_str(&previous)?;
            if before.event.room != source.event.room || before.event.sender != source.event.sender
                || before.peer != source.peer
                || before.text != source.text
            {
                let mut conflict = source.event;
                conflict.body = InboundBody::Message(Message::text(storage::key("slack-conflict", &serialized)?));
                return Ok((conflict, None));
            }
            // Mention and message deliveries can carry different file metadata.
            // Keep the same admission and enrich its still-deferred source.
            if before.files.is_empty() && !source.files.is_empty() {
                before.files = source.files;
                tx.execute("UPDATE messaging_slack_sources SET source=?3 WHERE connection=?1 AND event=?2",
                    params![connection,before.event.id,serde_json::to_string(&before)?])?;
            }
            let purpose = before.history.map_or(PreparationPurpose::Body, PreparationPurpose::Scrollback);
            Ok((before.event, Some((expires,purpose))))
        }))
    })).await?;
    if let Some((expires_ms, purpose)) = preparation {
        let key = db(storage::key("slack-prepare", &event.id))?;
        ingress
            .stage_handoff(Handoff {
                id: key.clone(),
                items: vec![HandoffItem {
                    event,
                    preparation: Some(Preparation {
                        token: key,
                        purpose,
                        expires_ms,
                        protection: None,
                    }),
                }],
                advance: None,
            })
            .await?;
    } else {
        let scope = ingress.clone();
        blocking(move || {
            scope.checked(true, |state| {
                if db(scope.storage().read(|conn| {
                    storage::intake_receipt(conn, scope.connection_name(), &event.id)
                }))?
                .is_some()
                {
                    db(scope.storage().intake(
                        state.revision(),
                        vec![storage::Candidate {
                            event,
                            decision: Err(Disposition::Conflict),
                            protection: None,
                        }],
                        None,
                    ))?;
                }
                Ok(())
            })
        })
        .await?;
    }
    Ok(())
}

async fn stage_peer(ingress: &Ingress, source: Source) -> Result<(), GatewayError> {
    let scope = ingress.clone();
    blocking(move || scope.checked(true, |_| db(scope.storage().write(|tx| {
        tx.execute(
            "INSERT OR IGNORE INTO messaging_slack_peer_posts(connection,event,source,expires)
             VALUES(?1,?2,?3,?4)",
            params![scope.connection_name(),source.event.id,serde_json::to_string(&source)?,now() + INPUT_MS],
        )?;
        Ok(())
    })))).await
}

async fn resume_peers(connection: &Connection, ingress: &Ingress, cancel: &CancellationToken) {
    if connection.settings.peer_bots.is_empty() {
        return;
    }
    let Ok(pending) = ingress.checked(true, |_| {
        db(ingress.storage().read(|db| {
            let mut query = db.prepare(
                "SELECT source FROM messaging_slack_peer_posts
             WHERE connection=?1 AND done=0 AND expires>?2 ORDER BY rowid LIMIT 16",
            )?;
            let rows = query.query_map(params![ingress.connection_name(), now()], |row| {
                storage::decode::<Source>(row, 0)
            })?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        }))
    }) else {
        return;
    };
    for source in pending {
        let event = source.event.id.clone();
        match admit_peer(connection, ingress, source, cancel).await {
            Err(GatewayError::TemporarilyUnavailable) => continue,
            Err(GatewayError::StaleConnection) => return,
            _ => {}
        }
        let _ = ingress.checked(true, |_| {
            db(ingress.storage().write(|tx| {
                tx.execute(
                    "UPDATE messaging_slack_peer_posts SET done=1 WHERE connection=?1 AND event=?2",
                    params![ingress.connection_name(), event],
                )?;
                Ok(())
            }))
        });
    }
}

async fn admit_peer(
    connection: &Connection,
    ingress: &Ingress,
    source: Source,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let peer = source.peer.as_ref().ok_or(GatewayError::Invalid)?;
    match connection.settings.peer_bots.get(&peer.bot) {
        Some(PeerBotMode::Always) => {}
        Some(PeerBotMode::Mention) if peer.mentioned => {}
        _ => return Err(GatewayError::Forbidden),
    }
    let actor = &source.event.sender.id;
    let verified = connection
        .api
        .scoped(
            ingress,
            "bots.info",
            json!({"bot":peer.bot,"team_id":connection.id.team}),
            cancel,
        )
        .await
        .map_err(Failure::read)?;
    let bot = &verified["bot"];
    if bot["id"] != peer.bot
        || bot["user_id"] != *actor
        || bot["deleted"] == true
        || string(bot, "app_id", b"A").is_none_or(|app| app == connection.id.app)
    {
        return Err(GatewayError::Forbidden);
    }
    conversation(connection, ingress, &source.event.room, Some(actor), cancel).await?;
    observe_actor(ingress, &source.event.room, actor, ActorKind::Shared).await?;
    stage(ingress, source).await
}

pub(crate) async fn resume(
    connection: &Connection,
    ingress: &Ingress,
    hub: &Hub,
    cancel: &CancellationToken,
) {
    let mut home = None;
    let mut recovered_callbacks = false;
    loop {
        if hub.ready() && matches!(is_inactive(connection, ingress), Ok(false)) {
            let poll = async {
                if !recovered_callbacks {
                    recovered_callbacks = callbacks::recover(ingress).is_ok();
                }
                if home.is_none_or(|refresh| now() >= refresh) {
                    home = Some(now().saturating_add(EVIDENCE_MS / 2));
                    if let Ok(Some(route)) = verify_home(connection, ingress, cancel).await {
                        crate::lock(&connection.verification)["home_route"] = json!(route);
                    }
                }
                if let Ok(items) = ingress.pending_preparations().await {
                    for item in items {
                        if matches!(
                            item.preparation.purpose,
                            PreparationPurpose::Body | PreparationPurpose::Scrollback(_)
                        ) {
                            let _ = hydrate(connection, ingress, item, cancel).await;
                        }
                    }
                }
                resume_peers(connection, ingress, cancel).await;
                reactions::resume(connection, ingress, cancel).await;
                if recovered_callbacks {
                    callbacks::resume(connection, ingress, cancel).await;
                }
            };
            tokio::select! { _ = cancel.cancelled() => return, _ = poll => {} }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    }
}

async fn verify_home(
    connection: &Connection,
    ingress: &Ingress,
    cancel: &CancellationToken,
) -> Result<Option<String>, GatewayError> {
    let destination = match (&connection.settings.home, &connection.settings.home_member) {
        (Some(room), None) => room.clone(),
        (None, Some(actor)) if actor != &connection.id.user => {
            let response = connection
                .api
                .scoped(
                    ingress,
                    "conversations.open",
                    json!({"users":actor,"team_id":connection.id.team,"return_im":true}),
                    cancel,
                )
                .await
                .map_err(Failure::read)?;
            let info = &response["channel"];
            let room = string(info, "id", b"D")
                .and_then(|chat| channel(chat, None))
                .filter(|_| info["is_im"] == true && info["user"] == *actor)
                .ok_or(GatewayError::Forbidden)?;
            ingress.checked(true, |_| {
                db(ingress.storage().write(|tx| {
                    tx.execute(
                        "INSERT OR IGNORE INTO messaging_slack_member_targets VALUES(?1,?2,?3)",
                        params![ingress.connection_name(), room.chat, actor],
                    )?;
                    Ok(())
                }))
            })?;
            room
        }
        _ => return Ok(None),
    };
    conversation(
        connection,
        ingress,
        &destination,
        connection.settings.home_member.as_deref(),
        cancel,
    )
    .await?;
    if let Some(thread) = &destination.thread {
        let response = connection.api.scoped(
            ingress, "conversations.replies",
            json!({"channel":destination.chat,"team_id":connection.id.team,"ts":thread,"limit":1}),
            cancel,
        ).await.map_err(Failure::read)?;
        let root = &response["messages"][0];
        if ts(root, "ts") != Some(thread)
            || ts(root, "thread_ts").is_some_and(|parent| parent != thread)
        {
            return Err(GatewayError::Forbidden);
        }
    }
    ingress
        .verify_destination(destination, now().saturating_add(EVIDENCE_MS))
        .await
        .map(Some)
}

pub(crate) async fn conversation(
    connection: &Connection,
    ingress: &Ingress,
    room: &Room,
    actor: Option<&str>,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    if is_inactive(connection, ingress)? {
        return Err(GatewayError::Forbidden);
    }
    let response = connection
        .api
        .scoped(
            ingress,
            "conversations.info",
            json!({"channel":room.chat,"team_id":connection.id.team}),
            cancel,
        )
        .await
        .map_err(Failure::read)?;
    let found = &response["channel"];
    if found["id"] != room.chat
        || found["is_archived"] == true
        || (found["is_im"] == true) != (room.kind == RoomKind::Direct)
    {
        return Err(GatewayError::Forbidden);
    }
    if room.kind == RoomKind::Direct {
        let user = string(found, "user", b"UW").ok_or(GatewayError::Forbidden)?;
        if actor.is_none() {
            let pinned = ingress.checked(true, |_| {
                db(ingress.storage().read(|conn| {
                    Ok(conn.query_row(
                        "SELECT actor FROM messaging_slack_member_targets WHERE connection=?1 AND channel=?2",
                        params![ingress.connection_name(), room.chat],
                        |row| row.get::<_, String>(0),
                    ).optional()?)
                }))
            })?;
            if let Some(pinned) = pinned
                && (pinned != user || connection.settings.home_member.as_deref() != Some(user))
            {
                return Err(GatewayError::Forbidden);
            }
        }
        return (actor.is_none_or(|actor| actor == user) && user != connection.id.user)
            .then_some(())
            .ok_or(GatewayError::Forbidden);
    }
    if found["is_member"] != true && found["is_mpim"] != true {
        return Err(GatewayError::Forbidden);
    }
    let Some(actor) = actor else { return Ok(()) };
    let mut cursor = String::new();
    for _ in 0..5 {
        let mut query = json!({"channel":room.chat,"team_id":connection.id.team,"limit":200});
        if !cursor.is_empty() {
            query["cursor"] = json!(cursor);
        }
        let page = connection
            .api
            .scoped(ingress, "conversations.members", query, cancel)
            .await
            .map_err(Failure::read)?;
        if page["members"]
            .as_array()
            .is_some_and(|ids| ids.iter().any(|id| id == actor))
        {
            return Ok(());
        }
        let Some(next) = page
            .pointer("/response_metadata/next_cursor")
            .and_then(Value::as_str)
            .filter(|next| !next.is_empty() && next.len() <= 1024 && *next != cursor)
        else {
            break;
        };
        cursor = next.to_owned();
    }
    Err(GatewayError::Forbidden)
}

async fn hydrate(
    connection: &Connection,
    ingress: &Ingress,
    item: DeferredInput,
    cancel: &CancellationToken,
) -> Result<(), GatewayError> {
    let id = &item.event.id;
    let token = &item.preparation.token;
    let stored = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
        Ok(conn.query_row("SELECT source FROM messaging_slack_sources WHERE connection=?1 AND event=?2 AND expires>?3",
            params![ingress.connection_name(),id,now()], |row| storage::decode::<Source>(row, 0)).optional()?)
    })))?.ok_or(GatewayError::Invalid)?;
    if matches!(item.preparation.purpose, PreparationPurpose::Scrollback(_)) {
        return context::hydrate(connection, ingress, &item, cancel, |root| {
            hydrate_message(connection, ingress, &item, stored, root, cancel)
        })
        .await;
    }
    ingress
        .authorize_preparation(id.clone(), token.clone())
        .await?;
    let room = &item.event.room;
    let actor = &item.event.sender.id;
    let result = conversation(connection, ingress, room, Some(actor), cancel).await;
    if result == Err(GatewayError::Forbidden) || result == Err(GatewayError::Invalid) {
        ingress
            .complete_preparation(id.clone(), token.clone(), Message::text(""))
            .await?;
        return Ok(());
    }
    result?;
    let message = hydrate_message(connection, ingress, &item, stored, vec![], cancel).await?;
    ingress
        .complete_preparation(id.clone(), token.clone(), message)
        .await?;
    Ok(())
}

async fn hydrate_message(
    connection: &Connection,
    ingress: &Ingress,
    item: &DeferredInput,
    stored: Source,
    root_files: Vec<FileSource>,
    cancel: &CancellationToken,
) -> Result<Message, GatewayError> {
    let mut message = Message::text(stored.text);
    let mut remaining = MAX_ADAPTER_INPUT_BYTES;
    let mut staged = BTreeSet::new();
    let root_files = root_files.into_iter().map(|file| FileSource {
        voice: false,
        ..file
    });
    let files = stored
        .files
        .into_iter()
        .chain(root_files)
        .filter(|file| staged.insert(file.id.clone()))
        .take(MAX_FILES);
    for file in files {
        if item.preparation.purpose == PreparationPurpose::Body {
            ingress
                .authorize_preparation(item.event.id.clone(), item.preparation.token.clone())
                .await?;
        }
        match receive_file(
            connection,
            ingress,
            &MediaSource::from(&item.event),
            &file,
            remaining,
            cancel,
        )
        .await
        {
            Ok((PreparedAudio::Attachment(attachment), bytes)) => {
                message.attachments.push(attachment);
                remaining -= bytes;
            }
            Ok((PreparedAudio::Transcript(text), bytes)) => {
                if message.text.len() + text.len() + 32 < MAX_TEXT {
                    if !message.text.is_empty() {
                        message.text.push('\n');
                    }
                    message.text.push_str("Slack audio: ");
                    message.text.push_str(&text);
                }
                remaining -= bytes;
            }
            Err(GatewayError::Forbidden | GatewayError::Invalid | GatewayError::Unsupported) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(message)
}

async fn receive_file(
    connection: &Connection,
    ingress: &Ingress,
    source: &MediaSource,
    file: &FileSource,
    remaining: usize,
    cancel: &CancellationToken,
) -> Result<(PreparedAudio, usize), GatewayError> {
    let response = connection
        .api
        .scoped(
            ingress,
            "files.info",
            json!({"file":file.id,"team_id":connection.id.team}),
            cancel,
        )
        .await
        .map_err(Failure::read)?;
    let metadata = &response["file"];
    if metadata["id"] != file.id
        || metadata["is_external"] == true
        || metadata["mode"] == "external"
    {
        return Err(GatewayError::Forbidden);
    }
    let limit = remaining.min(MAX_ADAPTER_FILE);
    let size = metadata["size"]
        .as_u64()
        .and_then(|size| usize::try_from(size).ok())
        .filter(|size| (1..=limit).contains(size))
        .ok_or(GatewayError::Invalid)?;
    let name = metadata["name"]
        .as_str()
        .filter(|name| valid_name(name))
        .ok_or(GatewayError::Invalid)?;
    let endpoint = metadata["url_private_download"]
        .as_str()
        .or_else(|| metadata["url_private"].as_str())
        .ok_or(GatewayError::Invalid)?;
    let purpose = if file.voice {
        MediaPurpose::VoiceNote
    } else {
        MediaPurpose::Attachment
    };
    let grant = ingress
        .authorize_adapter_media(
            source.clone(),
            AttachmentManifest {
                name: name.to_owned(),
                size: Some(size),
                sha256: None,
            },
            purpose,
            MEDIA_LIFETIME,
            limit,
        )
        .await?;
    let open = || async {
        connection
            .api
            .download(endpoint, cancel)
            .await
            .map_err(Failure::read)
    };
    let result = if file.voice {
        ingress
            .prepare_adapter_audio(
                grant,
                Some(record_voice),
                open,
                voice_codec::decode_voice_note,
            )
            .await?
    } else {
        PreparedAudio::Attachment(ingress.stage_attachment(grant, open).await?)
    };
    Ok((result, size))
}

fn record_voice(
    tx: &rusqlite::Transaction<'_>,
    connection: &str,
    source: &MediaSource,
) -> anyhow::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO messaging_slack_voice_sources VALUES(?1,?2,?3,?4)",
        params![
            connection,
            source.event_id,
            serde_json::to_string(&source.room)?,
            source.sender.id
        ],
    )?;
    Ok(())
}

fn parse_command(text: &str) -> Option<Command> {
    let text = text.strip_prefix('!')?;
    let (name, argument) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    let argument = argument.trim();
    Some(match name {
        "status" => Command::Status,
        "queue" => Command::Queue,
        "history" => Command::History,
        "new" | "reset" => Command::Reset,
        "model" if argument.is_empty() => Command::ModelPicker,
        "model" => Command::SelectModel {
            model: argument.into(),
        },
        "set-home" => Command::SetHome,
        "fetch-result" if !argument.is_empty() => Command::FetchResult {
            action: argument.into(),
        },
        "bg" if !argument.is_empty() => Command::Background {
            text: argument.into(),
        },
        "skill" => {
            let (skill, input) = argument.split_once(char::is_whitespace)?;
            Command::UseSkill {
                skill: skill.into(),
                input: input.trim().into(),
            }
        }
        "voice" => Command::SpokenReplies {
            change: match argument {
                "off" => SpokenReplyChange::Set(SpokenReplyMode::Off),
                "on" => SpokenReplyChange::Set(SpokenReplyMode::Always),
                "notes" => SpokenReplyChange::Set(SpokenReplyMode::VoiceNotes),
                _ => SpokenReplyChange::Status,
            },
        },
        _ => return None,
    })
}

async fn slash(
    connection: &Connection,
    ingress: &Ingress,
    payload: &Value,
    wire: &str,
    accepts_inline: bool,
) -> Result<Option<Acknowledgement>, GatewayError> {
    let Some(room) = string(payload, "channel_id", b"CDG").and_then(|chat| channel(chat, None))
    else {
        return Ok(None);
    };
    let Some(user) = string(payload, "user_id", b"UW") else {
        return Ok(None);
    };
    let sender = Sender {
        id: user.into(),
        roles: BTreeSet::new(),
    };
    let command = match payload["command"].as_str() {
        Some("/mercury-status") => RequesterCommand::Status,
        Some("/mercury-help") => RequesterCommand::Help,
        Some("/mercury-pair") if room.kind == RoomKind::Direct && accepts_inline => {
            observe(ingress, &room, user).await?;
            return match ingress.begin_pairing(room, sender).await {
                Ok(challenge) => {
                    let mut response = Acknowledgement::ephemeral(format!(
                        "Share pairing code `{}` with the Mercury profile owner.",
                        challenge.code,
                    ));
                    response.expires = Some(challenge.expires_at);
                    Ok(Some(response))
                }
                Err(GatewayError::Forbidden) => Ok(Some(Acknowledgement::ephemeral(
                    "Pairing is unavailable here.".into(),
                ))),
                Err(error) => Err(error),
            };
        }
        _ => return Ok(None),
    };
    observe(ingress, &room, user).await?;
    let request = RequesterInvocation {
        id: wire.to_owned(),
        room,
        sender,
        command,
    };
    // Socket Mode has no cursor. The stable transport occurrence supplies an
    // independent checkpoint so a retry can read the same private receipt.
    let stream = format!("slash-{wire}");
    let previous = ingress.checkpoint(stream.clone()).await?;
    let result = ingress
        .commit_requester_command(
            request.clone(),
            hermes_gateway_core::messaging::Advance {
                stream,
                previous,
                checkpoint: "committed".into(),
            },
        )
        .await?;
    let authorized =
        result == Disposition::Accepted && ingress.requester_command_response(&request).is_ok();
    let text = if authorized {
        match command {
            RequesterCommand::Status => "Mercury is connected.".to_owned(),
            RequesterCommand::Help => {
                let allowed = ingress
                    .commands(request.room.clone(), request.sender.clone())
                    .unwrap_or_default();
                let names = allowed
                    .iter()
                    .map(|command| format!("!{}", command.name))
                    .collect::<Vec<_>>();
                if names.is_empty() {
                    "Available slash commands: /mercury-status and /mercury-help.".to_owned()
                } else {
                    format!("Available in this conversation: {}.", names.join(", "))
                }
            }
            RequesterCommand::Identity => unreachable!(),
        }
    } else {
        "This Mercury command is unavailable here.".to_owned()
    };
    if accepts_inline {
        return Ok(Some(Acknowledgement::ephemeral(text)));
    }
    if authorized && let Some(endpoint) = payload["response_url"].as_str() {
        callbacks::stage(connection, ingress, request, endpoint, &text).await?;
    }
    Ok(None)
}
