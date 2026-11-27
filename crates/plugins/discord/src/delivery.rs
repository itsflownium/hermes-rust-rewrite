mod buckets;
mod feedback;
mod render;
pub(super) mod thread_updates;

pub(crate) use buckets::TABLES as DELIVERY_RATE_TABLES;
pub(crate) use feedback::TABLES as FEEDBACK_TABLES;
pub use render::ReplyMode;
#[cfg(test)]
pub(crate) use render::prepare;
pub(crate) use render::{DeliverySurface, PlanOptions, add_voice_note, prepare_with};

use super::{
    DiscordAdapter, DiscordClient, voice_note,
    manual_thread::{ManualThreadIngress, Reservation, verified_thread},
};
use crate::messaging::{
    DeliveryBody, DeliveryResult, EffectIdentity, EffectObservation, EffectOperation,
    EffectReceipt, GatewayError, Ingress, ProviderDeliveryState, ProvisionResource, ReceiptKind,
    RetryScope, Room, RoomKind, SendAttempt,
};
use render::{
    CONTENT_LIMIT, is_nonce, is_voice_nonce, limit, measure, nonce, prompt_payload,
    references_message, snowflake,
};
use reqwest::{Method, RequestBuilder, StatusCode, multipart};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const AUTO_THREAD_NAME: &str = "Mercury discussion";

const QUIET: u16 = 1 << 12;
const UNKNOWN_CHANNEL: u64 = 10_003;
const UNKNOWN_MESSAGE: u64 = 10_008;
const THREAD_EXISTS: u64 = 160_004;
const MAX_JSON: usize = 2 * 1024 * 1024;
const MAX_UPLOAD: usize = 24 * 1024 * 1024;

pub(super) fn manual_nonce(delivery: &str, ordinal: u32) -> String {
    nonce(delivery, ordinal, false)
}

pub(super) async fn send_as(
    adapter: &DiscordAdapter,
    attempt: SendAttempt,
    ingress: Ingress,
    cancel: CancellationToken,
) -> DeliveryResult {
    send_inner(
        &adapter.client,
        &adapter.bot.bot_id,
        attempt,
        ingress,
        cancel,
        Some(adapter),
    )
    .await
}

#[cfg(test)]
pub(crate) async fn send(
    client: &DiscordClient,
    bot_id: &str,
    attempt: SendAttempt,
    ingress: Ingress,
    cancel: CancellationToken,
) -> DeliveryResult {
    send_inner(client, bot_id, attempt, ingress, cancel, None).await
}

async fn send_inner(
    client: &DiscordClient,
    bot_id: &str,
    attempt: SendAttempt,
    ingress: Ingress,
    cancel: CancellationToken,
    adapter: Option<&DiscordAdapter>,
) -> DeliveryResult {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => DeliveryResult::Uncertain,
        result = dispatch(client, bot_id, &attempt, &ingress, adapter) => {
            match result { Ok(evidence) | Err(evidence) => evidence }
        }
    }
}

pub(crate) fn sends_voice_note(attempt: &SendAttempt) -> bool {
    attempt.payload.effect.operation_key.as_deref().is_some_and(is_voice_nonce)
}

pub(crate) async fn observe_create(
    ingress: &Ingress,
    bot_id: &str,
    room: &Room,
    dispatch: &Value,
) -> Result<(), GatewayError> {
    let Some(key) = dispatch["nonce"].as_str().filter(|value| is_nonce(value)) else {
        return Ok(());
    };
    let channel = room.thread.as_deref().unwrap_or(&room.chat);
    if is_voice_nonce(key) && !voice_note::accepted_note(dispatch) {
        return Ok(());
    }
    let Some(id) = own_message(dispatch, bot_id, channel) else {
        return Ok(());
    };
    let timestamp =
        ((id.parse::<u64>().map_err(|_| GatewayError::Invalid)? >> 22) as i64) + 1_420_070_400_000;
    ingress
        .observe_delivery(EffectObservation {
            event: format!("discord-message-create:{id}"),
            room: room.clone(),
            identity: EffectIdentity::OperationKey(key.to_owned()),
            recipient: None,
            observed_at_ms: timestamp,
            state: ProviderDeliveryState::Accepted,
            receipt: Some(id.to_owned()),
        })
        .await?;
    Ok(())
}

type SendResult<T> = Result<T, DeliveryResult>;

async fn dispatch(
    client: &DiscordClient,
    bot_id: &str,
    attempt: &SendAttempt,
    ingress: &Ingress,
    adapter: Option<&DiscordAdapter>,
) -> SendResult<DeliveryResult> {
    let room = &attempt.payload.room;
    if !snowflake(bot_id)
        || !snowflake(&room.chat)
        || room.thread.as_deref().is_some_and(|id| !snowflake(id))
    {
        return Err(DeliveryResult::Rejected);
    }

    let destination = resolved_room(attempt)?;
    let reservation = ingress
        .manual_thread_delivery(&attempt.delivery_id)
        .map_err(|_| retry(Duration::from_millis(500), RetryScope::Part))?;
    let manual = match reservation.as_ref() {
        Some(reservation) => Some((adapter.ok_or(DeliveryResult::Rejected)?, reservation)),
        None => None,
    };
    let client = &Rest {
        client,
        ingress,
        attempt,
        destination: &destination,
        manual,
    };
    match &attempt.payload.effect.operation {
        EffectOperation::Provision { resource } => {
            return provision(client, bot_id, attempt, ingress, *resource).await;
        }
        EffectOperation::Retire { .. }
        | EffectOperation::Message
        | EffectOperation::Publish { .. } => {}
        _ => return Err(DeliveryResult::Rejected),
    }
    let channel = destination.thread.as_deref().unwrap_or(&destination.chat);
    let verified = inspect_channel(client, channel).await?;
    if !matches_room(&verified, &destination) {
        return Err(DeliveryResult::Rejected);
    }

    if matches!(
        attempt.payload.effect.operation,
        EffectOperation::Publish { .. }
    ) && attempt.payload.effect.operation_key.is_none()
    {
        // A forum starter shares its ID with the thread. Mark the already
        // published starter as confirmed only after retrieving that own message.
        if !matches!(verified["type"].as_u64(), Some(11)) || room.thread.is_some() {
            return Err(DeliveryResult::Rejected);
        }
        let receipt = read_message(client, channel, channel).await?;
        if own_message(&receipt, bot_id, channel) != Some(channel) {
            return Err(DeliveryResult::Rejected);
        }
        return Ok(confirmed(channel, false));
    }

    if matches!(
        attempt.payload.effect.operation,
        EffectOperation::Retire { .. }
    ) {
        return retire(client, bot_id, attempt, channel).await;
    }
    if sends_voice_note(attempt) {
        return publish_voice_note(client, bot_id, attempt, ingress, channel, adapter).await;
    }
    if let Some(adapter) = adapter
        && matches!(attempt.payload.body, DeliveryBody::Progress { .. })
    {
        let feedback = &adapter.settings.delivery;
        feedback::publish(client, channel, feedback.typing, feedback.reactions).await;
    }
    publish(client, bot_id, attempt, ingress, channel).await
}

fn resolved_room(attempt: &SendAttempt) -> SendResult<Room> {
    let mut destination = attempt.payload.room.clone();
    if matches!(
        attempt.payload.effect.operation,
        EffectOperation::Publish { .. }
    ) {
        let [(_, receipt)] = attempt.effect.dependencies.as_slice() else {
            return Err(DeliveryResult::Rejected);
        };
        if destination.kind != RoomKind::Group
            || destination.thread.is_some()
            || receipt.kind != ReceiptKind::Destination
            || !snowflake(&receipt.id)
        {
            return Err(DeliveryResult::Rejected);
        }
        destination.thread = Some(receipt.id.clone());
    }
    Ok(destination)
}

async fn publish(
    client: &Rest<'_>,
    bot_id: &str,
    attempt: &SendAttempt,
    ingress: &Ingress,
    channel: &str,
) -> SendResult<DeliveryResult> {
    let markdown = matches!(&attempt.payload.body, DeliveryBody::Reply(message)
        if measure(&message.text) > CONTENT_LIMIT);
    let (mut payload, uploads) = render_message(attempt, ingress).await?;
    if let Some(target) = attempt
        .payload
        .edit_target
        .as_deref()
        .filter(|id| snowflake(id))
    {
        let original = read_message(client, channel, target).await;
        match original {
            Ok(message) if own_message(&message, bot_id, channel) == Some(target) => {}
            Ok(_) => return Err(DeliveryResult::Rejected),
            Err(error) => return Err(error),
        }
        let request = body(
            client.bot(Method::PATCH, &["channels", channel, "messages", target]),
            payload,
            uploads,
        )?;
        let edited = client
            .execute(request, Call::Edit)
            .await?
            .into_publication(Call::Edit, markdown)?;
        return if own_message(&edited, bot_id, channel) == Some(target) {
            Ok(confirmed(target, false))
        } else {
            Err(DeliveryResult::Uncertain)
        };
    }
    if attempt.payload.edit_target.is_some() {
        return Err(DeliveryResult::Rejected);
    }

    if !attempt.payload.effect.visibility.notify {
        payload["flags"] = json!(QUIET);
    }
    let key = match attempt.payload.effect.operation_key.as_ref() {
        Some(value) if is_nonce(value) => value.clone(),
        Some(_) => return Err(DeliveryResult::Rejected),
        // Only the exact edit fallback loses its original edit operation.
        None => nonce(&attempt.delivery_id, attempt.part, false),
    };
    payload["nonce"] = json!(&key);
    payload["enforce_nonce"] = json!(true);
    if references_message(&key)
        && let Some(anchor) = attempt.reply_anchor.as_deref().filter(|id| snowflake(id))
    {
        payload["message_reference"] = json!({
            "message_id":anchor, "channel_id":channel, "fail_if_not_exists":false,
        });
    }
    let request = body(
        client.bot(Method::POST, &["channels", channel, "messages"]),
        payload,
        uploads,
    )?;
    let created = client
        .execute(request, Call::Mutation)
        .await?
        .into_publication(Call::Mutation, markdown)?;
    if !created["nonce"].is_null() && created["nonce"].as_str() != Some(&key) {
        return Err(DeliveryResult::Uncertain);
    }
    let id = own_message(&created, bot_id, channel).ok_or(DeliveryResult::Uncertain)?;
    let marker = matches!(
        attempt.payload.body,
        DeliveryBody::Progress { .. } | DeliveryBody::Prompt { .. }
    );
    Ok(confirmed(id, marker))
}

async fn publish_voice_note(
    client: &Rest<'_>,
    bot_id: &str,
    attempt: &SendAttempt,
    ingress: &Ingress,
    channel: &str,
    adapter: Option<&DiscordAdapter>,
) -> SendResult<DeliveryResult> {
    let Some(key) = attempt.payload.effect.operation_key.as_deref().filter(|key| is_voice_nonce(key))
    else { return Err(DeliveryResult::Rejected) };
    if attempt.payload.edit_target.is_some()
        || !matches!(&attempt.payload.body, DeliveryBody::Reply(message) if message.attachments.is_empty())
    {
        return Err(DeliveryResult::Rejected);
    }
    // No provider mutation has occurred if synthesis, reading or encoding fails.
    let file = ingress.synthesize_reply(attempt).await.map_err(|_| DeliveryResult::AttachmentNotPublished)?;
    let wav = ingress.reply_artifact(attempt, &file).await.map_err(|_| DeliveryResult::AttachmentNotPublished)?;
    let note = crate::messaging::runtime::blocking(move || voice_note::encode(&wav))
        .await.map_err(|_| DeliveryResult::AttachmentNotPublished)?;
    let flags = voice_note::FLAG
        | if attempt.payload.effect.visibility.notify { 0 } else { u64::from(QUIET) };
    let payload = json!({
        "flags": flags, "nonce":key, "enforce_nonce": true,
        "attachments":[note.attachment()],
    });
    let audio = multipart::Part::bytes(note.bytes).file_name(voice_note::FILENAME)
        .mime_str(voice_note::MIME).map_err(|_| DeliveryResult::AttachmentNotPublished)?;
    let form = multipart::Form::new().text("payload_json", payload.to_string()).part("files[0]", audio);
    if adapter.is_some_and(|adapter| {
        !adapter.should_send_voice_note(ingress, &attempt.delivery_id, &attempt.payload.room)
    }) {
        return Err(DeliveryResult::AttachmentNotPublished);
    }
    let publication = client.execute(
        client.bot(Method::POST, &["channels", channel, "messages"]).multipart(form),
        Call::Mutation,
    ).await?;
    let created = publication.into_json(Call::Mutation).map_err(|error| match error {
        DeliveryResult::Rejected => DeliveryResult::AttachmentNotPublished,
        error => error,
    })?;
    if !voice_note::accepted_note(&created)
        || !created["nonce"].is_null() && created["nonce"].as_str() != Some(key)
    {
        return Err(DeliveryResult::Uncertain);
    }
    let id = own_message(&created, bot_id, channel).ok_or(DeliveryResult::Uncertain)?;
    Ok(confirmed(id, false))
}

async fn retire(
    client: &Rest<'_>,
    bot_id: &str,
    attempt: &SendAttempt,
    channel: &str,
) -> SendResult<DeliveryResult> {
    let target = attempt
        .effect
        .marker
        .as_ref()
        .filter(|marker| marker.kind == ReceiptKind::Presentation && snowflake(&marker.id))
        .ok_or(DeliveryResult::Rejected)?;
    match read_message(client, channel, &target.id).await {
        Ok(message) if own_message(&message, bot_id, channel) == Some(&target.id) => {}
        Ok(_) => return Err(DeliveryResult::Rejected),
        Err(DeliveryResult::EditNotPublished) => return Ok(confirmed(&target.id, false)),
        Err(error) => return Err(error),
    }
    let reply = client
        .execute(
            client.bot(
                Method::DELETE,
                &["channels", channel, "messages", &target.id],
            ),
            Call::Mutation,
        )
        .await?;
    if reply.status == StatusCode::NO_CONTENT || reply.unknown_message() {
        Ok(confirmed(&target.id, false))
    } else if reply.status.is_success() {
        Err(DeliveryResult::Uncertain)
    } else {
        Err(reply.failure(Call::Mutation))
    }
}

async fn provision(
    client: &Rest<'_>,
    bot_id: &str,
    attempt: &SendAttempt,
    ingress: &Ingress,
    resource: ProvisionResource,
) -> SendResult<DeliveryResult> {
    let room = &attempt.payload.room;
    if room.kind != RoomKind::Group || room.thread.is_some() {
        return Err(DeliveryResult::Rejected);
    }
    let parent = &room.chat;
    let verified = inspect_channel(client, parent).await?;
    if verified["id"].as_str() != Some(parent) {
        return Err(DeliveryResult::Rejected);
    }
    let thread = match resource {
        ProvisionResource::ThreadFromMessage { starter }
            if matches!(verified["type"].as_u64(), Some(0 | 5)) =>
        {
            let (_, reservation) = client.manual.ok_or(DeliveryResult::Rejected)?;
            let request = &reservation.request;
            if verified["guild_id"].as_str() != Some(&request.guild) {
                return Err(DeliveryResult::Rejected);
            }
            let [(_, receipt)] = attempt.effect.dependencies.as_slice() else {
                return Err(DeliveryResult::Rejected);
            };
            if attempt.effect.dependencies[0].0 != starter
                || receipt.kind != ReceiptKind::Message
                || !snowflake(&receipt.id)
            {
                return Err(DeliveryResult::Rejected);
            }
            let anchor = receipt.id.as_str();
            if let Some(exists) = known_thread(client, parent, anchor, bot_id).await? {
                return Ok(destination_receipt(exists));
            }
            match read_message(client, parent, anchor).await {
                Ok(message) if own_message(&message, bot_id, parent) == Some(anchor) => {}
                Ok(_) | Err(DeliveryResult::EditNotPublished) => {
                    return Err(DeliveryResult::Rejected);
                }
                Err(other) => return Err(other),
            }
            let created = client.execute(
                client.bot(Method::POST, &["channels", parent, "messages", anchor, "threads"])
                    .json(&json!({"name":request.spec.name, "auto_archive_duration":request.spec.archive})),
                Call::Mutation,
            ).await?;
            if created.body["code"].as_u64() == Some(THREAD_EXISTS) {
                return known_thread(client, parent, anchor, bot_id)
                    .await?
                    .map(destination_receipt)
                    .ok_or(DeliveryResult::Uncertain);
            }
            let body = created.into_json(Call::Mutation)?;
            if !verified_thread(&body, request, anchor, bot_id) {
                return Err(DeliveryResult::Uncertain);
            }
            body
        }
        ProvisionResource::Thread if matches!(verified["type"].as_u64(), Some(0 | 5)) => {
            let anchor = attempt
                .reply_anchor
                .as_deref()
                .filter(|id| snowflake(id))
                .ok_or(DeliveryResult::Rejected)?;
            if let Some(exists) = known_thread(client, parent, anchor, bot_id).await? {
                return Ok(destination_receipt(exists));
            }
            let created = client
                .execute(
                    client
                        .bot(
                            Method::POST,
                            &["channels", parent, "messages", anchor, "threads"],
                        )
                        .json(&json!({"name":AUTO_THREAD_NAME})),
                    Call::Mutation,
                )
                .await?;
            if created.body["code"].as_u64() == Some(THREAD_EXISTS) {
                return match known_thread(client, parent, anchor, bot_id).await? {
                    Some(exists) => Ok(destination_receipt(exists)),
                    None => Err(DeliveryResult::Uncertain),
                };
            }
            let body = created.into_json(Call::Mutation)?;
            if body["id"].as_str() != Some(anchor) {
                return Err(DeliveryResult::Uncertain);
            }
            body
        }
        ProvisionResource::Topic if matches!(verified["type"].as_u64(), Some(15 | 16)) => {
            let (mut message, uploads) = render_message(attempt, ingress).await?;
            message["flags"] = json!(QUIET);
            let request = body(
                client.bot(Method::POST, &["channels", parent, "threads"]),
                json!({"name":"Mercury reply", "message":message}),
                uploads,
            )?;
            let thread = client.json(request, Call::Mutation).await?;
            let id = thread["id"]
                .as_str()
                .filter(|value| snowflake(value))
                .ok_or(DeliveryResult::Uncertain)?;
            if own_message(&thread["message"], bot_id, id) != Some(id) {
                return Err(DeliveryResult::Uncertain);
            }
            thread
        }
        _ => return Err(DeliveryResult::Rejected),
    };
    let Some(id) = verified_owned_thread(&thread, parent, bot_id) else {
        return Err(DeliveryResult::Uncertain);
    };
    Ok(destination_receipt(id))
}

fn destination_receipt(id: &str) -> DeliveryResult {
    DeliveryResult::EffectConfirmed(EffectReceipt {
        kind: ReceiptKind::Destination,
        id: id.to_owned(),
    })
}

async fn known_thread<'a>(
    client: &Rest<'_>,
    parent: &str,
    id: &'a str,
    bot_id: &str,
) -> SendResult<Option<&'a str>> {
    let response = client
        .execute(client.bot(Method::GET, &["channels", id]), Call::Read)
        .await?;
    if response.status == StatusCode::NOT_FOUND
        && response.body["code"].as_u64() == Some(UNKNOWN_CHANNEL)
    {
        return Ok(None);
    }
    let value = response.into_json(Call::Read)?;
    if verified_owned_thread(&value, parent, bot_id) == Some(id)
        && client.manual.is_none_or(|(_, reservation)| {
            verified_thread(&value, &reservation.request, id, bot_id)
        })
    {
        Ok(Some(id))
    } else {
        Err(DeliveryResult::Rejected)
    }
}

fn verified_owned_thread<'a>(value: &'a Value, parent: &str, bot_id: &str) -> Option<&'a str> {
    if !matches!(value["type"].as_u64(), Some(10 | 11))
        || value["parent_id"].as_str() != Some(parent)
        || value["owner_id"].as_str() != Some(bot_id)
    {
        return None;
    }
    value["id"].as_str().filter(|id| snowflake(id))
}

fn matches_room(value: &Value, room: &Room) -> bool {
    match (&room.kind, room.thread.as_deref(), value["type"].as_u64()) {
        (RoomKind::Direct, None, Some(1)) | (RoomKind::Group, None, Some(0 | 5)) => {
            value["id"].as_str() == Some(&room.chat)
        }
        (RoomKind::Group, Some(thread), Some(10..=12)) => {
            value["id"].as_str() == Some(thread) && value["parent_id"].as_str() == Some(&room.chat)
        }
        _ => false,
    }
}

async fn inspect_channel(client: &Rest<'_>, channel: &str) -> SendResult<Value> {
    client
        .json(client.bot(Method::GET, &["channels", channel]), Call::Read)
        .await
}

async fn read_message(client: &Rest<'_>, channel: &str, id: &str) -> SendResult<Value> {
    client
        .json(
            client.bot(Method::GET, &["channels", channel, "messages", id]),
            Call::ReadMessage,
        )
        .await
}

async fn check_authority(ingress: &Ingress, attempt: &SendAttempt, room: &Room) -> SendResult<()> {
    // No mutation has been attempted if the current host grant disappeared.
    if room == &attempt.payload.room {
        ingress.recheck_send(attempt).await
    } else {
        ingress.recheck_send_destination(attempt, room).await
    }
    .map_err(|_| retry(Duration::from_millis(200), RetryScope::Destination))
}

fn own_message<'a>(message: &'a Value, bot_id: &str, channel: &str) -> Option<&'a str> {
    if message["channel_id"].as_str() != Some(channel)
        || message["author"]["id"].as_str() != Some(bot_id)
        || !message["webhook_id"].is_null()
    {
        return None;
    }
    message["id"].as_str().filter(|value| snowflake(value))
}

fn confirmed(id: &str, editable: bool) -> DeliveryResult {
    DeliveryResult::Confirmed {
        receipt: id.into(),
        editable_message: editable.then(|| id.into()),
    }
}

struct Upload {
    name: String,
    bytes: Vec<u8>,
}

async fn render_message(
    attempt: &SendAttempt,
    ingress: &Ingress,
) -> SendResult<(Value, Vec<Upload>)> {
    let mut payload =
        json!({"allowed_mentions":{"parse":[],"users":[],"roles":[],"replied_user":false}});
    let mut uploads = Vec::new();
    let mut upload_size = 0;
    match &attempt.payload.body {
        DeliveryBody::Reply(message) => {
            let export = measure(&message.text) > CONTENT_LIMIT;
            if !ingress.capabilities().attachments {
                if export {
                    return Err(DeliveryResult::AttachmentNotPublished);
                }
                if !message.attachments.is_empty() {
                    return Err(DeliveryResult::Rejected);
                }
            }
            if export {
                payload["content"] = json!("Full reply attached.");
                let bytes = message.text.as_bytes().to_vec();
                upload_size = bytes.len();
                uploads.push(Upload {
                    name: "reply.md".into(),
                    bytes,
                });
            } else if !message.text.is_empty() {
                payload["content"] = json!(&message.text);
            }
            for file in &message.attachments {
                let bytes =
                    ingress
                        .reply_artifact(attempt, file)
                        .await
                        .map_err(|error| match error {
                            GatewayError::Forbidden
                            | GatewayError::StaleConnection
                            | GatewayError::TemporarilyUnavailable => {
                                retry(Duration::from_millis(200), RetryScope::Destination)
                            }
                            _ => DeliveryResult::Rejected,
                        })?;
                upload_size += bytes.len();
                if upload_size > MAX_UPLOAD {
                    return Err(DeliveryResult::Rejected);
                }
                let name = limit(file.name(), 128).replace(['\r', '\n', '\\', '/', '"'], "_");
                uploads.push(Upload { name, bytes });
            }
        }
        DeliveryBody::Prompt { action_id, prompt } => {
            let (content, components) =
                prompt_payload(action_id, prompt).ok_or(DeliveryResult::Rejected)?;
            payload["content"] = json!(content);
            payload["components"] = json!(components);
        }
        DeliveryBody::Progress { text, .. } => {
            payload["content"] = json!(limit(text, CONTENT_LIMIT))
        }
        DeliveryBody::Retire { .. } => return Err(DeliveryResult::Rejected),
    }
    if !uploads.is_empty() {
        payload["attachments"] = Value::Array(
            uploads
                .iter()
                .enumerate()
                .map(|(id, file)| json!({"id":id,"filename":file.name}))
                .collect(),
        );
    } else if payload["content"]
        .as_str()
        .is_none_or(|text| text.trim().is_empty())
    {
        return Err(DeliveryResult::Rejected);
    }
    Ok((payload, uploads))
}

fn body(
    request: RequestBuilder,
    payload: Value,
    uploads: Vec<Upload>,
) -> SendResult<RequestBuilder> {
    if uploads.is_empty() {
        return Ok(request.json(&payload));
    }
    let mut form = multipart::Form::new().text("payload_json", payload.to_string());
    for (index, file) in uploads.into_iter().enumerate() {
        form = form.part(
            format!("files[{index}]"),
            multipart::Part::bytes(file.bytes).file_name(file.name),
        );
    }
    Ok(request.multipart(form))
}

#[derive(Clone, Copy)]
enum Call {
    Read,
    ReadMessage,
    Edit,
    Mutation,
}

impl Call {
    fn transport_failure(self) -> DeliveryResult {
        match self {
            Self::Read | Self::ReadMessage => {
                retry(Duration::from_millis(500), RetryScope::Destination)
            }
            Self::Edit | Self::Mutation => DeliveryResult::Uncertain,
        }
    }
}

struct HttpReply {
    status: StatusCode,
    body: Value,
}

impl HttpReply {
    fn unknown_message(&self) -> bool {
        self.status == StatusCode::NOT_FOUND && self.body["code"].as_u64() == Some(UNKNOWN_MESSAGE)
    }

    fn into_json(self, call: Call) -> SendResult<Value> {
        if !self.status.is_success() {
            Err(self.failure(call))
        } else if self.body.is_object() {
            Ok(self.body)
        } else {
            Err(call.transport_failure())
        }
    }

    fn into_publication(self, call: Call, generated_markdown: bool) -> SendResult<Value> {
        // Only provider responses that explicitly refuse the upload allow the
        // durable text alternative. Rate limits are handled before this step.
        let upload_refused = match self.status {
            StatusCode::PAYLOAD_TOO_LARGE | StatusCode::UNSUPPORTED_MEDIA_TYPE => true,
            StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN => matches!(
                self.body["code"].as_u64(),
                Some(40_005 | 50_045 | 50_046 | 50_110 | 50_123 | 50_146 | 400_001)
            ),
            _ => false,
        };
        if generated_markdown && upload_refused {
            return Err(DeliveryResult::AttachmentNotPublished);
        }
        self.into_json(call)
    }

    fn failure(&self, call: Call) -> DeliveryResult {
        if matches!(call, Call::ReadMessage | Call::Edit) && self.unknown_message() {
            return DeliveryResult::EditNotPublished;
        }
        if self.status.is_client_error()
            && self.status != StatusCode::REQUEST_TIMEOUT
            && self.status != StatusCode::CONFLICT
        {
            return DeliveryResult::Rejected;
        }
        call.transport_failure()
    }
}

struct Rest<'a> {
    client: &'a DiscordClient,
    ingress: &'a Ingress,
    attempt: &'a SendAttempt,
    destination: &'a Room,
    manual: Option<(&'a DiscordAdapter, &'a Reservation)>,
}

impl Rest<'_> {
    fn bot(&self, method: Method, segments: &[&str]) -> RequestBuilder {
        self.client.bot(method, segments)
    }

    async fn execute(&self, request: RequestBuilder, call: Call) -> SendResult<HttpReply> {
        let (client, request) = request.build_split();
        let request = request.map_err(|_| DeliveryResult::Rejected)?;
        let route = buckets::Route::from_request(&request);
        if let Some((delay, scope)) = buckets::wait(self.ingress, route.as_ref())
            .await
            .map_err(|_| retry(Duration::from_millis(500), RetryScope::Part))?
        {
            return Err(retry(delay, scope));
        }
        if matches!(call, Call::Edit | Call::Mutation) {
            if let Some((adapter, reservation)) = self.manual {
                adapter
                    .recheck_manual_thread(
                        self.ingress,
                        self.attempt,
                        self.destination,
                        reservation,
                    )
                    .await?;
            }
            check_authority(self.ingress, self.attempt, self.destination).await?;
        }
        execute_http(self.ingress, route.as_ref(), client, request, call, self.attempt.attempt).await
    }

    async fn json(&self, request: RequestBuilder, call: Call) -> SendResult<Value> {
        self.execute(request, call).await?.into_json(call)
    }
}

async fn execute_http(
    ingress: &Ingress,
    route: Option<&buckets::Route>,
    client: reqwest::Client,
    request: reqwest::Request,
    call: Call,
    attempt: u32,
) -> SendResult<HttpReply> {
    let mut response = client
        .execute(request)
        .await
        .map_err(|_| call.transport_failure())?;
    let status = response.status();
    let headers = response.headers().clone();
    let mut data = Vec::new();
    loop {
        match response.chunk().await {
            Ok(None) => break,
            Ok(Some(chunk)) if chunk.len().saturating_add(data.len()) <= MAX_JSON => {
                data.extend_from_slice(&chunk);
            }
            _ => {
                data.clear();
                break;
            }
        }
    }
    let body = serde_json::from_slice(&data).unwrap_or(Value::Null);
    if let Some(result) = buckets::observe(
        ingress,
        route,
        status,
        &headers,
        &body,
        attempt,
    )
    .await
    {
        return Err(result);
    }
    Ok(HttpReply { status, body })
}

fn retry(after: Duration, scope: RetryScope) -> DeliveryResult {
    DeliveryResult::Retryable { after, scope }
}

#[cfg(test)]
mod tests;
