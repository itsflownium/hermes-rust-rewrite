use crate::messaging::{
    DeliveryBody, DeliveryIntent, DeliveryPart, Effect, EffectDependency, EffectOperation, Message,
    PreparationFailure, Prompt, ProvisionResource, ReceiptKind, RoomKind,
    MAX_ATTACHMENT_PLAN_PARTS,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use unicode_segmentation::UnicodeSegmentation;

pub(super) const CONTENT_LIMIT: usize = 2_000;
const MAX_PARTS: usize = 128;
const FILES_PER_MESSAGE: usize = 5;
const MAX_FENCE_HEADER: usize = 96;
const NO_REPLY_PREFIX: &str = "m0n";
const REPLY_PREFIX: &str = "m0r";
const VOICE_PREFIX: &str = "m0v";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplyMode {
    Off,
    #[default]
    First,
    All,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DeliverySurface {
    #[default]
    Existing,
    ThreadFromMessage,
    ForumPost,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PlanOptions {
    pub reply_mode: ReplyMode,
    pub uploads: bool,
    pub max_inline_parts: usize,
    pub surface: DeliverySurface,
}

impl Default for PlanOptions {
    fn default() -> Self {
        Self {
            reply_mode: ReplyMode::First,
            uploads: true,
            max_inline_parts: 8,
            surface: DeliverySurface::Existing,
        }
    }
}

pub(super) fn snowflake(value: &str) -> bool {
    !value.starts_with('0')
        && value.len() <= 20
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<u64>().is_ok()
}

pub(super) fn nonce(delivery: &str, part: u32, reply: bool) -> String {
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"mercury-discord-message-v1");
    digest.update(&part.to_be_bytes());
    digest.update(delivery.as_bytes());
    let digest = digest.finish();
    let prefix = if reply { REPLY_PREFIX } else { NO_REPLY_PREFIX };
    format!("{prefix}{}", URL_SAFE_NO_PAD.encode(&digest.as_ref()[..16]))
}

pub(super) fn is_nonce(value: &str) -> bool {
    value.len() == 25
        && value
            .get(..3)
            .is_some_and(|prefix| matches!(prefix, NO_REPLY_PREFIX | REPLY_PREFIX | VOICE_PREFIX))
        && URL_SAFE_NO_PAD
            .decode(&value.as_bytes()[3..])
            .is_ok_and(|hash| hash.len() == 16)
}

pub(super) fn is_voice_nonce(key: &str) -> bool {
    key.starts_with(VOICE_PREFIX) && is_nonce(key)
}

pub(super) fn references_message(key: &str) -> bool {
    key.starts_with(REPLY_PREFIX)
}

#[cfg(test)]
pub(crate) fn prepare(intent: &DeliveryIntent) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    prepare_with(intent, PlanOptions::default())
}

pub(crate) fn prepare_with(
    intent: &DeliveryIntent,
    options: PlanOptions,
) -> Result<Vec<DeliveryPart>, PreparationFailure> {
    let room = &intent.room;
    if !snowflake(&room.chat) || room.thread.as_deref().is_some_and(|id| !snowflake(id)) {
        return Err(PreparationFailure::Rejected);
    }
    if let DeliveryBody::Retire { marker } = &intent.body {
        return Ok(vec![part(
            intent,
            intent.body.clone(),
            Effect {
                operation: EffectOperation::Retire {
                    marker: marker.clone(),
                },
                ..Effect::default()
            },
        )]);
    }
    let DeliveryBody::Reply(message) = &intent.body else {
        let reply = options.reply_mode != ReplyMode::Off;
        return Ok(vec![part(
            intent,
            intent.body.clone(),
            create_effect(intent, 0, reply),
        )]);
    };

    if !options.uploads && !message.attachments.is_empty() {
        return Err(PreparationFailure::Unsupported);
    }
    if options.surface != DeliverySurface::Existing
        && (room.kind != RoomKind::Group || room.thread.is_some() || intent.edit_target.is_some())
    {
        return Err(PreparationFailure::Rejected);
    }
    if options.surface == DeliverySurface::ThreadFromMessage
        && !intent.reply_anchor.as_deref().is_some_and(snowflake)
    {
        return Err(PreparationFailure::Rejected);
    }

    let mut messages: Vec<_> = split_markdown(&message.text)
        .into_iter()
        .filter(|text| !text.trim().is_empty())
        .map(Message::text)
        .collect();
    let markdown = options.uploads && messages.len() > options.max_inline_parts.max(1);
    let mut file_messages = Vec::new();
    let file_destinations = if markdown {
        &mut file_messages
    } else {
        &mut messages
    };
    for (index, files) in message.attachments.chunks(FILES_PER_MESSAGE).enumerate() {
        if index == file_destinations.len() {
            file_destinations.push(Message::text(""));
        }
        file_destinations[index]
            .attachments
            .extend_from_slice(files);
    }

    let mut plan = Vec::new();
    let mut messages = messages.into_iter();
    if options.surface != DeliverySurface::Existing {
        let (resource, starter) = match options.surface {
            DeliverySurface::ThreadFromMessage => {
                (ProvisionResource::Thread, Message::text("Open discussion"))
            }
            DeliverySurface::ForumPost if markdown => (
                // The forum post has its own committed thread ID before the
                // generated attachment or its complete text can be published.
                ProvisionResource::Topic,
                Message::text("Reply follows in this thread."),
            ),
            DeliverySurface::ForumPost => (
                ProvisionResource::Topic,
                messages.next().ok_or(PreparationFailure::Rejected)?,
            ),
            DeliverySurface::Existing => unreachable!(),
        };
        plan.push(part(
            intent,
            DeliveryBody::Reply(starter),
            Effect {
                operation: EffectOperation::Provision { resource },
                ..Effect::default()
            },
        ));
    }

    let attachment = markdown.then_some(plan.len() as u32);
    if markdown {
        plan.push(publication(
            intent,
            options,
            plan.len(),
            Message::text(&message.text),
            true,
            None,
        ));
    }
    for (index, message) in messages.enumerate() {
        plan.push(publication(
            intent,
            options,
            plan.len(),
            message,
            index == 0,
            attachment,
        ));
    }
    for message in file_messages {
        plan.push(publication(
            intent,
            options,
            plan.len(),
            message,
            false,
            None,
        ));
    }
    if options.surface == DeliverySurface::ForumPost && plan.len() == 1 {
        // Discord's forum creation publishes its starter atomically. This
        // continuation confirms that existing starter without a second write.
        plan.push(part(
            intent,
            plan[0].body.clone(),
            Effect {
                operation: publish_using_destination(),
                ..Effect::default()
            },
        ));
    }
    let limit = if markdown {
        MAX_ATTACHMENT_PLAN_PARTS
    } else {
        MAX_PARTS
    };
    if plan.is_empty() || plan.len() > limit {
        return Err(PreparationFailure::Rejected);
    }
    Ok(plan)
}

pub(crate) fn add_voice_note(intent: &DeliveryIntent, parts: &mut Vec<DeliveryPart>) {
    let DeliveryBody::Reply(message) = &intent.body else { return };
    if message.text.trim().is_empty() || parts.is_empty() || parts.len() >= MAX_PARTS {
        return;
    }
    let provisioned = matches!(parts[0].effect.operation, EffectOperation::Provision { .. });
    // Files and progress edits already have their own ordering and fallback.
    let voice_first = parts.len() == 1 && !provisioned && message.attachments.is_empty()
        && parts[0].edit_target.is_none()
        && matches!(&parts[0].body, DeliveryBody::Reply(text) if measure(&text.text) <= CONTENT_LIMIT);
    let ordinal = if voice_first { 0 } else { parts.len() as u32 };
    let key = nonce(&intent.id, ordinal, false);
    let mut effect = Effect {
        operation_key: Some(format!("{VOICE_PREFIX}{}", &key[NO_REPLY_PREFIX.len()..])),
        ..Effect::default()
    };
    if provisioned {
        effect.operation = publish_using_destination();
    }
    let voice = part(intent, DeliveryBody::Reply(Message::text(&message.text)), effect);
    if voice_first {
        let text = &mut parts[0];
        let reply = text.effect.operation_key.as_deref().is_some_and(references_message);
        text.effect.operation_key = Some(nonce(&intent.id, 1, reply));
        text.effect.attachment_fallback_for = Some(0);
        parts.insert(0, voice);
    } else {
        parts.push(voice);
    }
}

fn publication(
    intent: &DeliveryIntent,
    options: PlanOptions,
    ordinal: usize,
    message: Message,
    first: bool,
    attachment_fallback_for: Option<u32>,
) -> DeliveryPart {
    let inline = options.surface == DeliverySurface::Existing;
    let reply = inline
        && (options.reply_mode == ReplyMode::All
            || first && options.reply_mode == ReplyMode::First);
    let mut effect = create_effect(intent, ordinal as u32, reply);
    if !inline {
        effect.operation = publish_using_destination();
    }
    effect.attachment_fallback_for = attachment_fallback_for;
    let mut rendered = part(intent, DeliveryBody::Reply(message), effect);
    if inline
        && first
        && let Some(edit) = intent.edit_target.as_ref().filter(|id| snowflake(id))
    {
        rendered.edit_target = Some(edit.clone());
        rendered.effect.operation_key = None;
        rendered.effect.publish_after_definite_edit_refusal = true;
    }
    rendered
}

fn part(intent: &DeliveryIntent, body: DeliveryBody, effect: Effect) -> DeliveryPart {
    DeliveryPart {
        room: intent.room.clone(),
        body,
        effect,
        edit_target: None,
    }
}

fn create_effect(intent: &DeliveryIntent, ordinal: u32, reply: bool) -> Effect {
    Effect {
        operation_key: Some(nonce(&intent.id, ordinal, reply)),
        ..Effect::default()
    }
}

fn publish_using_destination() -> EffectOperation {
    EffectOperation::Publish {
        using: vec![EffectDependency {
            part: 0,
            kind: ReceiptKind::Destination,
        }],
    }
}

pub(super) fn measure(value: &str) -> usize {
    value.encode_utf16().count()
}

pub(super) fn limit(value: &str, units: usize) -> String {
    let mut remaining = units;
    value
        .graphemes(true)
        .take_while(|grapheme| {
            let size = measure(grapheme);
            if size > remaining {
                return false;
            }
            remaining -= size;
            true
        })
        .collect()
}

struct Fence {
    open: String,
    close: String,
    marker: char,
    length: usize,
}

fn fence_marker(line: &str) -> Option<(char, usize, &str)> {
    let plain = line.trim_end_matches(['\r', '\n']);
    let indent = plain.len() - plain.trim_start_matches(' ').len();
    if indent > 3 {
        return None;
    }
    let rest = &plain[indent..];
    let marker = rest.chars().next()?;
    if !matches!(marker, '`' | '~') {
        return None;
    }
    let length = rest
        .chars()
        .take_while(|character| *character == marker)
        .count();
    (length >= 3).then(|| (marker, length, &rest[length..]))
}

fn start_fence(line: &str, marker: char, length: usize, info: &str) -> Option<Fence> {
    if measure(line) > MAX_FENCE_HEADER || marker == '`' && info.contains('`') {
        return None;
    }
    Some(Fence {
        open: format!("{}\n", line.trim_end_matches(['\r', '\n'])),
        close: marker.to_string().repeat(length),
        marker,
        length,
    })
}

fn flush(buffer: &mut String, parts: &mut Vec<String>, fence: Option<&Fence>) -> usize {
    if let Some(fence) = fence {
        if !buffer.ends_with('\n') {
            buffer.push('\n');
        }
        buffer.push_str(&fence.close);
        parts.push(std::mem::replace(buffer, fence.open.clone()));
        measure(buffer)
    } else {
        parts.push(std::mem::take(buffer));
        0
    }
}

fn closing_units(fence: Option<&Fence>) -> usize {
    // A split inside a code block can require a newline before its closing fence.
    fence.map_or(0, |fence| fence.length + 1)
}

pub(super) fn split_markdown(text: &str) -> Vec<String> {
    if text.is_empty() {
        return vec![String::new()];
    }
    let mut parts = Vec::new();
    let mut buffer = String::new();
    let mut used = 0;
    let mut fence: Option<Fence> = None;
    for line in text.split_inclusive('\n') {
        if let Some((marker, length, info)) = fence_marker(line) {
            let closes = fence.as_ref().is_some_and(|fence| {
                fence.marker == marker && fence.length <= length && info.trim().is_empty()
            });
            let opens = if fence.is_none() {
                start_fence(line, marker, length, info)
            } else {
                None
            };
            if closes || opens.is_some() {
                let size = measure(line);
                if used + size + closing_units(opens.as_ref()) > CONTENT_LIMIT && !buffer.is_empty()
                {
                    used = flush(&mut buffer, &mut parts, fence.as_ref());
                }
                if used + size + closing_units(opens.as_ref()) <= CONTENT_LIMIT {
                    buffer.push_str(line);
                    used += size;
                    fence = opens;
                    continue;
                }
            }
        }
        for grapheme in line.graphemes(true) {
            let units = measure(grapheme);
            if used + units + closing_units(fence.as_ref()) > CONTENT_LIMIT && !buffer.is_empty() {
                used = flush(&mut buffer, &mut parts, fence.as_ref());
            }
            if used + units + closing_units(fence.as_ref()) <= CONTENT_LIMIT {
                buffer.push_str(grapheme);
                used += units;
            } else {
                // A single pathological cluster can exceed the transport limit.
                // Preserve every code point without splitting a UTF-8 character.
                for character in grapheme.chars() {
                    if used + character.len_utf16() + closing_units(fence.as_ref()) > CONTENT_LIMIT
                    {
                        used = flush(&mut buffer, &mut parts, fence.as_ref());
                    }
                    buffer.push(character);
                    used += character.len_utf16();
                }
            }
        }
    }
    if !buffer.is_empty() {
        flush(&mut buffer, &mut parts, fence.as_ref());
    }
    parts
}

fn button(action: &str, choice: &str, label: &str, style: u8) -> Value {
    json!({"type":2, "custom_id":format!("mrc:{action}:{choice}"), "label":label, "style":style})
}

pub(super) fn prompt_payload(action: &str, prompt: &Prompt) -> Option<(String, Vec<Value>)> {
    if action.is_empty()
        || action.len() > 85
        || !action
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return None;
    }
    let (content, buttons) = match prompt {
        Prompt::Approval {
            text,
            allow_session,
            allow_persistent,
        } => {
            let mut buttons = vec![button(action, "once", "Approve once", 1)];
            if *allow_session {
                buttons.push(button(action, "session", "Approve for session", 2));
            }
            if *allow_persistent {
                buttons.push(button(action, "always", "Approve persistently", 2));
            }
            buttons.push(button(action, "deny", "Deny", 4));
            (
                format!("Approval requested\n{}", limit(text, CONTENT_LIMIT - 19)),
                buttons,
            )
        }
        Prompt::Clarification { question, choices } => {
            let buttons = if choices.is_empty() {
                vec![button(action, "text", "Reply", 1)]
            } else {
                if choices.len() > 25 {
                    return None;
                }
                choices
                    .iter()
                    .enumerate()
                    .map(|(index, choice)| {
                        let label = limit(choice.trim(), 80);
                        let label = if label.is_empty() {
                            format!("Choice {}", index + 1)
                        } else {
                            label
                        };
                        button(action, &format!("c:{index}"), &label, 2)
                    })
                    .collect()
            };
            (
                format!(
                    "Clarification requested\n{}",
                    limit(question, CONTENT_LIMIT - 24)
                ),
                buttons,
            )
        }
    };
    Some((
        content,
        buttons
            .chunks(5)
            .map(|row| json!({"type":1, "components":row}))
            .collect(),
    ))
}
