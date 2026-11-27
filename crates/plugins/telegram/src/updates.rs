use super::address::{Destination, actor_id, chat_id};
use hermes_gateway_core::messaging::{
    ActorKind, Command, Control, InboundBody, InboundEvent, Message, Room, RoomKind, Sender,
    SpokenReplyChange, SpokenReplyMode,
};
use hermes_telegram::telegram::Bot;
use regex::RegexSet;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(super) const UPDATE_TYPES: &[&str] = &[
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "callback_query",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct File {
    pub id: String,
    pub name: String,
    pub size: Option<usize>,
    pub voice: bool,
}

pub(super) struct Incoming {
    pub event: InboundEvent,
    pub kind: ActorKind,
    pub message_id: i64,
    pub reply_to_bot: Option<i64>,
    pub edited: bool,
    pub files: Vec<File>,
    pub album: Option<String>,
}

pub(super) struct Tap<'a> {
    pub id: &'a str,
    pub data: &'a str,
    pub room: Room,
    pub user: i64,
    pub message: i64,
}

pub(super) fn id(update: &Value) -> Option<i64> {
    update["update_id"]
        .as_i64()
        .filter(|v| (1..i64::MAX).contains(v))
}

pub(super) fn callback<'a>(update: &'a Value, bot: &Bot) -> Option<Tap<'a>> {
    let tap = update.get("callback_query")?;
    let message = tap.get("message")?;
    let user = tap
        .pointer("/from/id")?
        .as_i64()
        .filter(|id| actor_id(*id))?;
    if tap.pointer("/from/is_bot")?.as_bool()?
        || message.pointer("/from/id")?.as_i64()? != bot.identity.id
    {
        return None;
    }
    let (destination, kind) = Destination::from_message(message)?;
    if kind == RoomKind::Direct && destination.chat != user {
        return None;
    }
    let id = tap["id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 256)?;
    let data = tap["data"]
        .as_str()
        .filter(|data| !data.is_empty() && data.len() <= 64)?;
    let message = message["message_id"].as_i64().filter(|id| *id > 0)?;
    Some(Tap {
        id,
        data,
        room: destination.room(kind),
        user,
        message,
    })
}

pub(super) fn message(
    update: &Value,
    event_id: &str,
    bot: &Bot,
    patterns: &RegexSet,
) -> Option<Incoming> {
    let (raw, edited) =
        if let Some(raw) = update.get("message").or_else(|| update.get("channel_post")) {
            (raw, false)
        } else {
            (
                update
                    .get("edited_message")
                    .or_else(|| update.get("edited_channel_post"))?,
                true,
            )
        };
    let (destination, room_kind) = Destination::from_message(raw)?;
    let room = destination.room(room_kind);
    let message_id = raw["message_id"].as_i64().filter(|id| *id > 0)?;
    let (sender, kind) = if let Some(sender_chat) = raw.get("sender_chat") {
        let sender = sender_chat["id"]
            .as_i64()
            .filter(|id| *id < 0 && chat_id(*id))?;
        if room_kind != RoomKind::Group
            || sender != destination.chat
            || raw.pointer("/chat/type")?.as_str()? != "channel"
        {
            return None;
        }
        (sender, ActorKind::Shared)
    } else {
        let user = raw
            .pointer("/from/id")?
            .as_i64()
            .filter(|id| actor_id(*id))?;
        if user == bot.identity.id
            || raw.pointer("/from/is_bot")?.as_bool()?
            || room_kind == RoomKind::Direct && user != destination.chat
        {
            return None;
        }
        (user, ActorKind::Person)
    };
    let (text, entities) = match raw.get("text").and_then(Value::as_str) {
        Some(text) => (text, raw.get("entities")),
        None => (
            raw["caption"].as_str().unwrap_or_default(),
            raw.get("caption_entities"),
        ),
    };
    let addressed = addressed_to_bot(text, entities, raw, bot, patterns);
    let mut text = text.trim().to_owned();
    let files = files(raw);
    if let Some(contact) = raw.get("contact") {
        let name = [
            contact["first_name"].as_str(),
            contact["last_name"].as_str(),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(" ");
        append_context(
            &mut text,
            &format!(
                "Telegram contact: {name}, {}",
                contact["phone_number"]
                    .as_str()
                    .unwrap_or("phone number unavailable")
            ),
        );
    }
    if let Some(location) = raw
        .get("location")
        .or_else(|| raw.pointer("/venue/location"))
        && let (Some(latitude), Some(longitude)) = (
            location["latitude"].as_f64(),
            location["longitude"].as_f64(),
        )
        && (-90.0..=90.0).contains(&latitude)
        && (-180.0..=180.0).contains(&longitude)
    {
        append_context(
            &mut text,
            &format!("Telegram location: {latitude:.6}, {longitude:.6}"),
        );
    }
    if let Some(emoji) = raw.pointer("/sticker/emoji").and_then(Value::as_str) {
        append_context(&mut text, &format!("Telegram sticker: {emoji}"));
    }
    if text.is_empty() && files.is_empty() {
        return None;
    }
    if text.len() > hermes_gateway_core::messaging::runtime::MAX_TEXT {
        return None;
    }
    let body = if edited && !text.is_empty() && files.is_empty() {
        InboundBody::Control(Control::ReplaceQueued {
            event_id: destination.message_key(message_id),
            text,
        })
    } else if !edited
        && kind == ActorKind::Person
        && let Some(command) = command(&text, &bot.identity.username)
    {
        command
    } else {
        InboundBody::Message(Message::text(text))
    };
    Some(Incoming {
        event: InboundEvent {
            id: event_id.to_owned(),
            room,
            sender: Sender {
                id: sender.to_string(),
                roles: Default::default(),
            },
            reply_anchor: Some(message_id.to_string()),
            mentioned: addressed,
            body,
        },
        kind,
        message_id,
        reply_to_bot: raw
            .get("reply_to_message")
            .filter(|reply| {
                reply.pointer("/from/id").and_then(Value::as_i64) == Some(bot.identity.id)
            })
            .and_then(|reply| reply["message_id"].as_i64())
            .filter(|id| *id > 0),
        edited,
        files,
        album: raw["media_group_id"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 128)
            .map(str::to_owned),
    })
}

fn append_context(text: &mut String, context: &str) {
    if !text.is_empty() {
        text.push('\n');
    }
    text.push_str(context);
}

fn addressed_to_bot(
    text: &str,
    entities: Option<&Value>,
    raw: &Value,
    bot: &Bot,
    patterns: &RegexSet,
) -> bool {
    let mut requested = false;
    let mut other = false;
    for item in entities.and_then(Value::as_array).into_iter().flatten() {
        match item["type"].as_str() {
            Some("text_mention")
                if item.pointer("/user/is_bot").and_then(Value::as_bool) == Some(true) =>
            {
                if item.pointer("/user/id").and_then(Value::as_i64) == Some(bot.identity.id) {
                    requested = true;
                } else {
                    other = true;
                }
            }
            Some("mention" | "bot_command") => {
                let Some(entity) = entity_text(text, item) else {
                    continue;
                };
                let recipient = entity.rsplit_once('@').map(|(_, target)| target);
                if let Some(recipient) = recipient {
                    if recipient.eq_ignore_ascii_case(&bot.identity.username) {
                        requested = true;
                    } else if item["type"] == "bot_command"
                        || recipient.to_ascii_lowercase().ends_with("bot")
                    {
                        other = true;
                    }
                }
            }
            _ => {}
        }
    }
    !other
        && (requested
            || raw
                .pointer("/reply_to_message/from/id")
                .and_then(Value::as_i64)
                == Some(bot.identity.id)
            || patterns.is_match(text))
}

fn entity_text<'a>(text: &'a str, entity: &Value) -> Option<&'a str> {
    let start = usize::try_from(entity["offset"].as_u64()?).ok()?;
    let len = usize::try_from(entity["length"].as_u64()?).ok()?;
    let end = start.checked_add(len)?;
    let mut offset = 0;
    let mut first = None;
    for (byte, character) in text.char_indices() {
        if offset == start {
            first = Some(byte);
        }
        if offset == end {
            return text.get(first?..byte);
        }
        offset += character.len_utf16();
    }
    (offset == end).then(|| text.get(first?..)).flatten()
}

fn command_parts<'a>(text: &'a str, username: &str) -> Option<(&'a str, &'a str)> {
    let mut words = text.strip_prefix('/')?.splitn(2, char::is_whitespace);
    let addressed = words.next()?;
    let name = match addressed.split_once('@') {
        Some((_, target)) if !target.eq_ignore_ascii_case(username) => return None,
        Some((name, _)) => name,
        None => addressed,
    };
    let argument = words.next().unwrap_or_default().trim();
    Some((name, argument))
}

fn command(text: &str, username: &str) -> Option<InboundBody> {
    let (name, argument) = command_parts(text, username)?;
    let command = match name {
        "status" => Command::Status,
        "queue" => Command::Queue,
        "history" => Command::History,
        "new" | "reset" => Command::Reset,
        "model" if argument.is_empty() => Command::ModelPicker,
        "model" => Command::SelectModel {
            model: argument.to_owned(),
        },
        "sethome" => Command::SetHome,
        "bg" if !argument.is_empty() => Command::Background {
            text: argument.to_owned(),
        },
        "skill" => {
            let (skill, input) = argument.split_once(char::is_whitespace)?;
            Command::UseSkill {
                skill: skill.to_owned(),
                input: input.trim().to_owned(),
            }
        }
        "voice" => Command::SpokenReplies {
            change: match argument {
                "on" => SpokenReplyChange::Set(SpokenReplyMode::Always),
                "off" => SpokenReplyChange::Set(SpokenReplyMode::Off),
                "notes" => SpokenReplyChange::Set(SpokenReplyMode::VoiceNotes),
                "toggle" => SpokenReplyChange::Toggle,
                _ => SpokenReplyChange::Status,
            },
        },
        _ => return None,
    };
    Some(InboundBody::Command(command))
}

pub(super) enum BusyControl {
    Interrupt,
    Steer(String),
    Redirect(String),
}

impl BusyControl {
    pub fn against(self, execution_generation: u64) -> Control {
        match self {
            Self::Interrupt => Control::Interrupt {
                execution_generation,
            },
            Self::Steer(text) => Control::Steer {
                execution_generation,
                text,
            },
            Self::Redirect(text) => Control::Redirect {
                execution_generation,
                text,
            },
        }
    }
}

pub(super) fn busy_control(text: &str, username: &str) -> Option<BusyControl> {
    let (name, argument) = command_parts(text, username)?;
    Some(match name {
        "stop" => BusyControl::Interrupt,
        "steer" if !argument.is_empty() => BusyControl::Steer(argument.to_owned()),
        "redirect" if !argument.is_empty() => BusyControl::Redirect(argument.to_owned()),
        _ => return None,
    })
}

pub(super) fn other_command(raw: &Value, bot: &Bot) -> bool {
    raw["text"]
        .as_str()
        .and_then(|text| text.split_whitespace().next())
        .and_then(|word| word.strip_prefix('/'))
        .and_then(|word| word.split_once('@'))
        .is_some_and(|(_, name)| !name.eq_ignore_ascii_case(&bot.identity.username))
}

fn files(message: &Value) -> Vec<File> {
    let mut files = Vec::new();
    for (kind, fallback) in [
        ("document", "document.bin"),
        ("audio", "audio.bin"),
        ("video", "video.mp4"),
        ("voice", "voice.ogg"),
        ("animation", "animation.mp4"),
        ("video_note", "video-note.mp4"),
        ("sticker", "sticker.webp"),
    ] {
        if let Some(file) = message
            .get(kind)
            .and_then(|file| read_file(file, fallback, kind == "voice"))
        {
            files.push(file);
        }
    }
    if let Some(photo) = message["photo"].as_array().and_then(|photos| {
        photos
            .iter()
            .rev()
            .find_map(|photo| read_file(photo, "photo.jpg", false))
    }) {
        files.push(photo);
    }
    files
}

fn read_file(raw: &Value, fallback: &str, voice: bool) -> Option<File> {
    let id = raw["file_id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 1024 && !id.chars().any(char::is_control))?;
    let name = raw["file_name"]
        .as_str()
        .filter(|name| {
            !name.is_empty()
                && !name.contains(['/', '\\'])
                && !name.chars().any(char::is_control)
                && name.len() <= 255
        })
        .unwrap_or(fallback);
    Some(File {
        id: id.to_owned(),
        name: name.to_owned(),
        size: raw["file_size"]
            .as_u64()
            .and_then(|n| usize::try_from(n).ok()),
        voice,
    })
}
