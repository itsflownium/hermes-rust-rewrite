use hermes_gateway_core::messaging::{Room, RoomKind};
use serde_json::{Map, Value, json};

const MAX_ID: i64 = (1 << 52) - 1;

pub(super) fn actor_id(id: i64) -> bool {
    (1..=MAX_ID).contains(&id)
}

pub(super) fn chat_id(id: i64) -> bool {
    id != 0 && (-MAX_ID..=MAX_ID).contains(&id)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Topic {
    Thread(i64),
    ChannelDirect(i64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Destination {
    pub chat: i64,
    pub topic: Option<Topic>,
}

fn topic_id(id: i64) -> bool {
    (1..=i32::MAX as i64).contains(&id)
}

impl Destination {
    pub fn from_message(message: &Value) -> Option<(Self, RoomKind)> {
        let chat = message
            .pointer("/chat/id")?
            .as_i64()
            .filter(|id| chat_id(*id))?;
        let chat_kind = message.pointer("/chat/type")?.as_str()?;
        let kind = match chat_kind {
            "private" if chat > 0 => RoomKind::Direct,
            "group" | "supergroup" | "channel" if chat < 0 => RoomKind::Group,
            _ => return None,
        };
        let direct_topic = message.get("direct_messages_topic");
        let thread = message.get("message_thread_id");
        let topic = if let Some(topic) = direct_topic {
            if thread.is_some() || kind != RoomKind::Group {
                return None;
            }
            Some(Topic::ChannelDirect(
                topic["topic_id"].as_i64().filter(|id| topic_id(*id))?,
            ))
        } else if let Some(thread) = thread {
            Some(Topic::Thread(thread.as_i64().filter(|id| topic_id(*id))?))
        } else if message.pointer("/chat/is_forum").and_then(Value::as_bool) == Some(true)
            && kind == RoomKind::Group
        {
            Some(Topic::Thread(1))
        } else {
            None
        };
        Some((Self { chat, topic }, kind))
    }

    pub fn from_room(room: &Room) -> Option<Self> {
        let chat = room.chat.parse::<i64>().ok().filter(|id| chat_id(*id))?;
        if chat.to_string() != room.chat || (chat > 0) != (room.kind == RoomKind::Direct) {
            return None;
        }
        let topic = match room.thread.as_deref() {
            None => None,
            Some(thread) => {
                let (kind, raw) = thread.split_once(':')?;
                let id = raw.parse::<i64>().ok().filter(|id| topic_id(*id))?;
                if id.to_string() != raw {
                    return None;
                }
                Some(match kind {
                    "thread" => Topic::Thread(id),
                    "direct-topic" if chat < 0 => Topic::ChannelDirect(id),
                    _ => return None,
                })
            }
        };
        Some(Self { chat, topic })
    }

    pub fn room(self, kind: RoomKind) -> Room {
        Room {
            kind,
            chat: self.chat.to_string(),
            thread: self.topic.map(|topic| match topic {
                Topic::Thread(id) => format!("thread:{id}"),
                Topic::ChannelDirect(id) => format!("direct-topic:{id}"),
            }),
        }
    }

    pub fn send_fields(self) -> Map<String, Value> {
        let mut fields = Map::from_iter([("chat_id".into(), json!(self.chat))]);
        match self.topic {
            Some(Topic::Thread(1)) if self.chat < 0 => {}
            Some(Topic::Thread(id)) => {
                fields.insert("message_thread_id".into(), json!(id));
            }
            Some(Topic::ChannelDirect(id)) => {
                fields.insert("direct_messages_topic_id".into(), json!(id));
            }
            None => {}
        }
        fields
    }

    pub fn matches_message(self, value: &Value) -> bool {
        Self::from_message(value).is_some_and(|(dest, _)| dest == self)
            || (self.topic == Some(Topic::Thread(1))
                && self.chat < 0
                && Self::from_message(value)
                    .is_some_and(|(dest, _)| dest.chat == self.chat && dest.topic.is_none()))
    }

    pub fn message_key(self, message: i64) -> String {
        format!("{}:{message}", self.chat)
    }
}
