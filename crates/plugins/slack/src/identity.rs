use hermes_gateway_core::messaging::{Room, RoomKind, VerifiedInstallation};
use serde_json::Value;

pub(crate) fn id(value: &str, prefixes: &[u8]) -> bool {
    (2..=64).contains(&value.len())
        && prefixes.contains(&value.as_bytes()[0])
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
}

pub(crate) fn timestamp(value: &str) -> bool {
    value.split_once('.').is_some_and(|(seconds, fraction)| {
        (1..=20).contains(&seconds.len())
            && fraction.len() == 6
            && seconds.bytes().all(|byte| byte.is_ascii_digit())
            && fraction.bytes().all(|byte| byte.is_ascii_digit())
    })
}

pub(crate) fn channel(value: &str, thread: Option<&str>) -> Option<Room> {
    (id(value, b"CDG") && thread.is_none_or(timestamp)).then(|| Room {
        kind: if value.starts_with('D') {
            RoomKind::Direct
        } else {
            RoomKind::Group
        },
        chat: value.to_owned(),
        thread: thread.map(str::to_owned),
    })
}

pub(crate) fn room(value: &Room) -> bool {
    channel(&value.chat, value.thread.as_deref()).is_some_and(|other| other == *value)
}

#[derive(Clone)]
pub(crate) struct Identity {
    pub app: String,
    pub team: String,
    pub enterprise: Option<String>,
    pub bot: String,
    pub user: String,
}

impl Identity {
    pub fn receiver(&self) -> String {
        format!("slack:{}", self.app)
    }

    pub fn proof(&self) -> VerifiedInstallation {
        VerifiedInstallation {
            installation: self.bot.clone(),
            tenant: Some(self.team.clone()),
            application: Some(self.app.clone()),
            receiver: Some(self.receiver()),
            origin: None,
            device: None,
            local_client: None,
        }
    }
}

pub(crate) fn string<'a>(value: &'a Value, field: &str, prefixes: &[u8]) -> Option<&'a str> {
    value[field].as_str().filter(|value| id(value, prefixes))
}

pub(crate) fn ts<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value[field].as_str().filter(|value| timestamp(value))
}
