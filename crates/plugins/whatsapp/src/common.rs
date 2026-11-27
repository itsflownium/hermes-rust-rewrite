use hermes_gateway_core::messaging::{
    AccessPolicy, ActorKind, Command, GatewayError, Ingress, MemberEvidence, Room, RoomKind,
    Sender, SpokenReplyChange, SpokenReplyMode,
    runtime::{db, now},
};
use reqwest::Url;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeSet,
    sync::{Mutex, MutexGuard},
};

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_whatsapp_aliases (
    connection TEXT NOT NULL, provider TEXT NOT NULL, actor TEXT NOT NULL,
    PRIMARY KEY(connection,provider)
);
";

pub(crate) const EVIDENCE_MS: i64 = 30 * 60 * 1000;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn strict_policy(mut policy: AccessPolicy) -> AccessPolicy {
    policy.scope.require_actor_evidence = true;
    policy
}

pub(crate) fn digits(value: &str) -> bool {
    (5..=24).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_digit())
}

pub(crate) fn bsuid(value: &str) -> bool {
    let Some((prefix, account)) = value.split_once('.') else {
        return false;
    };
    prefix.len() == 2
        && prefix.bytes().all(|b| b.is_ascii_uppercase())
        && (4..=96).contains(&account.len())
        && account.bytes().all(|b| b.is_ascii_alphanumeric())
}

pub(crate) fn wamid(value: &str) -> bool {
    value.starts_with("wamid.")
        && (8..=256).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._+-/=".contains(&b))
}

pub(crate) fn jid(value: &str) -> bool {
    let Some((user, server)) = value.split_once('@') else {
        return false;
    };
    match server {
        "s.whatsapp.net" | "lid" => digits(user),
        "g.us" => {
            (5..=44).contains(&user.len()) && user.bytes().all(|b| b.is_ascii_digit() || b == b'-')
        }
        _ => false,
    }
}

pub(crate) fn text_id(value: &str) -> bool {
    (1..=180).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

pub(crate) fn provider_millis(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())?
        .checked_mul(1000)
        .filter(|v| *v > 0)
}

pub(crate) fn room(chat: &str, group: bool) -> Room {
    Room {
        kind: if group {
            RoomKind::Group
        } else {
            RoomKind::Direct
        },
        chat: chat.into(),
        thread: None,
    }
}

pub(crate) fn sender(id: &str) -> Sender {
    Sender {
        id: id.into(),
        roles: BTreeSet::new(),
    }
}

pub(crate) fn is_loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    })
}

/// Seal a verified identity association without moving an earlier route or
/// silently combining accounts whose history has already diverged.
pub(crate) fn bind_aliases(
    ingress: &Ingress,
    first: &str,
    second: Option<&str>,
) -> Result<String, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().write(|tx| {
            let prior = |id: &str| {
                tx.query_row(
            "SELECT actor FROM messaging_whatsapp_aliases WHERE connection=?1 AND provider=?2",
            params![ingress.connection_name(), id], |row| row.get::<_,String>(0),
        ).optional()
            };
            let left = prior(first)?;
            let right = second.map(prior).transpose()?.flatten();
            if let (Some(left), Some(right)) = (&left, &right)
                && left != right
            {
                return Ok(Err(GatewayError::Conflict));
            }
            let actor = left.or(right).unwrap_or_else(|| first.to_owned());
            for provider in std::iter::once(first).chain(second) {
                tx.execute(
                    "INSERT OR IGNORE INTO messaging_whatsapp_aliases VALUES(?1,?2,?3)",
                    params![ingress.connection_name(), provider, actor],
                )?;
            }
            Ok(Ok(actor))
        }))?
    })
}

pub(crate) fn lookup_alias(
    ingress: &Ingress,
    provider: &str,
) -> Result<Option<String>, GatewayError> {
    ingress.checked(true, |_| {
        db(ingress.storage().read(|conn| {
            Ok(conn.query_row(
            "SELECT actor FROM messaging_whatsapp_aliases WHERE connection=?1 AND provider=?2",
            params![ingress.connection_name(),provider], |row| row.get(0),
        ).optional()?)
        }))
    })
}

pub(crate) async fn observe(
    ingress: &Ingress,
    room: &Room,
    actor: &str,
) -> Result<(), GatewayError> {
    ingress
        .observe_membership(
            actor.into(),
            ActorKind::Person,
            MemberEvidence {
                room: room.clone(),
                roles: BTreeSet::new(),
            },
            now().saturating_add(EVIDENCE_MS),
        )
        .await
}

pub(crate) fn permits(ingress: &Ingress, room: &Room, actor: &str) -> Result<bool, GatewayError> {
    ingress.checked(true, |state| {
        ingress.authorize_sender(&state.policy, room, &sender(actor))
    })
}

pub(crate) fn action(text: &str) -> Option<Command> {
    let (name, argument) = text
        .strip_prefix('/')?
        .split_once(char::is_whitespace)
        .unwrap_or((&text[1..], ""));
    let argument = argument.trim();
    Some(match name {
        "status" => Command::Status,
        "queue" => Command::Queue,
        "history" => Command::History,
        "reset" | "new" => Command::Reset,
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
                "on" => SpokenReplyChange::Set(SpokenReplyMode::Always),
                "off" => SpokenReplyChange::Set(SpokenReplyMode::Off),
                "notes" => SpokenReplyChange::Set(SpokenReplyMode::VoiceNotes),
                _ => SpokenReplyChange::Status,
            },
        },
        _ => return None,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MediaKind {
    Image,
    Video,
    Audio,
    Document,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct Media {
    pub id: String,
    pub kind: MediaKind,
    pub voice: bool,
    pub name: Option<String>,
    pub locator: Option<Value>,
}
