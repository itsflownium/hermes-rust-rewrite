use crate::{
    Factory,
    identity::{id, room},
};
use anyhow::{Context, Result, ensure};
use hermes_gateway_core::messaging::{AccessPolicy, Room, StockConfiguration};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub app_id: String,
    pub team_id: String,
    pub bot_id: String,
    pub enterprise_id: Option<String>,
    pub receiver_owner: bool,
    pub api_origin: Option<String>,
    pub access: AccessPolicy,
    pub flat_channels: BTreeSet<String>,
    pub continue_threads: bool,
    pub strict_threads: BTreeSet<String>,
    #[serde(default = "enabled_by_default")]
    pub thread_history: bool,
    pub shared_dm_history: bool,
    pub files: bool,
    #[serde(default = "enabled_by_default")]
    pub agent: bool,
    pub native_streams: bool,
    pub link_unfurls: bool,
    pub media_unfurls: bool,
    pub block_kit: bool,
    pub feedback_buttons: bool,
    pub typed_clarification: bool,
    pub peer_bots: BTreeMap<String, PeerBotMode>,
    pub broadcast_channels: BTreeSet<String>,
    pub home: Option<Room>,
    pub home_member: Option<String>,
    pub reaction_triggers: BTreeMap<String, ReactionTrigger>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerBotMode {
    Mention,
    Always,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReactionTrigger {
    pub source: Room,
    pub destination: Room,
    pub prompt: String,
}

impl Settings {
    pub(crate) fn api_origin(&self) -> Result<Url> {
        let Some(origin) = &self.api_origin else {
            return Ok(Url::parse("https://slack.com/api/").expect("Slack Web API URL"));
        };
        let url = Url::parse(origin).context("Invalid Slack fixture origin")?;
        ensure!(
            url.scheme() == "http"
                && loopback(&url)
                && url.path() == "/"
                && url.port().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "A custom Slack API origin must be an explicit HTTP loopback listener"
        );
        Ok(url.join("api/")?)
    }

    fn policy(&self) -> AccessPolicy {
        let mut policy = self.access.clone();
        policy.scope.require_actor_evidence = true;
        let rooms: BTreeSet<_> = policy
            .rooms
            .keys()
            .chain(policy.scope.rooms.keys())
            .chain(policy.scope.threads.keys())
            .filter(|room| id(room, b"CG"))
            .cloned()
            .collect();
        for room in rooms {
            policy
                .scope
                .rooms
                .entry(room)
                .or_default()
                .mention_required
                .get_or_insert(true);
        }
        policy
    }

    fn validate(&self) -> Result<()> {
        self.api_origin()?;
        ensure!(
            id(&self.app_id, b"A") && id(&self.team_id, b"T"),
            "Slack requires an app and workspace ID"
        );
        ensure!(
            id(&self.bot_id, b"B"),
            "Slack requires the installed bot ID"
        );
        ensure!(
            self.enterprise_id
                .as_deref()
                .is_none_or(|value| id(value, b"E")),
            "Invalid Slack enterprise ID"
        );
        ensure!(
            self.flat_channels.iter().all(|channel| id(channel, b"CG")),
            "Invalid Slack flat channel ID"
        );
        ensure!(
            self.strict_threads.iter().all(|channel| id(channel, b"CG")),
            "Invalid Slack strict thread channel ID"
        );
        ensure!(
            self.broadcast_channels
                .iter()
                .all(|channel| id(channel, b"CG")),
            "Invalid Slack broadcast channel ID"
        );
        ensure!(
            self.peer_bots
                .keys()
                .all(|bot| id(bot, b"B") && bot != &self.bot_id),
            "Invalid Slack peer bot ID"
        );
        ensure!(
            self.home.as_ref().is_none_or(room)
                && self
                    .home_member
                    .as_deref()
                    .is_none_or(|user| id(user, b"UW"))
                && !(self.home.is_some() && self.home_member.is_some()),
            "Invalid Slack home destination"
        );
        ensure!(
            self.reaction_triggers.iter().all(|(name, trigger)| {
                !name.is_empty()
                    && name.len() <= 80
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-+".contains(&byte))
                    && room(&trigger.source)
                    && room(&trigger.destination)
                    && !trigger.prompt.trim().is_empty()
                    && trigger.prompt.len() <= 4096
                    && !trigger.prompt.contains('\0')
            }),
            "Invalid Slack reaction trigger"
        );
        ensure!(
            !self.native_streams || self.agent,
            "Native streams require the Slack Agent view"
        );
        Ok(())
    }

    pub(crate) fn scopes(&self) -> Vec<&'static str> {
        let mut scopes = vec![
            "app_mentions:read",
            "channels:history",
            "channels:read",
            "chat:write",
            "commands",
            "groups:history",
            "groups:read",
            "im:history",
            "im:read",
            "mpim:history",
            "mpim:read",
            "users:read",
        ];
        if self.files {
            scopes.extend(["files:read", "files:write"]);
        }
        if self.agent {
            scopes.push("assistant:write");
        }
        if self.home_member.is_some() {
            scopes.push("im:write");
        }
        if !self.reaction_triggers.is_empty() {
            scopes.push("reactions:read");
        }
        scopes.sort_unstable();
        scopes
    }

    pub fn manifest(&self) -> Value {
        let mut events = vec![
            "app_mention",
            "app_uninstalled",
            "member_left_channel",
            "message.channels",
            "message.groups",
            "message.im",
            "message.mpim",
        ];
        if self.files {
            events.push("file_shared");
        }
        if !self.reaction_triggers.is_empty() {
            events.push("reaction_added");
        }
        if self.agent {
            events.extend(["app_context_changed", "agent_session_stopped"]);
        }
        let mut features = json!({
            "bot_user":{"display_name":"Mercury","always_online":false},
            "app_home":{"messages_tab_enabled":true,"messages_tab_read_only_enabled":false},
            "slash_commands":[
                {"command":"/mercury-status","description":"Check the Mercury connection"},
                {"command":"/mercury-help","description":"Show the Mercury commands available to you"},
            ],
        });
        if self.agent {
            features["agent_view"] =
                json!({"agent_description":"Work in Slack with the Mercury profile"});
        }
        if self.access.scope.pair_direct {
            features["slash_commands"].as_array_mut().expect("slash command list").push(
                json!({"command":"/mercury-pair","description":"Request access to this Mercury profile"})
            );
        }
        json!({
            "display_information":{"name":"Mercury"},
            "features":features,
            "oauth_config":{"scopes":{"bot":self.scopes()}},
            "settings":{
                "socket_mode_enabled":true,
                "event_subscriptions":{"bot_events":events},
                "interactivity":{"is_enabled":true},
                "org_deploy_enabled":self.enterprise_id.is_some(),
            },
        })
    }
}

fn enabled_by_default() -> bool {
    true
}

pub(crate) fn loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    })
}

pub(crate) fn configure(root: &Path, settings: Value) -> Result<StockConfiguration> {
    let settings: Settings = serde_json::from_value(settings)?;
    settings.validate()?;
    Ok(StockConfiguration {
        settings: serde_json::to_value(&settings)?,
        policy: settings.policy(),
        factory: Arc::new(Factory {
            root: root.to_path_buf(),
            verification: Arc::new(Mutex::new(json!({"manifest":settings.manifest()}))),
            settings,
        }),
    })
}

pub(crate) fn replace_credential(previous: &Value, next: &Value) -> bool {
    [
        "app_id",
        "team_id",
        "bot_id",
        "enterprise_id",
        "api_origin",
        "receiver_owner",
    ]
    .into_iter()
    .any(|field| previous[field] != next[field])
}
