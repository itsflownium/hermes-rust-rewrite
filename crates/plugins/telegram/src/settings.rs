use super::{Factory, address::Destination};
use anyhow::{Context, Result, ensure};
use hermes_gateway_core::messaging::{AccessPolicy, Room, StockConfiguration};
use hermes_telegram::telegram::Endpoints;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum IntakeMode {
    #[default]
    Polling,
    Webhook {
        public_origin: String,
    },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    pub bot_id: Option<i64>,
    pub api_origin: Option<String>,
    pub file_origin: Option<String>,
    pub proxy: Option<String>,
    pub local_files: Option<PathBuf>,
    pub intake: IntakeMode,
    pub access: AccessPolicy,
    pub wake_patterns: Vec<String>,
    pub home: Option<Room>,
    pub sync_commands: bool,
    pub typed_clarification: bool,
    pub drafts: bool,
    pub progress_messages: bool,
    pub rich_messages: bool,
}

impl Settings {
    pub(super) fn endpoints(&self) -> Result<Endpoints> {
        let defaults = Endpoints::default();
        let api = self
            .api_origin
            .as_deref()
            .map(Url::parse)
            .transpose()
            .context("Invalid Telegram API origin")?
            .unwrap_or(defaults.api);
        let files = self
            .file_origin
            .as_deref()
            .map(Url::parse)
            .transpose()
            .context("Invalid Telegram file origin")?
            .unwrap_or_else(|| api.clone());
        let proxy = self
            .proxy
            .as_deref()
            .map(Url::parse)
            .transpose()
            .context("Invalid Telegram proxy origin")?;
        let endpoints = Endpoints { api, files, proxy };
        endpoints.validate()?;
        Ok(endpoints)
    }

    pub(super) fn webhook_origin(&self) -> Result<Option<Url>> {
        let IntakeMode::Webhook { public_origin } = &self.intake else {
            return Ok(None);
        };
        let url = Url::parse(public_origin).context("Invalid Telegram webhook origin")?;
        ensure!(
            url.scheme() == "https"
                && url.host().is_some()
                && url.path() == "/"
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
                && matches!(url.port_or_known_default(), Some(443 | 80 | 88 | 8443)),
            "Telegram webhook mode requires a public HTTPS origin"
        );
        Ok(Some(url))
    }

    fn patterns(&self) -> Result<RegexSet> {
        ensure!(
            self.wake_patterns.len() <= 32 && self.wake_patterns.iter().all(|s| s.len() <= 512),
            "Too many or overly long Telegram wake patterns"
        );
        Ok(RegexSetBuilder::new(&self.wake_patterns)
            .size_limit(512 * 1024)
            .build()?)
    }

    fn policy(&self) -> AccessPolicy {
        let mut policy = self.access.clone();
        policy.scope.require_actor_evidence = true;
        policy
    }

    fn validate(&self) -> Result<()> {
        self.endpoints()?;
        self.webhook_origin()?;
        ensure!(
            self.bot_id.is_some_and(super::address::actor_id),
            "Telegram setup requires the numeric bot ID"
        );
        ensure!(
            self.local_files
                .as_ref()
                .is_none_or(|path| path.is_absolute()),
            "Telegram file root must be an absolute path"
        );
        if let Some(home) = &self.home {
            Destination::from_room(home)
                .filter(|destination| destination.topic.is_none())
                .context("Use /sethome in a Telegram topic to select it as home")?;
        }
        Ok(())
    }
}

pub(super) fn configure(root: &Path, settings: Value) -> Result<StockConfiguration> {
    let settings: Settings = serde_json::from_value(settings)?;
    settings.validate()?;
    let policy = settings.policy();
    let document = serde_json::to_value(&settings)?;
    let patterns = settings.patterns()?;
    Ok(StockConfiguration {
        settings: document,
        policy,
        factory: Arc::new(Factory {
            root: root.to_owned(),
            settings,
            patterns,
            verification: Arc::new(Mutex::new(Value::Null)),
        }),
    })
}

pub(super) fn replace_credential(previous: &Value, next: &Value) -> bool {
    next["bot_id"].as_i64().is_some() && previous["bot_id"] != next["bot_id"]
}
