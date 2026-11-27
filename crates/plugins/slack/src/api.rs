use crate::{
    identity::{Identity, string},
    settings::{Settings, loopback},
};
use futures_util::TryStreamExt;
use hermes_gateway_core::messaging::{
    ConnectionFailure, GatewayError, Ingress,
    runtime::{db, now},
};
use reqwest::{Client, Response, StatusCode, Url, header};
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, io, pin::Pin, time::Duration};
use tokio::io::AsyncRead;
use tokio_util::{io::StreamReader, sync::CancellationToken};

const MAX_JSON: usize = 2 * 1024 * 1024;

pub(crate) const TABLES: &str = "
CREATE TABLE IF NOT EXISTS messaging_slack_method_retry (
    connection TEXT NOT NULL, method TEXT NOT NULL, until_ms INTEGER NOT NULL,
    PRIMARY KEY(connection,method)
);
";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Failure {
    Credentials,
    Permission,
    Configuration,
    Rejected,
    RateLimited(Duration),
    Unknown,
    Cancelled,
}

impl Failure {
    pub fn connection(self) -> ConnectionFailure {
        match self {
            Self::Credentials => ConnectionFailure::Credentials,
            Self::Permission | Self::Rejected | Self::Configuration => {
                ConnectionFailure::Configuration
            }
            Self::RateLimited(_) | Self::Unknown => ConnectionFailure::Retryable,
            Self::Cancelled => ConnectionFailure::Stopped,
        }
    }

    pub fn read(self) -> GatewayError {
        match self {
            Self::Credentials | Self::Permission => GatewayError::Forbidden,
            Self::Rejected | Self::Configuration => GatewayError::Invalid,
            Self::RateLimited(_) | Self::Unknown => GatewayError::TemporarilyUnavailable,
            Self::Cancelled => GatewayError::StaleConnection,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credential {
    bot_token: String,
    app_token: Option<String>,
}

impl Credential {
    pub fn parse(input: &str, receiver: bool) -> Result<Self, Failure> {
        let bundle: Self = serde_json::from_str(input).map_err(|_| Failure::Credentials)?;
        if !valid_token(&bundle.bot_token, &["xoxb-", "xoxe.xoxb-"])
            || receiver != bundle.app_token.is_some()
            || bundle
                .app_token
                .as_deref()
                .is_some_and(|secret| !valid_token(secret, &["xapp-"]))
        {
            return Err(Failure::Credentials);
        }
        Ok(bundle)
    }
}

fn valid_token(value: &str, prefixes: &[&str]) -> bool {
    value.len() <= 4_096
        && prefixes.iter().any(|prefix| {
            value.strip_prefix(prefix).is_some_and(|secret| {
                secret.len() >= 16
                    && secret
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
        })
}

pub(crate) type Stream = Pin<Box<dyn AsyncRead + Send>>;

pub(crate) struct Api {
    http: Client,
    base: Url,
    credential: Credential,
    credential_binding: String,
}

impl Api {
    pub fn new(settings: &Settings, credential: Credential) -> Result<Self, Failure> {
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(|_| Failure::Configuration)?;
        let base = settings.api_origin().map_err(|_| Failure::Configuration)?;
        let credential_binding = hermes_gateway_core::messaging::storage::key(
            "slack-bot-credential",
            &credential.bot_token,
        )
        .map_err(|_| Failure::Configuration)?;
        Ok(Self {
            http,
            base,
            credential,
            credential_binding,
        })
    }

    pub fn credential_binding(&self) -> &str {
        &self.credential_binding
    }

    pub async fn identify(
        &self,
        settings: &Settings,
        cancel: &CancellationToken,
    ) -> Result<Identity, Failure> {
        let (auth, scopes) = self
            .call_with("auth.test", json!({}), &self.credential.bot_token, cancel)
            .await?;
        let bot = string(&auth, "bot_id", b"B").ok_or(Failure::Credentials)?;
        let user = string(&auth, "user_id", b"UW").ok_or(Failure::Credentials)?;
        let team = string(&auth, "team_id", b"T").ok_or(Failure::Credentials)?;
        let enterprise = string(&auth, "enterprise_id", b"E");
        if team != settings.team_id
            || bot != settings.bot_id
            || settings
                .enterprise_id
                .as_deref()
                .is_some_and(|expected| Some(expected) != enterprise)
        {
            return Err(Failure::Credentials);
        }
        if let Some(scopes) = scopes
            && settings
                .scopes()
                .iter()
                .any(|scope| !scopes.contains(*scope))
        {
            return Err(Failure::Permission);
        }
        let metadata = self
            .call("bots.info", json!({"bot":bot,"team_id":team}), cancel)
            .await?;
        let metadata = &metadata["bot"];
        let app = string(metadata, "app_id", b"A").ok_or(Failure::Credentials)?;
        if app != settings.app_id
            || string(metadata, "id", b"B") != Some(bot)
            || string(metadata, "user_id", b"UW").is_some_and(|value| value != user)
            || metadata["deleted"] == true
        {
            return Err(Failure::Credentials);
        }
        Ok(Identity {
            app: app.into(),
            bot: bot.into(),
            user: user.into(),
            team: team.into(),
            enterprise: enterprise.map(str::to_owned),
        })
    }

    pub async fn socket_url(&self, cancel: &CancellationToken) -> Result<Url, Failure> {
        let token = self
            .credential
            .app_token
            .as_deref()
            .ok_or(Failure::Credentials)?;
        let (response, _) = self
            .call_with("apps.connections.open", json!({}), token, cancel)
            .await?;
        let endpoint = response["url"].as_str().ok_or(Failure::Unknown)?;
        self.endpoint(endpoint, Endpoint::Socket)
    }

    pub async fn call(
        &self,
        method: &str,
        body: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, Failure> {
        Ok(self
            .call_with(method, body, &self.credential.bot_token, cancel)
            .await?
            .0)
    }

    /// A Slack workspace method cooldown survives receiver or owner restart.
    pub async fn scoped(
        &self,
        ingress: &Ingress,
        method: &str,
        body: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, Failure> {
        let until = ingress.checked(true, |_| db(ingress.storage().read(|conn| {
            Ok(conn.query_row("SELECT until_ms FROM messaging_slack_method_retry WHERE connection=?1 AND method=?2",
                params![ingress.connection_name(),method], |row| row.get::<_,i64>(0)).optional()?)
        }))).map_err(|error| if error == GatewayError::StaleConnection { Failure::Cancelled } else { Failure::Unknown })?;
        if let Some(until) = until.filter(|until| *until > now()) {
            return Err(Failure::RateLimited(Duration::from_millis(
                until.saturating_sub(now()).max(1) as u64,
            )));
        }
        let result = self.call(method, body, cancel).await;
        if let Err(Failure::RateLimited(delay)) = result {
            let deadline =
                now().saturating_add(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
            let _ = ingress.checked(true, |_| db(ingress.storage().write(|tx| {
                tx.execute("INSERT INTO messaging_slack_method_retry VALUES(?1,?2,?3)
                    ON CONFLICT(connection,method) DO UPDATE SET until_ms=max(until_ms,excluded.until_ms)",
                    params![ingress.connection_name(),method,deadline])?;
                Ok(())
            })));
        }
        result
    }

    async fn call_with(
        &self,
        method: &str,
        body: Value,
        token: &str,
        cancel: &CancellationToken,
    ) -> Result<(Value, Option<BTreeSet<String>>), Failure> {
        let url = self.base.join(method).map_err(|_| Failure::Configuration)?;
        let request = self.http.post(url).bearer_auth(token);
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Failure::Cancelled),
            result = async {
                let request = if body.as_object().is_some_and(|fields| fields.is_empty()) {
                    request
                } else if matches!(method, "bots.info" | "conversations.info" | "conversations.members"
                    | "conversations.replies" | "files.info" | "files.getUploadURLExternal"
                    | "users.info" | "reactions.get")
                {
                    let fields = body.as_object().ok_or(Failure::Configuration)?;
                    let params = fields.iter().map(|(key, value)| {
                        (key, value.as_str().map_or_else(|| value.to_string(), str::to_owned))
                    }).collect::<Vec<_>>();
                    request.form(&params)
                } else {
                    request.json(&body)
                };
                let response = request.send().await.map_err(|_| Failure::Unknown)?;
                decode(response).await
            } => result,
        };
        result
    }

    pub async fn download(
        &self,
        endpoint: &str,
        cancel: &CancellationToken,
    ) -> Result<Stream, Failure> {
        let url = self.endpoint(endpoint, Endpoint::Download)?;
        let request = self
            .http
            .get(url)
            .bearer_auth(&self.credential.bot_token)
            .send();
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(Failure::Cancelled),
            result = request => result.map_err(|_| Failure::Unknown)?,
        };
        check_http(&response)?;
        let stream = response
            .bytes_stream()
            .map_err(|_| io::Error::other("Slack file data unavailable"));
        Ok(Box::pin(StreamReader::new(stream)))
    }

    pub async fn transfer(
        &self,
        endpoint: &str,
        bytes: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        let url = self.endpoint(endpoint, Endpoint::Upload)?;
        let request = self
            .http
            .post(url)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .send();
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(Failure::Cancelled),
            result = request => result.map_err(|_| Failure::Unknown)?,
        };
        check_http(&response)
    }

    pub fn validate_upload(&self, endpoint: &str) -> Result<(), Failure> {
        self.endpoint(endpoint, Endpoint::Upload).map(drop)
    }

    pub fn validate_response(&self, endpoint: &str) -> Result<(), Failure> {
        self.endpoint(endpoint, Endpoint::Response).map(drop)
    }

    pub async fn private_response(
        &self,
        endpoint: &str,
        body: Value,
        cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        let url = self.endpoint(endpoint, Endpoint::Response)?;
        let request = self.http.post(url).json(&body).send();
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(Failure::Cancelled),
            result = request => result.map_err(|_| Failure::Unknown)?,
        };
        match response.status() {
            StatusCode::BAD_REQUEST | StatusCode::GONE => Err(Failure::Rejected),
            _ => check_http(&response),
        }
    }

    fn endpoint(&self, value: &str, kind: Endpoint) -> Result<Url, Failure> {
        if value.len() > 8_192 {
            return Err(Failure::Configuration);
        }
        let url = Url::parse(value).map_err(|_| Failure::Configuration)?;
        let host = url.host_str().ok_or(Failure::Configuration)?;
        let safe_authority =
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none();
        let local = loopback(&self.base)
            && url.host() == self.base.host()
            && url.port_or_known_default() == self.base.port_or_known_default();
        let allowed = match kind {
            Endpoint::Socket => {
                (local && url.scheme() == "ws")
                    || (url.scheme() == "wss"
                        && url.port_or_known_default() == Some(443)
                        && (host == "wss.slack.com"
                            || host.starts_with("wss-") && host.ends_with(".slack.com")))
            }
            Endpoint::Download => {
                (local && url.scheme() == "http")
                    || (url.scheme() == "https"
                        && url.port_or_known_default() == Some(443)
                        && host == "files.slack.com")
            }
            Endpoint::Upload => {
                (local && url.scheme() == "http")
                    || (url.scheme() == "https"
                        && url.port_or_known_default() == Some(443)
                        && host == "files.slack.com"
                        && url.path().starts_with("/upload/"))
            }
            Endpoint::Response => {
                url.path().starts_with("/actions/")
                    && ((local && url.scheme() == "http")
                        || (url.scheme() == "https"
                            && url.port_or_known_default() == Some(443)
                            && host == "hooks.slack.com"))
            }
        };
        (safe_authority && allowed)
            .then_some(url)
            .ok_or(Failure::Configuration)
    }
}

#[derive(Clone, Copy)]
enum Endpoint {
    Socket,
    Download,
    Upload,
    Response,
}

async fn decode(mut response: Response) -> Result<(Value, Option<BTreeSet<String>>), Failure> {
    check_http(&response)?;
    let scopes = response
        .headers()
        .get("x-oauth-scopes")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|scope| !scope.is_empty())
                .map(str::to_owned)
                .collect()
        });
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Failure::Unknown)? {
        if bytes.len() + chunk.len() > MAX_JSON {
            return Err(Failure::Unknown);
        }
        bytes.extend_from_slice(&chunk);
    }
    let body: Value = serde_json::from_slice(&bytes).map_err(|_| Failure::Unknown)?;
    match body["ok"].as_bool() {
        Some(true) => Ok((body, scopes)),
        Some(false) => Err(match body["error"].as_str() {
            Some(
                "invalid_auth"
                | "not_authed"
                | "token_revoked"
                | "account_inactive"
                | "not_allowed_token_type",
            ) => Failure::Credentials,
            Some("missing_scope" | "no_permission" | "access_denied" | "not_in_channel") => {
                Failure::Permission
            }
            Some("ratelimited") => Failure::RateLimited(Duration::from_secs(30)),
            Some(
                "invalid_arguments"
                | "bot_not_found"
                | "user_not_found"
                | "no_item"
                | "channel_not_found"
                | "message_not_found"
                | "file_not_found"
                | "is_archived"
                | "msg_too_long"
                | "thread_not_found"
                | "invalid_blocks"
                | "invalid_ts"
                | "not_supported"
                | "method_not_supported_for_channel_type"
                | "channel_type_not_supported"
                | "cannot_reply_to_message",
            ) => Failure::Rejected,
            _ => Failure::Unknown,
        }),
        None => Err(Failure::Unknown),
    }
}

fn check_http(response: &Response) -> Result<(), Failure> {
    match response.status() {
        StatusCode::TOO_MANY_REQUESTS => {
            let seconds = response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(30)
                .max(1);
            Err(Failure::RateLimited(Duration::from_secs(seconds)))
        }
        StatusCode::UNAUTHORIZED => Err(Failure::Credentials),
        StatusCode::FORBIDDEN => Err(Failure::Permission),
        StatusCode::NOT_FOUND => Err(Failure::Rejected),
        status if status.is_success() => Ok(()),
        _ => Err(Failure::Unknown),
    }
}
