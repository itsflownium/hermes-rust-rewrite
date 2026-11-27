//! Native Telegram Bot API transport. No Python process or gateway wrapper.
//! Protocol: https://core.telegram.org/bots/api#getupdates and #sendmessage.
use reqwest::{Response, Url, multipart};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::io::AsyncRead;
use tokio_util::{io::StreamReader, sync::CancellationToken};

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Token(String);
impl Token {
    pub fn parse(value: &str) -> Result<Self, Error> {
        let value = value.trim();
        let valid = value.len() <= 256
            && value.split_once(':').is_some_and(|(id, secret)| {
                !id.is_empty()
                    && id.bytes().all(|byte| byte.is_ascii_digit())
                    && secret.len() >= 30
                    && secret
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            });
        if !valid {
            return Err(Error::invalid(
                "Enter the complete bot token from @BotFather.",
            ));
        }
        Ok(Self(value.to_owned()))
    }
    fn value(&self) -> &str {
        &self.0
    }
    fn redact(&self, value: &str) -> String {
        value
            .replace(&self.0, "[redacted]")
            .chars()
            .take(300)
            .collect()
    }
}
impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TelegramToken([redacted])")
    }
}
impl<'de> Deserialize<'de> for Token {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value)
            .map_err(|_| serde::de::Error::custom("Invalid saved Telegram credential"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Invalid,
    Retryable,
    RateLimited,
    Conflict,
    Credentials,
    Cancelled,
    Rejected,
}
#[derive(Debug, Clone)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    pub retry_after: Option<u64>,
}
impl Error {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: ErrorKind::Invalid,
            message: message.into(),
            retry_after: None,
        }
    }
    fn cancelled() -> Self {
        Self {
            kind: ErrorKind::Cancelled,
            message: "Telegram connection stopped".into(),
            retry_after: None,
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy)]
pub enum Method {
    GetMe,
    GetWebhookInfo,
    GetUpdates,
    SendMessage,
    SetWebhook,
    DeleteWebhook,
    GetChat,
    GetChatMember,
    SetMyCommands,
    AnswerCallbackQuery,
    SendChatAction,
    EditMessageText,
    EditMessageReplyMarkup,
    SendMessageDraft,
    SendRichMessage,
    GetFile,
    SendDocument,
    SendVoice,
}
impl Method {
    pub fn name(self) -> &'static str {
        match self {
            Self::GetMe => "getMe",
            Self::GetWebhookInfo => "getWebhookInfo",
            Self::GetUpdates => "getUpdates",
            Self::SendMessage => "sendMessage",
            Self::SetWebhook => "setWebhook",
            Self::DeleteWebhook => "deleteWebhook",
            Self::GetChat => "getChat",
            Self::GetChatMember => "getChatMember",
            Self::SetMyCommands => "setMyCommands",
            Self::AnswerCallbackQuery => "answerCallbackQuery",
            Self::SendChatAction => "sendChatAction",
            Self::EditMessageText => "editMessageText",
            Self::EditMessageReplyMarkup => "editMessageReplyMarkup",
            Self::SendMessageDraft => "sendMessageDraft",
            Self::SendRichMessage => "sendRichMessage",
            Self::GetFile => "getFile",
            Self::SendDocument => "sendDocument",
            Self::SendVoice => "sendVoice",
        }
    }
}
pub type RequestFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, Error>> + Send + 'a>>;
/// Bot API and file transport. Profile configuration supplies nondefault origins.
pub struct Endpoints {
    pub api: Url,
    pub files: Url,
    pub proxy: Option<Url>,
}

impl Default for Endpoints {
    fn default() -> Self {
        let origin = Url::parse("https://api.telegram.org/").expect("Telegram API origin");
        Self {
            api: origin.clone(),
            files: origin,
            proxy: None,
        }
    }
}

impl Endpoints {
    pub fn validate(&self) -> Result<(), Error> {
        for origin in [&self.api, &self.files] {
            let loopback = origin.host().is_some_and(|host| match host {
                url::Host::Domain(host) => host.eq_ignore_ascii_case("localhost"),
                url::Host::Ipv4(host) => host.is_loopback(),
                url::Host::Ipv6(host) => host.is_loopback(),
            });
            if !(origin.scheme() == "https" || (origin.scheme() == "http" && loopback))
                || origin.host().is_none()
                || !origin.username().is_empty()
                || origin.password().is_some()
                || origin.path() != "/"
                || origin.query().is_some()
                || origin.fragment().is_some()
            {
                return Err(Error::invalid("Invalid Telegram API or file origin"));
            }
        }
        if self.proxy.as_ref().is_some_and(|url| {
            !matches!(url.scheme(), "http" | "https")
                || url.host().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
        }) {
            return Err(Error::invalid("Invalid Telegram proxy origin"));
        }
        Ok(())
    }
}

pub type FileStream = Pin<Box<dyn AsyncRead + Send>>;
pub type FileFuture<'a> = Pin<Box<dyn Future<Output = Result<FileStream, Error>> + Send + 'a>>;

pub struct Upload {
    pub field: &'static str,
    pub filename: String,
    pub content_type: Option<&'static str>,
    pub bytes: Vec<u8>,
}

pub trait Transport: Send + Sync {
    fn request<'a>(&'a self, token: &'a Token, method: Method, body: Value) -> RequestFuture<'a>;

    fn upload<'a>(
        &'a self,
        _token: &'a Token,
        _method: Method,
        _body: Value,
        _file: Upload,
    ) -> RequestFuture<'a> {
        Box::pin(async { Err(Error::invalid("Telegram uploads are unavailable")) })
    }

    fn download<'a>(&'a self, _token: &'a Token, _path: &'a str) -> FileFuture<'a> {
        Box::pin(async { Err(Error::invalid("Telegram downloads are unavailable")) })
    }
}

struct HttpTransport {
    client: reqwest::Client,
    endpoints: Endpoints,
}

impl HttpTransport {
    fn new(endpoints: Endpoints) -> Result<Self, Error> {
        endpoints.validate()?;
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(35));
        if let Some(proxy) = &endpoints.proxy {
            builder = builder.proxy(
                reqwest::Proxy::all(proxy.clone())
                    .map_err(|_| Error::invalid("Invalid Telegram proxy origin"))?,
            );
        }
        let client = builder
            .build()
            .map_err(|_| Error::invalid("Could not initialize the Telegram connection"))?;
        Ok(Self { client, endpoints })
    }

    fn endpoint(&self, token: &Token, method: Method) -> Url {
        let mut url = self.endpoints.api.clone();
        url.path_segments_mut()
            .expect("validated Telegram origin")
            .pop_if_empty()
            .push(&format!("bot{}", token.value()))
            .push(method.name());
        url
    }
}

fn request_failed(error: reqwest::Error) -> Error {
    Error {
        kind: ErrorKind::Retryable,
        message: if error.is_timeout() {
            "Telegram request timed out"
        } else {
            "Could not reach Telegram"
        }
        .into(),
        retry_after: Some(3),
    }
}

async fn telegram_result(mut response: Response, token: &Token) -> Result<Value, Error> {
    let status = response.status();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(request_failed)? {
        if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
            return Err(Error::invalid(
                "Telegram reply exceeds the native safe-load limit",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut envelope: Value = serde_json::from_slice(&bytes)
        .map_err(|_| Error::invalid("Telegram returned an unreadable reply"))?;
    if status.is_success() && envelope.get("ok") == Some(&Value::Bool(true)) {
        return envelope
            .as_object_mut()
            .and_then(|envelope| envelope.remove("result"))
            .ok_or_else(|| Error::invalid("Telegram reply has no result"));
    }
    let rejection = envelope["ok"] == false
        && envelope["error_code"].as_u64() == Some(u64::from(status.as_u16()));
    let kind = match status.as_u16() {
        401 if rejection => ErrorKind::Credentials,
        409 if rejection => ErrorKind::Conflict,
        429 if rejection => ErrorKind::RateLimited,
        400..=499 if rejection => ErrorKind::Rejected,
        _ => ErrorKind::Retryable,
    };
    let description = envelope["description"]
        .as_str()
        .unwrap_or("Telegram rejected the request");
    Err(Error {
        kind,
        message: token.redact(description),
        retry_after: envelope
            .pointer("/parameters/retry_after")
            .and_then(Value::as_u64)
            .map(|seconds| seconds.max(1)),
    })
}

impl Transport for HttpTransport {
    fn request<'a>(&'a self, token: &'a Token, method: Method, body: Value) -> RequestFuture<'a> {
        Box::pin(async move {
            let response = self
                .client
                .post(self.endpoint(token, method))
                .json(&body)
                .send()
                .await
                .map_err(request_failed)?;
            telegram_result(response, token).await
        })
    }

    fn upload<'a>(
        &'a self,
        token: &'a Token,
        method: Method,
        body: Value,
        file: Upload,
    ) -> RequestFuture<'a> {
        Box::pin(async move {
            let fields = body
                .as_object()
                .ok_or_else(|| Error::invalid("Telegram upload fields must be an object"))?;
            let mut form = multipart::Form::new();
            for (key, value) in fields {
                let text = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                form = form.text(key.clone(), text);
            }
            let mut part = multipart::Part::bytes(file.bytes).file_name(file.filename);
            if let Some(content_type) = file.content_type {
                part = part
                    .mime_str(content_type)
                    .map_err(|_| Error::invalid("Invalid Telegram upload media type"))?;
            }
            let response = self
                .client
                .post(self.endpoint(token, method))
                .multipart(form.part(file.field, part))
                .send()
                .await
                .map_err(request_failed)?;
            telegram_result(response, token).await
        })
    }

    fn download<'a>(&'a self, token: &'a Token, path: &'a str) -> FileFuture<'a> {
        Box::pin(async move {
            use futures_util::TryStreamExt;
            let mut url = self.endpoints.files.clone();
            {
                let mut segments = url
                    .path_segments_mut()
                    .expect("validated Telegram file origin");
                segments
                    .pop_if_empty()
                    .push("file")
                    .push(&format!("bot{}", token.value()));
                if path.len() > 2048 {
                    return Err(Error::invalid("Invalid Telegram file reference"));
                }
                for part in path.split('/') {
                    if part.is_empty()
                        || matches!(part, "." | "..")
                        || part.contains(['\\', '%', '?', '#'])
                        || part.chars().any(char::is_control)
                    {
                        return Err(Error::invalid("Invalid Telegram file reference"));
                    }
                    segments.push(part);
                }
            }
            let response = self.client.get(url).send().await.map_err(request_failed)?;
            if !response.status().is_success() {
                return Err(Error::invalid("Could not download the Telegram file"));
            }
            let stream = response
                .bytes_stream()
                .map_err(|_| std::io::Error::other("Could not read the Telegram file"));
            Ok(Box::pin(StreamReader::new(stream)) as FileStream)
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: i64,
    pub username: String,
    pub name: String,
}
#[derive(Debug, Clone)]
pub struct Bot {
    pub identity: Identity,
    pub can_join_groups: bool,
    pub receives_all_group_messages: bool,
    pub topic_mode: bool,
    pub user_topics: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incoming {
    pub bot_id: i64,
    pub update_id: i64,
    pub message_id: i64,
    pub user_id: i64,
    pub chat_id: i64,
    pub sender: String,
    pub text: String,
    pub timestamp: i64,
}
#[derive(Debug, Clone)]
pub struct PollBatch {
    pub messages: Vec<Incoming>,
    pub next_offset: i64,
    pub ignored: usize,
}
#[derive(Debug, Clone)]
pub struct Receipt {
    pub message_id: i64,
    pub timestamp: i64,
}

#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    token: Token,
}
impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TelegramClient([redacted])")
    }
}
impl Client {
    pub fn credential(&self) -> &Token {
        &self.token
    }
    pub fn new(token: Token) -> Result<Self, Error> {
        Self::with_endpoints(token, Endpoints::default())
    }
    pub fn with_endpoints(token: Token, endpoints: Endpoints) -> Result<Self, Error> {
        Ok(Self::with_transport(
            token,
            Arc::new(HttpTransport::new(endpoints)?),
        ))
    }
    pub fn with_transport(token: Token, transport: Arc<dyn Transport>) -> Self {
        Self { transport, token }
    }
    pub async fn request(
        &self,
        method: Method,
        body: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, Error> {
        tokio::select! {biased;_=cancel.cancelled()=>Err(Error::cancelled()),result=self.transport.request(&self.token,method,body)=>result.map_err(|mut error|{error.message=self.token.redact(&error.message);error})}
    }
    pub async fn upload(
        &self,
        method: Method,
        fields: Value,
        file: Upload,
        cancel: &CancellationToken,
    ) -> Result<Value, Error> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::cancelled()),
            result = self.transport.upload(&self.token, method, fields, file) => {
                result.map_err(|mut error| { error.message = self.token.redact(&error.message); error })
            },
        }
    }

    pub async fn download(
        &self,
        path: &str,
        cancel: &CancellationToken,
    ) -> Result<FileStream, Error> {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::cancelled()),
            result = self.transport.download(&self.token, path) => result,
        }
    }

    pub async fn identify(&self, cancel: &CancellationToken) -> Result<Bot, Error> {
        let user = self.request(Method::GetMe, json!({}), cancel).await?;
        if user.get("is_bot") != Some(&Value::Bool(true)) {
            return Err(Error::invalid("This credential is not a Telegram bot"));
        }
        let id = user
            .get("id")
            .and_then(Value::as_i64)
            .filter(|id| *id > 0)
            .ok_or_else(|| Error::invalid("Telegram did not identify this bot"))?;
        let username = user
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let name = user
            .get("first_name")
            .and_then(Value::as_str)
            .unwrap_or("Telegram")
            .to_owned();
        Ok(Bot {
            identity: Identity { id, username, name },
            can_join_groups: user["can_join_groups"].as_bool().unwrap_or(false),
            receives_all_group_messages: user["can_read_all_group_messages"]
                .as_bool()
                .unwrap_or(false),
            topic_mode: user["has_topics_enabled"].as_bool().unwrap_or(false),
            user_topics: user["allows_users_to_create_topics"]
                .as_bool()
                .unwrap_or(false),
        })
    }

    pub async fn verify(&self, cancel: &CancellationToken) -> Result<Identity, Error> {
        let identity = self.identify(cancel).await?.identity;
        let webhook = self
            .request(Method::GetWebhookInfo, json!({}), cancel)
            .await?;
        if webhook
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|url| !url.is_empty())
        {
            return Err(Error::invalid(
                "This bot has an existing webhook. Remove it in that integration or use a separate bot before connecting here.",
            ));
        }
        Ok(identity)
    }
    pub async fn poll(
        &self,
        bot_id: i64,
        offset: i64,
        cancel: &CancellationToken,
    ) -> Result<PollBatch, Error> {
        let value=self.request(Method::GetUpdates,json!({"offset":offset.max(0),"limit":100,"timeout":20,"allowed_updates":["message"]}),cancel).await?;
        let updates = value
            .as_array()
            .ok_or_else(|| Error::invalid("Telegram returned an invalid update batch"))?;
        let mut batch = PollBatch {
            messages: vec![],
            next_offset: offset,
            ignored: 0,
        };
        for update in updates {
            let Some(update_id) = update
                .get("update_id")
                .and_then(Value::as_i64)
                .filter(|id| (0..i64::MAX).contains(id))
            else {
                batch.ignored += 1;
                continue;
            };
            batch.next_offset = batch.next_offset.max(update_id.saturating_add(1));
            let Some(message) = update.get("message") else {
                batch.ignored += 1;
                continue;
            };
            let user_id = message.pointer("/from/id").and_then(Value::as_i64);
            let chat_id = message.pointer("/chat/id").and_then(Value::as_i64);
            let private = message.pointer("/chat/type").and_then(Value::as_str) == Some("private");
            if !private
                || user_id != chat_id
                || user_id.is_none_or(|id| !(1..=4_503_599_627_370_495).contains(&id))
                || message.pointer("/from/is_bot") != Some(&Value::Bool(false))
                || message.get("is_topic_message") == Some(&Value::Bool(true))
                || message.get("message_thread_id").is_some()
                || message.get("direct_messages_topic").is_some()
                || message.get("sender_chat").is_some()
            {
                batch.ignored += 1;
                continue;
            }
            let Some(text) = message.get("text").and_then(Value::as_str).filter(|text| {
                !text.trim().is_empty() && text.len() <= 32768 && text.chars().count() <= 4096
            }) else {
                batch.ignored += 1;
                continue;
            };
            let Some(message_id) = message
                .get("message_id")
                .and_then(Value::as_i64)
                .filter(|id| *id > 0)
            else {
                batch.ignored += 1;
                continue;
            };
            let sender = message
                .pointer("/from/username")
                .and_then(Value::as_str)
                .or_else(|| message.pointer("/from/first_name").and_then(Value::as_str))
                .map(str::to_owned)
                .unwrap_or_else(|| user_id.unwrap().to_string());
            batch.messages.push(Incoming {
                bot_id,
                update_id,
                message_id,
                user_id: user_id.unwrap(),
                chat_id: chat_id.unwrap(),
                sender,
                text: text.to_owned(),
                timestamp: message.get("date").and_then(Value::as_i64).unwrap_or(0),
            });
        }
        Ok(batch)
    }
    pub async fn send(
        &self,
        user_id: i64,
        text: &str,
        cancel: &CancellationToken,
    ) -> Result<Receipt, Error> {
        if user_id <= 0 {
            return Err(Error::invalid("Invalid Telegram direct chat ID"));
        }
        if text.trim().is_empty() || text.chars().count() > 4096 {
            return Err(Error::invalid("Enter a reply of 1–4096 characters"));
        }
        let result = self
            .request(
                Method::SendMessage,
                json!({"chat_id":user_id,"text":text,"link_preview_options":{"is_disabled":true}}),
                cancel,
            )
            .await?;
        let message_id = result
            .get("message_id")
            .and_then(Value::as_i64)
            .filter(|id| {
                *id > 0 && result.pointer("/chat/id").and_then(Value::as_i64) == Some(user_id)
            })
            .ok_or_else(|| Error::invalid("Telegram did not confirm delivery"))?;
        Ok(Receipt {
            message_id,
            timestamp: result.get("date").and_then(Value::as_i64).unwrap_or(0),
        })
    }
}

pub fn parse_allowlist(values: &[String]) -> Result<BTreeSet<i64>, Error> {
    let mut ids = BTreeSet::new();
    for value in values.iter().flat_map(|value| value.split([',', '\n'])) {
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        let id = value
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0 && *id <= 4_503_599_627_370_495)
            .ok_or_else(|| {
                Error::invalid("Allowed Telegram users must be numeric user IDs, one per box.")
            })?;
        ids.insert(id);
    }
    if ids.is_empty() {
        return Err(Error::invalid(
            "Add at least one allowed Telegram user ID before connecting.",
        ));
    }
    if ids.len() > 1000 {
        return Err(Error::invalid(
            "The native Telegram allowlist supports up to 1000 users",
        ));
    }
    Ok(ids)
}
