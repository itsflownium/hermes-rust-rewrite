use crate::{
    common::{Media, MediaKind, digits, is_loopback},
    intake::{MediaDescription, MediaReader, MediaStream},
};
use futures_util::TryStreamExt;
use hermes_gateway_core::messaging::{AdapterFuture, ConnectionFailure, GatewayError, Ingress};
use reqwest::{Client, Method, Response, StatusCode, Url, header, multipart};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, time::Duration};
use tokio_util::{io::StreamReader, sync::CancellationToken};

pub(super) const VERSION: &str = "v27.0";
const MAX_RESPONSE: usize = 2 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Credential {
    pub access_token: String,
    pub app_secret: String,
    pub verify_token: String,
}

impl Credential {
    pub fn parse(secret: &str) -> Result<Self, Failure> {
        let value: Self = serde_json::from_str(secret).map_err(|_| Failure::Credential)?;
        if ![
            (value.access_token.as_str(), 16, 4096),
            (&value.app_secret, 16, 256),
            (&value.verify_token, 16, 256),
        ]
        .iter()
        .all(|(token, minimum, maximum)| {
            (*minimum..=*maximum).contains(&token.len())
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        }) {
            return Err(Failure::Credential);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Bucket {
    App,
    Phone,
    Recipient,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Failure {
    Credential,
    Invalid,
    Permission,
    Unsupported,
    Window,
    Retry(Bucket, Duration),
    Unknown,
    Cancelled,
}

impl Failure {
    pub fn connection(self) -> ConnectionFailure {
        match self {
            Self::Credential => ConnectionFailure::Credentials,
            Self::Invalid | Self::Permission | Self::Unsupported | Self::Window => {
                ConnectionFailure::Configuration
            }
            Self::Retry(..) | Self::Unknown => ConnectionFailure::Retryable,
            Self::Cancelled => ConnectionFailure::Stopped,
        }
    }

    fn read(self) -> GatewayError {
        match self {
            Self::Credential | Self::Permission | Self::Window => GatewayError::Forbidden,
            Self::Invalid => GatewayError::Invalid,
            Self::Unsupported => GatewayError::Unsupported,
            Self::Retry(..) | Self::Unknown => GatewayError::TemporarilyUnavailable,
            Self::Cancelled => GatewayError::StaleConnection,
        }
    }
}

pub(super) struct Api {
    http: Client,
    origin: Url,
    pub credential: Credential,
}

impl Api {
    pub fn new(origin: Url, credential: Credential) -> Result<Self, Failure> {
        let http = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(25))
            .build()
            .map_err(|_| Failure::Invalid)?;
        Ok(Self {
            http,
            origin,
            credential,
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, Failure> {
        self.origin
            .join(&format!("{VERSION}/{path}"))
            .map_err(|_| Failure::Invalid)
    }

    pub async fn authenticate(
        &self,
        app: &str,
        account: &str,
        phone: &str,
        cancel: &CancellationToken,
    ) -> Result<bool, Failure> {
        let token = format!("{app}|{}", self.credential.app_secret);
        let response = self
            .http
            .get(self.endpoint("debug_token")?)
            .bearer_auth(token)
            .query(&[("input_token", &self.credential.access_token)]);
        let result = json_response(response, cancel).await?;
        let info = &result["data"];
        if info["app_id"] != app
            || info["is_valid"] != true
            || !info["scopes"].as_array().is_some_and(|scopes| {
                scopes.iter().any(|s| s == "whatsapp_business_messaging")
                    && scopes.iter().any(|s| s == "whatsapp_business_management")
            })
        {
            return Err(Failure::Credential);
        }
        if !self
            .edge_contains(account, "phone_numbers", phone, cancel)
            .await?
        {
            return Err(Failure::Permission);
        }
        self.subscribed(account, app, cancel).await
    }

    pub async fn subscribed(
        &self,
        account: &str,
        app: &str,
        cancel: &CancellationToken,
    ) -> Result<bool, Failure> {
        self.edge_contains(account, "subscribed_apps", app, cancel)
            .await
    }

    async fn edge_contains(
        &self,
        account: &str,
        edge: &str,
        id: &str,
        cancel: &CancellationToken,
    ) -> Result<bool, Failure> {
        let url = self.endpoint(&format!("{account}/{edge}"))?;
        let mut after = String::new();
        for _ in 0..8 {
            let mut request = self
                .http
                .get(url.clone())
                .bearer_auth(&self.credential.access_token)
                .query(&[("limit", "100")]);
            if !after.is_empty() {
                request = request.query(&[("after", &after)]);
            }
            let response = json_response(request, cancel).await?;
            let list = response["data"].as_array().ok_or(Failure::Unknown)?;
            if list
                .iter()
                .any(|row| row["id"] == id || row["whatsapp_business_api_data"]["id"] == id)
            {
                return Ok(true);
            }
            if response["paging"]["next"].as_str().is_none() {
                return Ok(false);
            }
            let next = response["paging"]["cursors"]["after"]
                .as_str()
                .filter(|next| !next.is_empty() && next.len() <= 1024 && *next != after)
                .ok_or(Failure::Unknown)?;
            after = next.to_owned();
        }
        Err(Failure::Unknown)
    }

    pub async fn send(
        &self,
        phone: &str,
        payload: Value,
        cancel: &CancellationToken,
    ) -> Result<Value, Failure> {
        self.call(
            Method::POST,
            &format!("{phone}/messages"),
            Some(payload),
            cancel,
        )
        .await
    }

    pub async fn upload(
        &self,
        phone: &str,
        name: &str,
        mime: &str,
        bytes: Vec<u8>,
        cancel: &CancellationToken,
    ) -> Result<String, Failure> {
        let file = multipart::Part::bytes(bytes)
            .file_name(name.to_owned())
            .mime_str(mime)
            .map_err(|_| Failure::Invalid)?;
        let body = multipart::Form::new()
            .text("messaging_product", "whatsapp")
            .text("type", mime.to_owned())
            .part("file", file);
        let request = self
            .http
            .post(self.endpoint(&format!("{phone}/media"))?)
            .bearer_auth(&self.credential.access_token)
            .multipart(body);
        let response = json_response(request, cancel).await?;
        response["id"]
            .as_str()
            .filter(|id| digits(id))
            .map(str::to_owned)
            .ok_or(Failure::Unknown)
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        cancel: &CancellationToken,
    ) -> Result<Value, Failure> {
        let request = self
            .http
            .request(method, self.endpoint(path)?)
            .bearer_auth(&self.credential.access_token);
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        json_response(request, cancel).await
    }

    fn media_endpoint(&self, endpoint: &str) -> Result<Url, Failure> {
        let url = Url::parse(endpoint).map_err(|_| Failure::Invalid)?;
        let same_fixture = is_loopback(&self.origin) && url.origin() == self.origin.origin();
        let meta = url.scheme() == "https"
            && url.port_or_known_default() == Some(443)
            && matches!(
                url.host_str(),
                Some("lookaside.fbsbx.com" | "graph.facebook.com")
            );
        if endpoint.len() > 8192
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || !(same_fixture || meta)
        {
            return Err(Failure::Permission);
        }
        Ok(url)
    }
}

impl MediaReader for super::Connection {
    fn signal<'a>(
        &'a self,
        _: &'a Ingress,
        source: &'a crate::intake::Source,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, ()> {
        Box::pin(async move {
            if self.settings.read_signals || self.settings.typing {
                let mut body = json!({"messaging_product":"whatsapp","status":"read","message_id":source.anchor});
                if self.settings.typing {
                    body["typing_indicator"] = json!({"type":"text"});
                }
                let _ = tokio::time::timeout(
                    Duration::from_millis(400),
                    self.api.send(&self.settings.phone_id, body, cancel),
                )
                .await;
            }
        })
    }

    fn describe<'a>(
        &'a self,
        _: &'a Ingress,
        media: &'a Media,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaDescription, GatewayError>> {
        Box::pin(async move {
            if !digits(&media.id) {
                return Err(GatewayError::Invalid);
            }
            let url = self.api.endpoint(&media.id).map_err(Failure::read)?;
            let request = self
                .api
                .http
                .get(url)
                .bearer_auth(&self.api.credential.access_token)
                .query(&[("phone_number_id", &self.settings.phone_id)]);
            let info = json_response(request, cancel)
                .await
                .map_err(Failure::read)?;
            if info["id"] != media.id || info["messaging_product"] != "whatsapp" {
                return Err(GatewayError::Forbidden);
            }
            let size = info["file_size"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(GatewayError::Invalid)?;
            let address = info["url"].as_str().ok_or(GatewayError::Invalid)?;
            self.api.media_endpoint(address).map_err(Failure::read)?;
            let advertised = info["mime_type"].as_str().ok_or(GatewayError::Invalid)?;
            let extension = match (media.kind, advertised.split(';').next().unwrap_or_default()) {
                (MediaKind::Image, "image/jpeg") => "jpg",
                (MediaKind::Image, "image/png") => "png",
                (MediaKind::Video, "video/mp4") => "mp4",
                (MediaKind::Audio, "audio/ogg" | "application/ogg") => "ogg",
                (MediaKind::Audio, "audio/mpeg") => "mp3",
                (MediaKind::Audio, "audio/wav") => "wav",
                (MediaKind::Document, _) => "bin",
                _ => return Err(GatewayError::Invalid),
            };
            let name = media
                .name
                .clone()
                .unwrap_or_else(|| format!("whatsapp.{extension}"));
            Ok(MediaDescription {
                name,
                size,
                endpoint: address.to_owned(),
            })
        })
    }

    fn open<'a>(
        &'a self,
        _: &'a Media,
        address: &'a MediaDescription,
        limit: usize,
        cancel: &'a CancellationToken,
    ) -> AdapterFuture<'a, Result<MediaStream, GatewayError>> {
        Box::pin(async move {
            let url = self
                .api
                .media_endpoint(&address.endpoint)
                .map_err(Failure::read)?;
            let req = self
                .api
                .http
                .get(url)
                .bearer_auth(&self.api.credential.access_token)
                .send();
            let response = tokio::select! {
                _ = cancel.cancelled() => return Err(GatewayError::StaleConnection),
                reply = req => reply.map_err(|_| GatewayError::TemporarilyUnavailable)?,
            };
            if !response.status().is_success() {
                return Err(
                    if response.status().is_server_error()
                        || response.status() == StatusCode::TOO_MANY_REQUESTS
                    {
                        GatewayError::TemporarilyUnavailable
                    } else {
                        GatewayError::Forbidden
                    },
                );
            }
            if response
                .content_length()
                .is_some_and(|size| size > limit as u64)
            {
                return Err(GatewayError::Invalid);
            }
            let bytes = response
                .bytes_stream()
                .map_err(|_| io::Error::other("WhatsApp media transfer failed"));
            Ok(Box::pin(StreamReader::new(bytes)) as MediaStream)
        })
    }
}

async fn json_response(
    request: reqwest::RequestBuilder,
    cancel: &CancellationToken,
) -> Result<Value, Failure> {
    tokio::select! {
        _ = cancel.cancelled() => Err(Failure::Cancelled),
        result = async {
            let response = request.send().await.map_err(|_| Failure::Unknown)?;
            read_response(response).await
        } => result,
    }
}

async fn read_response(mut response: Response) -> Result<Value, Failure> {
    let status = response.status();
    let after = response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
        .map_or(Duration::from_secs(30), |seconds| {
            Duration::from_secs(seconds.max(1))
        });
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Failure::Unknown)? {
        if body.len() + chunk.len() > MAX_RESPONSE {
            return Err(Failure::Unknown);
        }
        body.extend_from_slice(&chunk);
    }
    let json = serde_json::from_slice::<Value>(&body).unwrap_or_else(|_| json!({}));
    if status.is_success()
        && json.is_object()
        && !json.as_object().is_some_and(|value| value.is_empty())
        && json["error"].is_null()
    {
        return Ok(json);
    }
    Err(match json["error"]["code"].as_i64() {
        Some(131047) => Failure::Window,
        Some(190 | 102) => Failure::Credential,
        Some(4 | 17 | 32 | 613) => Failure::Retry(Bucket::App, after),
        Some(130429 | 131048) => Failure::Retry(Bucket::Phone, after),
        Some(131056) => Failure::Retry(Bucket::Recipient, after),
        Some(10 | 200 | 131005 | 131031) => Failure::Permission,
        Some(131051) => Failure::Unsupported,
        Some(100 | 131009 | 131021 | 131026 | 131030 | 131053) => Failure::Invalid,
        _ if status == StatusCode::TOO_MANY_REQUESTS => Failure::Retry(Bucket::Phone, after),
        _ if status == StatusCode::UNAUTHORIZED => Failure::Credential,
        _ => Failure::Unknown,
    })
}
