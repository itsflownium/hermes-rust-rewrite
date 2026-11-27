use crate::messaging::EnrollmentCredential;
use anyhow::{Context, Result, ensure};
use reqwest::{
    Client, Method, RequestBuilder, Response, StatusCode, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use serde::de::DeserializeOwned;
#[cfg(test)]
use std::net::IpAddr;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async_with_config,
    tungstenite::protocol::WebSocketConfig,
};

const BOOT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_JSON: usize = 4 * 1024 * 1024;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone)]
pub(super) struct DiscordClient {
    http: Client,
    auth: HeaderValue,
    credential: EnrollmentCredential,
    api: Url,
    proxy: Option<Url>,
    #[cfg(test)]
    fixture: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RestFailure {
    Credentials,
    Forbidden,
    Missing,
    RateLimited,
    Invalid,
    Unavailable,
}

impl DiscordClient {
    pub(super) fn official(credential: EnrollmentCredential, proxy: Option<&str>) -> Result<Self> {
        Self::new(
            credential,
            Url::parse("https://discord.com/api/v10/")?,
            proxy,
        )
    }

    #[cfg(test)]
    pub(super) fn for_fixture(credential: EnrollmentCredential, api: Url) -> Result<Self> {
        ensure!(
            api.scheme() == "http" && loopback(&api),
            "Invalid Discord fixture HTTP endpoint"
        );
        let mut client = Self::new(credential, api, None)?;
        client.fixture = true;
        Ok(client)
    }

    fn new(credential: EnrollmentCredential, api: Url, proxy: Option<&str>) -> Result<Self> {
        let mut auth = HeaderValue::from_str(&format!("Bot {}", credential.secret()))
            .map_err(|_| anyhow::anyhow!("Invalid Discord bot credential"))?;
        auth.set_sensitive(true);
        let proxy = proxy.map(parse_proxy).transpose()?;
        let mut builder = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(BOOT_TIMEOUT)
            .timeout(Duration::from_secs(30))
            .user_agent(
                "DiscordBot (https://github.com/itsflownium/mercury-hermes-rust-rewrite, 0.2)",
            );
        if let Some(proxy) = &proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy.as_str())?);
        }
        Ok(Self {
            http: builder.build()?,
            auth,
            credential,
            api,
            proxy,
            #[cfg(test)]
            fixture: false,
        })
    }

    pub(super) fn token(&self) -> &str {
        self.credential.secret()
    }

    pub(super) fn bot(&self, method: Method, segments: &[&str]) -> RequestBuilder {
        self.open(method, segments)
            .header(AUTHORIZATION, self.auth.clone())
    }

    /// Webhook and interaction callbacks authenticate with their own scoped token.
    pub(super) fn open(&self, method: Method, segments: &[&str]) -> RequestBuilder {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .expect("Discord API is an absolute HTTP URL")
            .pop_if_empty()
            .extend(segments);
        self.http.request(method, url)
    }

    #[cfg(test)]
    pub(super) fn fixture_media_url(&self, url: &Url) -> bool {
        self.fixture && url.scheme() == "http" && loopback(url)
            && url.origin() == self.api.origin()
    }

    pub(super) fn public_get(&self, url: Url) -> RequestBuilder {
        self.http.get(url)
    }

    pub(super) async fn get<T: DeserializeOwned>(
        &self,
        segments: &[&str],
    ) -> Result<T, RestFailure> {
        let response = self
            .bot(Method::GET, segments)
            .send()
            .await
            .map_err(|_| RestFailure::Unavailable)?;
        json_response(response).await
    }

    pub(super) fn gateway_url(&self, input: &str) -> Result<Url> {
        let mut url = Url::parse(input).context("Invalid Discord Gateway URL")?;
        let authority_ok = url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && url.path() == "/";
        #[cfg(test)]
        if self.fixture
            && authority_ok
            && url.scheme() == "ws"
            && loopback(&url)
            && url.host_str() == self.api.host_str()
        {
            url.set_query(Some("v=10&encoding=json"));
            return Ok(url);
        }
        let official_host = url.host_str().is_some_and(|host| {
            host == "gateway.discord.gg"
                || host
                    .strip_suffix(".discord.gg")
                    .is_some_and(|label| label.starts_with("gateway-") && !label.contains('.'))
        });
        ensure!(
            authority_ok
                && url.scheme() == "wss"
                && url.port_or_known_default() == Some(443)
                && official_host,
            "Discord Gateway URL is outside the verified provider"
        );
        url.set_query(Some("v=10&encoding=json"));
        Ok(url)
    }

    pub(super) async fn websocket(&self, url: &Url) -> Result<Socket> {
        let config = WebSocketConfig::default()
            .max_message_size(Some(8 * 1024 * 1024))
            .max_frame_size(Some(8 * 1024 * 1024));
        let socket = tokio::time::timeout(BOOT_TIMEOUT, async {
            if let Some(proxy) = &self.proxy {
                let stream = proxy_connect(proxy, url).await?;
                let (socket, _) =
                    client_async_tls_with_config(url.as_str(), stream, Some(config), None)
                        .await
                        .map_err(|_| anyhow::anyhow!("Discord Gateway handshake failed"))?;
                Ok::<Socket, anyhow::Error>(socket)
            } else {
                let (socket, _) = connect_async_with_config(url.as_str(), Some(config), false)
                    .await
                    .map_err(|_| anyhow::anyhow!("Discord Gateway handshake failed"))?;
                Ok(socket)
            }
        })
        .await
        .context("Discord Gateway connection timed out")??;
        Ok(socket)
    }
}

pub(super) async fn json_response<T: DeserializeOwned>(
    mut response: Response,
) -> Result<T, RestFailure> {
    match response.status() {
        StatusCode::UNAUTHORIZED => return Err(RestFailure::Credentials),
        StatusCode::FORBIDDEN => return Err(RestFailure::Forbidden),
        StatusCode::NOT_FOUND => return Err(RestFailure::Missing),
        StatusCode::TOO_MANY_REQUESTS => return Err(RestFailure::RateLimited),
        code if code.is_server_error() => return Err(RestFailure::Unavailable),
        code if !code.is_success() => return Err(RestFailure::Invalid),
        _ => {}
    }
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| RestFailure::Unavailable)?
    {
        if data.len().saturating_add(chunk.len()) > MAX_JSON {
            return Err(RestFailure::Invalid);
        }
        data.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&data).map_err(|_| RestFailure::Invalid)
}

pub(super) fn parse_proxy(value: &str) -> Result<Url> {
    let proxy = Url::parse(value).map_err(|_| anyhow::anyhow!("Invalid Discord proxy URL"))?;
    ensure!(
        proxy.scheme() == "http"
            && proxy.host_str().is_some()
            && proxy.username().is_empty()
            && proxy.password().is_none()
            && proxy.path() == "/"
            && proxy.query().is_none()
            && proxy.fragment().is_none(),
        "Discord proxy must be an HTTP CONNECT endpoint"
    );
    Ok(proxy)
}

async fn proxy_connect(proxy: &Url, gateway: &Url) -> Result<TcpStream> {
    let host = proxy
        .host_str()
        .context("Discord proxy host is unavailable")?;
    let port = proxy
        .port_or_known_default()
        .context("Discord proxy port is unavailable")?;
    let mut stream = TcpStream::connect((host.trim_matches(['[', ']']), port))
        .await
        .map_err(|_| anyhow::anyhow!("Discord proxy connection failed"))?;
    let authority = format!(
        "{}:{}",
        gateway.host_str().context("Missing Gateway host")?,
        gateway
            .port_or_known_default()
            .context("Missing Gateway port")?
    );
    let request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        ensure!(
            headers.len() < 8192,
            "Discord proxy response exceeded its header limit"
        );
        headers.push(stream.read_u8().await?);
    }
    let line = headers
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let mut status = line.split(|byte| *byte == b' ');
    ensure!(
        matches!(status.next(), Some(b"HTTP/1.1" | b"HTTP/1.0")) && status.next() == Some(b"200"),
        "Discord proxy refused the Gateway connection"
    );
    Ok(stream)
}

#[cfg(test)]
fn loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_urls_cannot_move_bot_credentials_to_a_foreign_origin() {
        let client = DiscordClient::official(
            EnrollmentCredential::new("fixture-secret".into()).unwrap(),
            None,
        )
        .unwrap();
        let url = client
            .gateway_url("wss://gateway-us-east1-b.discord.gg/?unexpected=1")
            .unwrap();
        assert_eq!(url.query(), Some("v=10&encoding=json"));
        for rejected in [
            "wss://gateway.discord.gg.evil.example/",
            "wss://gateway.discord.gg:8443/",
            "ws://gateway.discord.gg/",
            "wss://credential@gateway.discord.gg/",
            "wss://gateway.discord.gg/elsewhere",
        ] {
            assert!(client.gateway_url(rejected).is_err(), "{rejected}");
        }
    }
}
