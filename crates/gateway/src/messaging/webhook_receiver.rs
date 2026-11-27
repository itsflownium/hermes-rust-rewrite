//! An authenticated push endpoint owned by an active messaging connection.
use super::{AdapterFuture, GatewayError, Ingress, storage, transport::ReceiverLease};
use crate::Shared;
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{HeaderMap, HeaderName, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use ring::hmac;
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;

const MAX_WEBHOOK_BODY: usize = 2 * 1024 * 1024;

pub type WebhookHandler =
    dyn Fn(Ingress, Vec<u8>) -> AdapterFuture<'static, Result<(), GatewayError>> + Send + Sync;

#[derive(Clone)]
pub(super) struct Webhook {
    pub(super) id: String,
    auth: Authentication,
    receive: Arc<WebhookHandler>,
}

#[derive(Clone)]
enum Authentication {
    Token {
        header: HeaderName,
        secret: String,
    },
    Signed {
        header: HeaderName,
        key: Arc<hmac::Key>,
        verify_token: String,
    },
}

impl Ingress {
    /// Register a push receiver for this verified generation on the gateway listener.
    pub fn register_webhook(
        &self,
        header: HeaderName,
        secret: String,
        receive: Arc<WebhookHandler>,
    ) -> Result<String, GatewayError> {
        if secret.is_empty()
            || secret.len() > 256
            || !secret
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(GatewayError::Invalid);
        }
        let id = super::runtime::db(storage::key(
            "messaging-webhook",
            &(self.root(), self.name(), self.connection_name()),
        ))?;
        self.attach_webhook(Webhook {
            id: id.clone(),
            auth: Authentication::Token { header, secret },
            receive,
        })?;
        Ok(format!("/api/messaging/webhooks/{id}"))
    }
}

impl ReceiverLease {
    /// Bind one challenge and signed-body endpoint to this leased receiver.
    pub fn register_signed_webhook(
        &self,
        signature_header: HeaderName,
        signing_secret: &[u8],
        verify_token: String,
        receive: Arc<WebhookHandler>,
    ) -> Result<String, GatewayError> {
        self.check()?;
        if !(16..=256).contains(&signing_secret.len())
            || !(16..=256).contains(&verify_token.len())
            || verify_token.chars().any(char::is_control)
        {
            return Err(GatewayError::Invalid);
        }
        let scope = &self.scope;
        let receiver = scope
            .installation()
            .and_then(|proof| proof.receiver.as_deref())
            .ok_or(GatewayError::Forbidden)?;
        let id = super::runtime::db(storage::key(
            "messaging-signed-webhook",
            &(scope.root(), receiver),
        ))?;
        scope.attach_webhook(Webhook {
            id: id.clone(),
            auth: Authentication::Signed {
                header: signature_header,
                key: Arc::new(hmac::Key::new(hmac::HMAC_SHA256, signing_secret)),
                verify_token,
            },
            receive,
        })?;
        Ok(format!("/api/messaging/webhooks/{id}"))
    }
}

fn one_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a [u8]> {
    let all = headers.get_all(name);
    let mut values = all.iter();
    let value = values.next()?;
    values.next().is_none().then(|| value.as_bytes())
}

fn signature(headers: &HeaderMap, name: &HeaderName) -> Option<[u8; 32]> {
    let hex = std::str::from_utf8(one_header(headers, name)?)
        .ok()?
        .strip_prefix("sha256=")?;
    if hex.len() != 64 || !hex.is_ascii() {
        return None;
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(digest)
}

pub(crate) async fn challenge(
    State(owner): State<Arc<Shared>>,
    Path(id): Path<String>,
    uri: Uri,
) -> Response {
    let Some((
        _,
        Webhook {
            auth: Authentication::Signed { verify_token, .. },
            ..
        },
    )) = owner.messaging.find_webhook(&id).await
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(query) = uri.query().filter(|query| query.len() <= 4096) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let mut url = reqwest::Url::parse("https://localhost/").expect("fixed query parser URL");
    url.set_query(Some(query));
    let (mut mode, mut token, mut challenge) = (None, None, None);
    for (key, value) in url.query_pairs() {
        let slot = match key.as_ref() {
            "hub.mode" => &mut mode,
            "hub.verify_token" => &mut token,
            "hub.challenge" => &mut challenge,
            _ => continue,
        };
        if slot.replace(value).is_some() {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }
    if mode.as_deref() != Some("subscribe")
        || !token.is_some_and(|token| bool::from(token.as_bytes().ct_eq(verify_token.as_bytes())))
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    match challenge {
        Some(value)
            if (1..=128).contains(&value.len())
                && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            (StatusCode::OK, value.into_owned()).into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

pub(crate) async fn receive(
    State(owner): State<Arc<Shared>>,
    Path(id): Path<String>,
    request: Request,
) -> StatusCode {
    let Some((ingress, webhook)) = owner.messaging.find_webhook(&id).await else {
        return StatusCode::NOT_FOUND;
    };
    let signed = match &webhook.auth {
        Authentication::Token { header, secret } => {
            if !one_header(request.headers(), header)
                .is_some_and(|value| bool::from(value.ct_eq(secret.as_bytes())))
            {
                return StatusCode::FORBIDDEN;
            }
            None
        }
        Authentication::Signed { header, key, .. } => {
            let Some(signature) = signature(request.headers(), header) else {
                return StatusCode::FORBIDDEN;
            };
            Some((key, signature))
        }
    };

    let body = match tokio::time::timeout(
        Duration::from_secs(10),
        to_bytes(request.into_body(), MAX_WEBHOOK_BODY),
    )
    .await
    {
        Ok(Ok(body)) => body.to_vec(),
        Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE,
        Err(_) => return StatusCode::REQUEST_TIMEOUT,
    };
    if let Some((key, tag)) = &signed
        && hmac::verify(key, &body, tag).is_err()
    {
        return StatusCode::FORBIDDEN;
    }
    match (webhook.receive)(ingress, body).await {
        Ok(()) if signed.is_some() => StatusCode::OK,
        Ok(()) => StatusCode::NO_CONTENT,
        Err(GatewayError::Invalid | GatewayError::Unsupported) => StatusCode::BAD_REQUEST,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
